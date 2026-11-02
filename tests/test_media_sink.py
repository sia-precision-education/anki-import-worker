"""Deck media is stored several files at a time, straight from the package, before any card renders.

`warm` reads the package's media once, in archive order, and uploads it several files
at a time; a card then finds its file already stored. These tests hold it to what the
one-at-a-time path guaranteed: each distinct file is uploaded once, the cap counts each
byte once, and a file that fails is still tried when a card asks for it. Also: what it
holds at once (memory and disk), and reading the package from blob storage in ranges.
"""

import contextlib
import hashlib
import io
import logging
import os
import sys
import threading
import time
import types
import zipfile
from dataclasses import dataclass
from typing import BinaryIO

import pytest
from azure.core.exceptions import ResourceExistsError, ServiceRequestError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault("AZURE_STORAGE_CONNECTION_STRING", "UseDevelopmentStorage=true")
os.environ.setdefault("ANKI_CALLBACK_URL", "https://backend.invalid/api/v1/anki/callback")
os.environ.setdefault("ANKI_CALLBACK_SECRET", "test-secret")

_renderer = types.ModuleType("renderer")
_renderer.open_apkg = lambda *a, **k: None
sys.modules.setdefault("renderer", _renderer)

import worker


class _Store:
    """A container that records uploads and how many ran at once."""

    def __init__(
        self, *, fail_first: "set[str] | None" = None, always_fail: "set[str] | None" = None, delay: float = 0.0
    ) -> None:
        self.blobs: dict[str, bytes] = {}
        self.uploads: list[str] = []
        self.sent: list[tuple[type, int]] = []
        self.looked_up: list[str] = []
        self._fail_first = set(fail_first or ())
        self._always_fail = set(always_fail or ())
        self._delay = delay
        self._lock = threading.Lock()
        self._running = 0
        self.most_at_once = 0

    def get_blob_client(self, name: str) -> "_Blob":
        return _Blob(self, name)


class _Blob:
    def __init__(self, store: _Store, name: str) -> None:
        self._store = store
        self._name = name

    def exists(self) -> bool:
        self._store.looked_up.append(self._name)
        return self._name in self._store.blobs

    def upload_blob(self, data: BinaryIO, *, length: int, overwrite: bool, **_k: object) -> None:
        store = self._store
        with store._lock:
            store._running += 1
            store.most_at_once = max(store.most_at_once, store._running)
            store.uploads.append(self._name)
            failing = self._name in store._fail_first or self._name in store._always_fail
            store._fail_first.discard(self._name)
        try:
            time.sleep(store._delay)
            if failing:
                msg = "storage unavailable"
                raise OSError(msg)
            if not overwrite and self._name in store.blobs:
                msg = "The specified blob already exists."
                raise ResourceExistsError(msg)
            store.sent.append((type(data), length))
            store.blobs[self._name] = data.read()
        finally:
            with store._lock:
                store._running -= 1


@dataclass
class _File:
    name: str
    size: int
    data: "bytes | None" = None
    path: "str | None" = None

    def discard(self) -> None:
        if self.path is not None:
            os.unlink(self.path)
            self.path = None


class _Media:
    """A package's media as the sink sees it: declared sizes, then one read pass."""

    def __init__(self, files: dict[str, bytes], tmp_path, *, declared: "dict[str, int] | None" = None) -> None:
        self._files = files
        self._tmp = tmp_path
        self._declared = declared or {}
        self.yielded = 0
        self.large_on_disk_most = 0

    def entries(self) -> list[tuple[str, int]]:
        return [(name, self._declared.get(name, len(data))) for name, data in self._files.items()]

    def iter_files(self, *, in_memory_max: int):
        for name, data in self._files.items():
            self.yielded += 1
            if len(data) <= in_memory_max:
                yield _File(name, len(data), data=data)
            else:
                path = self._tmp / f"media_{self.yielded}"
                path.write_bytes(data)
                on_disk = sum(1 for p in self._tmp.iterdir() if p.name.startswith("media_"))
                self.large_on_disk_most = max(self.large_on_disk_most, on_disk)
                yield _File(name, len(data), path=str(path))


def _blob(data: bytes, ext: str) -> str:
    return f"anki-media/{hashlib.md5(data).hexdigest()}{ext}"


def _card_file(tmp_path, name: str, data: bytes) -> tuple[str, str]:
    """A file a card asks for, as rendering hands it to the sink."""
    path = tmp_path / f"card_{name}"
    path.write_bytes(data)
    return str(path), name


def test_warming_stores_each_file_once_and_several_at_a_time(tmp_path):
    files = {f"img{i}.png": f"image {i}".encode() for i in range(16)}
    store = _Store(delay=0.02)
    sink = worker.BlobMediaSink(store, "anki-media", 10 * worker._MB)

    stored = sink.warm(_Media(files, tmp_path))

    assert store.most_at_once > 1
    assert sorted(store.uploads) == sorted(stored.values()), "nothing uploaded twice"
    assert stored["img3.png"] == _blob(b"image 3", ".png")
    assert set(stored) == set(files), "returned by name, for the deck to render from"


def test_the_same_bytes_under_two_names_upload_once(tmp_path):
    files = {f"copy{i}.jpg": b"one picture" for i in range(6)}
    store = _Store(delay=0.02)
    sink = worker.BlobMediaSink(store, "anki-media", 10 * worker._MB)

    stored = sink.warm(_Media(files, tmp_path))

    assert store.uploads == [_blob(b"one picture", ".jpg")]
    assert set(stored.values()) == {_blob(b"one picture", ".jpg")}
    assert sink._total == len(b"one picture"), "the cap counts the bytes once"


def test_a_blob_already_there_counts_as_stored(tmp_path):
    """Names are content hashes, so one already in the container holds these bytes."""
    store = _Store()
    store.blobs[_blob(b"heart", ".png")] = b"heart"
    sink = worker.BlobMediaSink(store, "anki-media", 10 * worker._MB)

    stored = sink.warm(_Media({"heart.png": b"heart"}, tmp_path))

    assert stored == {"heart.png": _blob(b"heart", ".png")}
    assert store.looked_up == [], "a small file is one create-if-absent request, no lookup first"


def test_a_large_file_already_there_is_not_sent_again(tmp_path):
    video = os.urandom(worker._SINGLE_PUT_BYTES + 1)
    store = _Store()
    store.blobs[_blob(video, ".mp4")] = video
    sink = worker.BlobMediaSink(store, "anki-media", 100 * worker._MB)

    stored = sink.warm(_Media({"lecture.mp4": video}, tmp_path))

    assert stored == {"lecture.mp4": _blob(video, ".mp4")}
    assert store.uploads == [], "looked for first, rather than sent block by block to be refused"


def test_a_file_that_fails_while_warming_is_tried_again_when_a_card_asks(tmp_path):
    store = _Store(fail_first={_blob(b"heart", ".png")})
    sink = worker.BlobMediaSink(store, "anki-media", 10 * worker._MB)

    stored = sink.warm(_Media({"heart.png": b"heart", "lung.png": b"lung"}, tmp_path))
    assert set(stored) == {"lung.png"}
    assert sink._total == len(b"lung"), "a failed upload gives its bytes back to the cap"

    assert sink(*_card_file(tmp_path, "heart.png", b"heart")) == _blob(b"heart", ".png")
    assert store.uploads.count(_blob(b"heart", ".png")) == 2


def test_warm_up_failures_are_one_line_and_a_cards_failure_keeps_its_trace(tmp_path, caplog):
    """A storage outage would otherwise log every image's error twice."""
    store = _Store(always_fail={_blob(b"heart", ".png"), _blob(b"lung", ".png")})
    sink = worker.BlobMediaSink(store, "anki-media", 10 * worker._MB)

    with caplog.at_level(logging.INFO, logger="worker"):
        sink.warm(_Media({"heart.png": b"heart", "lung.png": b"lung"}, tmp_path))
        assert [r.getMessage() for r in caplog.records] == ["media warm-up left 2 of 2 files to rendering"]

        assert sink(*_card_file(tmp_path, "heart.png", b"heart")) is None
    assert caplog.records[-1].exc_info, "the card's attempt logs the full error"


def test_media_over_the_cap_is_left_to_the_cards(tmp_path):
    """The cap then keeps whatever the cards ask for first, as before."""
    media = _Media({"a.png": b"a" * 600, "b.png": b"b" * 600}, tmp_path)
    store = _Store()
    sink = worker.BlobMediaSink(store, "anki-media", 1000)

    assert sink.warm(media) == {}

    assert store.uploads == []
    assert media.yielded == 0, "the package is not read"
    assert sink(*_card_file(tmp_path, "b.png", b"b" * 600)) == _blob(b"b" * 600, ".png")
    assert sink(*_card_file(tmp_path, "a.png", b"a" * 600)) is None


def test_has_room_counts_what_is_stored(tmp_path):
    sink = worker.BlobMediaSink(_Store(), "anki-media", 1000)
    assert sink.has_room(1000)

    sink(*_card_file(tmp_path, "a.png", b"a" * 600))

    assert sink.has_room(400)
    assert not sink.has_room(401)


def test_the_cap_is_judged_on_the_sizes_the_package_declares(tmp_path):
    """Reading stops past a file's declared size, so declared sizes bound what is stored."""
    media = _Media({"a.png": b"a" * 10}, tmp_path, declared={"a.png": 5000})
    sink = worker.BlobMediaSink(_Store(), "anki-media", 1000)

    assert sink.warm(media) == {}


def test_a_large_file_is_uploaded_from_disk_not_held_whole_and_then_removed(tmp_path):
    """The storage client then sends a large file in blocks, so a video is never held in memory."""
    video = os.urandom(worker._SINGLE_PUT_BYTES + 17)
    store = _Store()
    sink = worker.BlobMediaSink(store, "anki-media", 100 * worker._MB)

    sink.warm(_Media({"lecture.mp4": video}, tmp_path))

    ((sent_as, length),) = store.sent
    assert sent_as is not bytes
    assert length == len(video)
    assert store.blobs[_blob(video, ".mp4")] == video, "fingerprinted in blocks, same name as the whole file"
    assert not [p for p in tmp_path.iterdir() if p.name.startswith("media_")], "the temp file is gone"


def test_one_large_file_uploads_while_the_next_is_read(tmp_path):
    files = {f"clip{i}.mp4": os.urandom(worker._SINGLE_PUT_BYTES + i + 1) for i in range(4)}
    media = _Media(files, tmp_path)
    sink = worker.BlobMediaSink(_Store(delay=0.05), "anki-media", 100 * worker._MB)

    stored = sink.warm(media)

    assert len(stored) == 4
    assert media.large_on_disk_most <= 2, "each can be 256 MB, so they never pile up on disk"


def test_warm_up_reads_only_so_far_ahead_of_its_uploads(tmp_path, monkeypatch):
    """Small files are held in memory until they upload, so the read waits on the uploads."""
    monkeypatch.setattr(worker, "_WARM_AHEAD", 3)
    files = {f"img{i}.png": f"image {i}".encode() for i in range(20)}
    media = _Media(files, tmp_path)
    store = _Store(delay=0.01)
    sink = worker.BlobMediaSink(store, "anki-media", 10 * worker._MB)
    most_ahead = 0
    upload = store.get_blob_client

    def counting(name: str) -> _Blob:
        nonlocal most_ahead
        most_ahead = max(most_ahead, media.yielded - len(store.uploads))
        return upload(name)

    store.get_blob_client = counting

    assert len(sink.warm(media)) == 20
    assert most_ahead <= 3 + 1, "those uploading, and the one just read"


# --- The job ---------------------------------------------------------------


class _Deck:
    total = 0
    skipped = 0
    deck_name = "Deck"

    def __init__(self, media) -> None:
        self.media = media
        self.events: list[str] = []
        self.seeded: dict = {}

    def seed_media(self, stored: dict) -> None:
        self.events.append("seeded media")
        self.seeded = stored


class _WarmFails:
    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def entries(self):
        return [("x.png", 1)]

    def iter_files(self, **_k):
        raise self._exc


def _run(monkeypatch, deck: _Deck, client_options: "dict | None" = None) -> list[dict]:
    sent: list[dict] = []
    blob = types.SimpleNamespace(get_blob_properties=lambda: types.SimpleNamespace(size=2))
    container = types.SimpleNamespace(get_blob_client=lambda _n: blob)
    service = types.SimpleNamespace(get_container_client=lambda _n: container, close=lambda: None)

    def connect(_conn: str, **options: object) -> types.SimpleNamespace:
        (client_options if client_options is not None else {}).update(options)
        return service

    monkeypatch.setattr(worker, "BlobServiceClient", types.SimpleNamespace(from_connection_string=connect))
    monkeypatch.setattr(worker, "open_apkg", lambda *_a, **_k: contextlib.nullcontext(deck))

    def post_deck(*_a: object) -> None:
        deck.events.append("posted")

    monkeypatch.setattr(worker, "_post_deck", post_deck)
    monkeypatch.setattr(worker, "_post_callback", lambda payload, _url: sent.append(payload))
    worker._process_job_sync({"job_id": "file-1", "container_name": "c", "apkg_blob_name": "x.apkg"})
    return sent


def test_a_job_warms_the_media_and_renders_from_it(monkeypatch, tmp_path):
    deck = _Deck(_Media({"heart.png": b"heart"}, tmp_path))
    monkeypatch.setattr(worker.BlobMediaSink, "_upload", lambda *_a: None)

    _run(monkeypatch, deck)

    assert deck.events == ["seeded media", "posted"]
    assert deck.seeded == {"heart.png": _blob(b"heart", ".png")}


def test_a_jobs_storage_client_sends_large_files_in_blocks(monkeypatch, tmp_path):
    options: dict = {}

    _run(monkeypatch, _Deck(_Media({}, tmp_path)), options)

    assert options == {"max_single_put_size": worker._SINGLE_PUT_BYTES}


def test_a_failed_warm_up_does_not_fail_the_deck(monkeypatch):
    deck = _Deck(_WarmFails(OSError("scratch disk full")))

    sent = _run(monkeypatch, deck)

    assert deck.events == ["posted"]
    assert sent == [], "no failure is reported"


def test_a_package_storage_cannot_read_is_left_to_the_queue(monkeypatch):
    """Not a bad deck: failing it would lose its images for good, so the queue retries."""
    deck = _Deck(_WarmFails(ServiceRequestError("connection reset")))

    with pytest.raises(ServiceRequestError):
        _run(monkeypatch, deck)

    assert deck.events == []


# --- Reading the package in ranges -------------------------------------------


class _RangedBlob:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.gets: list[tuple[int, int]] = []

    def get_blob_properties(self):
        return types.SimpleNamespace(size=len(self.data))

    def download_blob(self, *, offset: int, length: int):
        self.gets.append((offset, length))
        chunk = self.data[offset : offset + length]
        return types.SimpleNamespace(readall=lambda: chunk)


def _zip(files: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED) as zf:
        for name, data in files.items():
            zf.writestr(name, data)
    return buf.getvalue()


def test_a_package_reads_the_same_in_ranges_as_from_disk():
    files = {str(i): os.urandom(50_000 + i) for i in range(40)}
    blob = _RangedBlob(_zip(files))
    package = worker._blob_package(blob)

    with package(256 * 1024) as stream, zipfile.ZipFile(stream) as zf:
        assert {info.filename: zf.read(info) for info in zf.infolist()} == files
        assert zf.read("17") == files["17"], "out of order, too"


def test_one_pass_takes_large_windows():
    files = {str(i): os.urandom(100_000) for i in range(200)}
    blob = _RangedBlob(_zip(files))
    window = 2 * 1024 * 1024

    with worker._blob_package(blob)(window) as stream, zipfile.ZipFile(stream) as zf:
        for info in zf.infolist():
            zf.read(info)

    assert len(blob.gets) <= len(blob.data) // window + 4, "about one request per window, not one per file"


def test_the_range_reader_seeks_like_a_file():
    reader = worker._BlobRangeReader(_RangedBlob(b"0123456789"), 10)

    assert reader.seek(-3, io.SEEK_END) == 7
    assert reader.read(10) == b"789"
    assert reader.read(10) == b""
    assert reader.seek(2) == 2
    assert reader.seek(3, io.SEEK_CUR) == 5
    assert reader.read(2) == b"56"
    with pytest.raises(ValueError, match="negative"):
        reader.seek(-1)
