"""Deck media is stored several files at a time before any card renders.

Rendering used to upload each image between cards, one after another; `warm`
stores the media folder up front so a card finds its file already there. These
tests hold it to what the one-at-a-time path guaranteed: each distinct file is
uploaded once, the cap counts each byte once, and a file that fails is still
tried when a card asks for it.
"""

import contextlib
import hashlib
import logging
import os
import sys
import threading
import time
import types
from typing import BinaryIO

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
        return self._name in self._store.blobs

    def upload_blob(self, data: BinaryIO, *, length: int, **_k: object) -> None:
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
            store.sent.append((type(data), length))
            store.blobs[self._name] = data.read()
        finally:
            with store._lock:
                store._running -= 1


def _media(tmp_path, files: dict[str, bytes]) -> list[tuple[str, str]]:
    for name, data in files.items():
        (tmp_path / name).write_bytes(data)
    return [(str(tmp_path / name), name) for name in files]


def _blob(data: bytes, ext: str) -> str:
    return f"anki-media/{hashlib.md5(data).hexdigest()}{ext}"


def test_warming_stores_each_file_once_and_several_at_a_time(tmp_path):
    files = _media(tmp_path, {f"img{i}.png": f"image {i}".encode() for i in range(16)})
    store = _Store(delay=0.02)
    sink = worker.BlobMediaSink(store, "anki-media", 10 * worker._MB)

    sink.warm(files)
    stored = [sink(local, name) for local, name in files]

    assert store.most_at_once > 1
    assert sorted(store.uploads) == sorted(stored), "rendering found every file stored; nothing uploaded twice"
    assert stored[3] == _blob(b"image 3", ".png")


def test_the_same_bytes_under_two_names_upload_once(tmp_path):
    files = _media(tmp_path, {f"copy{i}.jpg": b"one picture" for i in range(6)})
    store = _Store(delay=0.02)
    sink = worker.BlobMediaSink(store, "anki-media", 10 * worker._MB)

    sink.warm(files)

    assert store.uploads == [_blob(b"one picture", ".jpg")]
    assert {sink(local, name) for local, name in files} == {_blob(b"one picture", ".jpg")}
    assert sink._total == len(b"one picture"), "the cap counts the bytes once"


def test_a_file_that_fails_while_warming_is_tried_again_when_a_card_asks(tmp_path):
    files = _media(tmp_path, {"heart.png": b"heart", "lung.png": b"lung"})
    store = _Store(fail_first={_blob(b"heart", ".png")})
    sink = worker.BlobMediaSink(store, "anki-media", 10 * worker._MB)

    sink.warm(files)
    assert sink._total == len(b"lung"), "a failed upload gives its bytes back to the cap"

    assert sink(*files[0]) == _blob(b"heart", ".png")
    assert store.uploads.count(_blob(b"heart", ".png")) == 2


def test_warm_up_failures_are_one_line_and_a_cards_failure_keeps_its_trace(tmp_path, caplog):
    """A storage outage would otherwise log every image's error twice."""
    files = _media(tmp_path, {"heart.png": b"heart", "lung.png": b"lung"})
    store = _Store(always_fail={_blob(b"heart", ".png"), _blob(b"lung", ".png")})
    sink = worker.BlobMediaSink(store, "anki-media", 10 * worker._MB)

    with caplog.at_level(logging.INFO, logger="worker"):
        sink.warm(files)
        assert [r.getMessage() for r in caplog.records] == ["media warm-up left 2 of 2 files to rendering"]

        assert sink(*files[0]) is None
    assert caplog.records[-1].exc_info, "the card's attempt logs the full error"


def test_a_folder_over_the_cap_is_left_to_the_cards(tmp_path):
    """The cap then keeps whatever the cards ask for first, as before."""
    files = _media(tmp_path, {"a.png": b"a" * 600, "b.png": b"b" * 600})
    store = _Store()
    sink = worker.BlobMediaSink(store, "anki-media", 1000)

    sink.warm(files)

    assert store.uploads == []
    assert sink(*files[1]) == _blob(b"b" * 600, ".png")
    assert sink(*files[0]) is None


def test_a_file_is_uploaded_from_disk_not_held_whole(tmp_path):
    """The storage client then sends a large file in blocks, so a video is never held in memory."""
    video = os.urandom(3 * 1024 * 1024 + 17)
    files = _media(tmp_path, {"lecture.mp4": video})
    store = _Store()
    sink = worker.BlobMediaSink(store, "anki-media", 10 * worker._MB)

    sink.warm(files)

    ((sent_as, length),) = store.sent
    assert sent_as is not bytes
    assert length == len(video)
    assert store.blobs[_blob(video, ".mp4")] == video, "fingerprinted in blocks, same name as the whole file"


class _Deck:
    total = 0
    skipped = 0
    deck_name = "Deck"

    def __init__(self, media) -> None:
        self._media = media
        self.events: list[str] = []

    def media_files(self):
        self.events.append("listed media")
        if isinstance(self._media, Exception):
            raise self._media
        return self._media


def _run(monkeypatch, deck: _Deck, client_options: "dict | None" = None) -> list[dict]:
    sent: list[dict] = []
    container = types.SimpleNamespace(
        get_blob_client=lambda _n: types.SimpleNamespace(
            download_blob=lambda: types.SimpleNamespace(readinto=lambda fh: fh.write(b"PK"))
        )
    )
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


def test_a_job_warms_the_media_before_it_renders(monkeypatch):
    deck = _Deck([])

    _run(monkeypatch, deck)

    assert deck.events == ["listed media", "posted"]


def test_a_jobs_storage_client_sends_large_files_in_blocks(monkeypatch):
    options: dict = {}

    _run(monkeypatch, _Deck([]), options)

    assert options == {"max_single_put_size": worker._SINGLE_PUT_BYTES}


def test_a_failed_warm_up_does_not_fail_the_deck(monkeypatch):
    deck = _Deck(OSError("media folder vanished"))

    sent = _run(monkeypatch, deck)

    assert deck.events == ["listed media", "posted"]
    assert sent == [], "no failure is reported"
