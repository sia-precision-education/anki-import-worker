# Copyright (C) 2026 SIA Precision Education — AGPL-3.0 (see LICENSE).
"""Anki .apkg import worker.

Polls an Azure Storage Queue for `AnkiImportRequest` jobs, reads the deck where
it lies in blob storage (in ranges, never downloaded whole), renders its cards to
HTML via `renderer.open_apkg`, uploads its media back to the same container
straight from the package, and POSTs the rendered
cards to the SIA backend's authenticated callback, one chunk at a time. The
boundary is plain data in both directions; the worker holds no SIA code and
persists nothing itself.

Shape mirrors the SIA exam-worker (async poll -> bounded concurrency ->
delete-on-success -> dequeue-count retry). The heavy work (blob I/O + the
blocking Rust-backed anki render) runs in a thread so the poll loop's event
loop stays responsive.
"""

from __future__ import annotations

import asyncio
import contextlib
import errno
import hashlib
import io
import json
import logging
import os
import signal
import sys
import threading
import time
from collections import Counter
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, wait
from itertools import islice
from math import ceil
from typing import TYPE_CHECKING, Any, BinaryIO
from urllib.parse import urlparse

import httpx
from azure.core.exceptions import AzureError, ResourceExistsError, ServiceResponseError
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.storage.queue.aio import QueueServiceClient
from dotenv import load_dotenv

from renderer import open_apkg
from worker_settings import WorkerSettings

if TYPE_CHECKING:
    from renderer import MediaFile, PackageMedia, PackageOpener, RenderedCard, RenderingDeck

load_dotenv()
settings = WorkerSettings.from_env()

logging.basicConfig(
    level=getattr(logging, settings.log_level, logging.INFO),
    format="%(asctime)s [anki-worker] %(levelname)s: %(message)s",
)
# basicConfig sets the root logger, so at INFO the Azure SDK logs full request and
# response headers on every queue poll.
logging.getLogger("azure").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

_MB = 1024 * 1024
# Media stored at once before rendering; with the package reader, under the HTTP pool's 10 connections.
_WARM_WORKERS = 8
# Files read from the package ahead of their uploads. Bounds what warm-up holds in
# memory: this many files of at most `_SINGLE_PUT_BYTES` each, plus the one just read.
_WARM_AHEAD = 2 * _WARM_WORKERS
# The storage client sends a file over this in 4 MB blocks read from disk, so no
# upload holds more than this much of a file, however large the file. Files up to
# this size are read from the package into memory; larger ones into a temp file.
_SINGLE_PUT_BYTES = 8 * _MB
# Media a deck may decode, in multiples of its media cap: one pass up front, then
# files that pass missed read again as cards ask. Anki's own exports stay under it.
_MEDIA_READS_PER_CAP = 2

# Version of the job/callback contract this worker speaks (see README, "Stable
# interface"). Bump the major only on a breaking change.
JOB_SCHEMA_VERSION = 1

_CONTENT_TYPES = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".gif": "image/gif",
    ".webp": "image/webp",
    ".svg": "image/svg+xml",
    ".bmp": "image/bmp",
    ".mp3": "audio/mpeg",
    ".ogg": "audio/ogg",
    ".wav": "audio/wav",
    ".m4a": "audio/mp4",
    ".mp4": "video/mp4",
    ".webm": "video/webm",
}


class _BlobRangeReader(io.RawIOBase):
    """A blob as a seekable file, read in ranges: the package is never downloaded whole.

    Wrapped in a `BufferedReader`, whose buffer size is the read-ahead per request.
    Every read error is an AzureError (the SDK has already retried it), failing the job
    so the queue retries it: the renderer treats other errors from a zip entry as that
    one file being unreadable, so an unwrapped transport error would cost images instead.
    """

    def __init__(self, blob_client: Any, size: int) -> None:
        self._bc = blob_client
        self._size = size
        self._pos = 0

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        base = {io.SEEK_SET: 0, io.SEEK_CUR: self._pos, io.SEEK_END: self._size}[whence]
        if base + offset < 0:
            # OSError, as a file raises: zipfile probes a short archive this way and reads
            # the error as "not a zip".
            raise OSError(errno.EINVAL, "negative seek position")
        self._pos = base + offset
        return self._pos

    def readinto(self, buffer: Any) -> int:
        n = min(len(buffer), self._size - self._pos)
        if n <= 0:
            return 0
        try:
            data = self._bc.download_blob(offset=self._pos, length=n).readall()
        except AzureError:
            raise
        except Exception as exc:
            msg = f"reading the deck failed: {exc}"
            raise ServiceResponseError(msg) from exc
        memoryview(buffer).cast("B")[: len(data)] = data
        self._pos += len(data)
        return len(data)


def _blob_package(blob_client: Any) -> PackageOpener:
    """Open the deck where it lies in blob storage, with the read-ahead the caller asks for."""
    size = blob_client.get_blob_properties().size

    def open_package(buffer_size: int) -> BinaryIO:
        return io.BufferedReader(_BlobRangeReader(blob_client, size), buffer_size=buffer_size)

    return open_package


class BlobMediaSink:
    """Uploads deck media into the user's container under a fixed prefix.

    Hash-dedups within and across cards, enforces a total-bytes cap, and
    returns the stored blob name (e.g. "anki-media/<md5>.jpg") that the renderer
    substitutes into the card HTML. The SIA backend rewrites those refs into
    signed media-proxy URLs on the callback.
    """

    def __init__(self, container_client: Any, media_prefix: str, max_bytes: int) -> None:
        self._cc = container_client
        self._prefix = media_prefix.rstrip("/")
        self._max = max_bytes
        self._total = 0
        self._by_hash: dict[str, str] = {}
        # `warm` stores from several threads; these guard the totals and the hash map.
        self._lock = threading.Lock()
        self._in_flight: dict[str, threading.Event] = {}

    def __call__(self, local_path: str, fname: str) -> str | None:
        try:
            md5, size = _md5_and_size(local_path)
        except OSError:
            logger.warning("could not read media file %s", fname)
            return None
        return self._put(md5, size, fname, lambda: open(local_path, "rb"))

    def has_room(self, size: int) -> bool:
        """Whether a file this large still fits under the cap; the deck skips reading one that does not."""
        with self._lock:
            return self._total + size <= self._max

    def warm(self, media: PackageMedia) -> dict[str, str]:
        """Store the deck's media several files at a time, before any card renders.

        Reads the package once, in archive order, and returns what it stored by
        name, for the deck to render from. A file that fails here is tried again
        when a card asks for it. Skipped when the sizes the package declares exceed
        the cap, so the cap keeps the files cards ask for first.
        """
        entries = media.entries()
        if sum(size for _, size in entries) > self._max:
            logger.info("deck media exceeds the %dMB cap; storing it as cards ask", self._max // _MB)
            return {}
        stored: dict[str, str] = {}
        ahead = threading.BoundedSemaphore(_WARM_AHEAD)
        uploads: list[tuple[str, Future[str | None]]] = []
        last_large: Future[str | None] | None = None
        with ThreadPoolExecutor(max_workers=_WARM_WORKERS) as pool:
            for file in media.iter_files(in_memory_max=_SINGLE_PUT_BYTES):
                if file.path is not None and last_large is not None:
                    # Large files go through disk; one uploads while the next is read.
                    wait([last_large])
                ahead.acquire()
                upload = pool.submit(self._warm_one, file)
                upload.add_done_callback(lambda _u: ahead.release())
                if file.path is not None:
                    last_large = upload
                uploads.append((file.name, upload))
            for name, upload in uploads:
                ref = upload.result()
                if ref is not None:
                    stored[name] = ref
        if len(stored) < len(entries):
            # One line: each of these is tried again when a card asks, and logs its error then.
            logger.warning("media warm-up left %d of %d files to rendering", len(entries) - len(stored), len(entries))
        return stored

    def _warm_one(self, file: MediaFile) -> str | None:
        try:
            if file.data is not None:
                data = file.data
                md5 = hashlib.md5(data).hexdigest()
                return self._put(md5, len(data), file.name, lambda: io.BytesIO(data), quiet=True)
            path = file.path
            if path is None:
                return None
            md5, size = _md5_and_size(path)
            return self._put(md5, size, file.name, lambda: open(path, "rb"), quiet=True)
        except OSError:
            return None
        finally:
            file.data = None
            file.discard()

    def _put(
        self, md5: str, size: int, fname: str, open_source: Callable[[], BinaryIO], *, quiet: bool = False
    ) -> str | None:
        with self._lock:
            if md5 in self._by_hash:
                return self._by_hash[md5]
            pending = self._in_flight.get(md5)
            if pending is None:
                if self._total + size > self._max:
                    logger.warning("media cap %dMB exceeded; skipping %s", self._max // _MB, fname)
                    return None
                self._total += size  # reserved; released below if the upload fails
                pending = self._in_flight[md5] = threading.Event()
                owner = True
            else:
                owner = False
        if not owner:
            # The same bytes under another name, already uploading.
            pending.wait()
            with self._lock:
                return self._by_hash.get(md5)

        ext = os.path.splitext(fname)[1].lower()
        blob_name = f"{self._prefix}/{md5}{ext}"
        stored = False
        try:
            self._upload(blob_name, size, ext, open_source)
            stored = True
        except Exception:
            if not quiet:
                logger.exception("media upload failed: %s", blob_name)
        finally:
            with self._lock:
                if stored:
                    self._by_hash[md5] = blob_name
                else:
                    self._total -= size
                del self._in_flight[md5]
            pending.set()
        return blob_name if stored else None

    def _upload(self, blob_name: str, size: int, ext: str, open_source: Callable[[], BinaryIO]) -> None:
        """Create the blob unless it is there. Names are content hashes, so one that is holds these bytes."""
        bc = self._cc.get_blob_client(blob_name)
        # A large file is looked for first, rather than sent block by block to be refused at the end.
        if size > _SINGLE_PUT_BYTES and bc.exists():
            return
        ct = _CONTENT_TYPES.get(ext)
        with open_source() as fh:
            try:
                bc.upload_blob(
                    fh,
                    length=size,
                    overwrite=False,
                    content_settings=ContentSettings(content_type=ct) if ct else None,
                )
            except ResourceExistsError:
                pass


def _md5_and_size(path: str) -> tuple[str, int]:
    """The file's md5 and size, read a block at a time."""
    digest = hashlib.md5()  # noqa: S324 — blob naming, not security
    size = 0
    with open(path, "rb") as fh:
        while block := fh.read(_MB):
            digest.update(block)
            size += len(block)
    return digest.hexdigest(), size


def _card_dict(c: Any) -> dict[str, Any]:
    return {
        "front_html": c.front_html,
        "back_html": c.back_html,
        "css": c.css,
        "deck": c.deck,
        "note_type": c.note_type,
        "cloze": c.cloze,
        "tags": c.tags,
        "media": c.media,
        "uid": c.uid,
    }


def _chunk_cards(cards: list[RenderedCard]) -> list[dict[str, Any]]:
    """Card dicts for one callback, each note type's stylesheet on its first card only.

    The backend keeps one stylesheet per note type. Repeating it on every card
    added about 19 MB to each 1,000-card chunk of an AnKing deck.
    """
    seen: set[str] = set()
    out = []
    for c in cards:
        card = _card_dict(c)
        if c.note_type in seen:
            card["css"] = ""
        seen.add(c.note_type)
        out.append(card)
    return out


def _job_status(deck: RenderingDeck) -> str:
    if deck.total == 0:
        return "failed"
    return "partial" if deck.skipped > 0 else "ok"


def _deck_stats(deck: RenderingDeck) -> dict[str, Any]:
    """Aggregate counts the backend needs for its deck overview.

    Read from the collection before any card is rendered, so every chunk carries
    the deck-wide figures; the backend only ever sees the deck a chunk at a time.
    Capped to the most-common few so a deck with pathologically many subdecks
    can't bloat every chunk.
    """
    subdecks: Counter[str] = Counter()
    for deck_path, n in deck.deck_counts.items():
        subdecks[(deck_path or "Default").split("::")[-1]] += n
    note_types: Counter[str] = Counter()
    for note_type, n in deck.note_type_counts.items():
        note_types[note_type or "Basic"] += n
    return {
        "total": deck.total,
        "subdecks": dict(subdecks.most_common(50)),
        "note_types": dict(note_types.most_common(50)),
    }


def _summary(deck: RenderingDeck) -> dict[str, Any]:
    # `degraded` is a running total while the deck renders, complete on the last
    # chunk: the one that completes the card count the backend finalises on.
    return {"imported": deck.total, "degraded": dict(deck.degraded), "skipped": deck.skipped}


def _failure_payload(job_id: str, error: str) -> dict[str, Any]:
    return {
        "schema_version": JOB_SCHEMA_VERSION,
        "job_id": job_id,
        "status": "failed",
        "deck_name": None,
        "cards": [],
        "stats": {"total": 0, "subdecks": {}, "note_types": {}},
        "summary": {"imported": 0, "degraded": {}, "skipped": 0},
        "error": error[:500],
    }


# A chunk POST is retried in place before the whole job is failed: a deploy-window
# 502 costs seconds here, versus re-rendering an entire deck (or, past the dequeue
# budget, a manual re-upload of up to 10 GB). Only transport errors and 5xx/429 are
# retried — a 4xx is a contract or token problem that will not fix itself.
_CALLBACK_ATTEMPTS = 3
_CALLBACK_BACKOFF_SECONDS = (2.0, 6.0)

# Lease renewal cadence, as a fraction of the visibility window: renewing at a
# third of it means two renewals can fail before the lease actually lapses. The
# floor keeps a small window from turning renewal into a hot loop — and is why
# the two are checked against each other at startup.
_LEASE_RENEW_DIVISOR = 3
_LEASE_RENEW_MIN_SECONDS = 30


def _lease_renew_interval() -> float:
    """Seconds between lease extensions for a job that is still running."""
    return max(settings.visibility_timeout_seconds // _LEASE_RENEW_DIVISOR, _LEASE_RENEW_MIN_SECONDS)


def _assert_lease_cadence() -> None:
    """Fail fast if the visibility window is too short to be renewed inside.

    The window is a deployment knob and the cadence is code, so they are only
    correct in relation to each other. If a renewal cannot land twice inside the
    window, a single failed extension lapses the lease — which is silently the
    bug renewal exists to fix: the deck comes back on the queue mid-render, is
    processed a second time, and the run that finishes cannot delete the message.
    """
    interval = _lease_renew_interval()
    visibility = settings.visibility_timeout_seconds
    if interval * 2 > visibility:
        msg = (
            f"VISIBILITY_TIMEOUT_SECONDS ({visibility}s) is too short for the lease "
            f"cadence ({interval:g}s): it must fit at least two renewals, or one failed "
            "extension lets the deck be rendered twice. Raise it to at least "
            f"{int(interval * 2)}s."
        )
        raise RuntimeError(msg)


def _callback_retryable(exc: Exception) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500 or exc.response.status_code == 429
    return isinstance(exc, httpx.RequestError)


def _post_callback(payload: dict[str, Any], callback_url: str) -> None:
    headers = {"X-Anki-Callback-Token": settings.callback_secret}
    for attempt in range(_CALLBACK_ATTEMPTS):
        try:
            with httpx.Client(timeout=httpx.Timeout(180.0)) as client:
                resp = client.post(callback_url, json=payload, headers=headers)
                resp.raise_for_status()
            break
        except Exception as exc:  # noqa: BLE001
            last = attempt == _CALLBACK_ATTEMPTS - 1
            if last or not _callback_retryable(exc):
                raise
            delay = _CALLBACK_BACKOFF_SECONDS[min(attempt, len(_CALLBACK_BACKOFF_SECONDS) - 1)]
            logger.warning(
                "callback attempt %d/%d failed for job_id=%s (%s); retrying in %.0fs",
                attempt + 1,
                _CALLBACK_ATTEMPTS,
                payload["job_id"],
                exc,
                delay,
            )
            time.sleep(delay)
    logger.info(
        "callback ok job_id=%s status=%s cards=%d -> %s",
        payload["job_id"],
        payload["status"],
        len(payload["cards"]),
        callback_url,
    )


def _post_deck(job_id: str, deck: RenderingDeck, callback_url: str, chunk_size: "int | None") -> None:
    """Render the deck and deliver it to the backend callback, a chunk at a time.

    Each chunk is rendered, posted and released before the next is rendered, so
    the worker holds one chunk, never the deck: a 35,000-card AnKing deck held
    whole needed about 3.7 GiB in a 2 GiB container. Each chunk is self-describing
    (`chunk_index`, `chunk_count`, `start_index`) and carries the deck-wide
    `stats`, so the backend finalises by card count without holding cross-chunk
    state. Any chunk POST that fails raises, so the queue redelivers the whole
    job; the backend dedupes replayed cards by their stable `uid`.

    Absent/zero `chunk_size` falls back to a single callback (v1 behaviour), so a
    new worker stays correct against a backend that predates chunking.
    """
    if not chunk_size or deck.total == 0:
        cards = [_card_dict(c) for c in deck]
        _post_callback(
            {
                "schema_version": JOB_SCHEMA_VERSION,
                "job_id": job_id,
                "status": _job_status(deck),
                "deck_name": deck.deck_name,
                "cards": cards,
                "stats": _deck_stats(deck),
                "summary": _summary(deck),
                "error": None,
            },
            callback_url,
        )
        return

    status = _job_status(deck)
    stats = _deck_stats(deck)
    chunk_count = ceil(deck.total / chunk_size)
    logger.info("chunking job_id=%s cards=%d into %d chunk(s) of %d", job_id, deck.total, chunk_count, chunk_size)
    rendering = iter(deck)
    for idx in range(chunk_count):
        # Rendered before the payload is built, so `summary` counts this chunk's
        # missing media; dropped after its POST, so one chunk is held at a time.
        cards = _chunk_cards(list(islice(rendering, chunk_size)))
        _post_callback(
            {
                "schema_version": JOB_SCHEMA_VERSION,
                "job_id": job_id,
                "status": status,
                "deck_name": deck.deck_name,
                "chunk_index": idx,
                "chunk_count": chunk_count,
                "start_index": idx * chunk_size,
                "cards": cards,
                "stats": stats,
                "summary": _summary(deck),
                "error": None,
            },
            callback_url,
        )
        del cards


def _warm_media(job_id: str, sink: BlobMediaSink, deck: RenderingDeck) -> None:
    """Store the deck's media before rendering. A failure here only leaves it to rendering.

    Except a storage error reading the package: rendering reads the same package, so
    the job fails and the queue retries it.
    """
    started = time.monotonic()
    try:
        stored = sink.warm(deck.media)
    except AzureError:
        raise
    except Exception:
        logger.exception("media warm-up failed job_id=%s; cards will store their own media", job_id)
        return
    deck.seed_media(stored)
    logger.info("media warm-up job_id=%s files=%d in %.1fs", job_id, len(stored), time.monotonic() - started)


def _validate_job(data: dict[str, Any]) -> "str | None":
    """Validate an inbound job against the v1 contract. Returns an error, or None.

    Non-SIA callers can rely on this: an unknown schema_version or a missing
    required field is rejected rather than silently mis-processed.
    """
    version = data.get("schema_version", JOB_SCHEMA_VERSION)
    try:
        version = int(version)
    except (TypeError, ValueError):
        return f"schema_version is not an integer: {version!r}"
    if version != JOB_SCHEMA_VERSION:
        return f"unsupported schema_version {version} (this worker speaks v{JOB_SCHEMA_VERSION})"
    for field in ("job_id", "container_name", "apkg_blob_name"):
        value = data.get(field)
        if not isinstance(value, str) or not value:
            return f"missing or empty required string field: {field}"

    # Optional per-job callback, so ONE worker can serve several backends (each
    # sends its own URL). Honoured only for allowlisted hosts — otherwise a
    # forged job could make us POST the shared secret to an attacker's server.
    callback_url = data.get("callback_url")
    if callback_url is not None:
        if not isinstance(callback_url, str) or not callback_url:
            return "callback_url must be a non-empty string when present"
        host = urlparse(callback_url).netloc
        if host not in settings.allowed_callback_hosts:
            return f"callback_url host not allowed: {host!r}"
    return None


def _out_of_disk(exc: BaseException | None) -> bool:
    """Whether a failure, or anything it was raised from, is a full disk."""
    while exc is not None:
        if isinstance(exc, OSError) and exc.errno in (errno.ENOSPC, errno.EDQUOT):
            return True
        if "database or disk is full" in str(exc):  # SQLITE_FULL, from anki's import
            return True
        exc = exc.__cause__ or exc.__context__
    return False


def _process_job_sync(message_data: dict[str, Any]) -> None:
    """Blocking unit of work: read the package -> upload media -> render -> callback."""
    error = _validate_job(message_data)
    if error:
        logger.error("rejecting job (%s): %s", error, message_data)
        return

    job_id = message_data["job_id"]
    container_name = message_data["container_name"]
    apkg_blob_name = message_data["apkg_blob_name"]
    media_prefix = message_data.get("media_prefix", "anki-media")
    max_cards = int(message_data.get("max_cards", 20000))
    max_media_mb = int(message_data.get("max_media_mb", 12 * 1024))
    # Optional (additive, still v1): a positive chunk_size means the backend
    # speaks the chunked-callback contract, so fan the deck across several POSTs.
    # Absent -> one callback (a backend that predates chunking).
    raw_chunk = message_data.get("chunk_size")
    chunk_size = int(raw_chunk) if isinstance(raw_chunk, int) and not isinstance(raw_chunk, bool) and raw_chunk > 0 else None
    # Optional (additive, still v1): a literal true drops <script> elements from the card
    # HTML (see renderer.remove_scripts), for a backend whose viewer never runs them. They were 80% of the
    # HTML in a student's AnKing deck. Absent -> the HTML exactly as Anki renders it.
    strip_scripts = message_data.get("strip_scripts") is True
    # Per-job callback (validated above) lets one worker serve prod + staging.
    callback_url = message_data.get("callback_url") or settings.callback_url

    blob_service = BlobServiceClient.from_connection_string(
        settings.azure_storage_connection_string, max_single_put_size=_SINGLE_PUT_BYTES
    )
    try:
        cc = blob_service.get_container_client(container_name)
        logger.info("reading deck job_id=%s blob=%s", job_id, apkg_blob_name)
        package = _blob_package(cc.get_blob_client(apkg_blob_name))

        sink = BlobMediaSink(cc, media_prefix, max_media_mb * _MB)
        try:
            with open_apkg(
                package,
                sink,
                max_cards=max_cards,
                max_media_read_bytes=_MEDIA_READS_PER_CAP * max_media_mb * _MB,
                strip_scripts=strip_scripts,
            ) as deck:
                logger.info(
                    "imported job_id=%s deck=%r cards=%d skipped=%d",
                    job_id,
                    deck.deck_name,
                    deck.total,
                    deck.skipped,
                )
                _warm_media(job_id, sink, deck)
                _post_deck(job_id, deck, callback_url, chunk_size)
        except (httpx.HTTPError, AzureError):
            # A callback that did not land, or the package could not be read from
            # storage: not a bad deck, so the queue redelivers the job.
            raise
        except Exception as exc:
            if _out_of_disk(exc):
                # The scratch disk is shared with the other job: not this deck's fault.
                raise
            logger.exception("render failed job_id=%s", job_id)
            _post_callback(_failure_payload(job_id, str(exc)), callback_url)
            return
    finally:
        blob_service.close()


def _post_job_abandoned(message_data: dict[str, Any], reason: str) -> None:
    """Report a job the worker has given up on, so the deck fails now, not in 30 minutes."""
    # Re-validated because this runs OUTSIDE _process_job_sync: never POST the
    # shared secret to a callback_url that has not passed the host allowlist.
    if _validate_job(message_data) is not None:
        return
    job_id = message_data["job_id"]
    callback_url = message_data.get("callback_url") or settings.callback_url
    try:
        _post_callback(_failure_payload(job_id, reason), callback_url)
    except Exception:
        logger.exception("could not report abandoned job job_id=%s", job_id)


class AnkiWorker:
    def __init__(self) -> None:
        self.queue_client: Any = None
        self.running = False
        self.current_tasks: set[asyncio.Task] = set()

    async def setup(self) -> None:
        _assert_lease_cadence()
        queue_service = QueueServiceClient.from_connection_string(
            settings.azure_storage_connection_string
        )
        self.queue_client = queue_service.get_queue_client(settings.anki_queue_name)
        try:
            await self.queue_client.create_queue()
            logger.info("created queue %s", settings.anki_queue_name)
        except ResourceExistsError:
            logger.info("queue %s already exists", settings.anki_queue_name)
        logger.info(
            "anki-worker setup complete (concurrency=%d, visibility=%ds, lease renewed every %gs)",
            settings.max_concurrent_jobs,
            settings.visibility_timeout_seconds,
            _lease_renew_interval(),
        )

    async def _keep_message_visible(self, message: Any, stop: asyncio.Event) -> None:
        """Extend a message's lease until `stop` is set.

        The refreshed pop receipt is written back onto `message`: Azure
        invalidates the previous receipt on every update, and the caller deletes
        with this same object afterwards. Skipping that write would make the
        delete fail and the deck be re-rendered — the exact bug this exists to
        stop.

        Best-effort. If renewal fails the loop exits and the message eventually
        reappears, which is the behaviour we had before this existed.
        """
        interval = _lease_renew_interval()
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
                return
            except TimeoutError:
                pass

            try:
                updated = await self.queue_client.update_message(
                    message, visibility_timeout=settings.visibility_timeout_seconds
                )
                if updated is not None and getattr(updated, "pop_receipt", None):
                    message.pop_receipt = updated.pop_receipt
                logger.debug("extended lease for message id=%s", getattr(message, "id", None))
            except Exception as exc:  # noqa: BLE001
                logger.warning("could not extend lease id=%s, letting it lapse: %s", getattr(message, "id", None), exc)
                return

    @staticmethod
    async def _stop_renewer(stop: asyncio.Event, renewer: asyncio.Task) -> None:
        """Stop the renewer and wait for it, so no renewal races a delete."""
        stop.set()
        if renewer.done():
            return
        renewer.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await renewer

    async def _abandon(
        self, queue_message: Any, message_data: dict[str, Any], reason: str, stop: asyncio.Event, renewer: asyncio.Task
    ) -> None:
        """Report the deck failed, then drop its message: the retry budget is spent."""
        logger.error("message id=%s exceeded MAX_RETRIES=%d; deleting", queue_message.id, settings.max_retries)
        # Still leased across the abandon callback (it retries, so it is not
        # instant), then stopped before the delete: a renewal in flight would
        # invalidate the receipt we delete with.
        await asyncio.to_thread(_post_job_abandoned, message_data, reason)
        await self._stop_renewer(stop, renewer)
        try:
            await self.queue_client.delete_message(queue_message)
        except Exception:
            logger.exception("failed to delete poisoned message id=%s", queue_message.id)

    async def _process_and_cleanup(self, queue_message: Any, message_data: dict[str, Any]) -> None:
        # Hold the lease for as long as the render actually takes. Without it a
        # deck slower than the window came back on the queue mid-run, was rendered
        # a second time alongside the run still in flight, and the run that
        # finished could not delete the message with its now-stale pop receipt.
        stop_renewing = asyncio.Event()
        renewer = asyncio.create_task(self._keep_message_visible(queue_message, stop_renewing))
        try:
            # A run that was killed (out of memory, say) never reached the except
            # below, so the budget is also checked on arrival: this many deliveries
            # came before this one and none of them finished.
            earlier = (queue_message.dequeue_count or 1) - 1
            if earlier >= settings.max_retries:
                reason = f"the import was interrupted {earlier} times"
                await self._abandon(queue_message, message_data, reason, stop_renewing, renewer)
                return
            await asyncio.to_thread(_process_job_sync, message_data)
            await self._stop_renewer(stop_renewing, renewer)
            await self.queue_client.delete_message(queue_message)
            logger.info("deleted message id=%s", queue_message.id)
        except Exception as exc:  # noqa: BLE001
            dequeue_count = queue_message.dequeue_count or 0
            logger.exception(
                "job failed for message id=%s (attempt=%d)", queue_message.id, dequeue_count + 1
            )
            # Past the budget the message is dropped rather than looped — but say so
            # first. A job abandoned mid-fan-out has posted no failure of its own, so
            # without this the deck read as still importing until the backend's
            # stranded-file reconciler noticed it half an hour later.
            if dequeue_count >= settings.max_retries:
                reason = f"import abandoned after {dequeue_count} attempt(s): {exc}"
                await self._abandon(queue_message, message_data, reason, stop_renewing, renewer)
        finally:
            # Belt and braces: the retry path above leaves the message to be
            # redelivered, which is correct, but the renewer must not outlive it.
            await self._stop_renewer(stop_renewing, renewer)

    async def poll_queue(self) -> None:
        while self.running:
            try:
                if len(self.current_tasks) >= settings.max_concurrent_jobs:
                    await asyncio.sleep(settings.queue_poll_interval)
                    continue

                messages = self.queue_client.receive_messages(
                    messages_per_page=1,
                    visibility_timeout=settings.visibility_timeout_seconds,
                )

                got_one = False
                async for message in messages:
                    got_one = True
                    try:
                        message_data = json.loads(message.content)
                    except json.JSONDecodeError:
                        logger.exception("invalid JSON in message id=%s; deleting", message.id)
                        await self.queue_client.delete_message(message)
                        continue

                    logger.info("received message id=%s", message.id)
                    task = asyncio.create_task(self._process_and_cleanup(message, message_data))
                    self.current_tasks.add(task)
                    task.add_done_callback(self.current_tasks.discard)

                if not got_one:
                    await asyncio.sleep(settings.queue_poll_interval)
            except Exception:
                logger.exception("poll loop error; backing off")
                await asyncio.sleep(settings.queue_poll_interval)

    async def start(self) -> None:
        await self.setup()
        self.running = True
        logger.info("anki-worker started; polling %s", settings.anki_queue_name)
        await self.poll_queue()

    async def stop(self) -> None:
        logger.info("stopping anki-worker…")
        self.running = False
        if self.current_tasks:
            logger.info("waiting for %d in-flight task(s) to drain", len(self.current_tasks))
            await asyncio.gather(*self.current_tasks, return_exceptions=True)
        if self.queue_client:
            try:
                await self.queue_client.close()
            except Exception:
                logger.exception("error closing queue client")
        logger.info("anki-worker stopped")


_worker: "AnkiWorker | None" = None


def _signal_handler(signum: int, _frame: Any) -> None:
    logger.info("received signal %s; shutting down", signum)
    if _worker is not None:
        asyncio.create_task(_worker.stop())


async def main() -> None:
    global _worker
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    _worker = AnkiWorker()
    try:
        await _worker.start()
    except KeyboardInterrupt:
        logger.info("keyboard interrupt")
    finally:
        await _worker.stop()


if __name__ == "__main__":
    asyncio.run(main())
    sys.exit(0)
