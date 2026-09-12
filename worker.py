# Copyright (C) 2026 SIA Precision Education — AGPL-3.0 (see LICENSE).
"""Anki .apkg import worker.

Polls an Azure Storage Queue for `AnkiImportRequest` jobs, downloads the deck
from blob storage, renders every card to HTML via `renderer.render_apkg`,
uploads referenced media back to the same container, and POSTs the rendered
cards to the SIA backend's authenticated callback. The boundary is plain data
in both directions; the worker holds no SIA code and persists nothing itself.

Shape mirrors the SIA exam-worker (async poll -> bounded concurrency ->
delete-on-success -> dequeue-count retry). The heavy work (blob I/O + the
blocking Rust-backed anki render) runs in a thread so the poll loop's event
loop stays responsive.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import signal
import sys
import tempfile
import time
from collections import Counter
from math import ceil
from typing import Any
from urllib.parse import urlparse

import httpx
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.storage.queue.aio import QueueServiceClient
from dotenv import load_dotenv

from renderer import RenderResult, render_apkg
from worker_settings import WorkerSettings

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

    def __call__(self, local_path: str, fname: str) -> "str | None":
        try:
            with open(local_path, "rb") as fh:
                data = fh.read()
        except OSError:
            logger.warning("could not read media file %s", fname)
            return None

        md5 = hashlib.md5(data).hexdigest()  # noqa: S324 — blob naming, not security
        if md5 in self._by_hash:
            return self._by_hash[md5]
        if self._total + len(data) > self._max:
            logger.warning("media cap %dMB exceeded; skipping %s", self._max // _MB, fname)
            return None

        ext = os.path.splitext(fname)[1].lower()
        blob_name = f"{self._prefix}/{md5}{ext}"
        try:
            bc = self._cc.get_blob_client(blob_name)
            if not bc.exists():
                ct = _CONTENT_TYPES.get(ext)
                bc.upload_blob(
                    data,
                    overwrite=True,
                    content_settings=ContentSettings(content_type=ct) if ct else None,
                )
        except Exception:
            logger.exception("media upload failed: %s", blob_name)
            return None

        self._total += len(data)
        self._by_hash[md5] = blob_name
        return blob_name


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


def _job_status(result: RenderResult) -> str:
    if result.imported == 0:
        return "failed"
    return "partial" if result.skipped > 0 else "ok"


def _deck_stats(result: RenderResult) -> dict[str, Any]:
    """Aggregate counts the backend needs for its deck overview.

    Computed here because the worker holds the whole rendered deck; the backend
    only ever sees it a chunk at a time. Capped to the most-common few so a deck
    with pathologically many subdecks can't bloat every chunk.
    """
    subdecks = Counter((c.deck or "Default").split("::")[-1] for c in result.cards)
    note_types = Counter(c.note_type or "Basic" for c in result.cards)
    return {
        "total": len(result.cards),
        "subdecks": dict(subdecks.most_common(50)),
        "note_types": dict(note_types.most_common(50)),
    }


def _result_payload(job_id: str, result: RenderResult) -> dict[str, Any]:
    return {
        "schema_version": JOB_SCHEMA_VERSION,
        "job_id": job_id,
        "status": _job_status(result),
        "deck_name": result.deck_name,
        "cards": [_card_dict(c) for c in result.cards],
        "stats": _deck_stats(result),
        "summary": {"imported": result.imported, "degraded": result.degraded, "skipped": result.skipped},
        "error": None,
    }


# A chunk POST is retried in place before the whole job is failed: a deploy-window
# 502 costs seconds here, versus re-rendering an entire deck (or, past the dequeue
# budget, a manual re-upload of up to 512 MB). Only transport errors and 5xx/429 are
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


def _post_result(job_id: str, result: RenderResult, callback_url: str, chunk_size: "int | None") -> None:
    """Deliver rendered cards to the backend callback.

    With a positive `chunk_size` (the backend advertising it speaks the chunked
    contract) the cards are fanned across several callbacks so no single POST
    ever carries a whole large deck — bounding payload size, request time, and
    the cost of a retry. Each chunk is self-describing (`chunk_index`,
    `chunk_count`, `start_index`) and carries the deck-wide `stats`/`summary`, so
    the final chunk can finalise without the backend holding cross-chunk state.
    Any chunk POST that fails raises, so the queue redelivers the whole job; the
    backend dedupes replayed cards by their stable `uid`.

    Absent/zero `chunk_size` falls back to a single callback (v1 behaviour), so a
    new worker stays correct against a backend that predates chunking.
    """
    cards = [_card_dict(c) for c in result.cards]
    if not chunk_size or chunk_size <= 0 or not cards:
        _post_callback(_result_payload(job_id, result), callback_url)
        return

    status = _job_status(result)
    stats = _deck_stats(result)
    summary = {"imported": result.imported, "degraded": result.degraded, "skipped": result.skipped}
    chunk_count = ceil(len(cards) / chunk_size)
    logger.info("chunking job_id=%s cards=%d into %d chunk(s) of %d", job_id, len(cards), chunk_count, chunk_size)
    for idx in range(chunk_count):
        start = idx * chunk_size
        _post_callback(
            {
                "schema_version": JOB_SCHEMA_VERSION,
                "job_id": job_id,
                "status": status,
                "deck_name": result.deck_name,
                "chunk_index": idx,
                "chunk_count": chunk_count,
                "start_index": start,
                "cards": cards[start : start + chunk_size],
                "stats": stats,
                "summary": summary,
                "error": None,
            },
            callback_url,
        )


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


def _process_job_sync(message_data: dict[str, Any]) -> None:
    """Blocking unit of work: download -> render -> upload media -> callback."""
    error = _validate_job(message_data)
    if error:
        logger.error("rejecting job (%s): %s", error, message_data)
        return

    job_id = message_data["job_id"]
    container_name = message_data["container_name"]
    apkg_blob_name = message_data["apkg_blob_name"]
    media_prefix = message_data.get("media_prefix", "anki-media")
    max_cards = int(message_data.get("max_cards", 20000))
    max_media_mb = int(message_data.get("max_media_mb", 750))
    # Optional (additive, still v1): a positive chunk_size means the backend
    # speaks the chunked-callback contract, so fan the deck across several POSTs.
    # Absent -> one callback (a backend that predates chunking).
    raw_chunk = message_data.get("chunk_size")
    chunk_size = int(raw_chunk) if isinstance(raw_chunk, int) and not isinstance(raw_chunk, bool) and raw_chunk > 0 else None
    # Per-job callback (validated above) lets one worker serve prod + staging.
    callback_url = message_data.get("callback_url") or settings.callback_url

    blob_service = BlobServiceClient.from_connection_string(settings.azure_storage_connection_string)
    tmp = tempfile.NamedTemporaryFile(prefix="apkg_", suffix=".apkg", delete=False)
    apkg_path = tmp.name
    tmp.close()
    try:
        cc = blob_service.get_container_client(container_name)
        logger.info("downloading deck job_id=%s blob=%s", job_id, apkg_blob_name)
        with open(apkg_path, "wb") as fh:
            cc.get_blob_client(apkg_blob_name).download_blob().readinto(fh)

        sink = BlobMediaSink(cc, media_prefix, max_media_mb * _MB)
        try:
            result = render_apkg(apkg_path, sink, max_cards=max_cards)
        except Exception as exc:
            logger.exception("render failed job_id=%s", job_id)
            _post_callback(
                {
                    "schema_version": JOB_SCHEMA_VERSION,
                    "job_id": job_id,
                    "status": "failed",
                    "deck_name": None,
                    "cards": [],
                    "stats": {"total": 0, "subdecks": {}, "note_types": {}},
                    "summary": {"imported": 0, "degraded": {}, "skipped": 0},
                    "error": str(exc)[:500],
                },
                callback_url,
            )
            return
        logger.info(
            "rendered job_id=%s deck=%r imported=%d skipped=%d",
            job_id,
            result.deck_name,
            result.imported,
            result.skipped,
        )
        _post_result(job_id, result, callback_url, chunk_size)
    finally:
        try:
            os.unlink(apkg_path)
        except OSError:
            pass
        blob_service.close()


def _post_job_abandoned(message_data: dict[str, Any], attempts: int, exc: Exception) -> None:
    """Report a job the worker has given up on, so the deck fails now, not in 30 minutes."""
    # Re-validated because this runs OUTSIDE _process_job_sync: never POST the
    # shared secret to a callback_url that has not passed the host allowlist.
    if _validate_job(message_data) is not None:
        return
    job_id = message_data["job_id"]
    callback_url = message_data.get("callback_url") or settings.callback_url
    try:
        _post_callback(
            {
                "schema_version": JOB_SCHEMA_VERSION,
                "job_id": job_id,
                "status": "failed",
                "deck_name": None,
                "cards": [],
                "stats": {"total": 0, "subdecks": {}, "note_types": {}},
                "summary": {"imported": 0, "degraded": {}, "skipped": 0},
                "error": f"import abandoned after {attempts} attempt(s): {exc}"[:500],
            },
            callback_url,
        )
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

    async def _process_and_cleanup(self, queue_message: Any, message_data: dict[str, Any]) -> None:
        # Hold the lease for as long as the render actually takes. Without it a
        # deck slower than the window came back on the queue mid-run, was rendered
        # a second time alongside the run still in flight, and the run that
        # finished could not delete the message with its now-stale pop receipt.
        stop_renewing = asyncio.Event()
        renewer = asyncio.create_task(self._keep_message_visible(queue_message, stop_renewing))
        try:
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
                logger.error("message id=%s exceeded MAX_RETRIES=%d; deleting", queue_message.id, settings.max_retries)
                # Still leased across the abandon callback (it retries, so it is
                # not instant), then stopped before the delete: a renewal in
                # flight would invalidate the receipt we delete with.
                await asyncio.to_thread(_post_job_abandoned, message_data, dequeue_count, exc)
                await self._stop_renewer(stop_renewing, renewer)
                try:
                    await self.queue_client.delete_message(queue_message)
                except Exception:
                    logger.exception("failed to delete poisoned message id=%s", queue_message.id)
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
