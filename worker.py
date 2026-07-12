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
import hashlib
import json
import logging
import os
import signal
import sys
import tempfile
from typing import Any

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


def _result_payload(job_id: str, result: RenderResult) -> dict[str, Any]:
    if result.imported == 0:
        status = "failed"
    elif result.skipped > 0:
        status = "partial"
    else:
        status = "ok"
    return {
        "schema_version": JOB_SCHEMA_VERSION,
        "job_id": job_id,
        "status": status,
        "deck_name": result.deck_name,
        "cards": [
            {
                "front_html": c.front_html,
                "back_html": c.back_html,
                "css": c.css,
                "deck": c.deck,
                "note_type": c.note_type,
                "cloze": c.cloze,
                "tags": c.tags,
                "media": c.media,
            }
            for c in result.cards
        ],
        "summary": {"imported": result.imported, "degraded": result.degraded, "skipped": result.skipped},
        "error": None,
    }


def _post_callback(payload: dict[str, Any]) -> None:
    headers = {"X-Anki-Callback-Token": settings.callback_secret}
    with httpx.Client(timeout=httpx.Timeout(180.0)) as client:
        resp = client.post(settings.callback_url, json=payload, headers=headers)
        resp.raise_for_status()
    logger.info(
        "callback ok job_id=%s status=%s cards=%d",
        payload["job_id"],
        payload["status"],
        len(payload["cards"]),
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
            payload = _result_payload(job_id, result)
            logger.info(
                "rendered job_id=%s deck=%r imported=%d skipped=%d",
                job_id,
                result.deck_name,
                result.imported,
                result.skipped,
            )
        except Exception as exc:
            logger.exception("render failed job_id=%s", job_id)
            payload = {
                "schema_version": JOB_SCHEMA_VERSION,
                "job_id": job_id,
                "status": "failed",
                "deck_name": None,
                "cards": [],
                "summary": {"imported": 0, "degraded": {}, "skipped": 0},
                "error": str(exc)[:500],
            }
        _post_callback(payload)
    finally:
        try:
            os.unlink(apkg_path)
        except OSError:
            pass
        blob_service.close()


class AnkiWorker:
    def __init__(self) -> None:
        self.queue_client: Any = None
        self.running = False
        self.current_tasks: set[asyncio.Task] = set()

    async def setup(self) -> None:
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
            "anki-worker setup complete (concurrency=%d, visibility=%ds)",
            settings.max_concurrent_jobs,
            settings.visibility_timeout_seconds,
        )

    async def _process_and_cleanup(self, queue_message: Any, message_data: dict[str, Any]) -> None:
        try:
            await asyncio.to_thread(_process_job_sync, message_data)
            await self.queue_client.delete_message(queue_message)
            logger.info("deleted message id=%s", queue_message.id)
        except Exception:
            dequeue_count = queue_message.dequeue_count or 0
            logger.exception(
                "job failed for message id=%s (attempt=%d)", queue_message.id, dequeue_count + 1
            )
            # The callback already records render failures on the SIA side, so a
            # poisoned message past MAX_RETRIES is dropped rather than looped.
            if dequeue_count >= settings.max_retries:
                logger.error("message id=%s exceeded MAX_RETRIES=%d; deleting", queue_message.id, settings.max_retries)
                try:
                    await self.queue_client.delete_message(queue_message)
                except Exception:
                    logger.exception("failed to delete poisoned message id=%s", queue_message.id)

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
