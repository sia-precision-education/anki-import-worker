# Copyright (C) 2026 SIA Precision Education — AGPL-3.0 (see LICENSE).
"""Worker configuration, read entirely from the environment.

Deliberately dependency-light (no pydantic) and self-contained: this worker
shares NO code with the SIA backend. Everything it needs to reach storage and
report results back is plain deployment config injected as env vars.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import urlparse


def _req(name: str) -> str:
    val = os.getenv(name)
    if not val:
        msg = f"Required environment variable {name} is not set"
        raise RuntimeError(msg)
    return val


@dataclass(frozen=True)
class WorkerSettings:
    # Storage (one connection string drives both the queue and blob clients).
    azure_storage_connection_string: str
    # Queue this worker polls for AnkiImportRequest jobs.
    anki_queue_name: str
    # DEFAULT destination for rendered cards, and the shared secret presented in
    # the X-Anki-Callback-Token header. A job may carry its own `callback_url`
    # (so one worker can serve several backends); this is the fallback when it
    # doesn't. These are the only things tying the worker to a specific SIA
    # deployment, and they are config, not code — it never imports from SIA.
    callback_url: str
    callback_secret: str
    # Knobs.
    max_concurrent_jobs: int = 2
    queue_poll_interval: int = 2
    # Must exceed the longest plausible deck render+upload, or Azure redelivers
    # the message mid-flight and two workers race the same deck.
    visibility_timeout_seconds: int = 900
    max_retries: int = 1
    log_level: str = "INFO"
    # Hosts this worker is permitted to POST results to. A job-supplied
    # `callback_url` is honoured ONLY if its host is in here — otherwise a forged
    # job could make the worker send the shared secret to an attacker's server.
    # Defaults to the host of `callback_url`.
    allowed_callback_hosts: tuple[str, ...] = ()

    @classmethod
    def from_env(cls) -> "WorkerSettings":
        callback_url = _req("ANKI_CALLBACK_URL")
        raw_hosts = os.getenv("ANKI_ALLOWED_CALLBACK_HOSTS", "")
        hosts = tuple(h.strip() for h in raw_hosts.split(",") if h.strip())
        return cls(
            azure_storage_connection_string=_req("AZURE_STORAGE_CONNECTION_STRING"),
            anki_queue_name=os.getenv("ANKI_QUEUE_NAME", "anki-requests"),
            callback_url=callback_url,
            callback_secret=_req("ANKI_CALLBACK_SECRET"),
            max_concurrent_jobs=int(os.getenv("MAX_CONCURRENT_JOBS", "2")),
            queue_poll_interval=int(os.getenv("QUEUE_POLL_INTERVAL", "2")),
            visibility_timeout_seconds=int(os.getenv("VISIBILITY_TIMEOUT_SECONDS", "900")),
            max_retries=int(os.getenv("MAX_RETRIES", "1")),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            allowed_callback_hosts=hosts or (urlparse(callback_url).netloc,),
        )
