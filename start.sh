#!/usr/bin/env bash
# Local dev entrypoint. Expects a .env with AZURE_STORAGE_CONNECTION_STRING,
# ANKI_CALLBACK_URL and ANKI_CALLBACK_SECRET (see README).
set -euo pipefail
cd "$(dirname "$0")"
exec python -u worker.py
