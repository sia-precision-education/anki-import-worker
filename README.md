# anki-import-worker

A standalone service that imports an Anki **`.apkg`** deck and renders every
card to HTML (templates, cloze, images, media), so it can be turned into
flashcards. It is built on the official [`anki`](https://pypi.org/project/anki/)
library to get Anki-faithful rendering of every collection format, including the
modern zstd-compressed `collection.anki21b`.

## License — AGPL-3.0

This program is licensed under the **GNU Affero General Public License v3.0 or
later** (see [`LICENSE`](./LICENSE)), because it links the `anki` library, which
is AGPL. This repository **is** the Corresponding Source that AGPL §13 requires
us to offer to users of the network service it powers.


## What it does

```
Azure Queue job ──▶ download .apkg from blob ──▶ render every card (anki)
                                                      │
                          upload referenced media ◀───┤
                                                      ▼
                              POST rendered cards to SIA callback
```

### Job message (queue → worker)

```json
{
  "schema_version": 1,
  "job_id": "<opaque correlation id, treated as a string>",
  "container_name": "<azure blob container to read the deck from / write media to>",
  "apkg_blob_name": "documents/original/<hash>.apkg",
  "media_prefix": "anki-media",
  "max_cards": 20000,
  "max_media_mb": 750,
  "callback_url": "https://<your-backend>/api/v1/internal/anki/complete"
}
```

`callback_url` is **optional**: supply it and the worker posts that job's results
there (so one worker can serve several backends); omit it and the worker uses its
configured `ANKI_CALLBACK_URL`. Either way the host must be allowlisted — see
`ANKI_ALLOWED_CALLBACK_HOSTS` below.

### Callback payload (worker → SIA), `POST $ANKI_CALLBACK_URL`

Header `X-Anki-Callback-Token: $ANKI_CALLBACK_SECRET`.

```json
{
  "schema_version": 1,
  "job_id": "...",
  "status": "ok | partial | failed",
  "deck_name": "Cardiology",
  "cards": [
    { "front_html": "...", "back_html": "...", "css": "...",
      "deck": "Cardiology::ECG", "note_type": "Basic", "cloze": false,
      "tags": ["step1"], "media": ["anki-media/<hash>.jpg"] }
  ],
  "summary": { "imported": 1234, "degraded": { "audio": 3 }, "skipped": 0 },
  "error": null
}
```

Media is uploaded to `<container>/<media_prefix>/<hash>.<ext>` and referenced in
the card HTML by that path; SIA rewrites those into signed media-proxy URLs.

## Stable interface (v1)

The job and callback shapes above are a **versioned, stable contract**, not an
internal detail. Both messages carry `schema_version` (currently `1`): the worker
**rejects** a job whose `schema_version` it doesn't speak (rather than
mis-processing it) and validates that the required fields — `job_id`,
`container_name`, `apkg_blob_name` — are present. Within a major version, changes
are additive only (new optional fields); a breaking change bumps the major.

The renderer is not SIA-specific — any caller can drive it:
- **`cli.py`** is the reference non-SIA consumer: `python cli.py deck.apkg`
  renders a deck with no Azure, queue, or callback involved at all.
- The queue/callback path is a generic Azure-blob + HTTP contract; nothing in it
  is specific to SIA beyond the callback URL and secret you configure.

## Configuration (environment)

| Var | Required | Purpose |
| --- | --- | --- |
| `AZURE_STORAGE_CONNECTION_STRING` | yes | Queue + blob access |
| `ANKI_CALLBACK_URL` | yes | SIA endpoint to POST rendered cards to |
| `ANKI_CALLBACK_SECRET` | yes | Shared secret for the callback header |
| `ANKI_QUEUE_NAME` | no (`anki-requests`) | Queue to poll |
| `ANKI_ALLOWED_CALLBACK_HOSTS` | no | Comma-separated hosts a job-supplied `callback_url` may target. Defaults to the host of `ANKI_CALLBACK_URL`. A job naming any other host is rejected, so a forged message can't make the worker send the shared secret elsewhere. |
| `MAX_CONCURRENT_JOBS` | no (`2`) | In-flight decks |
| `VISIBILITY_TIMEOUT_SECONDS` | no (`900`) | Must exceed the longest render |
| `MAX_RETRIES` | no (`1`) | Dequeue attempts before dropping |
| `LOG_LEVEL` | no (`INFO`) | |

## Local development

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

# Render a deck with NO Azure/SIA — proves fidelity, writes ./out/preview.html
python cli.py ~/Downloads/SomeDeck.apkg

# Run the worker against a real queue (needs a .env with the vars above)
./start.sh
```

> The `anki` 26.5 wheel needs **glibc ≥ 2.35** (Debian bookworm / Ubuntu 22.04+)
> and CPython 3.10–3.13. On older distros, pin an older `anki` line.

## Deploy

Build the image and ship it as its own container (see `scripts/deploy.sh`).
Provision the `anki-requests` queue (the worker also creates it on startup) and
set the env vars above on the container.
