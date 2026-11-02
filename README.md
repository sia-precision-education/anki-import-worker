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
Azure Queue job ──▶ read .apkg in blob (ranges) ──▶ anki imports the cards only
                                                         │
                 upload media straight from the .apkg ◀──┤
                                                         ▼
                      render each card ──▶ POST rendered cards to SIA callback
```

The deck is never downloaded whole, and Anki never unpacks its media: Anki imports
a copy of the package with an empty media list, and the worker reads each media
file from the package itself (one pass, in archive order) and uploads it. The
worker's disk holds the card database and a file or two, so its disk use does not
grow with the deck. A media file is held to the size the package declares for it
(and to 256 MB), so a file that unpacks past that is left out rather than filling
the disk; the card database is held to 1 GB before Anki unpacks it.

### Job message (queue → worker)

```json
{
  "schema_version": 1,
  "job_id": "<opaque correlation id, treated as a string>",
  "container_name": "<azure blob container to read the deck from / write media to>",
  "apkg_blob_name": "documents/original/<hash>.apkg",
  "media_prefix": "anki-media",
  "max_cards": 20000,
  "max_media_mb": 12288,
  "callback_url": "https://<your-backend>/api/v1/internal/anki/complete",
  "strip_scripts": true
}
```

`strip_scripts` is **optional**: `true` removes `<script>…</script>` elements,
contents included, from the card HTML before its media is resolved. A match that
holds a second `<script` opening is left as it is, and so is a `<script>` with no
end tag: the `<script` may be text in a comment, an attribute or a `<style>`, and
removing that match would delete card text. Send it when your
viewer never runs card scripts; in a large shared deck such as AnKing they are
most of the HTML. Absent (or anything but `true`), the HTML is Anki's own.

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

With a positive `chunk_size` in the job, the deck arrives across several
callbacks of at most that many cards, each rendered and sent before the next is
rendered. Each also carries `chunk_index`, `chunk_count`, `start_index` and the
deck-wide `stats` (`total`, `subdecks`, `note_types`). In a chunked callback a
note type's `css` is on its first card in that callback and empty on the rest
(look it up by `note_type`), and `summary.degraded` is a running total that is
complete on the last chunk.

Media is uploaded to `<container>/<media_prefix>/<hash>.<ext>` and referenced in
the card HTML by that path; SIA rewrites those into signed media-proxy URLs. A
blob already at that name is left as it is (the name is a hash of the bytes).

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
| `MAX_RETRIES` | no (`3`) | Dequeue attempts before dropping (`dequeue_count` is 1 on first delivery, so 1 = no retries) |
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
