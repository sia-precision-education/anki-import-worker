# Copyright (C) 2026 SIA Precision Education
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version. See the LICENSE file for the full text.
#
# This module links the `anki` package (AGPL-3.0). That is the entire reason
# this worker is a separate, self-contained, AGPL-licensed program: it talks
# to the proprietary SIA backend ONLY over a queue (job in) and an HTTP
# callback (rendered cards out), exchanging plain data — never by importing
# proprietary code or being imported by it.
"""Pure `.apkg` -> rendered-card transform built on the official `anki` library.

`render_apkg()` imports a deck into a throwaway Collection and reads the exact
HTML Anki itself would render (`Card.render_output()`), which is what gets us
faithful templates, cloze, and every collection format (incl. the modern
zstd-compressed `collection.anki21b`) without re-implementing Anki.

Media handling is delegated to a caller-supplied `MediaSink` so this module
stays free of any storage/SIA specifics: the CLI copies media to a folder, the
worker uploads it to blob storage. The sink decides the stored reference that
gets substituted into the card HTML (and may enforce size caps / dedup).
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import tempfile
import zipfile
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from urllib.parse import unquote

from anki.collection import (
    Collection,
    ImportAnkiPackageOptions,
    ImportAnkiPackageRequest,
)
from anki.consts import MODEL_CLOZE

from media_paths import safe_media_name, within_dir

logger = logging.getLogger(__name__)

# (absolute_local_path, original_filename) -> stored reference to put in the
# HTML (e.g. "anki-media/<hash>.jpg"), or None if it couldn't be stored.
MediaSink = Callable[[str, str], "str | None"]

# Anki cards reference media as bare local filenames (`<img src="x.jpg">`); we
# only ever rewrite those, never absolute/scheme/data URLs.
_IMG_SRC_RE = re.compile(r'(<img\b[^>]*?\bsrc\s*=\s*)(["\'])(.*?)\2', re.IGNORECASE | re.DOTALL)
# `card.question()/answer()` keep audio out of the HTML and leave a placeholder
# token; the filenames come from the AV-tag lists instead.
_AV_PLACEHOLDER_RE = re.compile(r"\[anki:play:[qa]:\d+\]")


@dataclass
class RenderedCard:
    front_html: str
    back_html: str
    css: str
    deck: str
    note_type: str
    cloze: bool
    tags: list[str]
    media: list[str] = field(default_factory=list)
    # Stable natural key: the note's guid (survives export/re-import — it's how
    # Anki itself dedupes on sync) plus the card's template ordinal. Lets the
    # backend derive a deterministic flashcard id so a redelivered chunk (the
    # queue re-renders the whole deck on any retry) dedupes instead of inserting
    # duplicates. Independent of render order.
    uid: str = ""


@dataclass
class RenderResult:
    deck_name: str
    cards: list[RenderedCard]
    imported: int
    degraded: dict[str, int]
    skipped: int


def _rewrite_img_media(html: str, store: Callable[[str], "str | None"], used: list[str]) -> str:
    """Rewrite bare `<img src="file">` refs to whatever the sink stored them as."""

    def repl(match: re.Match[str]) -> str:
        prefix, quote, src = match.group(1), match.group(2), match.group(3)
        if not src or "://" in src or src.startswith(("/", "data:", "#")):
            return match.group(0)
        ref = store(unquote(src))
        if not ref:
            return match.group(0)
        used.append(ref)
        return f"{prefix}{quote}{ref}{quote}"

    return _IMG_SRC_RE.sub(repl, html)


def _attach_audio(
    html: str,
    av_tags: Sequence[object],
    store: Callable[[str], "str | None"],
    used: list[str],
) -> tuple[str, int]:
    """Strip the inline play placeholders and append plain <audio> elements.

    v1 audio is "degrade, don't break": no Anki play UI, just a native player.
    """
    html = _AV_PLACEHOLDER_RE.sub("", html)
    players: list[str] = []
    for tag in av_tags or []:
        fname = getattr(tag, "filename", None)  # TTSTag has no .filename
        if not fname:
            continue
        ref = store(fname)
        if not ref:
            continue
        used.append(ref)
        players.append(f'<audio controls preload="none" src="{ref}"></audio>')
    if players:
        html = f'{html}<div class="anki-audio">{"".join(players)}</div>'
    return html, len(players)


# Decompression bounds. An .apkg is a zip that anki's importer unpacks to disk in
# full before we ever see a card, so a small malicious archive could exhaust the
# worker's disk (zip bomb). These bound the *decompressed* deck, read cheaply from
# the central directory. The ceiling sits well above any real deck (media alone is
# capped at 750 MB) while catching bombs, which advertise gigabytes-to-petabytes.
_MAX_UNCOMPRESSED_BYTES = 2 * 1024 * 1024 * 1024  # 2 GiB
_MAX_ARCHIVE_ENTRIES = 100_000


class ApkgTooLargeError(Exception):
    """The .apkg's decompressed size or entry count exceeds the safety bounds."""


def _guard_archive_bounds(
    apkg_path: str,
    *,
    max_uncompressed_bytes: int = _MAX_UNCOMPRESSED_BYTES,
    max_entries: int = _MAX_ARCHIVE_ENTRIES,
) -> None:
    """Reject an oversized/zip-bomb deck BEFORE anki extracts it to disk.

    Reads only the zip central directory (advertised uncompressed sizes + entry
    count) — no decompression — so it is cheap and fails fast. Raised errors
    propagate to the worker's render try/except, which reports a clean import
    failure rather than stranding the deck.
    """
    try:
        with zipfile.ZipFile(apkg_path) as zf:
            infos = zf.infolist()
    except zipfile.BadZipFile as exc:
        raise ApkgTooLargeError(f"not a valid .apkg archive: {exc}") from exc

    if len(infos) > max_entries:
        raise ApkgTooLargeError(f"archive has {len(infos)} entries (max {max_entries})")

    total = 0
    for info in infos:
        total += info.file_size
        if total > max_uncompressed_bytes:
            raise ApkgTooLargeError(
                f"decompressed size exceeds {max_uncompressed_bytes // (1024 * 1024)} MB"
            )


def render_apkg(
    apkg_path: str,
    media_sink: MediaSink,
    *,
    max_cards: int = 20000,
    max_uncompressed_bytes: int = _MAX_UNCOMPRESSED_BYTES,
    max_entries: int = _MAX_ARCHIVE_ENTRIES,
) -> RenderResult:
    """Import a `.apkg` into a throwaway collection and render every card to HTML."""
    _guard_archive_bounds(
        apkg_path, max_uncompressed_bytes=max_uncompressed_bytes, max_entries=max_entries
    )
    workdir = tempfile.mkdtemp(prefix="anki_render_")
    col_path = os.path.join(workdir, "collection.anki2")  # absent path -> created fresh
    col = Collection(col_path)
    cards: list[RenderedCard] = []
    deck_counts: dict[str, int] = {}
    degraded = {"audio": 0}
    total_cards = 0

    try:
        col.import_anki_package(
            ImportAnkiPackageRequest(
                package_path=str(apkg_path),
                options=ImportAnkiPackageOptions(
                    merge_notetypes=False,
                    with_scheduling=False,
                    with_deck_configs=False,
                ),
            )
        )

        media_dir = col.media.dir()
        media_cache: dict[str, "str | None"] = {}

        def store(fname: str) -> "str | None":
            """Resolve one media reference inside the collection's media dir.

            The single gate for both callers, because the name is attacker-
            controlled in each: `<img src>` in the card HTML and the AV tag's
            filename. `os.path.join` does NOT contain a traversal — an absolute
            second argument replaces the first outright — so an unchecked name
            let a crafted deck read any file the worker could and hand it to the
            sink, which uploads it to the uploader's own container.
            """
            if fname in media_cache:
                return media_cache[fname]
            ref = None
            safe = safe_media_name(fname)
            if safe is not None:
                local = os.path.join(media_dir, safe)
                if within_dir(media_dir, local) and os.path.isfile(local):
                    ref = media_sink(local, safe)
            elif fname:
                logger.warning("rejected media reference outside the media dir: %r", fname)
            media_cache[fname] = ref
            return ref

        card_ids = col.find_cards("")  # "" matches all cards
        total_cards = len(card_ids)

        for cid in card_ids:
            if len(cards) >= max_cards:
                logger.warning("max_cards=%d reached; %d cards skipped", max_cards, total_cards - max_cards)
                break
            card = col.get_card(cid)
            note = card.note()
            nt = card.note_type()
            out = card.render_output()  # bare HTML (no <style>); CSS carried separately

            used: list[str] = []
            front = _rewrite_img_media(out.question_text, store, used)
            back = _rewrite_img_media(out.answer_text, store, used)
            front, fa = _attach_audio(front, card.question_av_tags(), store, used)
            back, ba = _attach_audio(back, card.answer_av_tags(), store, used)
            degraded["audio"] += fa + ba

            deck = col.decks.name(card.current_deck_id())
            deck_counts[deck] = deck_counts.get(deck, 0) + 1

            cards.append(
                RenderedCard(
                    front_html=front,
                    back_html=back,
                    css=nt.get("css", "") or "",
                    deck=deck,
                    note_type=nt.get("name", "") or "",
                    cloze=nt.get("type") == MODEL_CLOZE,
                    tags=list(note.tags),
                    media=used,
                    uid=f"{note.guid}:{card.ord}",
                )
            )
    finally:
        col.close()  # releases the Rust backend's exclusive DB lock
        shutil.rmtree(workdir, ignore_errors=True)

    deck_name = max(deck_counts, key=lambda k: deck_counts[k]) if deck_counts else "Imported deck"
    return RenderResult(
        deck_name=deck_name,
        cards=cards,
        imported=len(cards),
        degraded=degraded,
        skipped=max(0, total_cards - len(cards)),
    )
