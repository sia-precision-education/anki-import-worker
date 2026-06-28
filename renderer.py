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
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from urllib.parse import unquote

from anki.collection import (
    Collection,
    ImportAnkiPackageOptions,
    ImportAnkiPackageRequest,
)
from anki.consts import MODEL_CLOZE

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


def render_apkg(apkg_path: str, media_sink: MediaSink, *, max_cards: int = 20000) -> RenderResult:
    """Import a `.apkg` into a throwaway collection and render every card to HTML."""
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
            if fname in media_cache:
                return media_cache[fname]
            local = os.path.join(media_dir, fname)
            ref = media_sink(local, fname) if os.path.isfile(local) else None
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
