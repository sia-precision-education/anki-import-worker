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

`open_apkg()` imports a deck into a throwaway Collection and reads the exact
HTML Anki itself would render (`Card.render_output()`), one card at a time,
which is what gets us faithful templates, cloze, and every collection format
(incl. the modern zstd-compressed `collection.anki21b`) without re-implementing
Anki. `render_apkg()` renders the whole deck at once, for callers that can hold it.

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
from collections import Counter
from collections.abc import Callable, Generator, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from html import unescape as html_unescape
from typing import Any
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
# only ever rewrite those, never absolute/scheme/data URLs. Matched as a whole
# TAG rather than just the attribute so an unresolvable ref can be replaced
# outright, not merely blanked.
_IMG_TAG_RE = re.compile(r"<img\b[^>]*>", re.IGNORECASE)
_SRC_ATTR_RE = re.compile(r'(\bsrc\s*=\s*)(["\'])(.*?)\2', re.IGNORECASE | re.DOTALL)

# Stands in for an image that could not be stored. A span, not a marked <img>:
# an <img> with no src renders 0x0, and generated content on a replaced element
# is unreliable, so there would be nothing for the client to make visible.
_MISSING_MEDIA_HTML = '<span data-anki-missing="1">Image unavailable</span>'
# `card.question()/answer()` keep audio out of the HTML and leave a placeholder
# token; the filenames come from the AV-tag lists instead.
_AV_PLACEHOLDER_RE = re.compile(r"\[anki:play:[qa]:\d+\]")
# A script element as a browser parses it: from `<script` to its end tag or, left
# open, to the end of the document. The name must end at whitespace, `/` or `>`.
_SCRIPT_RE = re.compile(r"<script(?=[\s/>])[^>]*>.*?(?:</script(?=[\s/>])[^>]*>|\Z)", re.IGNORECASE | re.DOTALL)


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


def remove_scripts(html: str) -> str:
    """The HTML without its `<script>` elements, contents included."""
    return _SCRIPT_RE.sub("", html)


def _rewrite_img_media(
    html: str, store: Callable[[str], "str | None"], used: list[str]
) -> tuple[str, int]:
    """Rewrite bare `<img src="file">` refs to whatever the sink stored them as.

    Returns the HTML and the count that could NOT be resolved. An unresolved ref
    used to be left verbatim, which the backend does not rewrite (it only touches
    the media prefix) and the client's sanitizer then strips for not matching the
    media proxy — leaving a src-less <img> at 0x0, so a card whose front WAS the
    image rendered as a blank face with nothing counted anywhere.
    """
    missing = 0

    def repl(match: re.Match[str]) -> str:
        nonlocal missing
        tag = match.group(0)
        attr = _SRC_ATTR_RE.search(tag)
        if attr is None:
            return tag
        src = attr.group(3)
        if not src or "://" in src or src.startswith(("/", "data:", "#")):
            return tag
        # Entity-decoded as well as percent-decoded: a field carrying
        # `fig1&amp;2.png` names a file that is `fig1&2.png` on disk, and
        # unquote() alone leaves the entity intact so the lookup misses.
        ref = store(html_unescape(unquote(src)))
        if not ref:
            missing += 1
            return _MISSING_MEDIA_HTML
        used.append(ref)
        rewritten = f"{attr.group(1)}{attr.group(2)}{ref}{attr.group(2)}"
        return tag[: attr.start()] + rewritten + tag[attr.end() :]

    return _IMG_TAG_RE.sub(repl, html), missing


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


class RenderingDeck:
    """An imported deck that renders its cards one at a time, as it is iterated.

    It holds no rendered card itself. Rendered whole, a 35,000-card AnKing deck is
    about 2.5 billion characters of HTML, more than the worker's memory, so the
    worker renders and sends one chunk at a time. Everything known before the first
    card is rendered is set on construction; only `degraded` grows while iterating.
    """

    def __init__(self, col: Collection, media_sink: MediaSink, max_cards: int, *, strip_scripts: bool = False) -> None:
        self._col = col
        self._sink = media_sink
        self._strip_scripts = strip_scripts
        self._media_dir = col.media.dir()
        self._media_cache: dict[str, str | None] = {}
        card_ids = col.find_cards("")  # "" matches all cards
        self._card_ids = list(card_ids[:max_cards])
        self.skipped = len(card_ids) - len(self._card_ids)
        if self.skipped:
            logger.warning("max_cards=%d reached; %d cards skipped", max_cards, self.skipped)
        self.degraded = {"audio": 0, "media": 0}

        # Each card's deck and note type from one query, read by both the counts
        # below and the rendered card, so the two cannot disagree.
        self._placement: dict[int, tuple[int, int]] = {
            cid: (did, mid)
            for cid, did, mid in col.db.all(
                "SELECT c.id, CASE WHEN c.odid != 0 THEN c.odid ELSE c.did END, n.mid"
                " FROM cards c JOIN notes n ON n.id = c.nid"
            )
        }
        self._deck_names: dict[int, str] = {}
        self.deck_counts: Counter[str] = Counter()
        self.note_type_counts: Counter[str] = Counter()
        for cid in self._card_ids:
            deck, nt = self._placed(cid)
            self.deck_counts[deck] += 1
            self.note_type_counts[nt.get("name", "") or ""] += 1

    @property
    def total(self) -> int:
        """Cards that iterating renders: the whole deck, up to `max_cards`."""
        return len(self._card_ids)

    @property
    def deck_name(self) -> str:
        """The deck holding the most cards; the first one seen wins a tie."""
        if not self.deck_counts:
            return "Imported deck"
        return max(self.deck_counts, key=lambda k: self.deck_counts[k])

    def __iter__(self) -> Iterator[RenderedCard]:
        for cid in self._card_ids:
            yield self._render(cid)

    def _placed(self, cid: int) -> tuple[str, dict[str, Any]]:
        """The card's deck name and note type (the current deck, for a filtered one)."""
        did, mid = self._placement[cid]
        if did not in self._deck_names:
            self._deck_names[did] = self._col.decks.name(did)
        return self._deck_names[did], self._col.models.get(mid) or {}

    def _store(self, fname: str) -> str | None:
        """Resolve one media reference inside the collection's media dir.

        The single gate for both callers, because the name is attacker-
        controlled in each: `<img src>` in the card HTML and the AV tag's
        filename. `os.path.join` does NOT contain a traversal — an absolute
        second argument replaces the first outright — so an unchecked name
        let a crafted deck read any file the worker could and hand it to the
        sink, which uploads it to the uploader's own container.
        """
        if fname in self._media_cache:
            return self._media_cache[fname]
        ref = None
        safe = safe_media_name(fname)
        if safe is not None:
            local = os.path.join(self._media_dir, safe)
            if within_dir(self._media_dir, local) and os.path.isfile(local):
                ref = self._sink(local, safe)
        elif fname:
            logger.warning("rejected media reference outside the media dir: %r", fname)
        self._media_cache[fname] = ref
        return ref

    def _render(self, cid: int) -> RenderedCard:
        card = self._col.get_card(cid)
        note = card.note()
        deck, nt = self._placed(cid)
        out = card.render_output()  # bare HTML (no <style>); CSS carried separately
        front, back = out.question_text, out.answer_text
        if self._strip_scripts:
            # Before media is resolved: an <img> written inside a script is not uploaded.
            front, back = remove_scripts(front), remove_scripts(back)

        used: list[str] = []
        front, fm = _rewrite_img_media(front, self._store, used)
        back, bm = _rewrite_img_media(back, self._store, used)
        front, fa = _attach_audio(front, card.question_av_tags(), self._store, used)
        back, ba = _attach_audio(back, card.answer_av_tags(), self._store, used)
        self.degraded["audio"] += fa + ba
        self.degraded["media"] += fm + bm

        return RenderedCard(
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


@contextmanager
def open_apkg(
    apkg_path: str,
    media_sink: MediaSink,
    *,
    max_cards: int = 20000,
    max_uncompressed_bytes: int = _MAX_UNCOMPRESSED_BYTES,
    max_entries: int = _MAX_ARCHIVE_ENTRIES,
    strip_scripts: bool = False,
) -> Generator[RenderingDeck, None, None]:
    """Import a `.apkg` into a throwaway collection, removed when the block exits.

    With `strip_scripts`, the rendered HTML has no `<script>` elements.
    """
    _guard_archive_bounds(
        apkg_path, max_uncompressed_bytes=max_uncompressed_bytes, max_entries=max_entries
    )
    workdir = tempfile.mkdtemp(prefix="anki_render_")
    col_path = os.path.join(workdir, "collection.anki2")  # absent path -> created fresh
    col = Collection(col_path)
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
        yield RenderingDeck(col, media_sink, max_cards, strip_scripts=strip_scripts)
    finally:
        col.close()  # releases the Rust backend's exclusive DB lock
        shutil.rmtree(workdir, ignore_errors=True)


def render_apkg(
    apkg_path: str,
    media_sink: MediaSink,
    *,
    max_cards: int = 20000,
    max_uncompressed_bytes: int = _MAX_UNCOMPRESSED_BYTES,
    max_entries: int = _MAX_ARCHIVE_ENTRIES,
) -> RenderResult:
    """Import a `.apkg` and render every card to HTML, holding the whole deck.

    For the CLI. The worker iterates `open_apkg` instead: a large deck held whole
    outgrows its container.
    """
    with open_apkg(
        apkg_path,
        media_sink,
        max_cards=max_cards,
        max_uncompressed_bytes=max_uncompressed_bytes,
        max_entries=max_entries,
    ) as deck:
        cards = list(deck)
        return RenderResult(
            deck_name=deck.deck_name,
            cards=cards,
            imported=len(cards),
            degraded=deck.degraded,
            skipped=deck.skipped,
        )
