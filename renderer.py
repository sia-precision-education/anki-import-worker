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

Anki imports only the deck's cards: it gets a copy of the package with an empty
media list, and the media is read from the package itself, one file at a time
(`PackageMedia`). So the disk holds the card database and a file or two, however
large the deck's media, and the package can be read where it lies (the worker
reads it from blob storage in ranges) instead of being downloaded first.

Media handling is delegated to a caller-supplied `MediaSink` so this module
stays free of any storage/SIA specifics: the CLI copies media to a folder, the
worker uploads it to blob storage. The sink decides the stored reference that
gets substituted into the card HTML (and may enforce size caps / dedup).
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import shutil
import tempfile
import unicodedata
import zipfile
import zlib
from collections import Counter
from collections.abc import Callable, Generator, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from html import unescape as html_unescape
from typing import Any, BinaryIO
from urllib.parse import unquote

import zstandard
from anki.collection import (
    Collection,
    ImportAnkiPackageOptions,
    ImportAnkiPackageRequest,
)
from anki.consts import MODEL_CLOZE
from anki.import_export_pb2 import MediaEntries
from google.protobuf.message import DecodeError

from media_paths import safe_media_name

logger = logging.getLogger(__name__)

# (absolute_local_path, original_filename) -> stored reference to put in the
# HTML (e.g. "anki-media/<hash>.jpg"), or None if it couldn't be stored. A sink
# with a size cap may also define `has_room(size) -> bool`: a file it has no room
# for is then not read from the package at all.
MediaSink = Callable[[str, str], "str | None"]

# Opens the package for reading, given how much to read ahead: a seekable binary
# stream. A path is opened as a local file; the worker passes one that reads blob ranges.
PackageOpener = Callable[[int], BinaryIO]

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
# A script element from `<script` to its end tag; the name must end at whitespace,
# `/` or `>`. Removed only when unambiguous (see `remove_scripts`): a script left in
# costs bytes, since the viewers never run one, while a wrong match deletes card text.
_SCRIPT_RE = re.compile(r"<script(?=[\s/>])[^>]*>.*?</script(?=[\s/>])[^>]*>", re.IGNORECASE | re.DOTALL)
_SCRIPT_OPEN_RE = re.compile(r"<script(?=[\s/>])", re.IGNORECASE)


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
    """The HTML without its unambiguous `<script>` elements, contents included.

    A match holding a second `<script` opening is kept whole: the first was text in a
    comment, an attribute or a `<style>`, or the script nests one, so the match may
    span card text. A script with no end tag is kept too.
    """
    return _SCRIPT_RE.sub(_drop_unambiguous_script, html)


def _drop_unambiguous_script(match: re.Match[str]) -> str:
    nested = _SCRIPT_OPEN_RE.search(match.string, match.start() + 1, match.end())
    return match.group(0) if nested else ""


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


# Package bounds. Anki unpacks only the card database (the collection) to disk, so that
# is what is bounded before import; media never reaches disk whole (see `PackageMedia`).
_MAX_COLLECTION_BYTES = 1024 * 1024 * 1024  # 1 GiB; AnKing's is about 100 MB
_MAX_ARCHIVE_ENTRIES = 100_000
# One media file, decoded. Larger ones are left out (the card shows "Image unavailable").
_MAX_MEDIA_FILE_BYTES = 256 * 1024 * 1024
# The media list itself: about 50 bytes a file, so 100,000 files fit many times over.
_MAX_MEDIA_LIST_BYTES = 64 * 1024 * 1024
# The card database, under the names anki looks for (anki2, anki21, anki21b).
_COLLECTION_ENTRY_RE = re.compile(r"collection\.anki2(?:1b?)?")
_GUARD_READ_BYTES = 1024 * 1024
# Read-ahead: one pass over every file takes large windows; a file looked up while
# cards render takes a small one, so it fetches little beyond itself.
SEQUENTIAL_READ_BYTES = 8 * 1024 * 1024
RANDOM_READ_BYTES = 256 * 1024
# What a modern package's media list is when it lists nothing.
_EMPTY_MEDIA_LIST = zstandard.ZstdCompressor().compress(MediaEntries().SerializeToString())


class ApkgTooLargeError(Exception):
    """The package is not a valid .apkg, or its card database exceeds the bounds."""


def _size_label(n: int) -> str:
    return f"{n / 1024**3:g} GB" if n >= 1024**3 else f"{n // 1024**2} MB"


def _is_zstd_package(zf: zipfile.ZipFile) -> bool:
    """Whether anki reads this package's entries as zstd: `meta` version 3 or later.

    `meta` is PackageMetadata{version = 1}, so a tag byte 0x08, then the version.
    Older packages (version 1-2, or no `meta`) store their entries as-is.
    """
    try:
        with zf.open("meta") as fh:
            raw = fh.read(2)
    except KeyError:
        return False
    return len(raw) == 2 and raw[0] == 0x08 and raw[1] >= 3


def _decoded(zf: zipfile.ZipFile, info: zipfile.ZipInfo, zstd: bool) -> BinaryIO:
    """An entry's bytes as anki reads them: zstd-decoded in a modern package."""
    raw = zf.open(info)
    if not zstd:
        return raw
    return zstandard.ZstdDecompressor().stream_reader(raw, read_across_frames=True, closefd=True)


def _guard_archive_bounds(
    zf: zipfile.ZipFile,
    *,
    zstd: bool,
    max_collection_bytes: int = _MAX_COLLECTION_BYTES,
    max_entries: int = _MAX_ARCHIVE_ENTRIES,
) -> None:
    """Reject a package before anki unpacks its card database, if that would be too large.

    Counts what each collection entry really unpacks to, a block at a time, writing
    nothing: the zip's own decompression (its CRC check catches an understated size),
    then the zstd anki decodes in a modern package. The zip index alone gives a modern
    package's compressed sizes, which let a 60 KB deck unpack to gigabytes. Raised
    errors reach the worker's render try/except, which reports a clean import failure.
    """
    infos = zf.infolist()
    if len(infos) > max_entries:
        raise ApkgTooLargeError(f"archive has {len(infos)} entries (max {max_entries})")
    too_large = ApkgTooLargeError(f"the deck's cards unpack to more than {_size_label(max_collection_bytes)}")
    try:
        for info in infos:
            if not _COLLECTION_ENTRY_RE.fullmatch(info.filename):
                continue
            total = 0
            with _decoded(zf, info, zstd and info.filename.endswith("b")) as source:
                while block := source.read(_GUARD_READ_BYTES):
                    total += len(block)
                    if total > max_collection_bytes:
                        raise too_large
    except (zipfile.BadZipFile, zlib.error, zstandard.ZstdError, EOFError) as exc:
        raise ApkgTooLargeError(f"not a valid .apkg archive: {exc}") from exc


@dataclass(frozen=True)
class _MediaEntry:
    name: str
    info: zipfile.ZipInfo
    # What the package says the file decodes to; reading stops past it.
    size: int


def _read_media_list(zf: zipfile.ZipFile, zstd: bool) -> list[_MediaEntry]:
    """The package's media files, in the order they sit in the archive.

    A modern package lists them in a zstd `MediaEntries` protobuf, each stored under
    its index (or `legacy_zip_filename`); an older one in JSON, `{"0": "name.jpg"}`.
    Names that are not bare filenames, and entries the archive lacks, are left out.
    """
    try:
        info = zf.getinfo("media")
    except KeyError:
        return []
    try:
        buf = io.BytesIO()
        with _decoded(zf, info, zstd) as fh:
            while (block := fh.read(_GUARD_READ_BYTES)) and buf.tell() <= _MAX_MEDIA_LIST_BYTES:
                buf.write(block)
        data = buf.getvalue()
        if len(data) > _MAX_MEDIA_LIST_BYTES:
            raise ApkgTooLargeError(f"the media list is larger than {_size_label(_MAX_MEDIA_LIST_BYTES)}")
        if zstd:
            listed = [
                (e.name, str(e.legacy_zip_filename) if e.HasField("legacy_zip_filename") else str(i), e.size)
                for i, e in enumerate(MediaEntries.FromString(data).entries)
            ]
        else:
            mapping = json.loads(data or b"{}")
            if not isinstance(mapping, dict):
                raise ApkgTooLargeError("not a valid .apkg archive: the media list is not a mapping")
            listed = [(name, str(member), None) for member, name in mapping.items() if isinstance(name, str)]
    except (zipfile.BadZipFile, zlib.error, zstandard.ZstdError, EOFError, DecodeError, ValueError) as exc:
        raise ApkgTooLargeError(f"not a valid .apkg archive: the media list is unreadable ({exc})") from exc

    entries: dict[str, _MediaEntry] = {}
    rejected = 0
    for name, member, size in listed:
        if safe_media_name(name) is None:
            rejected += 1
            continue
        try:
            member_info = zf.getinfo(member)
        except KeyError:
            continue
        # An older package's file is stored as-is, so the zip's own size is what it holds.
        entries.setdefault(name, _MediaEntry(name, member_info, member_info.file_size if size is None else size))
    if rejected:
        logger.warning("media list names %d file(s) that are not bare filenames; left out", rejected)
    return sorted(entries.values(), key=lambda e: e.info.header_offset)


class _MediaFileError(Exception):
    """One media file could not be read; the cards that show it degrade, the deck does not."""


@dataclass
class MediaFile:
    """One media file read from the package: in memory, or in a temp file the caller removes."""

    name: str
    size: int
    data: bytes | None = None
    path: str | None = None

    def discard(self) -> None:
        if self.path is not None:
            try:
                os.unlink(self.path)
            except OSError:
                pass
            self.path = None


class PackageMedia:
    """The deck's media, read from the package by name; anki never unpacks it.

    Each file is decoded a block at a time and held to the size the package declares
    (and to `_MAX_MEDIA_FILE_BYTES`), so a file that unpacks past its declared size is
    refused before it fills the disk. A file that cannot be read leaves only its own
    cards degraded. Storage errors from the package's stream are not caught here: they
    fail the job, so the queue retries it rather than the deck losing its images.
    Not thread-safe: the worker reads media from the job's thread only.
    """

    def __init__(
        self, zf: zipfile.ZipFile, zstd: bool, entries: list[_MediaEntry], opener: PackageOpener, scratch_dir: str
    ) -> None:
        self._zf = zf
        self._zstd = zstd
        self._entries = entries
        self._opener = opener
        self.scratch_dir = scratch_dir
        self._by_name = {e.name: e for e in entries}
        self._by_nfc: dict[str, _MediaEntry] = {}
        for e in entries:
            self._by_nfc.setdefault(unicodedata.normalize("NFC", e.name), e)

    def entries(self) -> list[tuple[str, int]]:
        """Every file as (name, declared size), in archive order."""
        return [(e.name, e.size) for e in self._entries]

    def lookup(self, name: str) -> _MediaEntry | None:
        return self._by_name.get(name) or self._by_nfc.get(unicodedata.normalize("NFC", name))

    def extract(self, name: str) -> str | None:
        """One file into a temp file in the scratch dir (the caller removes it), or None."""
        entry = self.lookup(name)
        if entry is None:
            return None
        try:
            return self._to_file(self._zf, entry)
        except _MediaFileError as exc:
            logger.warning("media file left out: %s", exc)
            return None

    def iter_files(self, *, in_memory_max: int) -> Iterator[MediaFile]:
        """Every readable file once, in archive order, read in one pass.

        Files up to `in_memory_max` come as bytes; larger ones as a temp file the
        caller removes with `MediaFile.discard()`. A file that cannot be read is
        logged and skipped (rendering will look it up again, and fail the same way).
        """
        stream = self._opener(SEQUENTIAL_READ_BYTES)
        try:
            with zipfile.ZipFile(stream) as zf:
                for entry in self._entries:
                    info = zf.getinfo(entry.info.filename)
                    try:
                        if entry.size <= in_memory_max:
                            buf = io.BytesIO()
                            self._copy(zf, entry, info, buf)
                            yield MediaFile(entry.name, buf.tell(), data=buf.getvalue())
                        else:
                            path = self._to_file(zf, entry, info)
                            yield MediaFile(entry.name, os.path.getsize(path), path=path)
                    except _MediaFileError as exc:
                        logger.warning("media file left out: %s", exc)
        finally:
            stream.close()

    def _to_file(self, zf: zipfile.ZipFile, entry: _MediaEntry, info: zipfile.ZipInfo | None = None) -> str:
        fd, path = tempfile.mkstemp(prefix="media_", dir=self.scratch_dir)
        try:
            with os.fdopen(fd, "wb") as out:
                self._copy(zf, entry, info or entry.info, out)
        except BaseException:
            os.unlink(path)
            raise
        return path

    def _copy(self, zf: zipfile.ZipFile, entry: _MediaEntry, info: zipfile.ZipInfo, out: BinaryIO) -> None:
        limit = min(entry.size, _MAX_MEDIA_FILE_BYTES)
        if entry.size > _MAX_MEDIA_FILE_BYTES:
            raise _MediaFileError(f"{entry.name!r} is larger than {_size_label(_MAX_MEDIA_FILE_BYTES)}")
        written = 0
        try:
            with _decoded(zf, info, self._zstd) as source:
                while block := source.read(_GUARD_READ_BYTES):
                    written += len(block)
                    if written > limit:
                        raise _MediaFileError(f"{entry.name!r} unpacks to more than the {limit} bytes it declares")
                    out.write(block)
        except (zipfile.BadZipFile, zlib.error, zstandard.ZstdError, EOFError) as exc:
            raise _MediaFileError(f"{entry.name!r} is unreadable ({exc})") from exc


def _write_stripped_package(zf: zipfile.ZipFile, zstd: bool, out_path: str) -> None:
    """The package minus its media: `meta`, the card database, and an empty media list.

    Anki imports this, so it unpacks the cards and nothing else; references to media
    stay in the card HTML exactly as written and resolve through `PackageMedia`.
    """
    with zipfile.ZipFile(out_path, "w", zipfile.ZIP_STORED) as out:
        for info in zf.infolist():
            if info.filename == "meta" or _COLLECTION_ENTRY_RE.fullmatch(info.filename):
                with zf.open(info) as src, out.open(zipfile.ZipInfo(info.filename), "w") as dst:
                    shutil.copyfileobj(src, dst, _GUARD_READ_BYTES)
        out.writestr("media", _EMPTY_MEDIA_LIST if zstd else b"{}")


def _as_opener(source: str | PackageOpener) -> PackageOpener:
    if callable(source):
        return source
    return lambda buffer_size: open(source, "rb", buffering=buffer_size)


class RenderingDeck:
    """An imported deck that renders its cards one at a time, as it is iterated.

    It holds no rendered card itself. Rendered whole, a 35,000-card AnKing deck is
    about 2.5 billion characters of HTML, more than the worker's memory, so the
    worker renders and sends one chunk at a time. Everything known before the first
    card is rendered is set on construction; only `degraded` grows while iterating.
    """

    def __init__(
        self,
        col: Collection,
        media_sink: MediaSink,
        max_cards: int,
        *,
        media: PackageMedia,
        strip_scripts: bool = False,
    ) -> None:
        self._col = col
        self._sink = media_sink
        self._has_room: Callable[[int], bool] = getattr(media_sink, "has_room", lambda _size: True)
        self._strip_scripts = strip_scripts
        self.media = media
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

    def seed_media(self, stored: Mapping[str, str]) -> None:
        """Media the caller already stored, by name, so rendering does not read it again."""
        self._media_cache.update(stored)

    def _placed(self, cid: int) -> tuple[str, dict[str, Any]]:
        """The card's deck name and note type (the current deck, for a filtered one)."""
        did, mid = self._placement[cid]
        if did not in self._deck_names:
            self._deck_names[did] = self._col.decks.name(did)
        return self._deck_names[did], self._col.models.get(mid) or {}

    def _store(self, fname: str) -> str | None:
        """Resolve one media reference: the stored reference, or None.

        The single gate for both callers, because the name is attacker-controlled
        in each: `<img src>` in the card HTML and the AV tag's filename. Only a bare
        filename the package lists is read, into a temp file named here, so no
        reference can reach a path of its choosing.
        """
        if fname in self._media_cache:
            return self._media_cache[fname]
        ref = None
        safe = safe_media_name(fname)
        entry = self.media.lookup(safe) if safe is not None else None
        if safe is not None and entry is not None and self._has_room(entry.size):
            local = self.media.extract(safe)
            if local is not None:
                try:
                    ref = self._sink(local, safe)
                finally:
                    os.unlink(local)
        elif safe is None and fname:
            logger.warning("rejected media reference that is not a bare filename: %r", fname)
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
    source: str | PackageOpener,
    media_sink: MediaSink,
    *,
    max_cards: int = 20000,
    max_collection_bytes: int = _MAX_COLLECTION_BYTES,
    max_entries: int = _MAX_ARCHIVE_ENTRIES,
    strip_scripts: bool = False,
) -> Generator[RenderingDeck, None, None]:
    """Import a package's cards into a throwaway collection, removed when the block exits.

    `source` is a path or a `PackageOpener`. Anki imports a copy of the package with
    an empty media list, so its media folder stays empty; the deck's media is read
    from the package through `RenderingDeck.media`. With `strip_scripts`, `<script>`
    elements are removed from the rendered HTML (see `remove_scripts`).
    """
    opener = _as_opener(source)
    workdir = tempfile.mkdtemp(prefix="anki_render_")
    stream = None
    col = None
    try:
        stream = opener(RANDOM_READ_BYTES)
        try:
            zf = zipfile.ZipFile(stream)
        except (zipfile.BadZipFile, zlib.error, EOFError) as exc:
            raise ApkgTooLargeError(f"not a valid .apkg archive: {exc}") from exc
        zstd = _is_zstd_package(zf)
        _guard_archive_bounds(zf, zstd=zstd, max_collection_bytes=max_collection_bytes, max_entries=max_entries)
        media = PackageMedia(zf, zstd, _read_media_list(zf, zstd), opener, workdir)

        stripped = os.path.join(workdir, "cards.apkg")
        _write_stripped_package(zf, zstd, stripped)
        col = Collection(os.path.join(workdir, "collection.anki2"))  # absent path -> created fresh
        col.import_anki_package(
            ImportAnkiPackageRequest(
                package_path=stripped,
                options=ImportAnkiPackageOptions(
                    merge_notetypes=False,
                    with_scheduling=False,
                    with_deck_configs=False,
                ),
            )
        )
        os.unlink(stripped)
        yield RenderingDeck(col, media_sink, max_cards, media=media, strip_scripts=strip_scripts)
    finally:
        if col is not None:
            col.close()  # releases the Rust backend's exclusive DB lock
        if stream is not None:
            stream.close()  # a ZipFile given a stream leaves it open
        shutil.rmtree(workdir, ignore_errors=True)


def render_apkg(
    source: str | PackageOpener,
    media_sink: MediaSink,
    *,
    max_cards: int = 20000,
    max_collection_bytes: int = _MAX_COLLECTION_BYTES,
    max_entries: int = _MAX_ARCHIVE_ENTRIES,
) -> RenderResult:
    """Import a `.apkg` and render every card to HTML, holding the whole deck.

    For the CLI. The worker iterates `open_apkg` instead: a large deck held whole
    outgrows its container.
    """
    with open_apkg(
        source,
        media_sink,
        max_cards=max_cards,
        max_collection_bytes=max_collection_bytes,
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
