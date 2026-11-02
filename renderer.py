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
import lzma
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
# `meta` is PackageMetadata{version}: 2 bytes. Anki reads it whole, so a padded one is refused.
_MAX_META_BYTES = 1024
# One media file, decoded. Larger ones are left out (the card shows "Image unavailable").
_MAX_MEDIA_FILE_BYTES = 256 * 1024 * 1024
# The media list itself: about 60 bytes a file, so 100,000 files fit twice over.
_MAX_MEDIA_LIST_BYTES = 16 * 1024 * 1024
_GUARD_READ_BYTES = 1024 * 1024
# Read-ahead: one pass over every file takes large windows; a file looked up while
# cards render takes a small one, so it fetches little beyond itself.
SEQUENTIAL_READ_BYTES = 8 * 1024 * 1024
RANDOM_READ_BYTES = 256 * 1024
# What a modern package's media list is when it lists nothing.
_EMPTY_MEDIA_LIST = zstandard.ZstdCompressor().compress(MediaEntries().SerializeToString())
# A zip entry that cannot be read: corrupt or truncated data, an unsupported compression
# method, encryption, or a header name that is not valid UTF-8. A package's stream raises
# only what its opener raises (the worker's raises AzureError), so none of these means
# storage is down.
_UNREADABLE = (
    zipfile.BadZipFile,
    zlib.error,
    zstandard.ZstdError,
    lzma.LZMAError,
    EOFError,
    OSError,
    NotImplementedError,
    RuntimeError,
    UnicodeDecodeError,
)


class ApkgTooLargeError(Exception):
    """The package is not a valid .apkg, or its card database exceeds the bounds."""


def _size_label(n: int) -> str:
    return f"{n / 1024**3:g} GB" if n >= 1024**3 else f"{n // 1024**2} MB"


def _invalid(exc: BaseException | str) -> ApkgTooLargeError:
    return ApkgTooLargeError(f"not a valid .apkg archive: {exc}")


def _read_meta(zf: zipfile.ZipFile) -> bytes | None:
    """The package's `meta` entry, refused if it is not tiny; None for an older package without one."""
    try:
        info = zf.getinfo("meta")
    except KeyError:
        return None
    try:
        with zf.open(info) as fh:
            meta = fh.read(_MAX_META_BYTES + 1)
    except _UNREADABLE as exc:
        raise _invalid(exc) from exc
    if len(meta) > _MAX_META_BYTES:
        raise _invalid("its metadata is too large")
    return meta


def _meta_version(meta: bytes | None) -> int | None:
    """PackageMetadata{version = 1}: a tag byte 0x08, then the version."""
    if meta is None or len(meta) < 2 or meta[0] != 0x08:
        return None
    return meta[1]


def _is_zstd_package(zf: zipfile.ZipFile) -> bool:
    """Whether anki reads this package's entries as zstd: `meta` version 3 or later.

    Older packages (version 1-2, or no `meta`) store their entries as-is.
    """
    version = _meta_version(_read_meta(zf))
    return version is not None and version >= 3


@dataclass(frozen=True)
class _Layout:
    """What anki reads from a package: its `meta`, and the one card database for its version."""

    meta: bytes | None
    zstd: bool
    collection: zipfile.ZipInfo


def _read_layout(zf: zipfile.ZipFile, *, max_entries: int = _MAX_ARCHIVE_ENTRIES) -> _Layout:
    """The package's version and the card database anki will import from it.

    Version 3 packages hold `collection.anki21b` (zstd); version 2 `collection.anki21`;
    version 1 `collection.anki2`. A package without `meta` is version 2 when it has an
    anki21 file, else version 1.
    """
    infos = zf.infolist()
    if len(infos) > max_entries:
        raise ApkgTooLargeError(f"archive has {len(infos)} entries (max {max_entries})")
    meta = _read_meta(zf)
    version = _meta_version(meta)
    if version is not None and version >= 3:
        name = "collection.anki21b"
    elif version == 2 or (version is None and "collection.anki21" in zf.namelist()):
        name = "collection.anki21"
    else:
        name = "collection.anki2"
    try:
        collection = zf.getinfo(name)
    except KeyError:
        raise _invalid(f"it has no {name}") from None
    return _Layout(meta=meta, zstd=name.endswith("b"), collection=collection)


def _decoded(zf: zipfile.ZipFile, info: zipfile.ZipInfo, zstd: bool) -> BinaryIO:
    """An entry's bytes as anki reads them: zstd-decoded in a modern package."""
    raw = zf.open(info)
    if not zstd:
        return raw
    return zstandard.ZstdDecompressor().stream_reader(raw, read_across_frames=True, closefd=True)


def _write_stripped_package(
    zf: zipfile.ZipFile, layout: _Layout, out_path: str, *, max_collection_bytes: int = _MAX_COLLECTION_BYTES
) -> None:
    """The package minus its media: `meta`, the one card database, and an empty media list.

    Anki imports this, so it unpacks the cards and nothing else; references to media
    stay in the card HTML exactly as written and resolve through `PackageMedia`. Every
    byte written is counted against the bound, and so is what a zstd database decodes
    to, so neither padding nor a compression bomb can fill the disk.
    """
    too_large = ApkgTooLargeError(f"the deck's cards unpack to more than {_size_label(max_collection_bytes)}")
    with zipfile.ZipFile(out_path, "w", zipfile.ZIP_STORED) as out:
        if layout.meta is not None:
            out.writestr("meta", layout.meta)
        written = 0
        with out.open(zipfile.ZipInfo(layout.collection.filename), "w") as dst:
            try:
                src = zf.open(layout.collection)
            except _UNREADABLE as exc:
                raise _invalid(exc) from exc
            with src:
                while True:
                    try:
                        block = src.read(_GUARD_READ_BYTES)
                    except _UNREADABLE as exc:
                        raise _invalid(exc) from exc
                    if not block:
                        break
                    written += len(block)
                    if written > max_collection_bytes:
                        raise too_large
                    dst.write(block)
        out.writestr("media", _EMPTY_MEDIA_LIST if layout.zstd else b"{}")
    if not layout.zstd:
        return
    # What anki unpacks the database to, counted from the copy now on local disk.
    with zipfile.ZipFile(out_path) as copy:
        unpacked = 0
        try:
            with _decoded(copy, copy.getinfo(layout.collection.filename), zstd=True) as fh:
                while block := fh.read(_GUARD_READ_BYTES):
                    unpacked += len(block)
                    if unpacked > max_collection_bytes:
                        raise too_large
        except _UNREADABLE as exc:
            raise _invalid(exc) from exc


@dataclass(frozen=True)
class _MediaEntry:
    name: str
    info: zipfile.ZipInfo
    # What the package says the file decodes to; reading stops past it.
    size: int
    # A modern package's own fingerprint of the file, when it gives one.
    sha1: bytes | None = None


def _varint(data: bytes, pos: int) -> tuple[int, int]:
    value = shift = 0
    while pos < len(data) and shift < 64:
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if byte < 0x80:
            return value, pos
        shift += 7
    raise DecodeError("truncated varint")


def _listed_count(data: bytes, *, stop_after: int) -> int:
    """How many entries a serialized `MediaEntries` holds, counting no further than `stop_after + 1`.

    Read off the wire format, so a list of millions of 2-byte empty entries is refused
    before any becomes an object: parsed, each costs about a hundred times its size.
    """
    count = pos = 0
    while pos < len(data) and count <= stop_after:
        key, pos = _varint(data, pos)
        wire_type = key & 7
        if wire_type == 0:
            _, pos = _varint(data, pos)
        elif wire_type == 2:
            length, pos = _varint(data, pos)
            pos += length
        elif wire_type in (1, 5):
            pos += 8 if wire_type == 1 else 4
        else:
            raise DecodeError(f"unexpected wire type {wire_type}")
        if key >> 3 == 1:  # MediaEntries.entries
            count += 1
    return count


def _read_media_list(zf: zipfile.ZipFile, zstd: bool) -> list[_MediaEntry]:
    """The package's media files, in the order they sit in the archive.

    A modern package lists them in a zstd `MediaEntries` protobuf, each stored under
    its index (or `legacy_zip_filename`); an older one in JSON, `{"0": "name.jpg"}`.
    A list naming more files than the archive holds is refused. Names that are not
    bare filenames, entries the archive lacks, and a second name for an entry already
    named are left out: aliases would read one entry once per name.
    """
    try:
        info = zf.getinfo("media")
    except KeyError:
        return []
    too_many = _invalid("the media list names more files than the archive holds")
    members = len(zf.infolist())
    try:
        buf = io.BytesIO()
        with _decoded(zf, info, zstd) as fh:
            while (block := fh.read(_GUARD_READ_BYTES)) and buf.tell() <= _MAX_MEDIA_LIST_BYTES:
                buf.write(block)
        data = buf.getvalue()
        if len(data) > _MAX_MEDIA_LIST_BYTES:
            raise ApkgTooLargeError(f"the media list is larger than {_size_label(_MAX_MEDIA_LIST_BYTES)}")
        if zstd:
            if _listed_count(data, stop_after=members) > members:
                raise too_many
            listed = [
                (
                    e.name,
                    str(e.legacy_zip_filename) if e.HasField("legacy_zip_filename") else str(i),
                    e.size,
                    e.sha1 or None,
                )
                for i, e in enumerate(MediaEntries.FromString(data).entries)
            ]
        else:
            mapping = json.loads(data or b"{}")
            if not isinstance(mapping, dict):
                raise _invalid("the media list is not a mapping")
            listed = [(name, str(member), None, None) for member, name in mapping.items() if isinstance(name, str)]
    except (*_UNREADABLE, DecodeError, ValueError) as exc:
        raise _invalid(f"the media list is unreadable ({exc})") from exc
    if len(listed) > members:
        raise too_many

    entries: dict[str, _MediaEntry] = {}
    claimed: set[str] = set()
    left_out = 0
    for name, member, size, sha1 in listed:
        if safe_media_name(name) is None or member in claimed or name in entries:
            left_out += 1
            continue
        try:
            member_info = zf.getinfo(member)
        except KeyError:
            continue
        claimed.add(member)
        # An older package's file is stored as-is, so the zip's own size is what it holds.
        entries[name] = _MediaEntry(name, member_info, member_info.file_size if size is None else size, sha1)
    if left_out:
        logger.warning("media list names %d file(s) that are not bare filenames or are named twice; left out", left_out)
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
    refused before it fills the disk. Everything decoded counts against
    `max_read_bytes`, stored or not; past it the remaining files are left out. A file
    that cannot be read leaves only its own cards degraded. Errors from the package's
    own stream (storage) and from writing the temp file (the disk) are not caught here.
    Not thread-safe: the worker reads media from the job's thread only.
    """

    def __init__(
        self,
        zf: zipfile.ZipFile,
        zstd: bool,
        entries: list[_MediaEntry],
        opener: PackageOpener,
        scratch_dir: str,
        *,
        max_read_bytes: int | None = None,
    ) -> None:
        self._zf = zf
        self._zstd = zstd
        self._entries = entries
        self._opener = opener
        self.scratch_dir = scratch_dir
        self._max_read = max_read_bytes
        self._read = 0
        self._by_name = {e.name: e for e in entries}
        self._by_nfc: dict[str, _MediaEntry] = {}
        for e in entries:
            self._by_nfc.setdefault(unicodedata.normalize("NFC", e.name), e)

    def entries(self) -> list[tuple[str, int]]:
        """Every file that can be stored, as (name, declared size), in archive order.

        A file over `_MAX_MEDIA_FILE_BYTES` is never stored, so it is not counted here.
        """
        return [(e.name, e.size) for e in self._readable()]

    def lookup(self, name: str) -> _MediaEntry | None:
        return self._by_name.get(name) or self._by_nfc.get(unicodedata.normalize("NFC", name))

    @property
    def spent(self) -> bool:
        """Whether the reads allowed for this deck's media are used up."""
        return self._max_read is not None and self._read >= self._max_read

    def extract(self, name: str) -> str | None:
        """One file into a temp file in the scratch dir (the caller removes it), or None."""
        entry = self.lookup(name)
        if entry is None or self.spent:
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
                for entry in self._readable():
                    if self.spent:
                        break
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

    def _readable(self) -> list[_MediaEntry]:
        return [e for e in self._entries if e.size <= _MAX_MEDIA_FILE_BYTES]

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
        if entry.size > _MAX_MEDIA_FILE_BYTES:
            raise _MediaFileError(f"{entry.name!r} is larger than {_size_label(_MAX_MEDIA_FILE_BYTES)}")
        try:
            source = _decoded(zf, info, self._zstd)
        except _UNREADABLE as exc:
            raise _MediaFileError(f"{entry.name!r} is unreadable ({exc})") from exc
        written = 0
        with source:
            while True:
                try:
                    block = source.read(_GUARD_READ_BYTES)
                except _UNREADABLE as exc:
                    raise _MediaFileError(f"{entry.name!r} is unreadable ({exc})") from exc
                if not block:
                    return
                written += len(block)
                if written > entry.size:
                    raise _MediaFileError(f"{entry.name!r} unpacks to more than the {entry.size} bytes it declares")
                self._read += len(block)
                if self._max_read is not None and self._read > self._max_read:
                    raise _MediaFileError(
                        f"the deck's media reads passed {_size_label(self._max_read)}; the rest is left out"
                    )
                # Outside the read's try: a full disk is not this file's fault, so it fails the job.
                out.write(block)


@contextmanager
def _open_zip(opener: PackageOpener, buffer_size: int) -> Generator[zipfile.ZipFile, None, None]:
    stream = opener(buffer_size)
    try:
        try:
            zf = zipfile.ZipFile(stream)
        except (zipfile.BadZipFile, zlib.error, EOFError, OSError) as exc:
            raise _invalid(exc) from exc
        with zf:
            yield zf
    finally:
        stream.close()  # a ZipFile given a stream leaves it open


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
        # Results by archive entry, so a name spelled another way does not read its file again.
        self._ref_by_member: dict[str, str | None] = {}
        # Stored references by the package's own fingerprint, so a file listed under a
        # second name is not read (or refused by a full cap) a second time.
        self._ref_by_sha1: dict[bytes, str] = {}
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
        for name, ref in stored.items():
            if (entry := self.media.lookup(name)) is not None:
                self._ref_by_member[entry.info.filename] = ref

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
        if entry is not None:
            member = entry.info.filename
            if member in self._ref_by_member:
                ref = self._ref_by_member[member]
            else:
                ref = self._read_and_store(entry)
                self._ref_by_member[member] = ref
        elif safe is None and fname:
            logger.warning("rejected media reference that is not a bare filename: %r", fname)
        self._media_cache[fname] = ref
        return ref

    def _read_and_store(self, entry: _MediaEntry) -> str | None:
        """Store one listed file, unless the package's fingerprint shows it is stored already."""
        ref = self._ref_by_sha1.get(entry.sha1) if entry.sha1 else None
        if ref is None and self._has_room(entry.size):
            local = self.media.extract(entry.name)
            if local is not None:
                try:
                    ref = self._sink(local, entry.name)
                finally:
                    os.unlink(local)
            if ref is not None and entry.sha1:
                self._ref_by_sha1[entry.sha1] = ref
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
    max_media_read_bytes: int | None = None,
    strip_scripts: bool = False,
) -> Generator[RenderingDeck, None, None]:
    """Import a package's cards into a throwaway collection, removed when the block exits.

    `source` is a path or a `PackageOpener`. Anki imports a copy of the package with
    an empty media list, so its media folder stays empty; the deck's media is read
    from the package through `RenderingDeck.media`, up to `max_media_read_bytes`
    decoded. With `strip_scripts`, `<script>` elements are removed from the rendered
    HTML (see `remove_scripts`).
    """
    opener = _as_opener(source)
    workdir = tempfile.mkdtemp(prefix="anki_render_")
    try:
        stripped = os.path.join(workdir, "cards.apkg")
        # One pass in archive order: the layout, then the card database copied out.
        with _open_zip(opener, SEQUENTIAL_READ_BYTES) as setup:
            layout = _read_layout(setup, max_entries=max_entries)
            _write_stripped_package(setup, layout, stripped, max_collection_bytes=max_collection_bytes)
        with _open_zip(opener, RANDOM_READ_BYTES) as zf:
            media = PackageMedia(
                zf,
                layout.zstd,
                _read_media_list(zf, layout.zstd),
                opener,
                workdir,
                max_read_bytes=max_media_read_bytes,
            )
            col = Collection(os.path.join(workdir, "collection.anki2"))  # absent path -> created fresh
            try:
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
                col.close()  # releases the Rust backend's exclusive DB lock
    finally:
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
