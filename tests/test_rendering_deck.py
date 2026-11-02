"""What `RenderingDeck` renders, from a real Anki collection.

It counts its cards before rendering any, and the worker sends those counts on
every chunk, so they must match the rendered cards. With `strip_scripts` no card
carries a `<script>…</script>` element. Before anki unpacks a deck, the guard counts
what it unpacks to. Needs the AGPL `anki` package; skipped without it.
"""

import importlib.util
import json
import os
import pathlib
import sys
import unicodedata
import zipfile
from collections import Counter

import pytest
import zstandard

pytest.importorskip("anki")
from anki.collection import Collection, ExportAnkiPackageOptions  # noqa: E402
from anki.import_export_pb2 import MediaEntries

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _load_renderer():
    # By path: the queue tests install a stub under the module name "renderer".
    spec = importlib.util.spec_from_file_location("renderer_under_test", ROOT / "renderer.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # dataclasses look their module up while it loads
    spec.loader.exec_module(module)
    return module


renderer = _load_renderer()


def _sink(_local: str, safe: str) -> str:
    return f"anki-media/{safe}"


@pytest.fixture(scope="module")
def apkg(tmp_path_factory) -> str:
    """6 Basic cards in Step1::Pharm; 1 Basic + 2 two-deletion Cloze notes in Step1::Micro."""
    workdir = tmp_path_factory.mktemp("deck")
    col = Collection(str(workdir / "source.anki2"))
    try:
        basic = col.models.by_name("Basic")
        cloze = col.models.by_name("Cloze")
        pharm = col.decks.id("Step1::Pharm")
        micro = col.decks.id("Step1::Micro")
        for i in range(6):
            note = col.new_note(basic)
            note["Front"] = f"Drug {i}"
            note["Back"] = f"Class {i}"
            col.add_note(note, pharm)
        note = col.new_note(basic)
        note["Front"] = 'Gram stain <img src="absent.png">'
        note["Back"] = "Positive"
        col.add_note(note, micro)
        for i in range(2):
            note = col.new_note(cloze)
            note["Text"] = f"{{{{c1::Bug {i}}}}} causes {{{{c2::disease {i}}}}}"
            col.add_note(note, micro)
        return _export(col, workdir)
    finally:
        col.close()


@pytest.fixture(scope="module")
def scripted_apkg(tmp_path_factory) -> str:
    """2 cards whose template carries scripts, one of them writing an <img> tag."""
    workdir = tmp_path_factory.mktemp("scripted")
    col = Collection(str(workdir / "source.anki2"))
    try:
        models = col.models
        nt = models.new("Scripted")
        for name in ("Front", "Back"):
            models.add_field(nt, models.new_field(name))
        tmpl = models.new_template("Card 1")
        tmpl["qfmt"] = "{{Front}}<script>var hint = '<img src=\"only-in-script.png\">';</script>"
        tmpl["afmt"] = '{{FrontSide}}<hr id=answer>{{Back}}<SCRIPT type="text/javascript">if (a < b) flip()</SCRIPT >'
        models.add_template(nt, tmpl)
        models.add(nt)
        nt = models.by_name("Scripted")
        deck_id = col.decks.id("Scripted")
        for i in range(2):
            note = col.new_note(nt)
            note["Front"] = f"<b>Question {i}</b>"
            note["Back"] = f"Answer {i}"
            col.add_note(note, deck_id)
        return _export(col, workdir)
    finally:
        col.close()


@pytest.fixture(scope="module", params=[False, True], ids=["modern", "older"])
def media_apkg(request, tmp_path_factory) -> str:
    """2 cards showing pictures and a sound the deck carries, under awkward names."""
    workdir = tmp_path_factory.mktemp("media")
    col = Collection(str(workdir / "source.anki2"))
    try:
        for name, data in _MEDIA.items():
            assert col.media.write_data(name, data) == name
        basic = col.models.by_name("Basic")
        note = col.new_note(basic)
        note["Front"] = '<img src="heart.png"> <img src="fig 1&amp;2.png">'
        note["Back"] = "Heart [sound:beat.mp3]"
        col.add_note(note, col.decks.id("Cardio"))
        note = col.new_note(basic)
        note["Front"] = '<img src="café.jpg"> <img src="../outside.png">'
        note["Back"] = "Cafe"
        col.add_note(note, col.decks.id("Cardio"))
        return _export(col, workdir, legacy=request.param)
    finally:
        col.close()


_MEDIA = {
    "heart.png": b"\x89PNG heart",
    "fig 1&2.png": b"\x89PNG figure",
    "café.jpg": b"\xff\xd8 cafe",
    "beat.mp3": b"ID3 beat",
}


class _RecordingSink:
    def __init__(self) -> None:
        self.received: dict[str, bytes] = {}

    def __call__(self, local: str, name: str) -> str:
        self.received[name] = pathlib.Path(local).read_bytes()
        return f"anki-media/{name}"


def test_anki_unpacks_no_media_and_cards_take_it_from_the_package(media_apkg):
    sink = _RecordingSink()
    with renderer.open_apkg(media_apkg, sink) as deck:
        assert os.listdir(deck._col.media.dir()) == [], "anki was given an empty media list"
        cards = list(deck)
        assert not [p for p in os.listdir(deck.media.scratch_dir) if p.startswith("media_")], "temp files removed"

    assert sink.received == _MEDIA, "every file the cards show, byte for byte"
    html = "".join(c.front_html + c.back_html for c in cards)
    for name in _MEDIA:
        assert f"anki-media/{name}" in html
    # One player attached; the reference that leads outside is the only one missing,
    # once per side (the Basic back repeats the front).
    assert deck.degraded == {"audio": 1, "media": 2}


def test_media_is_listed_in_archive_order_with_its_sizes(media_apkg):
    with renderer.open_apkg(media_apkg, _sink) as deck:
        listed = deck.media.entries()
        read = [(f.name, f.size, f.data) for f in deck.media.iter_files(in_memory_max=1024)]

    assert sorted(listed) == sorted((name, len(data)) for name, data in _MEDIA.items())
    assert read == [(name, size, _MEDIA[name]) for name, size in listed], "one pass, in the listed order"


def test_media_already_stored_is_not_read_again(media_apkg):
    sink = _RecordingSink()
    with renderer.open_apkg(media_apkg, sink) as deck:
        deck.seed_media({name: f"stored/{name}" for name in _MEDIA})
        cards = list(deck)

    assert sink.received == {}
    assert "stored/heart.png" in cards[0].front_html + cards[1].front_html


def test_a_file_the_sink_has_no_room_for_is_not_read(media_apkg):
    class _Full(_RecordingSink):
        def has_room(self, _size: int) -> bool:
            return False

    sink = _Full()
    with renderer.open_apkg(media_apkg, sink) as deck:
        deck.media.extract = lambda _name: pytest.fail("read a file the sink could not take")
        list(deck)

    assert sink.received == {}


def test_the_same_bytes_under_another_name_are_not_lost_to_a_full_cap(tmp_path):
    """Over the cap, a file whose bytes are already stored resolves through the package's own sha1."""
    workdir = tmp_path / "dup"
    workdir.mkdir()
    col = Collection(str(workdir / "source.anki2"))
    try:
        for name in ("a.png", "b.png"):
            assert col.media.write_data(name, b"\x89PNG same bytes") == name
        for name in ("a.png", "b.png"):
            note = col.new_note(col.models.by_name("Basic"))
            note["Front"] = f'<img src="{name}">'
            note["Back"] = name
            col.add_note(note, col.decks.id("Dup"))
        apkg = _export(col, workdir)
    finally:
        col.close()

    class _OneFileCap(_RecordingSink):
        def has_room(self, _size: int) -> bool:
            return not self.received

    sink = _OneFileCap()
    with renderer.open_apkg(apkg, sink) as deck:
        cards = list(deck)

    assert len(sink.received) == 1
    assert deck.degraded["media"] == 0
    assert all("anki-media/" in c.front_html for c in cards)


def test_a_file_is_read_once_however_its_name_is_spelled(media_apkg):
    """Two Unicode spellings of one name, in references or in what warm-up stored, find one file."""
    decomposed = unicodedata.normalize("NFD", "café.jpg")
    with renderer.open_apkg(media_apkg, _RecordingSink()) as deck:
        reads: list[str] = []
        extract = deck.media.extract
        deck.media.extract = lambda name: reads.append(name) or extract(name)
        assert deck._store("café.jpg") == deck._store(decomposed) == "anki-media/café.jpg"
        assert len(reads) == 1

    with renderer.open_apkg(media_apkg, _RecordingSink()) as deck:
        deck.seed_media({"café.jpg": "stored/cafe"})
        deck.media.extract = lambda _name: pytest.fail("read a file warm-up stored")
        assert deck._store(decomposed) == "stored/cafe"


def test_cleanup_runs_even_when_closing_the_collection_fails(media_apkg, monkeypatch):
    real_close = renderer.Collection.close

    def failing_close(self, *args, **kwargs):
        real_close(self, *args, **kwargs)
        msg = "close failed"
        raise RuntimeError(msg)

    monkeypatch.setattr(renderer.Collection, "close", failing_close)
    with pytest.raises(RuntimeError, match="close failed"), renderer.open_apkg(media_apkg, _sink) as deck:
        scratch = pathlib.Path(deck.media.scratch_dir)

    assert not scratch.exists()


def test_a_large_file_is_read_to_a_temp_file(media_apkg):
    with renderer.open_apkg(media_apkg, _sink) as deck:
        files = list(deck.media.iter_files(in_memory_max=0))
        assert all(f.data is None and pathlib.Path(f.path).read_bytes() == _MEDIA[f.name] for f in files)
        for f in files:
            f.discard()
        assert not [p for p in os.listdir(deck.media.scratch_dir) if p.startswith("media_")]


def test_the_package_is_read_through_an_opener(media_apkg):
    """How the worker reads a deck where it lies: small read-ahead to render, large for one pass."""
    opened: list[int] = []

    def opener(buffer_size: int):
        opened.append(buffer_size)
        return open(media_apkg, "rb", buffering=buffer_size)

    with renderer.open_apkg(opener, _sink) as deck:
        list(deck.media.iter_files(in_memory_max=1024))
        list(deck)

    # Set-up in one pass, small reads while cards render, one more pass to store media.
    assert opened == [renderer.SEQUENTIAL_READ_BYTES, renderer.RANDOM_READ_BYTES, renderer.SEQUENTIAL_READ_BYTES]


def _export(col: Collection, workdir: pathlib.Path, *, legacy: bool = False) -> str:
    out = workdir / "deck.apkg"
    col.export_anki_package(
        out_path=str(out),
        options=ExportAnkiPackageOptions(
            with_scheduling=False, with_deck_configs=False, with_media=True, legacy=legacy
        ),
        limit=None,
    )
    return str(out)


def test_counts_match_the_rendered_cards(apkg):
    with renderer.open_apkg(apkg, _sink) as deck:
        assert deck.total == 11
        assert deck.skipped == 0
        cards = list(deck)

        assert deck.deck_counts == Counter(c.deck for c in cards)
        assert deck.note_type_counts == Counter(c.note_type for c in cards)
        assert deck.deck_name == "Step1::Pharm"
        # Once per side: the Basic back repeats the front, image included.
        assert deck.degraded["media"] == 2


def test_counts_cover_only_the_cards_under_the_cap(apkg):
    with renderer.open_apkg(apkg, _sink, max_cards=4) as deck:
        assert deck.total == 4
        assert deck.skipped == 7
        cards = list(deck)

        assert len(cards) == 4
        assert deck.deck_counts == Counter(c.deck for c in cards)
        assert deck.note_type_counts == Counter(c.note_type for c in cards)


def test_render_apkg_still_returns_the_whole_deck(apkg):
    with renderer.open_apkg(apkg, _sink) as deck:
        streamed = [c.uid for c in deck]

    result = renderer.render_apkg(apkg, _sink)

    assert [c.uid for c in result.cards] == streamed
    assert result.imported == 11
    assert result.skipped == 0
    assert result.deck_name == "Step1::Pharm"
    assert result.degraded == {"audio": 0, "media": 2}
    assert all(c.css for c in result.cards), "the CLI preview still gets each card's stylesheet"


@pytest.mark.parametrize(
    ("html", "expected"),
    [
        ("<b>Q</b><script>tick()</script>", "<b>Q</b>"),
        ('a<SCRIPT type="text/javascript">x()</SCRIPT >b', "ab"),
        ("a<script>if (a < b) {}</script>b<script src=x.js></script>c", "abc"),
        ("a<script>x</scriptx>y</script>b", "ab"),  # `</scriptx>` does not end it
        ("a<script/>b</script>c", "ac"),  # not a void element: still runs to its end tag
        ("<noscript>n</noscript><scripts>s</scripts><style>.a{}</style>", None),
        # No end tag: left in place, so a `<script` that is not a tag costs no card text.
        ('a<script>var s = "</div>";', None),
        ("a<script/>b", None),
        ('<!-- <script src="x.js"> -->Answer: <b>beta-lactam</b>', None),
        ('<div title="see <script> note">Answer: <b>beta-lactam</b></div>', None),
        # A second `<script` inside the match: the first was not a tag, or the script
        # nests one. Kept whole, so the card text it spans survives.
        ("<!-- the <script> below shows hints --><script>hint()</script><div>Answer</div>", None),
        ('<div title="see <script> note">Answer</div><script>x()</script>', None),
        ("<style>/* no <script> here */ .a{}</style><div>Answer</div><script>x()</script>", None),
        ('<script><!-- document.write("<script></script>"); --></script><div>Answer</div>', None),
        # Only the ambiguous match is kept; the side's other scripts still go.
        ("<script>a()</script><!-- <script> --><script>b()</script>X", "<!-- <script> --><script>b()</script>X"),
        ("<!-- <script>old()</script> --><div>Answer</div>", "<!--  --><div>Answer</div>"),
    ],
)
def test_remove_scripts_removes_only_unambiguous_scripts(html, expected):
    assert renderer.remove_scripts(html) == (html if expected is None else expected)


def test_strip_scripts_leaves_no_script_on_either_side(scripted_apkg):
    with renderer.open_apkg(scripted_apkg, _sink, strip_scripts=True) as deck:
        cards = list(deck)
        assert deck.degraded["media"] == 0, "an <img> written inside a script is not looked up"

    assert len(cards) == 2
    for card in cards:
        assert "<script" not in (card.front_html + card.back_html).lower()
        assert "<b>Question" in card.front_html
        assert "<b>Question" in card.back_html
        assert "Answer" in card.back_html


def test_scripts_are_kept_unless_asked(scripted_apkg):
    with renderer.open_apkg(scripted_apkg, _sink) as deck:
        cards = list(deck)

    assert all("<script>" in card.front_html for card in cards)
    assert all("<SCRIPT" in card.back_html for card in cards)


# --- What a deck unpacks to -------------------------------------------------

_LIMIT = 10 * 1024 * 1024
_V3 = bytes([0x08, 3])


def _package(tmp_path: pathlib.Path, version: int | None, entries: dict[str, bytes], name: str = "crafted.apkg") -> str:
    """A package written by hand, `meta` naming its format version (3 = modern); None for no `meta`."""
    path = tmp_path / name
    with zipfile.ZipFile(path, "w") as zf:
        if version is not None:
            zf.writestr("meta", bytes([0x08, version]))
        for entry, data in entries.items():
            zf.writestr(entry, data)
    return str(path)


def _zstd(data: bytes) -> bytes:
    return zstandard.ZstdCompressor().compress(data)


def _media_list(*entries: tuple[str, int] | tuple[str, int, bytes]) -> bytes:
    listed = MediaEntries()
    for name, size, *sha1 in entries:
        listed.entries.add(name=name, size=size, sha1=sha1[0] if sha1 else b"")
    return _zstd(listed.SerializeToString())


def _opened(path: str):
    """The package's zip and media, as `open_apkg` reads them, without importing it."""
    zf = zipfile.ZipFile(path)
    zstd = renderer._is_zstd_package(zf)
    return zf, renderer.PackageMedia(
        zf, zstd, renderer._read_media_list(zf, zstd), renderer._as_opener(path), str(pathlib.Path(path).parent)
    )


def _strip(path: str, out: pathlib.Path, limit: int = _LIMIT) -> None:
    with zipfile.ZipFile(path) as zf:
        renderer._write_stripped_package(zf, renderer._read_layout(zf), str(out), max_collection_bytes=limit)


def test_a_card_database_that_unpacks_past_the_limit_is_refused(tmp_path):
    bomb = _package(tmp_path, 3, {"collection.anki21b": _zstd(b"\x00" * (_LIMIT + 1)), "media": _media_list()})
    with zipfile.ZipFile(bomb) as zf:
        assert sum(info.file_size for info in zf.infolist()) < 1024 * 1024, "its zip index looks tiny"

    with pytest.raises(renderer.ApkgTooLargeError, match="the deck's cards unpack to more than 10 MB"):
        _strip(bomb, tmp_path / "out.apkg")
    with (
        pytest.raises(renderer.ApkgTooLargeError),
        renderer.open_apkg(bomb, _sink, max_collection_bytes=_LIMIT),
    ):
        pytest.fail("anki must not unpack the deck")


def test_a_card_database_within_the_limit_passes(tmp_path):
    _strip(_package(tmp_path, 2, {"collection.anki21": b"\x00" * _LIMIT}), tmp_path / "out.apkg")


def test_every_byte_copied_is_counted_even_where_zstd_yields_nothing(tmp_path):
    """Skippable frames decode to nothing, but are copied byte for byte."""
    skippable = b"\x50\x2a\x4d\x18" + (_LIMIT).to_bytes(4, "little") + b"\x00" * _LIMIT
    padded = _package(tmp_path, 3, {"collection.anki21b": _zstd(b"db") + skippable})

    with pytest.raises(renderer.ApkgTooLargeError, match="unpack to more than"):
        _strip(padded, tmp_path / "out.apkg")


def test_a_padded_meta_is_refused(tmp_path):
    padded = _package(tmp_path, None, {"meta": _V3 + b"\x00" * 4096, "collection.anki21b": _zstd(b"db")})

    with pytest.raises(renderer.ApkgTooLargeError, match="metadata is too large"):
        _strip(padded, tmp_path / "out.apkg")


@pytest.mark.filterwarnings("ignore:Duplicate name")
def test_only_the_card_database_anki_reads_is_copied(tmp_path):
    """One entry, once: a package naming the same file eight times gets no extra allowance."""
    path = tmp_path / "dupes.apkg"
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("meta", bytes([0x08, 2]))
        for _ in range(8):
            zf.writestr("collection.anki21", b"\x00" * (_LIMIT // 2))
        zf.writestr("collection.anki2", b"\x00" * (_LIMIT // 2))

    _strip(str(path), tmp_path / "out.apkg")

    with zipfile.ZipFile(tmp_path / "out.apkg") as out:
        assert sorted(out.namelist()) == ["collection.anki21", "media", "meta"]


@pytest.mark.parametrize(
    ("version", "names", "read"),
    [
        (3, ["collection.anki2", "collection.anki21b"], "collection.anki21b"),
        (2, ["collection.anki2", "collection.anki21"], "collection.anki21"),
        (1, ["collection.anki2"], "collection.anki2"),
        (None, ["collection.anki2", "collection.anki21"], "collection.anki21"),
        (None, ["collection.anki2"], "collection.anki2"),
    ],
)
def test_the_card_database_follows_the_package_version(tmp_path, version, names, read):
    package = _package(tmp_path, version, dict.fromkeys(names, b"db"))
    with zipfile.ZipFile(package) as zf:
        layout = renderer._read_layout(zf)
    assert layout.collection.filename == read
    assert layout.zstd is (read == "collection.anki21b")


def test_a_package_without_its_card_database_is_refused(tmp_path):
    with (
        zipfile.ZipFile(_package(tmp_path, 3, {"collection.anki2": b"db"})) as zf,
        pytest.raises(renderer.ApkgTooLargeError, match="no collection.anki21b"),
    ):
        renderer._read_layout(zf)


def test_media_does_not_count_toward_the_card_database_limit(tmp_path):
    package = _package(
        tmp_path,
        3,
        {
            "collection.anki21b": _zstd(b"db"),
            "0": _zstd(b"\x00" * (_LIMIT + 1)),
            "media": _media_list(("big.wav", _LIMIT + 1)),
        },
    )
    _strip(package, tmp_path / "out.apkg")


def test_a_media_file_past_its_declared_size_is_left_out(tmp_path):
    """A bomb: it declares 100 bytes and unpacks to 30 MB. Reading stops past 100."""
    bomb = _package(
        tmp_path,
        3,
        {
            "0": _zstd(b"\x00" * (3 * _LIMIT)),
            "1": _zstd(b"fine"),
            "media": _media_list(("bomb.png", 100), ("ok.png", 4)),
        },
    )
    zf, media = _opened(bomb)
    with zf:
        assert media.extract("bomb.png") is None
        assert [(f.name, f.data) for f in media.iter_files(in_memory_max=1024)] == [("ok.png", b"fine")]
        assert not [p for p in tmp_path.iterdir() if p.name.startswith("media_")], "the partial file is removed"


def test_a_media_file_over_the_per_file_limit_is_not_read_or_counted(tmp_path, monkeypatch):
    monkeypatch.setattr(renderer, "_MAX_MEDIA_FILE_BYTES", 10)
    package = _package(
        tmp_path, 3, {"0": _zstd(b"x" * 11), "1": _zstd(b"ok"), "media": _media_list(("big.mp4", 11), ("ok.png", 2))}
    )
    zf, media = _opened(package)
    with zf:
        assert media.extract("big.mp4") is None
        assert media.entries() == [("ok.png", 2)], "one huge video cannot push the deck over the media cap"


def test_a_compressible_file_is_stored_as_what_it_unpacks_to(tmp_path):
    """12 MB of silence compresses to almost nothing; the sink (and its cap) sees all 12 MB."""
    silence = b"\x00" * (12 * 1024 * 1024)
    package = _package(tmp_path, 3, {"0": _zstd(silence), "media": _media_list(("silence.wav", len(silence)))})
    zf, media = _opened(package)
    with zf:
        path = media.extract("silence.wav")
        assert pathlib.Path(path).stat().st_size == len(silence)


def test_a_modern_media_entry_that_is_not_zstd_is_left_out(tmp_path):
    package = _package(tmp_path, 3, {"0": b"plain bytes", "media": _media_list(("x.png", 11))})
    zf, media = _opened(package)
    with zf:
        assert media.extract("x.png") is None


def _corrupt_member(path: pathlib.Path, member: str) -> None:
    """Overwrite the middle of a member's stored bytes in place."""
    with zipfile.ZipFile(path) as zf:
        info = zf.getinfo(member)
    raw = bytearray(path.read_bytes())
    start = info.header_offset + 30 + len(info.filename.encode()) + len(info.extra)
    middle = start + info.compress_size // 2
    raw[middle : middle + 8] = b"\xff" * 8
    path.write_bytes(bytes(raw))


def _mark_encrypted(path: pathlib.Path, member: str) -> None:
    """Set the encrypted flag on a member, in its local header and its central directory record."""
    with zipfile.ZipFile(path) as zf:
        offset = zf.getinfo(member).header_offset
    raw = bytearray(path.read_bytes())
    raw[offset + 6] |= 0x1
    at = raw.find(b"PK\x01\x02")
    while at != -1:
        name_len = int.from_bytes(raw[at + 28 : at + 30], "little")
        if raw[at + 46 : at + 46 + name_len] == member.encode():
            raw[at + 8] |= 0x1
        at = raw.find(b"PK\x01\x02", at + 4)
    path.write_bytes(bytes(raw))


def test_an_entry_zipfile_cannot_read_leaves_out_that_file_only(tmp_path):
    """Corrupt bzip2 raises OSError and an encrypted entry RuntimeError; neither may fail the deck."""
    path = tmp_path / "older.apkg"
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("meta", bytes([0x08, 2]))
        zf.writestr(zipfile.ZipInfo("0"), b"bz" * 50_000, compress_type=zipfile.ZIP_BZIP2)
        zf.writestr("1", b"secret")
        zf.writestr("2", b"fine")
        zf.writestr("media", json.dumps({"0": "corrupt.png", "1": "locked.png", "2": "ok.png"}))
    _corrupt_member(path, "0")
    _mark_encrypted(path, "1")

    zf, media = _opened(str(path))
    with zf:
        assert media.extract("corrupt.png") is None
        assert media.extract("locked.png") is None
        assert [f.name for f in media.iter_files(in_memory_max=1 << 20)] == ["ok.png"]


def test_an_older_package_is_read_as_stored(tmp_path):
    """Anki copies an older package's media as-is, so zstd-looking bytes there are not unpacked."""
    packed = _zstd(b"\x00" * 1000)
    older = _package(tmp_path, 2, {"0": packed, "media": json.dumps({"0": "x.bin"}).encode()})
    zf, media = _opened(older)
    with zf:
        assert media.entries() == [("x.bin", len(packed))]
        assert pathlib.Path(media.extract("x.bin")).read_bytes() == packed


def test_names_that_are_not_bare_filenames_are_not_listed(tmp_path):
    package = _package(
        tmp_path, 3, {"0": _zstd(b"a"), "1": _zstd(b"b"), "media": _media_list(("../escape.png", 1), ("ok.png", 1))}
    )
    zf, media = _opened(package)
    with zf:
        assert media.entries() == [("ok.png", 1)]


def test_one_entry_is_listed_under_one_name(tmp_path):
    """`legacy_zip_filename` lets many names point at one entry, which warm-up would read once per name."""
    listed = MediaEntries()
    for i in range(5):
        listed.entries.add(name=f"alias{i}.png", size=1, legacy_zip_filename=0)
    package = _package(
        tmp_path, 3, {"0": _zstd(b"a"), "1": b"", "2": b"", "3": b"", "media": _zstd(listed.SerializeToString())}
    )
    zf, media = _opened(package)
    with zf:
        assert media.entries() == [("alias0.png", 1)]


def test_a_media_list_naming_more_files_than_the_archive_holds_is_refused(tmp_path):
    package = _package(tmp_path, 3, {"0": _zstd(b"a"), "media": _media_list(*((f"{i}.png", 1) for i in range(10)))})
    with zipfile.ZipFile(package) as zf, pytest.raises(renderer.ApkgTooLargeError, match="more files than"):
        renderer._read_media_list(zf, True)


def test_a_media_list_of_empty_entries_is_refused_before_it_is_parsed(tmp_path, monkeypatch):
    """Millions of 2-byte entries compress to almost nothing; parsed, they took 1.5 GB."""
    monkeypatch.setattr(
        renderer, "MediaEntries", type("Unparsed", (), {"FromString": lambda _d: pytest.fail("parsed the list")})
    )
    package = _package(tmp_path, 3, {"0": _zstd(b"a"), "media": _zstd(b"\x0a\x00" * 1_000_000)})
    with zipfile.ZipFile(package) as zf, pytest.raises(renderer.ApkgTooLargeError, match="more files than"):
        renderer._read_media_list(zf, True)


def test_media_entries_are_counted_off_the_wire():
    listed = MediaEntries()
    listed.entries.add(name="a.png", size=300, sha1=b"\x01" * 20)
    listed.entries.add(name="b.png", size=1, legacy_zip_filename=7)
    listed.entries.add()
    data = listed.SerializeToString()

    assert renderer._listed_count(data, stop_after=10) == 3
    assert renderer._listed_count(b"\x0a\x00" * 1_000_000, stop_after=5) == 6, "stops once over"
    with pytest.raises(renderer.DecodeError):
        renderer._listed_count(b"\x0a\xff", stop_after=10)


def test_media_reads_stop_at_the_decks_budget(tmp_path):
    """Identical files are stored once, so only a budget on what is decoded bounds the reads."""
    zeros = b"\x00" * 100
    files = {str(i): _zstd(zeros) for i in range(5)}
    package = _package(tmp_path, 3, {**files, "media": _media_list(*((f"{i}.png", len(zeros)) for i in range(5)))})
    with zipfile.ZipFile(package) as zf:
        entries = renderer._read_media_list(zf, True)
        media = renderer.PackageMedia(
            zf, True, entries, renderer._as_opener(package), str(tmp_path), max_read_bytes=250
        )
        assert media.extract("0.png") is not None
        assert media.extract("1.png") is not None
        assert media.extract("2.png") is None, "crosses the budget"
        assert media.spent
        assert media.extract("3.png") is None
        assert list(media.iter_files(in_memory_max=1024)) == []

        fresh = renderer.PackageMedia(
            zf, True, entries, renderer._as_opener(package), str(tmp_path), max_read_bytes=250
        )
        assert [f.name for f in fresh.iter_files(in_memory_max=1024)] == ["0.png", "1.png"]


def test_the_media_budget_reaches_the_deck(media_apkg):
    with renderer.open_apkg(media_apkg, _sink, max_media_read_bytes=12) as deck:
        assert deck.media._max_read == 12
        list(deck)
        assert deck.media.spent
        assert deck.degraded["media"] > 0


def _garble_local_name(path: pathlib.Path, member: str) -> None:
    """Mark a member's local header name as UTF-8 and make it invalid UTF-8; the central name stays."""
    with zipfile.ZipFile(path) as zf:
        offset = zf.getinfo(member).header_offset
    raw = bytearray(path.read_bytes())
    raw[offset + 7] |= 0x08  # general purpose bit 11: the name is UTF-8
    raw[offset + 30] = 0xFF
    path.write_bytes(bytes(raw))


def test_a_header_name_that_is_not_utf8_leaves_out_that_file_only(tmp_path):
    path = tmp_path / "older.apkg"
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("meta", bytes([0x08, 2]))
        zf.writestr("0", b"bad")
        zf.writestr("1", b"fine")
        zf.writestr("media", json.dumps({"0": "bad.png", "1": "ok.png"}))
    _garble_local_name(path, "0")

    zf, media = _opened(str(path))
    with zf:
        assert media.extract("bad.png") is None
        assert [f.name for f in media.iter_files(in_memory_max=1024)] == ["ok.png"]


def test_a_reference_finds_its_file_whatever_its_unicode_form(tmp_path):
    decomposed = unicodedata.normalize("NFD", "café.jpg")
    package = _package(tmp_path, 3, {"0": _zstd(b"cafe"), "media": _media_list((decomposed, 4))})
    zf, media = _opened(package)
    with zf:
        assert pathlib.Path(media.extract("café.jpg")).read_bytes() == b"cafe"


@pytest.mark.parametrize(
    "entries",
    [{"media": _zstd(b"\xff\xff not a protobuf")}, {"media": b"not zstd"}],
    ids=["not a media list", "not zstd"],
)
def test_an_unreadable_media_list_is_refused(tmp_path, entries):
    with (
        zipfile.ZipFile(_package(tmp_path, 3, entries)) as zf,
        pytest.raises(renderer.ApkgTooLargeError, match="not a valid .apkg archive"),
    ):
        renderer._read_media_list(zf, True)


def test_a_file_that_is_not_a_zip_is_refused(tmp_path):
    path = tmp_path / "deck.apkg"
    path.write_bytes(b"not a zip at all")
    with (
        pytest.raises(renderer.ApkgTooLargeError, match="not a valid .apkg archive"),
        renderer.open_apkg(str(path), _sink),
    ):
        pytest.fail("nothing to import")


def test_the_default_limit_reads_as_gb():
    assert renderer._size_label(renderer._MAX_COLLECTION_BYTES) == "1 GB"
