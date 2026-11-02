"""What `RenderingDeck` renders, from a real Anki collection.

It counts its cards before rendering any, and the worker sends those counts on
every chunk, so they must match the rendered cards. With `strip_scripts` no card
carries a `<script>…</script>` element. Before anki unpacks a deck, the guard counts
what it unpacks to. Needs the AGPL `anki` package; skipped without it.
"""

import importlib.util
import pathlib
import sys
import zipfile
from collections import Counter

import pytest
import zstandard

pytest.importorskip("anki")
from anki.collection import Collection, ExportAnkiPackageOptions  # noqa: E402

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


@pytest.fixture(scope="module")
def media_apkg(tmp_path_factory) -> str:
    """1 card showing a picture the deck carries."""
    workdir = tmp_path_factory.mktemp("media")
    col = Collection(str(workdir / "source.anki2"))
    try:
        stored = col.media.write_data("heart.png", b"\x89PNG heart")
        note = col.new_note(col.models.by_name("Basic"))
        note["Front"] = f'<img src="{stored}">'
        note["Back"] = "Heart"
        col.add_note(note, col.decks.id("Cardio"))
        return _export(col, workdir)
    finally:
        col.close()


def test_media_files_lists_the_decks_files_and_nothing_that_leads_out(media_apkg, tmp_path):
    outside = tmp_path / "secret.txt"
    outside.write_text("not deck media")
    with renderer.open_apkg(media_apkg, _sink) as deck:
        media_dir = pathlib.Path(deck._media_dir)
        (media_dir / "link.png").symlink_to(outside)
        (media_dir / "folder").mkdir()

        files = deck.media_files()

    assert files == [(str(media_dir / "heart.png"), "heart.png")]


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


@pytest.fixture(scope="module", params=[False, True], ids=["modern", "older"])
def silent_apkg(request, tmp_path_factory) -> str:
    """12 MB of silence as a WAV: it compresses to almost nothing in a modern deck."""
    workdir = tmp_path_factory.mktemp("silent")
    col = Collection(str(workdir / "source.anki2"))
    try:
        stored = col.media.write_data("silence.wav", b"\x00" * (12 * 1024 * 1024))
        note = col.new_note(col.models.by_name("Basic"))
        note["Front"] = f"[sound:{stored}]"
        note["Back"] = "Silence"
        col.add_note(note, col.decks.id("Audio"))
        return _export(col, workdir, legacy=request.param)
    finally:
        col.close()


def _package(tmp_path: pathlib.Path, version: int, entries: dict[str, bytes]) -> str:
    """A package written by hand, `meta` naming its format version (3 = modern)."""
    path = tmp_path / "crafted.apkg"
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("meta", bytes([0x08, version]))
        for name, data in entries.items():
            zf.writestr(name, data)
    return str(path)


def _zstd(data: bytes) -> bytes:
    return zstandard.ZstdCompressor().compress(data)


def test_a_deck_is_held_to_what_it_unpacks_to(silent_apkg):
    with pytest.raises(renderer.ApkgTooLargeError, match="the deck unpacks to more than 10 MB"):
        renderer._guard_archive_bounds(silent_apkg, max_uncompressed_bytes=_LIMIT)

    with (
        pytest.raises(renderer.ApkgTooLargeError),
        renderer.open_apkg(silent_apkg, _sink, max_uncompressed_bytes=_LIMIT),
    ):
        pytest.fail("anki must not unpack the deck")


def test_a_deck_within_the_limit_passes(silent_apkg):
    renderer._guard_archive_bounds(silent_apkg, max_uncompressed_bytes=2 * _LIMIT)


def test_a_bomb_is_refused_though_its_zip_index_looks_tiny(tmp_path):
    bomb = _package(
        tmp_path, 3, {"collection.anki21b": _zstd(b"db"), "0": _zstd(b"\x00" * (3 * _LIMIT)), "media": _zstd(b"")}
    )
    with zipfile.ZipFile(bomb) as zf:
        assert sum(info.file_size for info in zf.infolist()) < 1024 * 1024

    with pytest.raises(renderer.ApkgTooLargeError, match="the deck unpacks to more than 10 MB"):
        renderer._guard_archive_bounds(bomb, max_uncompressed_bytes=_LIMIT)


def test_a_modern_media_entry_that_is_not_zstd_is_refused(tmp_path):
    with pytest.raises(renderer.ApkgTooLargeError, match="not a valid .apkg archive"):
        renderer._guard_archive_bounds(_package(tmp_path, 3, {"0": b"plain bytes"}), max_uncompressed_bytes=_LIMIT)


def test_an_older_package_is_counted_as_stored(tmp_path):
    """Anki copies an older package's media as-is, so zstd-looking bytes there are not unpacked."""
    older = _package(tmp_path, 2, {"0": _zstd(b"\x00" * (3 * _LIMIT))})

    renderer._guard_archive_bounds(older, max_uncompressed_bytes=_LIMIT)


def test_the_default_limit_reads_as_gb():
    assert renderer._size_label(renderer._MAX_UNCOMPRESSED_BYTES) == "2 GB"
