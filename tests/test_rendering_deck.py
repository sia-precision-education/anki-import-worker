"""What `RenderingDeck` renders, from a real Anki collection.

It counts its cards before rendering any, and the worker sends those counts on
every chunk, so they must match the rendered cards. With `strip_scripts` no card
carries a `<script>`. Needs the AGPL `anki` package; skipped without it.
"""

import importlib.util
import pathlib
import sys
from collections import Counter

import pytest

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


def _export(col: Collection, workdir: pathlib.Path) -> str:
    out = workdir / "deck.apkg"
    col.export_anki_package(
        out_path=str(out),
        options=ExportAnkiPackageOptions(with_scheduling=False, with_deck_configs=False, with_media=True, legacy=False),
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
        ('a<script>var s = "</div>";', "a"),  # left open, a browser hides the rest too
        ("a<script/>b", "a"),  # not a void element: still runs to its end tag
        ("<noscript>n</noscript><scripts>s</scripts><style>.a{}</style>", None),
    ],
)
def test_remove_scripts_ends_each_script_where_a_browser_does(html, expected):
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
