"""The worker renders and sends a deck one chunk at a time.

Held whole, a rendered 35,000-card AnKing deck was about 2.5 billion characters
of HTML; the 2 GiB container was killed ~20s in, every time the message came
back. `_FakeDeck` stands in for `renderer.RenderingDeck` and counts the cards it
has rendered, which is how these tests see that a chunk is sent before the next
one is built.
"""

import contextlib
import os
import sys
import types
from collections import Counter

import httpx
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault("AZURE_STORAGE_CONNECTION_STRING", "UseDevelopmentStorage=true")
os.environ.setdefault("ANKI_CALLBACK_URL", "https://backend.invalid/api/v1/anki/callback")
os.environ.setdefault("ANKI_CALLBACK_SECRET", "test-secret")

_renderer = types.ModuleType("renderer")
_renderer.open_apkg = lambda *a, **k: None  # noqa: ARG005
sys.modules.setdefault("renderer", _renderer)

import worker  # noqa: E402

CALLBACK = "https://backend.invalid/api/v1/anki/callback"


def _card(i: int, *, note_type: str = "AnKingOverhaul", deck: str = "AnKing::Pharm", missing: int = 0):
    return types.SimpleNamespace(
        front_html=f"front {i}",
        back_html=f"back {i}",
        css=f"/* {note_type} */",
        deck=deck,
        note_type=note_type,
        cloze=False,
        tags=[],
        media=[],
        uid=f"guid{i}:0",
        missing=missing,
    )


class _FakeDeck:
    def __init__(self, cards: list, *, skipped: int = 0, fail_at: "int | None" = None) -> None:
        self._cards = cards
        self._fail_at = fail_at
        self.skipped = skipped
        self.degraded = {"audio": 0, "media": 0}
        self.deck_counts = Counter(c.deck for c in cards)
        self.note_type_counts = Counter(c.note_type for c in cards)
        self.rendered = 0

    @property
    def total(self) -> int:
        return len(self._cards)

    @property
    def deck_name(self) -> str:
        return max(self.deck_counts, key=lambda k: self.deck_counts[k]) if self.deck_counts else "Imported deck"

    def __iter__(self):
        for i, card in enumerate(self._cards):
            if i == self._fail_at:
                msg = "template exploded"
                raise RuntimeError(msg)
            self.rendered += 1
            self.degraded["media"] += card.missing
            yield card


def _deliver(monkeypatch, deck: _FakeDeck, chunk_size: "int | None" = 1000) -> list[tuple[dict, int]]:
    """Each payload posted, with how many cards had been rendered when it was."""
    sent: list[tuple[dict, int]] = []
    monkeypatch.setattr(worker, "_post_callback", lambda payload, _url: sent.append((payload, deck.rendered)))
    worker._post_deck("file-1", deck, CALLBACK, chunk_size)
    return sent


def test_each_chunk_is_sent_before_the_next_is_rendered(monkeypatch):
    sent = _deliver(monkeypatch, _FakeDeck([_card(i) for i in range(2500)]))

    assert [rendered for _payload, rendered in sent] == [1000, 2000, 2500]
    assert [len(payload["cards"]) for payload, _rendered in sent] == [1000, 1000, 500]


def test_every_chunk_carries_the_deck_wide_figures(monkeypatch):
    cards = [_card(i) for i in range(1500)] + [
        _card(i, deck="AnKing::Micro::Bugs", note_type="Cloze") for i in range(1500, 2500)
    ]
    sent = _deliver(monkeypatch, _FakeDeck(cards, skipped=7))

    # What the worker computed when it held the whole rendered deck.
    expected_stats = {
        "total": 2500,
        "subdecks": dict(Counter((c.deck or "Default").split("::")[-1] for c in cards).most_common(50)),
        "note_types": dict(Counter(c.note_type or "Basic" for c in cards).most_common(50)),
    }
    for idx, (payload, _rendered) in enumerate(sent):
        assert payload["chunk_index"] == idx
        assert payload["chunk_count"] == 3
        assert payload["start_index"] == idx * 1000
        assert payload["stats"] == expected_stats
        assert payload["status"] == "partial"
        assert payload["deck_name"] == "AnKing::Pharm"
        assert payload["summary"]["imported"] == 2500
        assert payload["summary"]["skipped"] == 7
    assert [c["uid"] for payload, _ in sent for c in payload["cards"]] == [c.uid for c in cards]


def test_a_stylesheet_rides_on_the_first_card_of_its_note_type_in_each_chunk(monkeypatch):
    cards = [_card(i, note_type="Basic" if i % 2 else "Cloze") for i in range(10)]
    sent = _deliver(monkeypatch, _FakeDeck(cards), chunk_size=4)

    assert len(sent) == 3
    for payload, _rendered in sent:
        seen: set[str] = set()
        for card in payload["cards"]:
            first = card["note_type"] not in seen
            seen.add(card["note_type"])
            assert card["css"] == (f"/* {card['note_type']} */" if first else "")


def test_missing_images_are_counted_in_full_on_the_last_chunk(monkeypatch):
    cards = [_card(i, missing=1 if i % 700 == 0 else 0) for i in range(2500)]
    sent = _deliver(monkeypatch, _FakeDeck(cards))

    assert sent[-1][0]["summary"]["degraded"]["media"] == sum(c.missing for c in cards)


def test_an_empty_deck_is_one_failed_callback(monkeypatch):
    sent = _deliver(monkeypatch, _FakeDeck([]))

    assert len(sent) == 1
    assert sent[0][0]["status"] == "failed"
    assert sent[0][0]["cards"] == []


def test_a_backend_without_chunking_gets_the_whole_deck_in_one_callback(monkeypatch):
    cards = [_card(i) for i in range(5)]
    sent = _deliver(monkeypatch, _FakeDeck(cards), chunk_size=None)

    assert len(sent) == 1
    payload = sent[0][0]
    assert "chunk_index" not in payload
    assert [c["uid"] for c in payload["cards"]] == [c.uid for c in cards]
    assert all(c["css"] for c in payload["cards"]), "the single-callback contract is unchanged"


class _FakeBlobService:
    def get_container_client(self, _name):
        return self

    def get_blob_client(self, _name):
        return self

    def download_blob(self):
        return self

    def readinto(self, fh):
        fh.write(b"PK")

    def close(self):
        pass


def _run_job(monkeypatch, deck: _FakeDeck, post, **job) -> dict:
    """Run one job; returns the options the deck was opened with."""
    opened: dict = {}

    def open_apkg(*_a, **options):
        opened.update(options)
        return contextlib.nullcontext(deck)

    monkeypatch.setattr(
        worker, "BlobServiceClient", types.SimpleNamespace(from_connection_string=lambda _s: _FakeBlobService())
    )
    monkeypatch.setattr(worker, "open_apkg", open_apkg)
    monkeypatch.setattr(worker, "_post_callback", post)
    worker._process_job_sync(
        {"job_id": "file-1", "container_name": "c", "apkg_blob_name": "documents/original/x.apkg", "chunk_size": 2}
        | job
    )
    return opened


def test_a_render_failure_part_way_reports_the_deck_failed(monkeypatch):
    sent: list[dict] = []
    _run_job(monkeypatch, _FakeDeck([_card(i) for i in range(5)], fail_at=3), lambda p, _url: sent.append(p))

    assert [p["status"] for p in sent] == ["ok", "failed"]
    assert len(sent[0]["cards"]) == 2
    assert "template exploded" in sent[1]["error"]


def test_a_callback_that_does_not_land_is_left_to_the_queue(monkeypatch):
    """Not a bad deck: raising lets the queue redeliver instead of failing the deck."""
    sent: list[dict] = []

    def post(payload, _url):
        sent.append(payload)
        msg = "backend down"
        raise httpx.ConnectError(msg)

    with pytest.raises(httpx.ConnectError):
        _run_job(monkeypatch, _FakeDeck([_card(i) for i in range(5)]), post)

    assert [p["status"] for p in sent] == ["ok"], "no failure is reported for a deck that rendered"


@pytest.mark.parametrize(
    ("job", "strips"),
    [({"strip_scripts": True}, True), ({}, False), ({"strip_scripts": "true"}, False), ({"strip_scripts": 1}, False)],
)
def test_scripts_are_stripped_only_when_the_job_says_true(monkeypatch, job, strips):
    opened = _run_job(monkeypatch, _FakeDeck([_card(0)]), lambda _p, _url: None, **job)

    assert opened["strip_scripts"] is strips
