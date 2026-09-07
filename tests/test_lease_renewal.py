"""A deck that takes longer than the visibility timeout must keep its lease.

Without renewal the queue made the message visible again mid-render, so a large
deck was rendered a SECOND time on the same replica while the first run was still
going — and the run that finished could not delete the message, because its pop
receipt had been invalidated by the redelivery. The deck was therefore re-rendered
and re-fanned-out (up to 100 chunk POSTs a pass) on every redelivery until the
24h TTL, with the retry budget consumed by runs that had SUCCEEDED — so at
dequeue_count >= 3 the student got a spurious status:"failed" for a deck that had
imported fine three times over.

`renderer` is stubbed here because it pulls in the AGPL anki package to RENDER,
and nothing in these tests renders. The coroutines are driven with `asyncio.run`
rather than a marker, so this suite still needs nothing but pytest itself.
"""

import asyncio
import dataclasses
import os
import sys
import types

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault("AZURE_STORAGE_CONNECTION_STRING", "UseDevelopmentStorage=true")
os.environ.setdefault("ANKI_CALLBACK_URL", "https://backend.invalid/api/v1/anki/callback")
os.environ.setdefault("ANKI_CALLBACK_SECRET", "test-secret")

_renderer = types.ModuleType("renderer")
_renderer.RenderResult = object
_renderer.render_apkg = lambda *a, **k: None  # noqa: ARG005
sys.modules.setdefault("renderer", _renderer)

import worker  # noqa: E402
from worker import AnkiWorker  # noqa: E402


@pytest.fixture(autouse=True)
def _fast_renewals(monkeypatch):
    """Renew every 10ms instead of every 300s, without changing the lease length.

    The interval is `max(visibility // DIVISOR, MIN)`, so a huge divisor leaves
    the visibility timeout the worker actually asks Azure for untouched — which
    is the value these tests assert on.
    """
    monkeypatch.setattr(worker, "_LEASE_RENEW_DIVISOR", 10**6)
    monkeypatch.setattr(worker, "_LEASE_RENEW_MIN_SECONDS", 0.01)


class _Message:
    def __init__(self, receipt: str = "receipt-0", dequeue_count: int = 1) -> None:
        self.id = "msg-1"
        self.pop_receipt = receipt
        self.dequeue_count = dequeue_count
        self.content = "{}"


class _FakeQueueClient:
    """Rejects a delete carrying a stale receipt, exactly as the service does."""

    def __init__(self, *, renew_fails: bool = False) -> None:
        self.renewals: list[int] = []
        self.deleted: list[str] = []
        self.events: list[str] = []
        self._issued = 0
        self._live_receipt = "receipt-0"
        self._renew_fails = renew_fails

    async def update_message(self, message, visibility_timeout=None):
        if self._renew_fails:
            msg = "lease renewal exploded"
            raise RuntimeError(msg)
        self._issued += 1
        self._live_receipt = f"receipt-{self._issued}"
        self.renewals.append(visibility_timeout)
        self.events.append("renew")
        return types.SimpleNamespace(pop_receipt=self._live_receipt)

    async def delete_message(self, message):
        self.events.append("delete")
        if message.pop_receipt != self._live_receipt:
            msg = f"stale pop receipt {message.pop_receipt!r} (live: {self._live_receipt!r})"
            raise RuntimeError(msg)
        self.deleted.append(message.pop_receipt)


def _worker(queue: _FakeQueueClient) -> AnkiWorker:
    w = AnkiWorker()
    w.queue_client = queue
    return w


def _job(seconds: float, *, fail: bool = False):
    """Stand-in for the blocking render, which runs in a thread."""

    def run(_message_data):
        import time

        time.sleep(seconds)
        if fail:
            msg = "render blew up"
            raise RuntimeError(msg)

    return run


def test_lease_is_renewed_while_the_deck_renders(monkeypatch):
    async def scenario():
        queue = _FakeQueueClient()
        monkeypatch.setattr(worker, "_process_job_sync", _job(0.12))

        await _worker(queue)._process_and_cleanup(_Message(), {})

        assert len(queue.renewals) >= 2, "a job outliving the window must be renewed more than once"
        assert set(queue.renewals) == {worker.settings.visibility_timeout_seconds}

    asyncio.run(scenario())


def test_delete_uses_the_refreshed_receipt(monkeypatch):
    """The renewed receipt must be written back onto the message we delete with.

    Azure invalidates the previous receipt on every update, so renewing and then
    deleting with the ORIGINAL receipt fails the delete — the same outcome as
    never renewing at all, and the reason the deck was re-rendered.
    """

    async def scenario():
        monkeypatch.setattr(worker, "_process_job_sync", _job(0.05))
        queue = _FakeQueueClient()
        message = _Message()

        await _worker(queue)._process_and_cleanup(message, {})

        assert queue.renewals, "nothing was renewed, so this proves nothing"
        assert message.pop_receipt != "receipt-0"
        assert queue.deleted == [message.pop_receipt]

    asyncio.run(scenario())


def test_no_renewal_races_the_delete(monkeypatch):
    """A renewal in flight during the delete would invalidate the receipt mid-call."""

    async def scenario():
        monkeypatch.setattr(worker, "_process_job_sync", _job(0.05))
        queue = _FakeQueueClient()

        await _worker(queue)._process_and_cleanup(_Message(), {})

        assert queue.renewals
        assert queue.events[-1] == "delete"
        assert queue.events.count("delete") == 1

    asyncio.run(scenario())


def test_renewer_does_not_outlive_the_job(monkeypatch):
    async def scenario():
        monkeypatch.setattr(worker, "_process_job_sync", _job(0.05))
        queue = _FakeQueueClient()

        await _worker(queue)._process_and_cleanup(_Message(), {})
        settled = len(queue.renewals)
        await asyncio.sleep(0.08)

        assert len(queue.renewals) == settled, "the renewal task leaked past its job"

    asyncio.run(scenario())


def test_lease_is_held_across_the_abandon_callback(monkeypatch):
    """The last attempt posts a failure (with retries) before deleting — still leased."""

    async def scenario():
        monkeypatch.setattr(worker, "_process_job_sync", _job(0.02, fail=True))
        posted: list[str] = []

        def _abandon(message_data, attempts, exc):  # noqa: ARG001
            import time

            time.sleep(0.05)
            posted.append("failed")

        monkeypatch.setattr(worker, "_post_job_abandoned", _abandon)
        queue = _FakeQueueClient()

        await _worker(queue)._process_and_cleanup(_Message(dequeue_count=worker.settings.max_retries), {})

        assert posted == ["failed"]
        assert queue.deleted, "the poisoned message must still be deletable after the callback"
        assert queue.events[-1] == "delete"

    asyncio.run(scenario())


def test_a_retryable_failure_leaves_the_message_and_stops_renewing(monkeypatch):
    """Below the budget the message is left to redeliver — but not left leased."""

    async def scenario():
        monkeypatch.setattr(worker, "_process_job_sync", _job(0.02, fail=True))
        queue = _FakeQueueClient()

        await _worker(queue)._process_and_cleanup(_Message(dequeue_count=1), {})
        settled = len(queue.renewals)
        await asyncio.sleep(0.05)

        assert queue.deleted == []
        assert len(queue.renewals) == settled

    asyncio.run(scenario())


def test_a_failing_renewal_does_not_fail_the_job(monkeypatch):
    """Renewal is best-effort: losing it costs a redelivery, not the deck."""

    async def scenario():
        monkeypatch.setattr(worker, "_process_job_sync", _job(0.05))
        queue = _FakeQueueClient(renew_fails=True)

        await _worker(queue)._process_and_cleanup(_Message(), {})

        assert queue.deleted == ["receipt-0"], "the original receipt is still the live one"

    asyncio.run(scenario())


def test_the_cadence_is_checked_against_the_window(monkeypatch):
    """A window too short to renew inside is the original bug, silently back.

    The window is a deployment knob and the cadence is code, so nothing but a
    startup check keeps the two in a working relationship.
    """
    monkeypatch.setattr(worker, "_LEASE_RENEW_DIVISOR", 3)
    monkeypatch.setattr(worker, "_LEASE_RENEW_MIN_SECONDS", 30)
    monkeypatch.setattr(worker, "settings", dataclasses.replace(worker.settings, visibility_timeout_seconds=45))

    with pytest.raises(RuntimeError, match="too short for the lease cadence"):
        worker._assert_lease_cadence()


def test_the_shipped_defaults_pass_that_check(monkeypatch):
    monkeypatch.setattr(worker, "_LEASE_RENEW_DIVISOR", 3)
    monkeypatch.setattr(worker, "_LEASE_RENEW_MIN_SECONDS", 30)

    worker._assert_lease_cadence()

    assert worker._lease_renew_interval() * 2 <= worker.settings.visibility_timeout_seconds


def test_setup_refuses_to_start_on_a_bad_window(monkeypatch):
    """It must fail at startup, not on the first deck that needs renewing."""
    monkeypatch.setattr(worker, "_LEASE_RENEW_DIVISOR", 3)
    monkeypatch.setattr(worker, "_LEASE_RENEW_MIN_SECONDS", 30)
    monkeypatch.setattr(worker, "settings", dataclasses.replace(worker.settings, visibility_timeout_seconds=10))

    async def scenario():
        with pytest.raises(RuntimeError, match="too short for the lease cadence"):
            await AnkiWorker().setup()

    asyncio.run(scenario())
