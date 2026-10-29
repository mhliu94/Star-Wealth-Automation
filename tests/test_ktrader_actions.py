"""Adapter-to-journal/FOK integration; all broker and clock operations are fake."""
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
import hashlib

import pytest

from starwealth.errors import OutcomeUnknown
from starwealth.fok import FokExecutor
from starwealth.journal import Journal
from starwealth.ktrader import Inbox, Listener
from starwealth.ktrader_broker import BrokerAdapter, intent_id


def command(**changes):
    return {"type": "LIMIT_ORDER_FOK", "account_id": "STAR-DONG", "command_id": "test-action",
            "symbol": "AAPL", "side": "BUY", "qty_shares": 10, "limit_price": "100.00", **changes}


def row(order_id="new-order", **changes):
    return {"orderId": order_id, "subAccountId": "private-account", "marketCode": "us", "stockCode": "AAPL",
            "direction": 1, "orderType": 3, "orderStatus": 40, "quantity": "10", "filledQuantity": "0",
            "filledPrice": "0", "price": "100.00", "currency": "USD", "expType": 1,
            "submittedTimestamp": 1793203200000, "canCancel": 1, **changes}


class Broker:
    def __init__(self, history):
        self.state = {"account": {"subAcctId": "private-account"}}
        self.history = list(history)
        self.calls = []
        self.cancel_error = None

    def market_status(self):
        return {"status": "OPEN"}

    def prepare(self, order, *, max_notional=None):
        order.validate()
        return {"estimated_notional": str(Decimal(order.quantity) * Decimal(order.price))}

    def submit(self, order, *, journal, intent_id, live, max_notional):
        self.calls.append(("submit", order, intent_id, live))
        if not live:
            return self.prepare(order, max_notional=max_notional)
        journal.reserve(intent_id, "create", {"quantity": order.quantity})
        journal.finish(intent_id, "acknowledged", result={"orderId": "new-order"})
        return {"orderId": "new-order", "accountId": "private-account", "session": "private-session"}

    def order_history(self, **query):
        self.calls.append(("history", query))
        result = self.history.pop(0) if len(self.history) > 1 else self.history[0]
        if isinstance(result, BaseException):
            raise result
        return deepcopy(result)

    def cancel(self, order_id, *, journal, intent_id, live):
        self.calls.append(("cancel", order_id, intent_id, live))
        if live:
            journal.reserve(intent_id, "cancel", {"orderId": order_id})
            if self.cancel_error:
                journal.finish(intent_id, "unknown")
                raise self.cancel_error
            journal.finish(intent_id, "acknowledged", result={"orderId": order_id})
        return {"orderId": order_id}


@pytest.fixture
def journal(tmp_path):
    value = Journal(tmp_path / "orders.sqlite3")
    yield value
    value.close()


def calls(broker, action):
    return [call for call in broker.calls if call[0] == action]


@pytest.mark.parametrize("selector", [
    {"type": "LIMIT_ORDER_FOK"},
    {"type": "LIMIT_ORDER", "time_in_force": "FOK"},
    {"type": "LIMIT_ORDER", "cancel_unfilled": True},
])
def test_three_fok_forms_cancel_only_exact_unfilled_remainder(tmp_path, journal, selector):
    unrelated = row("other-order")
    partial = row(orderStatus=60, filledQuantity="4", filledPrice="99.9")
    canceled = row(orderStatus=80, filledQuantity="4", filledPrice="99.9", canCancel=0)
    broker = Broker([[unrelated, partial], [unrelated, partial], [unrelated, canceled]])
    sleeps = []
    adapter = BrokerAdapter(broker, journal, live=True, execution_path=tmp_path / "fok.sqlite3", sleep=sleeps.append)
    action = command(**selector)
    try:
        result = adapter.execute(action)
        assert result == {"state": "acknowledged", "order_id": "new-order", "status": "CANCELED"}
        assert [call[1] for call in calls(broker, "cancel")] == ["new-order"]
        assert calls(broker, "cancel")[0][2] == FokExecutor.cancel_intent_id(intent_id(action))
        assert len(calls(broker, "submit")) == 1
        assert sleeps == [5, 2]
        assert adapter.execute(action) == result
        assert len(calls(broker, "submit")) == 1
        assert len(calls(broker, "cancel")) == 1
        assert journal.order_correlations() == {"new-order": {"command_id": "test-action"}}
    finally:
        adapter.close()


def test_fok_full_fill_is_never_cancelled(tmp_path, journal):
    broker = Broker([[row(orderStatus=50, filledQuantity="10", filledPrice="100", canCancel=0)]])
    adapter = BrokerAdapter(broker, journal, live=True, execution_path=tmp_path / "fok.sqlite3", sleep=lambda _: None)
    try:
        assert adapter.execute(command())["status"] == "FILLED"
        assert calls(broker, "cancel") == []
    finally:
        adapter.close()


def test_fok_account_mismatch_never_cancels_unknown_order(tmp_path, journal):
    broker = Broker([[row(subAccountId="other-account")]])
    adapter = BrokerAdapter(broker, journal, live=True, execution_path=tmp_path / "fok.sqlite3", sleep=lambda _: None)
    try:
        with pytest.raises(OutcomeUnknown):
            adapter.execute(command())
        assert calls(broker, "cancel") == []
    finally:
        adapter.close()


def test_fok_dry_run_prepares_without_cancellation_or_wait(journal):
    broker = Broker([])
    sleeps = []
    adapter = BrokerAdapter(broker, journal, sleep=sleeps.append)
    result = adapter.execute(command())
    assert result["state"] == "dry_run"
    assert result["synthetic_fok"] is True
    assert not calls(broker, "cancel")
    assert not calls(broker, "history")
    assert sleeps == []
    assert journal.get(intent_id(command())) is None


def test_bulk_cancel_is_symbol_scoped_and_confirms_terminal_state(journal):
    target, other = row("aapl-order"), row("tsla-order", stockCode="TSLA")
    closed = row("aapl-order", orderStatus=70, canCancel=0)
    broker = Broker([[target, other], [target, other], [closed, other]])
    adapter = BrokerAdapter(broker, journal, live=True, sleep=lambda _: None)
    result = adapter.execute(command(type="CANCEL_OPEN_ORDERS"))
    assert result == {"state": "acknowledged", "candidates": 1, "terminal": 1}
    assert [call[1] for call in calls(broker, "cancel")] == ["aapl-order"]
    first_query = calls(broker, "history")[0][1]
    assert first_query["statuses"] == [10, 20, 21, 40, 60]
    assert first_query["from_date"] == "2000-01-01"


def test_bulk_all_symbols_without_symbol(journal):
    first, second = row("a"), row("b", stockCode="TSLA")
    closed_first, closed_second = row("a", orderStatus=70, canCancel=0), row("b", stockCode="TSLA", orderStatus=70, canCancel=0)
    broker = Broker([[first, second], [first, second], [closed_first, second],
                     [closed_first, second], [closed_first, closed_second]])
    action = command(type="CANCEL_OPEN_ORDERS")
    action.pop("symbol")
    result = BrokerAdapter(broker, journal, live=True, sleep=lambda _: None).execute(action)
    assert result["terminal"] == 2
    assert [call[1] for call in calls(broker, "cancel")] == ["a", "b"]


def test_bulk_terminal_race_avoids_cancel(journal):
    broker = Broker([[row()], [row(orderStatus=50, filledQuantity="10", filledPrice="100", canCancel=0)]])
    result = BrokerAdapter(broker, journal, live=True).execute(command(type="CANCEL_OPEN_ORDERS"))
    assert result["terminal"] == 1
    assert calls(broker, "cancel") == []


@pytest.mark.parametrize("flag", [0, None, True, "1"])
def test_bulk_never_cancels_without_integer_eligibility(journal, flag):
    broker = Broker([[row(canCancel=flag)]])
    result = BrokerAdapter(broker, journal, live=True).execute(command(type="CANCEL_OPEN_ORDERS"))
    assert result["candidates"] == 0
    assert calls(broker, "cancel") == []


def test_bulk_route_mismatch_aborts_discovery_before_any_cancel(journal):
    broker = Broker([[row("valid"), row("invalid", subAccountId="other-account")]])
    with pytest.raises(ValueError, match="route"):
        BrokerAdapter(broker, journal, live=True).execute(command(type="CANCEL_OPEN_ORDERS"))
    assert calls(broker, "cancel") == []


@pytest.mark.parametrize("scope", [{"trading_mode": "G"}, {"parent_id": "smart-parent"},
                                   {"client_order_id": "smart-child"}, {"deadline": "2026-10-29T23:00:00Z"}])
def test_bulk_unsupported_scope_does_not_expand_to_symbol_wide_cancel(journal, scope):
    broker = Broker([[row()]])
    with pytest.raises(ValueError):
        BrokerAdapter(broker, journal, live=True, sleep=lambda _: None).execute(command(type="CANCEL_OPEN_ORDERS", **scope))
    assert broker.calls == []


@pytest.mark.parametrize("selector", [
    {"type": "LIMIT_ORDER_FOK", "time_in_force": "IOC"},
    {"type": "LIMIT_ORDER", "time_in_force": "GTC", "cancel_unfilled": True},
    {"type": "LIMIT_ORDER_FOK", "cancel_unfilled": "false"},
])
def test_fok_does_not_silently_overwrite_conflicting_constraints(journal, selector):
    broker = Broker([])
    with pytest.raises(ValueError):
        BrokerAdapter(broker, journal).execute(command(**selector))
    assert broker.calls == []


def test_bulk_prior_cancel_journal_entry_is_reconciled_without_resend(journal):
    action = command(type="CANCEL_OPEN_ORDERS")
    cancellation = "bulk-cancel:" + hashlib.sha256((intent_id(action) + "\0new-order").encode()).hexdigest()
    journal.reserve(cancellation, "cancel", {"orderId": "new-order"})
    journal.finish(cancellation, "unknown")
    broker = Broker([[row()], [row()], [row(orderStatus=70, canCancel=0)]])
    result = BrokerAdapter(broker, journal, live=True, sleep=lambda _: None).execute(action)
    assert result["terminal"] == 1
    assert calls(broker, "cancel") == []


def test_bulk_cancel_timeout_can_be_resolved_by_reads(journal):
    broker = Broker([[row()], [row()], [row(orderStatus=70, canCancel=0)]])
    broker.cancel_error = TimeoutError()
    result = BrokerAdapter(broker, journal, live=True, sleep=lambda _: None).execute(command(type="CANCEL_OPEN_ORDERS"))
    assert result["terminal"] == 1
    assert len(calls(broker, "cancel")) == 1


def test_bulk_unconfirmed_cancel_never_acknowledges_or_resends(journal):
    broker = Broker([[row()]])
    adapter = BrokerAdapter(broker, journal, live=True, sleep=lambda _: None)
    action = command(type="CANCEL_OPEN_ORDERS")
    for _ in range(2):
        with pytest.raises(OutcomeUnknown):
            adapter.execute(action)
    assert len(calls(broker, "cancel")) == 1


def test_disabled_listener_still_executes_cancel(tmp_path, journal):
    broker = Broker([[row(canCancel=0)]])
    adapter = BrokerAdapter(broker, journal, live=True)
    inbox = Inbox(tmp_path / "inbox.sqlite3")
    try:
        inbox.set_enabled("STAR-DONG", False)
        now = datetime.now(timezone.utc)
        listener = Listener(adapter, inbox, string_id="STAR-DONG", numerical_id=24, clock=lambda: now.timestamp())
        action = command(type="CANCEL_OPEN_ORDERS", ts=now.isoformat())
        assert listener.accept(action) == "queued"
        assert listener.execute_due() is True
        state = inbox.db.execute("SELECT state FROM commands WHERE id=?", (action["command_id"],)).fetchone()[0]
        assert state == "acknowledged"
        assert len(calls(broker, "history")) == 1
    finally:
        inbox.close()
