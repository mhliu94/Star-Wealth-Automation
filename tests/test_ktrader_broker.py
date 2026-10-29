from decimal import Decimal

import pytest

from starwealth.errors import DuplicateIntent
from starwealth.journal import Journal
from starwealth.ktrader_broker import BrokerAdapter, intent_id
from starwealth.orders import StaleQuoteError


class Broker:
    def __init__(self, status="OPEN", share_price="100.02"):
        self.status = status
        self.share_price = Decimal(share_price)
        self.reads = 0
        self.prepared = []
        self.submitted = []

    def market_status(self):
        self.reads += 1
        return {"status": self.status}

    def prepare(self, order, *, max_notional=None):
        order.validate()
        self.prepared.append(order)
        price = Decimal(order.price) if order.price else self.share_price
        estimate = Decimal(order.quantity) * price
        if max_notional is not None and estimate > Decimal(max_notional):
            raise ValueError("Order exceeds max_notional")
        return {"estimated_notional": str(estimate), "params": {"accountId": "private-account"}}

    def submit(self, order, *, journal, intent_id, live, max_notional):
        self.submitted.append((order, live, max_notional))
        plan = self.prepare(order, max_notional=max_notional)
        if live:
            journal.reserve(intent_id, "create", {"quantity": order.quantity})
            journal.finish(intent_id, "acknowledged")
            return {"orderId": "private-order", "accountId": "private-account", "session": "private-session"}
        return plan


def command(**updates):
    return {"type": "MARKET_ORDER", "account_id": "STAR-DONG", "command_id": "order-1",
            "symbol": "AAPL", "side": "BUY", "qty_shares": 2, **updates}


@pytest.fixture
def journal(tmp_path):
    value = Journal(tmp_path / "orders.sqlite3")
    yield value
    value.close()


@pytest.mark.parametrize("status,session", [("OPEN", "regular"), ("PRE_OPEN", "extended"),
                                           ("AFTER_OPEN", "extended"), ("NIGHT", "overnight")])
def test_limit_selects_active_session_and_preserves_price(journal, status, session):
    broker = Broker(status)
    outcome = BrokerAdapter(broker, journal).execute(command(type="LIMIT_ORDER", limit_price=99.99))
    order, live, cap = broker.submitted[0]
    assert (order.kind, order.session, order.price, order.quantity) == ("limit", session, "99.99", "2")
    assert cap == "199.98"
    assert live is False
    assert outcome["state"] == "dry_run"
    assert "private-account" not in str(outcome)


@pytest.mark.parametrize("status", ["PRE_OPEN", "AFTER_OPEN", "NIGHT", "CLOSED", "UNKNOWN"])
def test_market_rejects_outside_regular_without_converting(journal, status):
    broker = Broker(status)
    with pytest.raises(ValueError):
        BrokerAdapter(broker, journal).execute(command())
    assert broker.submitted == []


@pytest.mark.parametrize("changes", [
    {"qty_shares": 0}, {"qty_shares": -1}, {"qty_shares": True}, {"qty_shares": 1.25},
    {"qty_shares": "NaN"}, {"qty_shares": "Infinity"}, {"qty_shares": None},
    {"notional_usd": 100}, {"side": "buy"}, {"side": []}, {"symbol": "AAPL;command"},
    {"command_id": ""}, {"account_id": None}, {"limit_price": 90},
    {"type": "LIMIT_ORDER", "limit_price": "-10"},
    {"type": "LIMIT_ORDER", "qty_shares": None, "notional_usd": 100, "limit_price": 90},
])
def test_invalid_order_rejected_before_broker_reads(journal, changes):
    broker = Broker()
    with pytest.raises(ValueError):
        BrokerAdapter(broker, journal).execute(command(**changes))
    assert broker.reads == 0
    assert broker.submitted == []


@pytest.mark.parametrize("kind", ["CURRENCY_CONVERSION", "START_ALGO_TRADING", "STOP_ALGO_TRADING"])
def test_unsupported_commands_cannot_degrade_to_regular_orders(journal, kind):
    broker = Broker()
    with pytest.raises(ValueError, match="Unsupported"):
        BrokerAdapter(broker, journal).execute(command(type=kind))
    assert broker.reads == 0


def test_notional_market_uses_fresh_book_and_floors_whole_shares(journal):
    broker = Broker(share_price="100.02")
    outcome = BrokerAdapter(broker, journal).execute(command(qty_shares=None, notional_usd=250))
    assert broker.prepared[0].quantity == "1"
    assert outcome["quantity"] == "2"
    assert outcome["max_notional"] == "250"
    assert broker.submitted[0][2] == "250"


def test_notional_does_not_round_up_an_unaffordable_share(journal):
    broker = Broker()
    with pytest.raises(ValueError, match="insufficient"):
        BrokerAdapter(broker, journal).execute(command(qty_shares=None, notional_usd=100))
    assert broker.submitted == []


def test_notional_requires_successful_realtime_preparation(journal):
    broker = Broker()
    def stale(*args, **kwargs):
        raise StaleQuoteError("stale")
    broker.prepare = stale
    with pytest.raises(StaleQuoteError):
        BrokerAdapter(broker, journal).execute(command(qty_shares=None, notional_usd=250))
    assert broker.submitted == []


def test_configured_cap_blocks_limit_and_amount_before_reads(journal):
    broker = Broker()
    adapter = BrokerAdapter(broker, journal, max_notional="100")
    with pytest.raises(ValueError, match="max_notional"):
        adapter.execute(command(type="LIMIT_ORDER", limit_price="100"))
    with pytest.raises(ValueError, match="max_notional"):
        adapter.execute(command(qty_shares=None, notional_usd=101))
    assert broker.reads == 0


def test_share_market_passes_configured_cap_to_existing_checks(journal):
    broker = Broker()
    with pytest.raises(ValueError, match="max_notional"):
        BrokerAdapter(broker, journal, max_notional="100").execute(command())
    assert broker.submitted[0][2] == "100"
    assert journal.get(intent_id(command())) is None


def test_share_market_without_configured_cap_uses_book_estimate(journal):
    broker = Broker()
    outcome = BrokerAdapter(broker, journal).execute(command(qty_shares=2.0))
    assert outcome["max_notional"] == "200.04"
    assert outcome["quantity"] == "2"


def test_live_redelivery_cannot_resubmit_even_after_adapter_restart(journal):
    broker = Broker()
    first = BrokerAdapter(broker, journal, live=True).execute(command())
    assert first["state"] == "acknowledged"
    assert first["order_id"] == "private-order"
    assert "private-account" not in str(first)
    assert "private-session" not in str(first)
    reads = broker.reads
    with pytest.raises(DuplicateIntent):
        BrokerAdapter(broker, journal, live=True).execute(command())
    assert broker.reads == reads
    assert len(broker.submitted) == 1


def test_delayed_orders_share_market_semantics(journal):
    broker = Broker()
    outcome = BrokerAdapter(broker, journal).execute(command(type="DELAYED_MARKET_ORDER"))
    assert outcome["kind"] == "market"
    assert outcome["session"] == "regular"


def test_intents_are_stable_bounded_and_account_scoped():
    value = command(command_id="x" * 5000)
    assert intent_id(value) == intent_id(dict(value))
    assert len(intent_id(value)) < 128
    assert intent_id(value) != intent_id({**value, "account_id": "another-account"})
    assert intent_id(command(account_id="a:b", command_id="c")) != intent_id(command(account_id="a", command_id="b:c"))


@pytest.mark.parametrize("option", [
    {"trading_mode": "G"}, {"parent_id": "parent"}, {"client_order_id": "child"},
    {"cancel_unfilled": True}, {"cancel_unfilled": "false"},
    {"time_in_force": "FOK"}, {"time_in_force": "IOC"}, {"time_in_force": "GTC"},
    {"deadline": "2099-01-01T00:00:00Z"},
])
def test_unsupported_execution_constraints_rejected_before_reads(journal, option):
    broker = Broker()
    with pytest.raises(ValueError):
        BrokerAdapter(broker, journal).execute(command(**option))
    assert broker.reads == 0


def test_day_order_with_no_automatic_cancellation_is_supported(journal):
    broker = Broker()
    result = BrokerAdapter(broker, journal).execute(command(time_in_force="DAY", cancel_unfilled=False))
    assert result["state"] == "dry_run"
