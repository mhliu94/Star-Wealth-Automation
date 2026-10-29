"""Regression coverage for the native API's case-sensitive query/model contract.

These fixtures are synthetic. Their field names follow the populated native
response shape; no real account values or broker responses are included.
"""
from contextlib import nullcontext
from copy import deepcopy
import json

import pytest

from starwealth.account_data import account_snapshot, normalize_holdings
from starwealth.client import Client
from starwealth.errors import ProtocolError
from starwealth.reporting import Reporter


def native_holding(stock="AAPL", **updates):
    return {
        "marketCode": "us", "stockCode": stock, "symbol": "us" + stock,
        "subAccountId": "S", "currency": "USD", "quantity": "5",
        "quantityAvail": "3", "cost": "31.25", "sellAble": True,
        "buyAble": True, "price": "35", "marketValue": "175",
        "unrealizedProfitLoss": "18.75", "productType": 1, "type": 1,
        **updates,
    }


class NativeBroker:
    """The observed contract returns valid empty pages for an uppercase filter."""
    def __init__(self):
        self.holdings = [native_holding(), native_holding("MSFT", quantity="2", quantityAvail="2",
                                                        marketValue="70", unrealizedProfitLoss="7.50")]
        self.orders = [{"orderId": "synthetic-one", "marketCode": "us", "stockCode": "AAPL"},
                       {"orderId": "synthetic-two", "marketCode": "us", "stockCode": "MSFT"}]
        self.calls = []

    def call(self, operation, params, **_):
        self.calls.append((operation, deepcopy(params)))
        if operation == "asset_summary":
            return {"assetsBreakdown": {"USD": {"currency": "USD", "availableBalance": "123.45"}}}
        assert operation in {"holdings", "orders"}
        rows = self.holdings if operation == "holdings" else self.orders
        if params.get("market") not in (None, ["us"]):
            rows = []
        if params.get("stockCode"):
            rows = [row for row in rows if row["stockCode"] == params["stockCode"]]
        start, count = params.get("start", 0), params.get("count", 100)
        return {"start": start, "count": count, "total": len(rows),
                "list": deepcopy(rows[start:start + count])}


def client_for(state):
    transport = NativeBroker()
    client = Client(state, transport=transport)
    client.auth.ensure_user = lambda: None
    return client, transport


def test_native_lowercase_holdings_filter_preserves_populated_pages(state):
    client, native = client_for(state)
    assert native.call("holdings", {"market": ["US"]})["total"] == 0
    unfiltered = native.call("holdings", {})["list"]
    assert client.holdings(page_size=1) == unfiltered
    assert len(unfiltered) == 2


def test_native_lowercase_history_filter_preserves_all_orders(state):
    client, native = client_for(state)
    assert native.call("orders", {"market": ["US"]})["total"] == 0
    assert client.order_history(page_size=1) == native.orders


def test_direct_orders_command_uses_the_populated_native_route(state):
    client, native = client_for(state)
    assert client.orders("2026-10-29")["list"] == native.orders


@pytest.mark.parametrize("command", ["holdings", "orders"])
def test_cli_reads_use_the_same_populated_native_route(state, monkeypatch, command):
    from starwealth import cli
    client, native = client_for(state)
    monkeypatch.setattr(cli, "profile_lock", lambda _: nullcontext())
    monkeypatch.setattr(cli.Client, "load", lambda *args, **kwargs: client)
    result = cli.run(cli.parser().parse_args([command]))
    rows = result["list"] if isinstance(result, dict) else result
    assert rows == (native.holdings if command == "holdings" else native.orders)


def test_native_cost_and_sellable_fields_produce_positions():
    rows = [native_holding(), native_holding("MSFT", sellAble=False)]
    assert normalize_holdings(rows, sub_account_id="S") == [
        {"symbol": "AAPL", "qty": 5, "avg_price": 31.25, "available_qty": 3},
        {"symbol": "MSFT", "qty": 5, "avg_price": 31.25, "available_qty": 0},
    ]


@pytest.mark.parametrize("change", [
    {"avgCost": "32.00"}, {"sellable": False},
    {"cost": "NaN"}, {"sellAble": "true"},
])
def test_conflicting_or_invalid_native_fields_cannot_produce_cash_only_snapshot(change):
    with pytest.raises(ProtocolError):
        normalize_holdings([native_holding(**change)], sub_account_id="S")


def test_matching_native_and_wrapper_fields_are_unambiguous():
    result = normalize_holdings([native_holding(avgCost="31.2500", sellable=True)], sub_account_id="S")
    assert result[0]["avg_price"] == 31.25
    assert result[0]["available_qty"] == 3


def test_foreign_subaccount_holding_rejects_whole_snapshot(state):
    client, native = client_for(state)
    native.holdings[1]["subAccountId"] = "FOREIGN"
    with pytest.raises(ProtocolError):
        account_snapshot(client, "STAR-DONG", 24, True)


def test_native_broker_to_kafka_payload_contains_stocks(state, tmp_path):
    class CapturedProducer:
        def __init__(self):
            self.sent = []
        def produce(self, topic, *, key, value, on_delivery):
            self.sent.append((topic, key, json.loads(value)))
            on_delivery(None, object())
        def flush(self, timeout):
            return 0

    client, _ = client_for(state)
    producer = CapturedProducer()
    reporter = Reporter(producer, tmp_path / "outbox.sqlite3", account_id="STAR-DONG", account_num_id=24)
    try:
        reporter.publish_account(account_snapshot(client, "STAR-DONG", 24, True))
        assert reporter.flush() == 1
    finally:
        reporter.close()
    topic, key, payload = producer.sent[0]
    assert topic == "account-details"
    assert key == b"STAR-DONG"
    assert payload["cash"] == 123.45
    assert [(position["symbol"], position["qty"]) for position in payload["positions"]] == [("AAPL", 5), ("MSFT", 2)]
    assert all(position["avg_price"] == 31.25 for position in payload["positions"])
    assert "subAccountId" not in json.dumps(payload)
