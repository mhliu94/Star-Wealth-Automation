from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from starwealth.account_data import account_snapshot, cash_balances, normalize_holdings
from starwealth.client import Client
from starwealth.errors import ProtocolError
from starwealth.transport import OPERATIONS


def summary():
    return {"assetsBreakdown": {"USD": {"currency": "USD", "availableBalance": "123.45"},
                                "HKD": {"currency": "HKD", "availableBalance": "-10.5"}},
            "assetsSummary": {"availableBalance": {"USD": "999999.00"},
                              "cashPurchasingPower": {"USD": "999999.00"}}}


def position(**updates):
    return {"marketCode": "us", "stockCode": "AAPL", "currency": "USD", "quantity": "5",
            "quantityAvail": "3", "avgCost": "123.45", "sellable": True, **updates}


def page(rows, *, start=0, total=None, **updates):
    return {"list": rows, "start": start, "count": len(rows),
            "total": len(rows) if total is None else total, **updates}


def reader(state, pages):
    client = Client(state)
    calls = []
    def read(operation, params):
        calls.append((operation, params))
        return pages.pop(0)
    client.read = read
    return client, calls


def test_cash_is_native_not_converted_total_or_purchasing_power():
    assert cash_balances(summary()) == {"USD": 123.45, "HKD": -10.5}


@pytest.mark.parametrize("value", [None, True, {}, [], "NaN", "Infinity", "1e9999", "1e-9999"])
def test_invalid_cash_never_becomes_zero(value):
    data = summary()
    data["assetsBreakdown"]["USD"]["availableBalance"] = value
    with pytest.raises(ProtocolError):
        cash_balances(data)


@pytest.mark.parametrize("data", [None, {}, {"totalAssets": {"USD": "100"}}, {"assetsBreakdown": {}},
    {"assetsBreakdown": {"usd": {"availableBalance": "1"}}},
    {"assetsBreakdown": {"USD": {"currency": "HKD", "availableBalance": "1"}}}])
def test_missing_native_cash_or_bad_currency_fails(data):
    with pytest.raises(ProtocolError):
        cash_balances(data)


def test_zero_and_decimal_cash_valid():
    data = {"assetsBreakdown": {"USD": {"availableBalance": Decimal("0")}}}
    assert cash_balances(data) == {"USD": 0.0}


def test_holdings_map_verified_fields_and_available_quantity():
    assert normalize_holdings([position()]) == [
        {"symbol": "AAPL", "qty": 5.0, "avg_price": 123.45, "available_qty": 3.0}]
    assert normalize_holdings([position(sellable=False)])[0]["available_qty"] == 0
    assert normalize_holdings([position(quantity="0", quantityAvail="0", avgCost="0")]) == []


@pytest.mark.parametrize("change", [
    {"marketCode": "HK"}, {"marketCode": None}, {"stockCode": "aapl"}, {"stockCode": None},
    {"currency": "HKD"}, {"quantity": None}, {"quantityAvail": None}, {"quantityAvail": "6"},
    {"quantityAvail": "-1"}, {"avgCost": "NaN"}, {"avgCost": "-1"},
    {"sellable": 1}, {"sellable": None},
])
def test_holdings_schema_differences_fail_instead_of_inventing_fields(change):
    with pytest.raises(ProtocolError):
        normalize_holdings([position(**change)])


def test_duplicate_positions_are_not_double_counted():
    with pytest.raises(ProtocolError, match="duplicate"):
        normalize_holdings([position(), position()])


def test_account_snapshot_has_routing_and_no_guessed_available_cash():
    class Broker:
        asset_summary = staticmethod(summary)
        holdings = staticmethod(lambda: [position()])
    data = account_snapshot(Broker(), "STAR-DONG", 24, True)
    assert data["account_id"] == "STAR-DONG"
    assert data["account_num_id"] == 24
    assert data["cash"] == 123.45
    assert data["positions"][0]["available_qty"] == 3
    assert data["trading_enabled"] is True
    assert "available_cash_by_currency" not in data
    assert datetime.fromisoformat(data["ts"]).tzinfo == timezone.utc


def test_snapshot_requires_confirmed_usd_balance():
    class Broker:
        asset_summary = staticmethod(lambda: {"assetsBreakdown": {"HKD": {"availableBalance": "50"}}})
        holdings = staticmethod(list)
    with pytest.raises(ProtocolError, match="USD"):
        account_snapshot(Broker(), "STAR-DONG", 24, True)


def test_asset_summary_endpoint_and_identity(state):
    client, calls = reader(state, [summary()])
    assert client.asset_summary() == summary()
    assert calls == [("asset_summary", client.account_params())]
    assert OPERATIONS["asset_summary"] == ("tapi.fosunhanig.com", "/portfolio/v3/AssetSummary", "user")


def test_complete_holdings_pagination_advances_by_returned_records(state):
    rows = [position(stockCode=stock) for stock in ("AAPL", "TSLA", "GOOG")]
    client, calls = reader(state, [page(rows[:2], total=3), page(rows[2:], start=2, total=3)])
    assert client.holdings(page_size=2) == rows
    assert [params["start"] for _, params in calls] == [0, 2]
    assert [params["count"] for _, params in calls] == [2, 2]
    assert all(operation == "holdings" for operation, _ in calls)


def test_orders_range_and_statuses_apply_to_every_page(state):
    rows = [{"orderId": "one"}, {"orderId": "two"}]
    client, calls = reader(state, [page(rows[:1], total=2), page(rows[1:], start=1, total=2)])
    assert client.order_history(from_date="2026-10-01", to_date="2026-10-29",
                                statuses=[40, 60], stock_code="AAPL", page_size=1) == rows
    for operation, params in calls:
        assert operation == "orders"
        assert params["fromDate"] == "2026-10-01"
        assert params["toDate"] == "2026-10-29"
        assert params["statusArr"] == [40, 60]
        assert params["stockCode"] == "AAPL"
        assert params["market"] == ["US"]


def test_default_lookback_includes_90_calendar_days(state):
    client, calls = reader(state, [page([])])
    assert client.order_history(to_date="2026-10-29") == []
    assert calls[0][1]["fromDate"] == "2026-08-01"


def test_native_empty_order_list_can_be_null(state):
    client, _ = reader(state, [{"list": None, "count": 0, "start": 0, "total": 0}])
    assert client.order_history() == []


@pytest.mark.parametrize("operation,rows", [("holdings", []), ("orders", None)])
def test_native_empty_pages_echo_requested_capacity(state, operation, rows):
    client, _ = reader(state, [{"list": rows, "count": 100, "start": 0, "total": 0}])
    result = client.holdings() if operation == "holdings" else client.order_history()
    assert result == []


def test_native_partial_pages_advance_by_records_not_echoed_capacity(state):
    rows = [{"orderId": "one"}, {"orderId": "two"}, {"orderId": "three"}]
    client, calls = reader(state, [page(rows[:2], total=3, count=100),
                                   page(rows[2:], start=2, total=3, count=100)])
    assert client.order_history() == rows
    assert [params["start"] for _, params in calls] == [0, 2]


def test_count_cannot_be_neither_capacity_nor_actual_records(state):
    client, _ = reader(state, [page([{"orderId": "one"}], count=2)])
    with pytest.raises(ProtocolError, match="count"):
        client.order_history()


@pytest.mark.parametrize("broken", [
    None, {}, {"list": [], "start": 0, "count": 0, "total": True},
    {"list": [], "start": 1, "count": 0, "total": 1},
    {"list": [], "start": 0, "count": 1, "total": 1},
    {"list": None, "start": 0, "count": 0, "total": 1},
    {"list": [], "start": 0, "count": 0, "total": 1},
    {"list": [{}], "start": 0, "count": 1, "total": 1},
    {"list": [{"orderId": "one"}], "start": 0, "count": 1, "total": 0},
])
def test_incomplete_or_malformed_pages_raise_not_partial_results(state, broken):
    client, _ = reader(state, [broken])
    with pytest.raises(ProtocolError):
        client.order_history()


def test_changed_total_mid_pagination_fails(state):
    client, _ = reader(state, [page([{"orderId": "one"}], total=2),
                               page([{"orderId": "two"}], start=1, total=3)])
    with pytest.raises(ProtocolError, match="changed"):
        client.order_history(page_size=1)


def test_repeated_record_mid_pagination_fails(state):
    client, _ = reader(state, [page([{"orderId": "one"}], total=2),
                               page([{"orderId": "one"}], start=1, total=2)])
    with pytest.raises(ProtocolError, match="repeated"):
        client.order_history(page_size=1)


def test_max_pages_does_not_return_truncated_history(state):
    client, _ = reader(state, [page([{"orderId": "one"}], total=2)])
    with pytest.raises(ProtocolError, match="max_pages"):
        client.order_history(page_size=1, max_pages=1)


@pytest.mark.parametrize("options", [
    {"from_date": "2026-11-01", "to_date": "2026-10-01"}, {"from_date": "bad"},
    {"lookback_days": 0}, {"lookback_days": True}, {"statuses": []}, {"statuses": [True]},
    {"statuses": [999]}, {"page_size": 101}, {"page_size": True}, {"max_pages": 0},
    {"stock_code": "aapl"}, {"stock_code": []}, {"stock_code": "AAPL;command"},
])
def test_invalid_history_query_never_reads_broker(state, options):
    client, calls = reader(state, [])
    with pytest.raises(ValueError):
        client.order_history(**options)
    assert calls == []
