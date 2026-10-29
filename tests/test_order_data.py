from datetime import datetime, timezone

import pytest

from starwealth.order_data import normalize_order


OBSERVED = datetime(2026, 10, 29, 12, 1, tzinfo=timezone.utc)
SUBMITTED = int(datetime(2026, 10, 29, 12, tzinfo=timezone.utc).timestamp())


def row(**changes):
    return {"orderId": "9007199254740993123", "subAccountId": "test-subaccount",
            "stockCode": "AAPL", "marketCode": "us", "currency": "USD", "direction": 1,
            "orderType": 3, "orderStatus": 40, "quantity": "10", "filledQuantity": "0",
            "filledPrice": "0", "price": "99.10", "submittedTimestamp": SUBMITTED,
            "expType": 1, "canCancel": 1, **changes}


def normalize(value, **kwargs):
    return normalize_order(value, sub_account_id="test-subaccount", observed_at=OBSERVED, **kwargs)


def test_documented_fields_map_without_broker_account_identifiers():
    result = normalize(row(), command_id="command-1", client_order_id="client-1")
    assert result["order_id"] == "9007199254740993123"
    assert result["symbol"] == "AAPL"
    assert result["side"] == "BUY"
    assert result["order_type"] == "LIMIT"
    assert result["status"] == "NEW"
    assert result["limit_price"] == 99.1
    assert result["quantity"] == 10 and result["filled_quantity"] == 0
    assert result["average_fill_price"] is None
    assert result["time_in_force"] == "DAY" and result["cancelable"] is True
    assert result["command_id"] == "command-1" and result["client_order_id"] == "client-1"
    assert "test-subaccount" not in str(result)


@pytest.mark.parametrize("status,expected,filled", [
    (10, "PENDING_NEW", 0), (20, "PENDING_NEW", 0), (21, "PENDING_NEW", 0),
    (40, "NEW", 0), (50, "FILLED", 10), (60, "PARTIALLY_FILLED", 3),
    (70, "CANCELED", 0), (80, "CANCELED", 3), (90, "REJECTED", 0),
    (100, "EXPIRED", 0), (999, "UNKNOWN", 0),
])
def test_documented_status_mapping_preserves_partial_cancellation(status, expected, filled):
    result = normalize(row(orderStatus=status, filledQuantity=str(filled), filledPrice="99.15"))
    assert result["status"] == expected
    assert result["filled_quantity"] == filled
    assert result["average_fill_price"] == (99.15 if filled else None)
    assert result["cancelable"] is (status in {10, 20, 21, 40, 60})


@pytest.mark.parametrize("kind,expected", [(1, "LIMIT"), (3, "LIMIT"), (4, "LIMIT"), (5, "LIMIT"),
    (2, "MARKET"), (9, "MARKET"), (31, "STOP_LIMIT"), (32, "STOP_LIMIT"),
    (33, "TRAILING_STOP"), (34, "STOP"), (35, "OTHER"), (999, "OTHER")])
def test_order_types_and_stop_trigger(kind, expected):
    result = normalize(row(orderType=kind, trigPrice="98.50", direction=2, expType=2))
    assert result["order_type"] == expected
    assert result["side"] == "SELL" and result["time_in_force"] == "GTC"
    assert ("limit_price" in result) is (expected in {"LIMIT", "STOP_LIMIT"})
    assert ("stop_price" in result) is (expected in {"STOP", "STOP_LIMIT"})


@pytest.mark.parametrize("flag", [None, "1", True, 1.0, 2, 0])
def test_cancel_requires_exact_integer_permission(flag):
    assert normalize(row(canCancel=flag))["cancelable"] is False


@pytest.mark.parametrize("changes", [
    {"subAccountId": "another-account"}, {"subAccountId": None}, {"marketCode": "hk"},
    {"direction": True}, {"direction": "1"}, {"orderType": "3"}, {"orderStatus": False},
    {"orderId": 9007199254740993123}, {"quantity": "NaN"}, {"quantity": "-1"},
    {"filledQuantity": "Infinity"}, {"filledPrice": "NaN"}, {"quantity": 0},
    {"filledQuantity": "1e-999"}, {"quantity": "1e999999"},
    {"orderStatus": 50, "filledQuantity": "9"}, {"orderStatus": 60, "filledQuantity": "10"},
    {"filledQuantity": "11"}, {"price": None}, {"orderType": 31, "trigPrice": None},
])
def test_invalid_data_never_claims_an_execution(changes):
    with pytest.raises(ValueError):
        normalize(row(**changes))


def test_seconds_and_milliseconds_have_same_creation_time():
    seconds = normalize(row())
    millis = normalize(row(submittedTimestamp=SUBMITTED * 1000))
    assert seconds["created_at"] == millis["created_at"] == "2026-10-29T12:00:00Z"
    assert seconds["updated_at"] == "2026-10-29T12:01:00Z"


@pytest.mark.parametrize("stamp", [1, -1, 999999999999999999, "1793275200", True, 1793275200.5])
def test_malformed_or_implausible_epoch_is_not_replaced_with_observation(stamp):
    with pytest.raises(ValueError):
        normalize(row(submittedTimestamp=stamp))


@pytest.mark.parametrize("submitted,zone,expected", [
    ("2026-10-29 08:00:00", "America/New_York", "2026-10-29T12:00:00Z"),
    ("2026-10-29T20:00:00", "UTC+08:00", "2026-10-29T12:00:00Z"),
    ("2026-10-29T12:00:00Z", None, "2026-10-29T12:00:00Z"),
])
def test_explicit_local_time_fallback(submitted, zone, expected):
    assert normalize(row(submittedTimestamp=None, submittedTime=submitted, timeZone=zone))["created_at"] == expected


@pytest.mark.parametrize("submitted,zone", [
    ("10/29/26 08:00", "America/New_York"), ("2026-10-29 08:00:00", None),
    ("2026-10-29 08:00:00", "not-a-zone"),
    ("2026-11-01 01:30:00", "America/New_York"),
    ("2026-03-08 02:30:00", "America/New_York"),
])
def test_fallback_rejects_ambiguous_missing_or_nonexistent_time(submitted, zone):
    with pytest.raises(ValueError):
        normalize(row(submittedTimestamp=None, submittedTime=submitted, timeZone=zone))


def test_observation_never_precedes_broker_creation():
    result = normalize(row(submittedTimestamp=SUBMITTED + 120))
    assert result["updated_at"] == result["created_at"]


def test_missing_fill_price_remains_unknown_for_actual_fill():
    result = normalize(row(orderStatus=80, filledQuantity="3", filledPrice=None))
    assert result["filled_quantity"] == 3 and result["average_fill_price"] is None
