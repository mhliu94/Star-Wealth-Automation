"""Normalize Fosun/Star OrderList rows without exposing broker account IDs.

Field/status reference: fosunwealth/fw-trading, fosun_skills/fosun-trading/
doc/OrderList.md. The published timestamp unit is unspecified; accept sane
epoch seconds or milliseconds, never infer order creation from observation.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import math
import re
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .reporting import validate_order


STATUS = {10: "PENDING_NEW", 20: "PENDING_NEW", 21: "PENDING_NEW", 40: "NEW",
          50: "FILLED", 60: "PARTIALLY_FILLED", 70: "CANCELED", 80: "CANCELED",
          90: "REJECTED", 100: "EXPIRED"}
ACTIVE = {10, 20, 21, 40, 60}
TYPES = {1: "LIMIT", 2: "MARKET", 3: "LIMIT", 4: "LIMIT", 5: "LIMIT", 9: "MARKET",
         31: "STOP_LIMIT", 32: "STOP_LIMIT", 33: "TRAILING_STOP", 34: "STOP", 35: "OTHER"}
DATE_TIME = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})?$")


def decimal_number(value, name, *, positive=False):
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError(f"Missing or invalid order {name}")
    try:
        exact = Decimal(str(value))
        if not exact.is_finite() or exact < 0 or (positive and exact <= 0):
            raise ValueError()
        approximate = float(exact)
        if not math.isfinite(approximate) or (exact != 0 and approximate == 0):
            raise ValueError()
        converted = int(exact) if exact == exact.to_integral_value() else approximate
        return converted
    except (ValueError, InvalidOperation, OverflowError) as exc:
        raise ValueError(f"Missing or invalid order {name}") from exc


def sane_created(value):
    if not 2000 <= value.year < 2100:
        raise ValueError("Order creation timestamp is outside the supported range")
    return value.astimezone(timezone.utc)


def creation_time(row):
    stamp = row.get("submittedTimestamp")
    if stamp not in (None, "", 0):
        if type(stamp) is not int:
            raise ValueError("submittedTimestamp must be integer epoch seconds or milliseconds")
        try:
            return sane_created(datetime.fromtimestamp(stamp / 1000 if stamp >= 10**12 else stamp, timezone.utc))
        except (ValueError, OverflowError, OSError) as exc:
            raise ValueError("Invalid order creation timestamp") from exc
    if isinstance(stamp, bool):
        raise ValueError("Invalid order creation timestamp")
    value = row.get("submittedTime")
    if not isinstance(value, str) or not DATE_TIME.fullmatch(value):
        raise ValueError("Order creation time is missing or not an explicit date-time")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        return sane_created(parsed)
    name = row.get("timeZone")
    if not isinstance(name, str) or not name:
        raise ValueError("Order creation time requires a timezone")
    offset = re.fullmatch(r"(?:UTC|GMT)([+-])(\d{2}):(\d{2})", name)
    try:
        if offset:
            hours, minutes = int(offset[2]), int(offset[3])
            if hours > 23 or minutes > 59:
                raise ValueError()
            zone = timezone((1 if offset[1] == "+" else -1) * timedelta(hours=hours, minutes=minutes))
        else:
            zone = ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError("Unsupported order timezone") from exc
    first, second = parsed.replace(tzinfo=zone, fold=0), parsed.replace(tzinfo=zone, fold=1)
    if first.utcoffset() != second.utcoffset() or first.astimezone(timezone.utc).astimezone(zone).replace(tzinfo=None) != parsed:
        raise ValueError("Order creation time is ambiguous or nonexistent in its timezone")
    return sane_created(first)


def normalize_order(row, *, sub_account_id, observed_at=None, command_id=None, client_order_id=None):
    if not isinstance(row, dict) or row.get("subAccountId") != sub_account_id or not isinstance(sub_account_id, str) or not sub_account_id:
        raise ValueError("Order account route mismatch")
    if str(row.get("marketCode", "")).upper() != "US":
        raise ValueError("Only US order rows are supported")
    direction, raw_type, raw_status = row.get("direction"), row.get("orderType"), row.get("orderStatus")
    if type(direction) is not int or direction not in (1, 2):
        raise ValueError("Invalid order direction")
    if type(raw_type) is not int or type(raw_status) is not int:
        raise ValueError("Invalid order type or status")
    created = creation_time(row)
    if observed_at is None:
        observed_at = datetime.now(timezone.utc)
    elif isinstance(observed_at, str):
        observed_at = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
    if not isinstance(observed_at, datetime) or observed_at.utcoffset() is None:
        raise ValueError("Order observation time requires a timezone")
    filled = decimal_number(row.get("filledQuantity"), "filledQuantity")
    raw_price = row.get("filledPrice")
    fill_price = decimal_number(raw_price, "filledPrice") if raw_price not in (None, "") else None
    result = {
        "order_id": row.get("orderId"), "symbol": row.get("stockCode"),
        "created_at": created.isoformat().replace("+00:00", "Z"),
        "updated_at": max(created, observed_at.astimezone(timezone.utc)).isoformat().replace("+00:00", "Z"),
        "side": "BUY" if direction == 1 else "SELL", "order_type": TYPES.get(raw_type, "OTHER"),
        "status": STATUS.get(raw_status, "UNKNOWN"),
        "quantity": decimal_number(row.get("quantity"), "quantity", positive=True),
        "filled_quantity": filled, "average_fill_price": fill_price if filled else None,
    }
    if result["order_type"] in {"LIMIT", "STOP_LIMIT"}:
        result["limit_price"] = decimal_number(row.get("price"), "price", positive=True)
    if result["order_type"] in {"STOP", "STOP_LIMIT"}:
        result["stop_price"] = decimal_number(row.get("trigPrice"), "trigPrice", positive=True)
    if row.get("currency"):
        result["currency"] = row["currency"]
    if type(row.get("expType")) is int and row["expType"] in (1, 2):
        result["time_in_force"] = "DAY" if row["expType"] == 1 else "GTC"
    if command_id is not None:
        result["command_id"] = command_id
    if client_order_id is not None:
        result["client_order_id"] = client_order_id
    if raw_status not in STATUS:
        result["message"] = "Unmapped broker order status"
    result = validate_order(result)
    result["cancelable"] = type(row.get("canCancel")) is int and row["canCancel"] == 1 and raw_status in ACTIVE
    return result
