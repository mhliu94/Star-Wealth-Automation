"""US whole-share orders with explicit sessions and no implicit market conversion."""
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
import re
import time
from zoneinfo import ZoneInfo

SESSION_FLAGS = {"regular": None, "extended": 2, "overnight": 4}
ACTIVE = {"regular": {"OPEN"}, "extended": {"OPEN", "PRE_OPEN", "AFTER_OPEN"},
          "overnight": {"NIGHT"}}


class StaleQuoteError(ValueError):
    """A fresh read may resolve this; it never authorizes an order retry."""


def number(value, label):
    if isinstance(value, (float, bool)):
        raise ValueError(f"{label} must be a decimal string or integer")
    try:
        result = Decimal(value)
    except (ValueError, TypeError, InvalidOperation) as exc:
        raise ValueError(f"Invalid {label}") from exc
    if not result.is_finite() or result <= 0:
        raise ValueError(f"{label} must be finite and positive")
    return result


def symbol(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Z][A-Z0-9.-]{0,19}", value):
        raise ValueError("Use an uppercase US equity symbol")
    return value


def quote_timestamp(data, now):
    day = str(data.get("qtDate", ""))
    clock = str(data.get("qtTime", "")).zfill(9)
    if len(day) != 8 or len(clock) != 9:
        raise ValueError("Missing quote timestamp")
    parsed = datetime.strptime(day + clock, "%Y%m%d%H%M%S%f")
    candidates = [parsed.replace(tzinfo=ZoneInfo("America/New_York"), fold=f).timestamp() for f in (0, 1)]
    valid = [v for v in candidates if -2 <= now - v <= 5]
    if not valid:
        raise StaleQuoteError("Quote is stale or in the future; no marketable order was prepared. Use an explicit limit price or refresh later")
    return min(valid, key=lambda v: abs(now - v))


@dataclass(frozen=True)
class Order:
    symbol: str
    side: str
    quantity: str
    kind: str = "limit"
    session: str = "extended"
    validity: str = "day"
    price: str | None = None
    price_bound: str | None = None

    def validate(self):
        symbol(self.symbol)
        if self.side not in {"buy", "sell"}:
            raise ValueError("side must be buy or sell")
        q = number(self.quantity, "quantity")
        if q != q.to_integral_value():
            raise ValueError("This client supports whole shares only")
        if self.session == "all":
            raise ValueError("All-session routing is not verified for this account; select regular, extended, or overnight")
        if self.kind not in {"limit", "market", "marketable-limit"} or self.session not in SESSION_FLAGS:
            raise ValueError("Unsupported order type/session")
        if self.validity not in {"day", "gtc"}:
            raise ValueError("Use day or gtc; IOC/FOK is not implemented")
        if self.kind == "market" and (self.session != "regular" or self.validity != "day"):
            raise ValueError("Native US market orders require regular session and day validity")
        if self.session == "overnight" and self.validity != "day":
            raise ValueError("This client requires day validity for overnight orders")
        if self.kind == "limit":
            number(self.price, "limit price")
            if self.price_bound is not None:
                raise ValueError("Use price for a limit order, not price_bound")
        elif self.kind == "marketable-limit":
            number(self.price_bound, "price bound")
            if self.price is not None:
                raise ValueError("Marketable-limit derives its price from the live book")
        elif self.price is not None or self.price_bound is not None:
            raise ValueError("Native market orders do not take a price")


def build_order(order, market, quote, account, *, now=None, max_notional=None):
    order.validate()
    now = time.time() if now is None else now
    status = market.get("status")
    q = quote.get("quote", {})
    if q.get("mkt") != "us" or q.get("rawSymbol") != order.symbol:
        raise ValueError("Quote identity mismatch")
    if status not in ACTIVE[order.session] or q.get("mktStatus") != status:
        raise ValueError(f"Session {order.session} cannot execute in broker state {status}; refresh or select the active session")
    if q.get("tradable") != 1 or q.get("tradeStatus") != 0:
        raise ValueError("Broker does not report the equity as normally tradable")
    if order.session == "overnight" and q.get("nightTrading") is not True:
        raise ValueError("Broker does not mark this stock as overnight eligible")
    if order.kind in {"market", "marketable-limit"}:
        delays = [q.get("delay"), quote.get("delay")]
        if any(v is True for v in delays) or not any(v is False for v in delays):
            raise ValueError("Confirmed realtime quotes are required for market or marketable-limit orders")
    quantity = number(order.quantity, "quantity")
    price = number(order.price, "price") if order.kind == "limit" else None
    if order.kind in {"market", "marketable-limit"}:
        book = quote.get("orderbook", {})
        quote_timestamp(book, now)
        power = book.get("power")
        if type(power) is not int or not 0 <= power <= 8:
            raise ValueError("Invalid order-book scale")
        buys, sells = book.get("buyOrders", []), book.get("sellOrders", [])
        if not buys or not sells:
            raise ValueError("Two-sided live order book required")
        bid = number(buys[0].get("p"), "bid") / (10 ** power)
        ask = number(sells[0].get("p"), "ask") / (10 ** power)
        if bid > ask:
            raise ValueError("Crossed order book")
        selected = sells[0] if order.side == "buy" else buys[0]
        number(selected.get("v"), "available quote volume")
        estimate = ask if order.side == "buy" else bid
        if order.kind == "marketable-limit":
            bound = number(order.price_bound, "price bound")
            if (order.side == "buy" and estimate > bound) or (order.side == "sell" and estimate < bound):
                raise ValueError("Live price exceeds your price bound")
            # An opposite-side limit can fill immediately but never guarantees execution.
            price = estimate
    else:
        estimate = price
    if price is not None:
        # Conservative price grid; do not silently round a caller's limit.
        tick = Decimal("0.01") if price >= 1 else Decimal("0.0001")
        if price % tick:
            raise ValueError("Price is outside this client's supported precision; use cents above $1")
    if max_notional is not None and quantity * estimate > number(max_notional, "max notional"):
        raise ValueError("Order exceeds max_notional")
    params = {**account, "orderType": 9 if order.kind == "market" else 3,
              "direction": 1 if order.side == "buy" else 2, "marketCode": "US", "stockCode": order.symbol,
              "currency": "USD", "quantity": format(quantity, "f"), "expType": 1 if order.validity == "day" else 2}
    if price is not None:
        params["price"] = format(price, "f")
    flag = SESSION_FLAGS[order.session]
    if flag is not None:
        params["timeInForce"] = flag
    return {"operation": "create", "params": params, "session": order.session, "broker_status": status,
            "requested_type": order.kind, "prepared_at": now,
            "estimated_notional": format(quantity * estimate, "f"),
            "note": "A marketable limit is still a limit order; fills are not guaranteed." if order.kind == "marketable-limit" else ""}
