"""Read-only diagnostics: distinguish the active session from the regular close."""
from datetime import datetime, timezone
import time
from zoneinfo import ZoneInfo


def age_seconds(data, now):
    day = str(data.get("qtDate", ""))
    clock = str(data.get("qtTime", "")).zfill(9)
    if len(day) != 8 or len(clock) != 9:
        return None
    try:
        parsed = datetime.strptime(day + clock, "%Y%m%d%H%M%S%f")
    except ValueError:
        return None
    candidates = [parsed.replace(tzinfo=ZoneInfo("America/New_York"), fold=f).timestamp() for f in (0, 1)]
    return min((now - value for value in candidates), key=abs)


def freshness_report(stock, market, response, *, now=None):
    now = time.time() if now is None else now
    quote = response.get("quote", {})
    status = market.get("status")
    field = "nightQuote" if status == "NIGHT" else "prepost" if status in {"PRE_OPEN", "AFTER_OPEN"} else None
    active = quote.get(field, {}) if field else quote
    book_age = age_seconds(response.get("orderbook", {}), now)
    flags = [response.get("delay"), quote.get("delay")]
    def rounded(value):
        return round(value, 3) if value is not None else None
    return {
        "symbol": stock,
        "received_at_utc": datetime.fromtimestamp(now, timezone.utc).isoformat(),
        "broker_status": status,
        "quote_status_matches": quote.get("mktStatus") == status,
        "quote_identity_matches": quote.get("mkt") == "us" and quote.get("rawSymbol") == stock,
        "delay_flags": {"response": flags[0], "quote": flags[1]},
        "realtime_indicated": not any(value is True for value in flags) and any(value is False for value in flags),
        "book_age_seconds": rounded(book_age),
        "book_timestamp_fresh": book_age is not None and -2 <= book_age <= 5,
        "maximum_book_age_seconds": 5,
        "active_last_trade_field": "quote." + field if field else "quote",
        "active_last_trade_age_seconds": rounded(age_seconds(active, now)),
        "regular_last_trade_age_seconds": rounded(age_seconds(quote, now)),
        "note": "Trade age can grow when no trades occur. Marketable pricing uses the timestamped bid/ask book, never a previous-session last price.",
    }
