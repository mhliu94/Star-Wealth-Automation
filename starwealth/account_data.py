"""Account reporting from verified broker fields, with no guessed zero balances.

Cash: the broker's public commonOutsideRouter.85ee2948.js labels the native
assetsBreakdown[currency].availableBalance as cash. assetsSummary contains FX
converted totals. Native Holdings v6 uses cost/sellAble; the public OpenAPI
counterpart calls these avgCost/sellable. Both require explicit typed values.
"""
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import math
import re

from .errors import ProtocolError
from .orders import symbol


def finite_number(value, field):
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ProtocolError(f"Missing or invalid account {field}")
    try:
        parsed = Decimal(str(value))
        if not parsed.is_finite():
            raise ValueError
        result = float(parsed)
        if not math.isfinite(result) or (parsed != 0 and result == 0):
            raise ValueError
    except (ValueError, InvalidOperation, OverflowError) as exc:
        raise ProtocolError(f"Missing or invalid account {field}") from exc
    return result


def cash_balances(response):
    """Return native currency balances; never substitute converted assets or margin."""
    breakdown = response.get("assetsBreakdown") if isinstance(response, dict) else None
    if not isinstance(breakdown, dict) or not breakdown:
        raise ProtocolError("Asset summary has no native currency breakdown")
    balances = {}
    for currency, row in breakdown.items():
        if not isinstance(currency, str) or not re.fullmatch(r"[A-Z]{3}", currency):
            raise ProtocolError("Asset summary has an invalid currency")
        if not isinstance(row, dict) or row.get("currency", currency) != currency:
            raise ProtocolError("Asset summary currency identity does not match")
        balances[currency] = finite_number(row.get("availableBalance"), "cash balance")
    return balances


def normalize_holdings(rows, *, sub_account_id=None):
    """Normalize the documented US-equity position fields, rejecting schema changes."""
    if not isinstance(rows, list):
        raise ProtocolError("Holdings must be a complete list")
    positions, seen = [], set()
    for row in rows:
        if not isinstance(row, dict):
            raise ProtocolError("Invalid holding record")
        if sub_account_id is not None and row.get("subAccountId") != sub_account_id:
            raise ProtocolError("Holding account route mismatch")
        market = row.get("marketCode")
        if not isinstance(market, str) or market.upper() != "US":
            raise ProtocolError("US holdings query returned an unverified market")
        stock = row.get("stockCode")
        try:
            stock = symbol(stock)
        except ValueError as exc:
            raise ProtocolError("Holding stock identity is invalid") from exc
        if stock in seen:
            raise ProtocolError("Holdings contain a duplicate stock")
        seen.add(stock)
        if row.get("currency") != "USD":
            raise ProtocolError("US holding currency does not match")
        quantity = finite_number(row.get("quantity"), "holding quantity")
        available = finite_number(row.get("quantityAvail"), "available quantity")
        cost_fields = [finite_number(row[key], "average cost") for key in ("cost", "avgCost") if key in row]
        if not cost_fields or any(value != cost_fields[0] for value in cost_fields):
            raise ProtocolError("Holding average cost is missing or conflicting")
        average = cost_fields[0]
        sell_fields = [row[key] for key in ("sellAble", "sellable") if key in row]
        if not sell_fields or any(type(value) is not bool for value in sell_fields):
            raise ProtocolError("Holding sellable flag is missing or invalid")
        if any(value != sell_fields[0] for value in sell_fields):
            raise ProtocolError("Holding sellable flags conflict")
        if available < 0 or average < 0 or (quantity >= 0 and available > quantity):
            raise ProtocolError("Holding quantities or cost are inconsistent")
        if quantity != 0:
            positions.append({"symbol": stock, "qty": quantity, "avg_price": average,
                              "available_qty": available if sell_fields[0] else 0.0})
    return positions


def account_snapshot(client, account_id, account_num_id, trading_enabled):
    """Build KTrader's account-details payload only after complete broker reads."""
    if not isinstance(account_id, str) or not account_id.strip():
        raise ValueError("account_id must be a nonempty string")
    if type(account_num_id) is not int or account_num_id < 1:
        raise ValueError("account_num_id must be a positive integer")
    if type(trading_enabled) is not bool:
        raise ValueError("trading_enabled must be a boolean")
    cash = cash_balances(client.asset_summary())
    if "USD" not in cash:
        raise ProtocolError("Asset summary has no confirmed USD cash balance")
    state = getattr(client, "state", None)
    sub_account_id = state["account"]["subAcctId"] if isinstance(state, dict) else None
    positions = normalize_holdings(client.holdings(), sub_account_id=sub_account_id)
    return {"account_id": account_id, "account_num_id": account_num_id,
            "cash": cash["USD"], "cash_by_currency": cash, "positions": positions,
            "ts": datetime.now(timezone.utc).isoformat(), "trading_enabled": trading_enabled}
