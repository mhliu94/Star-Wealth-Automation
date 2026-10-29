"""Translate KTrader commands into the existing Star Wealth order safeguards.

Dollar-sized market orders are rounded down to whole shares using a validated
live book. A market order's notional check is an estimate, not an execution-price
guarantee. Routing, scheduling and the account enable switch belong to the runner.
"""
from dataclasses import replace
from decimal import Decimal, ROUND_FLOOR
import hashlib
import json

from .orders import Order, number, symbol


SUPPORTED_ORDERS = frozenset({"MARKET_ORDER", "DELAYED_MARKET_ORDER", "LIMIT_ORDER"})


def intent_id(command):
    """Persistently identify a command across Kafka redelivery and process restarts."""
    values = [command.get("account_id"), command.get("command_id")]
    if any(not isinstance(value, str) or not value.strip() for value in values):
        raise ValueError("account_id and command_id must be nonempty strings")
    encoded = json.dumps(values, ensure_ascii=True, separators=(",", ":")).encode("ascii")
    return "ktrader:" + hashlib.sha256(encoded).hexdigest()


def decimal_value(value, label):
    # KTrader's JSON producer emits numeric values, including integral floats.
    # Convert the decimal text to avoid importing binary floating-point error.
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError(f"{label} must be a positive number")
    return number(str(value), label)


def decimal_text(value):
    return format(value, "f")


class BrokerAdapter:
    def __init__(self, client, journal, *, live=False, max_notional=None):
        self.client = client
        self.journal = journal
        self.live = live
        self.max_notional = (decimal_value(max_notional, "max_notional")
                             if max_notional is not None else None)

    def execute(self, command):
        if not isinstance(command, dict):
            raise ValueError("Command must be a JSON object")
        kind = command.get("type")
        if not isinstance(kind, str) or kind not in SUPPORTED_ORDERS:
            raise ValueError("Unsupported Star Wealth command; supported orders are "
                             "MARKET_ORDER, DELAYED_MARKET_ORDER and LIMIT_ORDER")
        self._validate_execution_options(command)
        identity = intent_id(command)
        stock = command.get("symbol")
        if not isinstance(stock, str):
            raise ValueError("symbol must be a US equity symbol")
        stock = symbol(stock.strip().upper())
        side = command.get("side")
        if not isinstance(side, str) or side not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")

        has_quantity = command.get("qty_shares") is not None
        has_notional = command.get("notional_usd") is not None
        if has_quantity == has_notional:
            raise ValueError("Specify exactly one of qty_shares or notional_usd")
        quantity = decimal_value(command["qty_shares"], "qty_shares") if has_quantity else None
        if quantity is not None and quantity != quantity.to_integral_value():
            raise ValueError("Star Wealth supports whole shares only")
        if quantity is not None:
            quantity = quantity.to_integral_value()
        amount = decimal_value(command["notional_usd"], "notional_usd") if has_notional else None
        price = None
        if kind == "LIMIT_ORDER":
            if not has_quantity:
                raise ValueError("LIMIT_ORDER requires qty_shares")
            price = decimal_value(command.get("limit_price"), "limit_price")
            estimate = quantity * price
            if self.max_notional is not None and estimate > self.max_notional:
                raise ValueError("Order exceeds configured max_notional")
        elif command.get("limit_price") is not None:
            raise ValueError("Market orders must not contain limit_price")
        if amount is not None and self.max_notional is not None and amount > self.max_notional:
            raise ValueError("notional_usd exceeds configured max_notional")

        # Avoid quote/authentication traffic for an already submitted live intent.
        if self.live:
            self.journal.assert_new(identity)
        status = self.client.market_status().get("status")
        sessions = {"OPEN": "regular", "PRE_OPEN": "extended", "AFTER_OPEN": "extended", "NIGHT": "overnight"}
        if status not in sessions:
            raise ValueError("Broker session is closed or unsupported")
        if kind != "LIMIT_ORDER" and status != "OPEN":
            raise ValueError("Native market orders require the regular session; send LIMIT_ORDER outside it")
        order = Order(stock, side.lower(), decimal_text(quantity or Decimal(1)),
                      kind="limit" if kind == "LIMIT_ORDER" else "market",
                      session=sessions[status], price=decimal_text(price) if price is not None else None)
        order.validate()

        if amount is not None:
            # prepare enforces realtime entitlement, tradability and a fresh,
            # two-sided book before any quantity is derived from the quote.
            one_share = self.client.prepare(order)
            per_share = number(one_share["estimated_notional"], "per-share estimate")
            quantity = (amount / per_share).to_integral_value(rounding=ROUND_FLOOR)
            if quantity < 1:
                raise ValueError("notional_usd is insufficient for one whole share")
            order = replace(order, quantity=decimal_text(quantity))
            cap = amount
        elif price is not None:
            cap = quantity * price
        elif self.max_notional is not None:
            cap = self.max_notional
        else:
            prepared = self.client.prepare(order)
            cap = number(prepared["estimated_notional"], "estimated_notional")
        if self.max_notional is not None:
            cap = min(cap, self.max_notional)

        result = self.client.submit(order, journal=self.journal, intent_id=identity,
                                    live=self.live, max_notional=decimal_text(cap))
        outcome = {
            "state": "acknowledged" if self.live else "dry_run",
            "intent_id": identity, "symbol": stock, "side": side,
            "quantity": order.quantity, "kind": order.kind, "session": order.session,
            "max_notional": decimal_text(cap),
        }
        if not self.live:
            outcome["estimated_notional"] = result["estimated_notional"]
        # Never return broker response bodies, account identifiers or session data.
        # An acknowledged write is deliberately not described as a filled order.
        return outcome

    @staticmethod
    def _validate_execution_options(command):
        if command.get("trading_mode") == "G":
            raise ValueError("Smart selling mode G requires order feedback and is unsupported")
        if command.get("parent_id") is not None or command.get("client_order_id") is not None:
            raise ValueError("Smart selling child-order commands require order feedback and are unsupported")
        if command.get("cancel_unfilled") is not None and command["cancel_unfilled"] is not False:
            raise ValueError("cancel_unfilled is unsupported")
        tif = command.get("time_in_force")
        if tif is not None and tif != "DAY":
            raise ValueError("Only DAY time_in_force is supported by the Kafka adapter")
        if command.get("deadline") is not None:
            raise ValueError("Order deadlines require automatic cancellation and are unsupported")
