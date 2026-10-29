"""Translate KTrader commands into the existing Star Wealth order safeguards.

Dollar-sized market orders are rounded down to whole shares using a validated
live book. A market order's notional check is an estimate, not an execution-price
guarantee. Routing, scheduling and the account enable switch belong to the runner.
"""
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_FLOOR
import hashlib
import json
import time

from .orders import Order, number, symbol
from .errors import OutcomeUnknown, StarWealthError
from .order_data import normalize_order


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
    def __init__(self, client, journal, *, live=False, max_notional=None, execution_path=None, sleep=time.sleep, stop_requested=lambda: False):
        self.client = client
        self.journal = journal
        self.live = live
        self.max_notional = (decimal_value(max_notional, "max_notional")
                             if max_notional is not None else None)
        self.sleep = sleep
        self.stop_requested = stop_requested
        self._order_search = {}
        self.fok = None
        if execution_path is not None:
            from .fok import FokExecutor
            self.fok = FokExecutor(execution_path, submit=self._fok_submit, detail=self.order_view,
                cancel=self._cancel, resolve_submit=self._resolve_submit,
                resolve_cancel=lambda cid: self.journal.get(cid) is not None, sleep=sleep)

    def close(self):
        if self.fok is not None:
            self.fok.close()

    def recover(self):
        return self.fok.recover_pending() if self.live and self.fok else []

    def _resolve_submit(self, identity):
        prior = self.journal.get(identity)
        if prior is None or prior["state"] == "rejected":
            return False  # The API journal reserves before any financial write.
        return self.journal.acknowledged_order(identity)

    def _fok_submit(self, command, identity):
        from .fok import SubmissionRejected
        regular = {**command, "type": "LIMIT_ORDER", "time_in_force": "DAY", "cancel_unfilled": False}
        try:
            result = self._execute_order(regular)
        except (ValueError, StarWealthError) as exc:
            prior = self.journal.get(identity)
            if prior is None or prior["state"] == "rejected":
                raise SubmissionRejected("FOK preparation or broker submission was rejected") from exc
            raise
        return result["order_id"]

    def order_view(self, order_id):
        # OrderList is the documented shared model. Never infer filled/canceled
        # from a missing row or an acknowledgement.
        search = self._order_search.get(str(order_id))
        if search is None:
            for result, params, updated in self.journal.db.execute("""SELECT i.result,a.params,i.updated
                    FROM intents i JOIN attempts a ON i.request_id=a.request_id
                    WHERE i.operation='create' AND i.state='acknowledged'"""):
                if result and str(json.loads(result).get("orderId")) == str(order_id):
                    start = (datetime.fromtimestamp(updated, timezone.utc) - timedelta(days=1)).date().isoformat()
                    search = {"from_date": start, "stock_code": json.loads(params).get("stockCode")}
                    self._order_search[str(order_id)] = search
                    break
        for row in self.client.order_history(**(search or {"from_date": "2000-01-01"})):
            if str(row.get("orderId")) == str(order_id):
                return normalize_order(row, sub_account_id=self.client.state["account"]["subAcctId"])
        raise OutcomeUnknown("Acknowledged order is not visible in broker history yet")

    def _cancel(self, order_id, identity):
        return self.client.cancel(str(order_id), journal=self.journal, intent_id=identity, live=self.live)

    def execute(self, command):
        if not isinstance(command, dict):
            raise ValueError("Command must be a JSON object")
        if command.get("type") == "CANCEL_OPEN_ORDERS":
            return self.cancel_open_orders(command)
        is_fok = command.get("type") == "LIMIT_ORDER_FOK" or (
            command.get("type") == "LIMIT_ORDER" and
            (command.get("time_in_force") == "FOK" or command.get("cancel_unfilled") is True))
        if self.live and self.fok and self.fok.pending():
            self.recover()
            if self.fok.pending():
                raise OutcomeUnknown("An earlier FOK remains unresolved; new submissions are paused")
        if is_fok:
            if command.get("time_in_force") not in (None, "DAY", "FOK"):
                raise ValueError("Synthetic FOK conflicts with the requested time_in_force")
            if command.get("cancel_unfilled") is not None and type(command["cancel_unfilled"]) is not bool:
                raise ValueError("cancel_unfilled must be a JSON boolean")
            # Validate every constraint other than the explicitly requested
            # synthetic FOK flags before reserving financial intent.
            self._validate_execution_options({**command, "time_in_force": "DAY", "cancel_unfilled": False})
            from .fok import expected_order
            fok_command = {**command, "type": "LIMIT_ORDER_FOK"}
            expected_order(fok_command)
            if not self.live:
                result = self._execute_order({**command, "type": "LIMIT_ORDER", "time_in_force": "DAY", "cancel_unfilled": False})
                return {**result, "synthetic_fok": True}
            if self.fok is None:
                raise ValueError("Synthetic FOK requires persistent execution state")
            return self.fok.execute(fok_command, intent_id(command))
        return self._execute_order(command)

    def _execute_order(self, command):
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

        if self.live:
            self.journal.associate(identity, command["command_id"], command.get("client_order_id"))
        if self.stop_requested():
            raise ValueError("Shutdown requested before order submission")
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
        else:
            outcome["order_id"] = str(result["orderId"])
            self._order_search[outcome["order_id"]] = {"stock_code": stock,
                "from_date": (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()}
        # Never return broker response bodies, account identifiers or session data.
        # An acknowledged write is deliberately not described as a filled order.
        return outcome

    def cancel_open_orders(self, command):
        self._validate_execution_options(command)
        identity = intent_id(command)
        target = command.get("symbol")
        if target is not None:
            if not isinstance(target, str):
                raise ValueError("Cancellation symbol must be a string")
            target = symbol(target.strip().upper()) if target.strip() else None
        # Complete discovery before the first cancellation, including older GTC
        # orders. Pagination errors abort without silently cancelling a subset.
        rows = self.client.order_history(from_date="2000-01-01", statuses=[10, 20, 21, 40, 60])
        candidates = []
        for row in rows:
            if str(row.get("marketCode", "")).upper() != "US":
                continue
            if target and row.get("stockCode") != target:
                continue
            view = normalize_order(row, sub_account_id=self.client.state["account"]["subAcctId"])
            if view["cancelable"]:
                start = (datetime.fromisoformat(view["created_at"].replace("Z", "+00:00")) - timedelta(days=1)).date().isoformat()
                self._order_search[view["order_id"]] = {"stock_code": view["symbol"], "from_date": start}
                candidates.append(view["order_id"])
        if not self.live:
            return {"state": "dry_run", "candidates": len(candidates)}
        confirmed, unknown = 0, 0
        for order_id in candidates:
            try:
                view = self.order_view(order_id)
                if view["status"] in {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}:
                    confirmed += 1
                    continue
                if not view["cancelable"]:
                    unknown += 1
                    continue
                cancellation_id = "bulk-cancel:" + hashlib.sha256((identity + "\0" + order_id).encode()).hexdigest()
                if self.journal.get(cancellation_id) is None:
                    try:
                        self._cancel(order_id, cancellation_id)
                    except Exception:
                        # The durable write journal decides whether a send was
                        # attempted. Confirm by reads, never by a second cancel.
                        pass
                terminal = False
                for delay in (0, 1, 1, 2):
                    if delay:
                        self.sleep(delay)
                    view = self.order_view(order_id)
                    if view["status"] in {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}:
                        terminal = True
                        break
                confirmed += int(terminal)
                unknown += int(not terminal)
            except Exception:
                unknown += 1
        if unknown:
            raise OutcomeUnknown(f"Bulk cancellation: {confirmed} terminal, {unknown} unresolved; inspect broker reports")
        return {"state": "acknowledged", "candidates": len(candidates), "terminal": confirmed}

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
