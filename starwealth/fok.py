"""Crash-safe best-effort FOK: submit a limit, then cancel an unfilled remainder.

This is not exchange-atomic fill-or-kill. Partial fills can occur. Network
operations are injected; this module never opens broker or Kafka connections.
"""
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import sqlite3
import time

from .errors import OutcomeUnknown
from .orders import symbol

TERMINAL = frozenset({"FILLED", "CANCELED", "REJECTED", "EXPIRED"})
OPEN = frozenset({"NEW", "PENDING_NEW", "PARTIALLY_FILLED", "OPEN", "PENDING"})
STATUSES = TERMINAL | OPEN | {"PENDING_CANCEL", "UNKNOWN"}


class SubmissionRejected(ValueError):
    """Caller verified that submission did not write, or was explicitly rejected."""


def decimal(value):
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError("Expected a finite decimal quantity or price")
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("Expected a finite decimal quantity or price") from exc
    if not result.is_finite():
        raise ValueError("Expected a finite decimal quantity or price")
    return result


def expected_order(command):
    if not isinstance(command, dict) or command.get("type") != "LIMIT_ORDER_FOK":
        raise ValueError("Synthetic FOK requires LIMIT_ORDER_FOK")
    stock = command.get("symbol")
    if not isinstance(stock, str):
        raise ValueError("Invalid FOK symbol")
    stock = symbol(stock.strip().upper())
    side = command.get("side")
    if side not in ("BUY", "SELL"):
        raise ValueError("Invalid FOK side")
    quantity, price = decimal(command.get("qty_shares")), decimal(command.get("limit_price"))
    if quantity <= 0 or quantity != quantity.to_integral_value() or price <= 0:
        raise ValueError("Synthetic FOK requires positive whole shares and a positive limit")
    if command.get("notional_usd") is not None:
        raise ValueError("Synthetic FOK requires explicit shares, not notional sizing")
    return {"symbol": stock, "side": side, "quantity": quantity, "limit_price": price}


class FokExecutor:
    """Callbacks use durable API journals for every financial write.

    ``resolve_submit`` returns an acknowledged order ID, ``False`` only when
    that journal proves no write or explicit rejection, and ``None`` when the
    outcome remains uncertain. ``submit`` may raise ``SubmissionRejected`` only
    with the same evidence. Other failures never authorize a new submission.
    ``resolve_cancel`` returns ``False`` only when the journal proves the cancel
    was never reserved; ``True`` or ``None`` preserves the no-resend marker.
    """

    def __init__(self, path, *, submit, detail, cancel, resolve_submit=None, resolve_cancel=None, clock=time.time, sleep=time.sleep):
        Path(path).parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("""CREATE TABLE IF NOT EXISTS fok_intents (
            intent_id TEXT PRIMARY KEY, digest TEXT NOT NULL, command TEXT NOT NULL,
            order_id TEXT, state TEXT NOT NULL, cancel_started INTEGER NOT NULL DEFAULT 0,
            filled TEXT NOT NULL DEFAULT '0', status TEXT, updated REAL NOT NULL)""")
        self.db.commit()
        self.submit, self.detail, self.cancel = submit, detail, cancel
        self.resolve_submit = resolve_submit
        self.resolve_cancel = resolve_cancel
        self.clock, self.sleep = clock, sleep

    def close(self):
        self.db.close()

    def _row(self, identity):
        return self.db.execute("SELECT * FROM fok_intents WHERE intent_id=?", (identity,)).fetchone()

    def _state(self, identity, state):
        with self.db:
            self.db.execute("UPDATE fok_intents SET state=?,updated=? WHERE intent_id=?", (state, self.clock(), identity))

    @staticmethod
    def cancel_intent_id(identity):
        return "fok-cancel:" + hashlib.sha256(identity.encode()).hexdigest()

    def execute(self, command, intent_id):
        expected = expected_order(command)
        if not isinstance(intent_id, str) or not 1 <= len(intent_id) <= 128:
            raise ValueError("A stable FOK intent ID is required")
        payload = json.dumps(command, sort_keys=True, separators=(",", ":"), allow_nan=False)
        digest = hashlib.sha256(payload.encode()).hexdigest()
        row = self._row(intent_id)
        if row is not None:
            if row["digest"] != digest:
                raise ValueError("FOK intent reused with different contents")
            return self._resume(row, expected)

        # Serialize the unresolved-submission gate and new reservation together.
        self.db.execute("BEGIN IMMEDIATE")
        try:
            if self.db.execute("SELECT 1 FROM fok_intents WHERE order_id IS NULL AND state!='confirmed' LIMIT 1").fetchone():
                raise OutcomeUnknown("An earlier FOK submission needs reconciliation before any new FOK submission")
            self.db.execute("INSERT INTO fok_intents (intent_id,digest,command,state,updated) VALUES (?,?,?,?,?)",
                            (intent_id, digest, payload, "pending_submit", self.clock()))
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

        try:
            self.sleep(5)
            order_id = self.submit(command, intent_id)
            if not isinstance(order_id, str) or not order_id.strip():
                raise ValueError("Submission returned no broker order ID")
            with self.db:
                self.db.execute("UPDATE fok_intents SET order_id=?,state='submitted',updated=? WHERE intent_id=?",
                                (order_id, self.clock(), intent_id))
        except SubmissionRejected:
            return self._reject_submission(intent_id)
        except Exception as exc:
            self._state(intent_id, "unknown_submit")
            raise OutcomeUnknown("FOK submission is uncertain; reconcile broker orders before any new FOK intent") from exc
        return self._drive(intent_id, expected, fresh=True)

    def _resume(self, row, expected):
        if row["state"] == "confirmed":
            return self._outcome(row["order_id"], row["status"])
        if row["order_id"] is None:
            if self.resolve_submit is not None:
                try:
                    order_id = self.resolve_submit(row["intent_id"])
                except Exception as exc:
                    raise OutcomeUnknown("FOK journal could not resolve the original submission") from exc
                if order_id is False:
                    return self._reject_submission(row["intent_id"])
                if order_id:
                    return self.reconcile_submission(row["intent_id"], order_id)
            raise OutcomeUnknown("FOK submission has no recorded broker ID; reconcile before retrying")
        return self._drive(row["intent_id"], expected, fresh=False)

    def _reject_submission(self, identity):
        with self.db:
            self.db.execute("UPDATE fok_intents SET state='confirmed',status='REJECTED',updated=? WHERE intent_id=? AND order_id IS NULL",
                            (self.clock(), identity))
        return self._outcome(None, "REJECTED")

    @staticmethod
    def _outcome(order_id, status):
        return {"state": "rejected" if status in {"REJECTED", "EXPIRED"} else "acknowledged",
                "order_id": order_id, "status": status}

    def _observe(self, identity, expected):
        row = self._row(identity)
        view = self.detail(row["order_id"])
        if not isinstance(view, dict) or view.get("order_id") != row["order_id"]:
            raise ValueError("FOK broker order identity mismatch")
        for key in ("symbol", "side"):
            if view.get(key) != expected[key]:
                raise ValueError("FOK broker order attributes mismatch")
        if view.get("order_type") != "LIMIT":
            raise ValueError("FOK broker order type mismatch")
        for key in ("quantity", "limit_price"):
            if decimal(view.get(key)) != expected[key]:
                raise ValueError("FOK broker order quantity/price mismatch")
        status, cancelable = view.get("status"), view.get("cancelable")
        if not isinstance(status, str) or status not in STATUSES or type(cancelable) is not bool:
            raise ValueError("FOK broker order status/cancelability is not verified")
        filled = decimal(view.get("filled_quantity"))
        if not decimal(row["filled"]) <= filled <= expected["quantity"]:
            raise ValueError("FOK fill quantity regressed or exceeds order quantity")
        if status == "FILLED" and filled != expected["quantity"]:
            raise ValueError("FOK filled status contradicts fill quantity")
        with self.db:
            self.db.execute("UPDATE fok_intents SET filled=?,status=?,updated=? WHERE intent_id=?",
                            (str(filled), status, self.clock(), identity))
        return view

    def _terminal(self, identity, view):
        if view["status"] not in TERMINAL:
            return None
        self._state(identity, "confirmed")
        return self._outcome(view["order_id"], view["status"])

    def _maybe_cancel(self, identity, expected, view):
        if not (view["status"] in OPEN and view["cancelable"] and
                decimal(view["filled_quantity"]) < expected["quantity"]):
            return
        with self.db:
            claim = self.db.execute("UPDATE fok_intents SET cancel_started=1,state='cancelling',updated=? WHERE intent_id=? AND cancel_started=0",
                                    (self.clock(), identity)).rowcount
        if claim:
            try:
                self.cancel(view["order_id"], self.cancel_intent_id(identity))
            except Exception:
                # A timeout can occur after successful cancellation. Only
                # read broker state from here; never send a second cancel.
                self._state(identity, "cancel_unknown")
            else:
                self._state(identity, "cancel_sent")

    def _drive(self, identity, expected, *, fresh):
        try:
            view = self._observe(identity, expected)
            result = self._terminal(identity, view)
            if result is not None:
                return result
            if not fresh and self._row(identity)["cancel_started"] and self.resolve_cancel is not None:
                # Resolve this crash window once per recovery, never in the
                # polling loop. Only absence from the write journal proves
                # that authentication failed/crashed before a cancel was sent.
                if self.resolve_cancel(self.cancel_intent_id(identity)) is False:
                    with self.db:
                        self.db.execute("UPDATE fok_intents SET cancel_started=0,state='submitted',updated=? WHERE intent_id=?",
                                        (self.clock(), identity))
            if fresh:
                self.sleep(2)
                view = self._observe(identity, expected)
                result = self._terminal(identity, view)
                if result is not None:
                    return result
            self._maybe_cancel(identity, expected, view)
            for pause in (0, 1, 1, 2):
                if pause:
                    self.sleep(pause)
                view = self._observe(identity, expected)
                result = self._terminal(identity, view)
                if result is not None:
                    return result
                # Some brokers do not permit cancellation until the order is
                # acknowledged. Use each fresh observation to make the first
                # cancellation attempt, preserving the durable one-write guard.
                self._maybe_cancel(identity, expected, view)
        except Exception as exc:
            self._state(identity, "unknown")
            raise OutcomeUnknown("FOK broker outcome is uncertain; reconciliation required, no automatic write retry") from exc
        self._state(identity, "unknown")
        raise OutcomeUnknown("FOK cancellation is not confirmed; reconcile broker orders before another intent")

    def pending(self):
        """Return durable work for startup reconciliation, including unknown submits."""
        rows = self.db.execute("SELECT * FROM fok_intents WHERE state!='confirmed' ORDER BY updated,intent_id").fetchall()
        return [{"intent_id": r["intent_id"], "order_id": r["order_id"], "state": r["state"],
                 "cancel_started": bool(r["cancel_started"]), "command": json.loads(r["command"])} for r in rows]

    def recover(self):
        """Reconcile every unfinished record; known IDs may receive their first cancel."""
        results = []
        for record in self.pending():
            try:
                outcome = self.execute(record["command"], record["intent_id"])
            except OutcomeUnknown:
                outcome = {"state": "unknown", "order_id": record["order_id"]}
            results.append({"intent_id": record["intent_id"], **outcome})
        return results

    def recover_pending(self):
        return self.recover()

    def reconcile_submission(self, intent_id, order_id):
        """Attach a manually reconciled broker ID, verify it, then finish recovery.

        Only use after an operator has identified the original broker order. An
        ID with mismatching attributes is never attached and never cancelled.
        """
        row = self._row(intent_id)
        if row is None or row["order_id"] is not None:
            raise ValueError("Reconciliation requires an unresolved FOK submission")
        if not isinstance(order_id, str) or not order_id.strip():
            raise ValueError("A reconciled broker order ID is required")
        expected = expected_order(json.loads(row["command"]))
        # Tentatively associate inside a transaction. Attribute verification is
        # read-only; rollback preserves the submission gate on a mismatch.
        try:
            self.db.execute("UPDATE fok_intents SET order_id=? WHERE intent_id=?", (order_id, intent_id))
            self._observe(intent_id, expected)
        except Exception as exc:
            self.db.rollback()
            # _observe commits only after all attributes have passed.
            raise OutcomeUnknown("Reconciled broker order could not be verified") from exc
        self._state(intent_id, "submitted")
        return self._drive(intent_id, expected, fresh=False)
