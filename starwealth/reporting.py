"""KTrader snapshots with a durable outbox; this module never submits trades."""
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import sqlite3


MAX_SEQUENCE = 9007199254740991
STATUSES = {"PENDING_NEW", "NEW", "PARTIALLY_FILLED", "PENDING_CANCEL", "FILLED",
            "CANCELED", "REJECTED", "EXPIRED", "UNKNOWN"}
ORDER_TYPES = {"MARKET", "LIMIT", "STOP", "STOP_LIMIT", "TRAILING_STOP", "OTHER"}
REQUIRED = {"order_id", "created_at", "updated_at", "symbol", "side", "order_type",
            "status", "quantity", "filled_quantity", "average_fill_price"}
OPTIONAL = {"limit_price", "stop_price", "notional", "currency", "time_in_force",
            "command_id", "client_order_id", "message"}


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def text_field(value, name, maximum, *, whitespace=True):
    if not isinstance(value, str) or not value or value != value.strip() or len(value) > maximum:
        raise ValueError(f"Invalid {name}")
    if not whitespace and any(char.isspace() for char in value):
        raise ValueError(f"Invalid {name}")
    return value


def moment(value):
    if not isinstance(value, str) or len(value) > 40 or "T" not in value.upper():
        raise ValueError("Snapshot timestamp requires an offset-bearing date-time")
    try:
        result = datetime.fromisoformat(value.upper().replace("Z", "+00:00"))
        if result.utcoffset() is None:
            raise ValueError()
        return result.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except (ValueError, OverflowError) as exc:
        raise ValueError("Snapshot timestamp requires an offset-bearing date-time") from exc


def number(value, name, *, nullable=False, minimum=0, positive=False):
    if value is None and nullable:
        return None
    if type(value) not in (int, float):
        raise ValueError(f"Invalid {name}")
    try:
        valid = math.isfinite(value)
    except OverflowError:
        valid = False
    if not valid or (minimum is not None and value < minimum) or (positive and value <= 0):
        raise ValueError(f"Invalid {name}")
    return value


def validate_order(snapshot):
    if not isinstance(snapshot, dict) or REQUIRED - snapshot.keys() or snapshot.keys() - REQUIRED - OPTIONAL:
        raise ValueError("Order snapshot has missing or unsupported fields")
    result = dict(snapshot)
    for name, maximum in (("order_id", 256), ("symbol", 64), ("command_id", 256),
                          ("client_order_id", 256), ("time_in_force", 32)):
        if name in result:
            text_field(result[name], name, maximum, whitespace=name != "symbol")
    if any(not isinstance(result[name], str) for name in ("side", "order_type", "status")):
        raise ValueError("Invalid order side, type, or status")
    if result["side"] not in {"BUY", "SELL"} or result["order_type"] not in ORDER_TYPES or result["status"] not in STATUSES:
        raise ValueError("Invalid order side, type, or status")
    result["created_at"], result["updated_at"] = moment(result["created_at"]), moment(result["updated_at"])
    if datetime.fromisoformat(result["updated_at"]) < datetime.fromisoformat(result["created_at"]):
        raise ValueError("Order update precedes creation")
    quantity = number(result["quantity"], "quantity", nullable=True, positive=True)
    filled = number(result["filled_quantity"], "filled_quantity")
    number(result["average_fill_price"], "average_fill_price", nullable=True)
    if quantity is None:
        number(result.get("notional"), "notional", positive=True)
    if quantity is not None and filled > quantity:
        raise ValueError("Order fill exceeds quantity")
    if result["status"] == "FILLED" and (filled <= 0 or (quantity is not None and filled != quantity)):
        raise ValueError("FILLED requires a complete positive fill")
    if result["status"] == "PARTIALLY_FILLED" and (filled <= 0 or (quantity is not None and filled >= quantity)):
        raise ValueError("PARTIALLY_FILLED requires an incomplete positive fill")
    for field in ("limit_price", "stop_price", "notional"):
        if field in result:
            number(result[field], field, nullable=field != "notional", positive=field == "notional")
    for types, field in (({"LIMIT", "STOP_LIMIT"}, "limit_price"), ({"STOP", "STOP_LIMIT"}, "stop_price")):
        if result["order_type"] in types:
            number(result.get(field), field)
    if "currency" in result:
        currency = result["currency"]
        if not isinstance(currency, str) or len(currency) != 3 or not all("A" <= c <= "Z" for c in currency):
            raise ValueError("Invalid currency")
    if "message" in result and (not isinstance(result["message"], str) or len(result["message"]) > 2000):
        raise ValueError("Invalid message")
    return result


class ReportingError(RuntimeError):
    """One or more snapshots remain pending for an identical retry."""


class Reporter:
    def __init__(self, producer, path, *, account_id, account_num_id,
                 account_topic="account-details", order_topic="order-status", delivery_timeout=10):
        self.account_id = text_field(account_id, "account_id", 128)
        if type(account_num_id) is not int or account_num_id < 1:
            raise ValueError("account_num_id must be a positive integer")
        self.account_num_id = account_num_id
        self.producer, self.account_topic, self.order_topic = producer, account_topic, order_topic
        self.delivery_timeout = number(delivery_timeout, "delivery_timeout", positive=True)
        Path(path).parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("""CREATE TABLE IF NOT EXISTS report_orders (
            account TEXT NOT NULL, order_id TEXT NOT NULL, payload TEXT NOT NULL,
            PRIMARY KEY(account,order_id))""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS report_outbox (
            id INTEGER PRIMARY KEY, account TEXT NOT NULL, topic TEXT NOT NULL,
            message_key TEXT NOT NULL, payload TEXT NOT NULL)""")
        self.db.commit()
        self._seen = set()

    def close(self):
        self.db.close()

    def tracked_open_orders(self):
        """Return latest normalized unfinished orders for targeted reconciliation."""
        terminal = {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}
        rows = self.db.execute("SELECT payload FROM report_orders WHERE account=? ORDER BY order_id",
                               (self.account_id,))
        return [snapshot for row in rows if (snapshot := json.loads(row[0]))["status"] not in terminal]

    def _queue(self, topic, key, payload):
        serialized = encoded(payload)
        if len(serialized.encode()) > 65536:
            raise ValueError("Snapshot exceeds 64 KiB")
        self.db.execute("INSERT INTO report_outbox(account,topic,message_key,payload) VALUES (?,?,?,?)",
                        (self.account_id, topic, key, serialized))

    def publish_account(self, snapshot):
        """Validate and queue an account snapshot; flush performs publication."""
        if not isinstance(snapshot, dict):
            raise ValueError("Account snapshot must be an object")
        for name, expected in (("account_id", self.account_id), ("account_num_id", self.account_num_id)):
            if name in snapshot and (type(snapshot[name]) is not type(expected) or snapshot[name] != expected):
                raise ValueError("Account snapshot identity mismatch")
        result = {key: value for key, value in snapshot.items() if key not in {"account_id", "account_num_id"}}
        required = {"cash", "cash_by_currency", "positions", "ts", "trading_enabled"}
        if required - result.keys() or result.keys() - required - {"available_cash_by_currency"}:
            raise ValueError("Account snapshot has missing or unsupported fields")
        number(result["cash"], "cash", minimum=None)
        if type(result["trading_enabled"]) is not bool or not isinstance(result["positions"], list):
            raise ValueError("Invalid account positions or trading state")
        result["ts"] = moment(result["ts"])
        for name in ("cash_by_currency", "available_cash_by_currency"):
            mapping = result.get(name, {})
            if not isinstance(mapping, dict):
                raise ValueError("Invalid cash map")
            for currency, amount in mapping.items():
                if not isinstance(currency, str) or len(currency) != 3 or not all("A" <= c <= "Z" for c in currency):
                    raise ValueError("Invalid cash currency")
                number(amount, "cash balance", minimum=None)
        for position in result["positions"]:
            if not isinstance(position, dict) or not {"symbol", "qty", "avg_price"} <= position.keys():
                raise ValueError("Invalid position")
            if position.keys() - {"symbol", "qty", "avg_price", "available_qty"}:
                raise ValueError("Unsupported position fields")
            text_field(position["symbol"], "symbol", 64, whitespace=False)
            number(position["qty"], "position qty", minimum=None)
            number(position["avg_price"], "avg_price", nullable=True)
            if "available_qty" in position:
                number(position["available_qty"], "available_qty", nullable=True, minimum=None)
        result.update(account_id=self.account_id, account_num_id=self.account_num_id)
        with self.db:
            pending = self.db.execute("SELECT payload FROM report_outbox WHERE account=? AND topic=? AND message_key=?",
                                      (self.account_id, self.account_topic, self.account_id)).fetchall()
            if any(datetime.fromisoformat(json.loads(row[0])["ts"]) > datetime.fromisoformat(result["ts"])
                   for row in pending):
                raise ValueError("Pending account snapshot is newer than this observation")
            # Account snapshots replace the full current balance/position state.
            # Keep only the latest during outages; order sequence events are retained.
            self.db.execute("DELETE FROM report_outbox WHERE account=? AND topic=? AND message_key=?",
                            (self.account_id, self.account_topic, self.account_id))
            self._queue(self.account_topic, self.account_id, result)
        return result

    def queue_order(self, snapshot):
        result = validate_order(snapshot)
        identifier = result["order_id"]
        result.update(schema_version=1, account_id=self.account_id, account_num_id=self.account_num_id)
        key = json.dumps([self.account_id, identifier], separators=(",", ":"))
        with self.db:
            row = self.db.execute("SELECT payload FROM report_orders WHERE account=? AND order_id=?",
                                  (self.account_id, identifier)).fetchone()
            previous = json.loads(row[0]) if row else None
            if previous:
                if result["created_at"] != previous["created_at"]:
                    raise ValueError("Order creation time changed")
                if result["filled_quantity"] < previous["filled_quantity"]:
                    raise ValueError("Cumulative fill regressed")
                meaningful = lambda value: {k: v for k, v in value.items() if k not in {"sequence", "updated_at"}}
                if meaningful(result) == meaningful(previous):
                    pending = self.db.execute("SELECT 1 FROM report_outbox WHERE account=? AND topic=? AND message_key=?",
                                              (self.account_id, self.order_topic, key)).fetchone()
                    if identifier in self._seen or pending:
                        return False
                    result = previous  # Replay once per restart, with the exact existing sequence.
                else:
                    result["sequence"] = previous["sequence"] + 1
                    if datetime.fromisoformat(result["updated_at"]) < datetime.fromisoformat(previous["updated_at"]):
                        result["updated_at"] = previous["updated_at"]
            else:
                result["sequence"] = 0
            if type(result["sequence"]) is not int or not 0 <= result["sequence"] <= MAX_SEQUENCE:
                raise ValueError("Order sequence exceeds the contract")
            self._queue(self.order_topic, key, result)
            self.db.execute("INSERT OR REPLACE INTO report_orders VALUES (?,?,?)",
                            (self.account_id, identifier, encoded(result)))
        self._seen.add(identifier)
        return True

    def _deliver(self, topic, key, payload):
        reports = []
        self.producer.produce(topic, key=key.encode(), value=payload.encode(),
                              on_delivery=lambda error, message: reports.append(error))
        self.producer.flush(self.delivery_timeout)
        # Other entities may still be pending after an earlier timeout. This
        # message's delivery callback is the authoritative acknowledgement.
        if not reports or any(error is not None for error in reports):
            raise ReportingError("Kafka delivery was not confirmed")

    def flush(self, max_records=None):
        """Deliver durable records; a failed order never prevents other entities."""
        if max_records is not None and (type(max_records) is not int or max_records < 1):
            raise ValueError("max_records must be a positive integer or None")
        sent, failures, attempted, blocked = 0, 0, 0, set()
        # Keep current balances deliverable even when a large order backlog has
        # a failing stream. Stream rows from SQLite instead of loading payloads.
        rows = self.db.execute("""SELECT id,topic,message_key,payload FROM report_outbox
            WHERE account=? ORDER BY (topic=?) DESC,id""", (self.account_id, self.account_topic))
        for identifier, topic, key, payload in rows:
            if (topic, key) in blocked:
                continue
            if max_records is not None and attempted >= max_records:
                break
            attempted += 1
            # Bounded flushes service at most one event per stream, advancing
            # its next sequence on the next tick. Unbounded flushes drain it.
            if max_records is not None:
                blocked.add((topic, key))
            try:
                self._deliver(topic, key, payload)
            except Exception:
                blocked.add((topic, key))
                failures += 1
                continue
            with self.db:
                self.db.execute("DELETE FROM report_outbox WHERE id=?", (identifier,))
            if topic == self.order_topic:
                self._seen.add(json.loads(payload)["order_id"])
            sent += 1
        rows.close()
        if failures:
            raise ReportingError(f"{failures} snapshot stream(s) remain pending; {sent} delivered")
        return sent
