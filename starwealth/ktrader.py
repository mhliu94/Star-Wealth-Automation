"""Durable, single-account KTrader command listener.

The API journal guards financial writes; the inbox also deduplicates rejected,
delayed and control commands across Kafka redeliveries and process restarts.
"""
import argparse
from datetime import datetime
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import signal
import sqlite3
import threading
import time

from .client import Client
from .errors import OutcomeUnknown, StarWealthError
from .journal import Journal
from .ktrader_broker import BrokerAdapter
from .storage import profile_lock

LOG = logging.getLogger("starwealth.ktrader")


def timestamp(value):
    if not isinstance(value, str):
        raise ValueError("ts/execute_at must be an ISO timestamp with timezone")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError()
        return parsed.timestamp()
    except (ValueError, OverflowError) as exc:
        raise ValueError("ts/execute_at must be an ISO timestamp with timezone") from exc


def positive_seconds(value):
    try:
        result = float(value)
    except (ValueError, TypeError) as exc:
        raise ValueError("Seconds must be finite and positive") from exc
    if isinstance(value, bool) or not math.isfinite(result) or result <= 0:
        raise ValueError("Seconds must be finite and positive")
    return result


def parse_message(raw):
    def invalid_constant(_):
        raise ValueError("Nonfinite JSON number")
    def finite_float(value):
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError("Nonfinite JSON number")
        return parsed
    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate JSON key")
            result[key] = value
        return result
    if not raw or len(raw) > 65536:
        raise ValueError("Empty or oversized command")
    value = json.loads(raw, parse_constant=invalid_constant, parse_float=finite_float, object_pairs_hook=unique_object)
    if not isinstance(value, dict):
        raise ValueError("Command must be a JSON object")
    return value


class Inbox:
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("""CREATE TABLE IF NOT EXISTS commands (
            account TEXT, id TEXT, digest TEXT NOT NULL, payload TEXT NOT NULL,
            due REAL NOT NULL, state TEXT NOT NULL, PRIMARY KEY(account,id))""")
        self.db.execute("CREATE TABLE IF NOT EXISTS settings (account TEXT PRIMARY KEY, enabled INTEGER NOT NULL)")
        # A crash during execution must never automatically resubmit a write.
        recovered = self.db.execute("UPDATE commands SET state='unknown' WHERE state='running'").rowcount
        self.db.commit()
        if recovered:
            LOG.error("Recovered %s interrupted commands as unknown; reconcile broker orders and journal", recovered)

    def close(self):
        self.db.close()

    def enabled(self, account):
        row = self.db.execute("SELECT enabled FROM settings WHERE account=?", (account,)).fetchone()
        return bool(row[0]) if row else True

    def set_enabled(self, account, enabled):
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO settings VALUES (?,?)", (account, int(enabled)))

    def add(self, command, due):
        payload = json.dumps(command, sort_keys=True, separators=(",", ":"), allow_nan=False)
        digest = hashlib.sha256(payload.encode()).hexdigest()
        account, cid = command["account_id"], command["command_id"]
        prior = self.db.execute("SELECT digest,state FROM commands WHERE account=? AND id=?", (account, cid)).fetchone()
        if prior:
            if prior[0] != digest:
                raise ValueError("command_id reused with different contents")
            return False
        with self.db:
            self.db.execute("INSERT INTO commands VALUES (?,?,?,?,?,?)", (account, cid, digest, payload, due, "queued"))
        return True

    def finish(self, command, state):
        with self.db:
            self.db.execute("UPDATE commands SET state=? WHERE account=? AND id=?", (state, command["account_id"], command["command_id"]))

    def due(self, account, now):
        row = self.db.execute("SELECT payload,due FROM commands WHERE account=? AND state='queued' AND due<=? ORDER BY due,rowid LIMIT 1", (account, now)).fetchone()
        return (json.loads(row[0]), row[1]) if row else None


class Listener:
    def __init__(self, adapter, inbox, *, string_id, numerical_id, max_age=300, clock=time.time):
        self.adapter, self.inbox = adapter, inbox
        self.string_id, self.numerical_id = string_id, numerical_id
        self.max_age, self.clock = max_age, clock

    def accept(self, command):
        if command.get("account_id") != self.string_id:
            return "ignored"
        if "account_num_id" in command and (type(command["account_num_id"]) is not int or command["account_num_id"] != self.numerical_id):
            raise ValueError("Mismatched account_num_id")
        cid = command.get("command_id")
        if not isinstance(cid, str) or not cid.strip() or len(cid) > 256:
            raise ValueError("command_id must contain 1-256 characters")
        now = self.clock()
        issued = timestamp(command.get("ts"))
        if not -30 <= now - issued <= self.max_age:
            raise ValueError("Command is stale or timestamp is in the future")
        if not isinstance(command.get("type"), str):
            raise ValueError("Command type is required")
        due = now
        if command["type"] == "DELAYED_MARKET_ORDER":
            due = timestamp(command["execute_at"]) if command.get("execute_at") else now + positive_seconds(command.get("delay_seconds"))
            due = max(now, due)
        if not self.inbox.add(command, due):
            return "duplicate"
        if command["type"] not in {"SET_TRADING_ENABLED", "CANCEL_OPEN_ORDERS"} and not self.inbox.enabled(self.string_id):
            self.inbox.finish(command, "disabled")
            return "disabled"
        return "queued"

    def execute_due(self):
        item = self.inbox.due(self.string_id, self.clock())
        if item is None:
            return False
        command, due = item
        # Log a digest, never arbitrary input, broker response, or credentials.
        tag = hashlib.sha256(command["command_id"].encode()).hexdigest()[:12]
        self.inbox.finish(command, "running")
        try:
            if self.clock() - due > self.max_age:
                raise ValueError("Scheduled command is overdue; not executed after downtime")
            if command["type"] == "SET_TRADING_ENABLED":
                enabled = command.get("trading_enabled")
                if type(enabled) is not bool:
                    raise ValueError("trading_enabled must be a JSON boolean")
                self.inbox.set_enabled(self.string_id, enabled)
                state = "enabled" if enabled else "disabled"
            elif command["type"] != "CANCEL_OPEN_ORDERS" and not self.inbox.enabled(self.string_id):
                state = "disabled"
            else:
                result = self.adapter.execute(command)
                state = result["state"]
        except OutcomeUnknown:
            state = "unknown"
            LOG.error("command=%s outcome unknown; reconcile broker orders and journal before another intent", tag)
        except (StarWealthError, ValueError) as exc:
            state = "rejected"
            # Library validation messages contain no secrets; don't log arbitrary exceptions.
            LOG.warning("command=%s rejected: %s", tag, exc)
        except Exception as exc:
            state = "unknown"
            LOG.error("command=%s stopped with %s; inspect journal before another intent", tag, type(exc).__name__)
        self.inbox.finish(command, state)
        LOG.info("command=%s state=%s account=%s account_num_id=%s", tag, state, self.string_id, self.numerical_id)
        return True


def consume(consumer, listener, stop):
    from confluent_kafka import KafkaError
    while not stop.is_set():
        msg = consumer.poll(1.0)
        if msg is not None:
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    LOG.warning("Kafka error code=%s", msg.error().code())
            else:
                try:
                    outcome = listener.accept(parse_message(msg.value()))
                    if outcome != "ignored":
                        LOG.info("Kafka partition=%s offset=%s state=%s", msg.partition(), msg.offset(), outcome)
                except (ValueError, UnicodeError, TypeError, OverflowError):
                    LOG.warning("Rejected invalid command partition=%s offset=%s", msg.partition(), msg.offset())
                # The durable inbox is written before committing delivery; DB failures
                # terminate the listener without advancing Kafka's offset.
                consumer.commit(message=msg, asynchronous=False)
        if not stop.is_set():
            listener.execute_due()


def parser():
    p = argparse.ArgumentParser(description="Star Wealth listener for KTrader trading-commands")
    config = Path(os.environ.get("STARWEALTH_CONFIG_DIR", str(Path.home() / "star-configs")))
    p.add_argument("--bootstrap-servers", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "45.32.121.19:9092"))
    p.add_argument("--consumer-group-id", default=os.environ.get("STARWEALTH_KAFKA_GROUP_ID"), help="Default: star-24 live, star-24-dry-run in dry-run mode")
    p.add_argument("--topic", default=os.environ.get("KAFKA_TRADING_COMMANDS_TOPIC", "trading-commands"))
    p.add_argument("--numerical-id", "--numeric-id", type=int, default=24)
    p.add_argument("--string-id", default="STAR-DONG")
    p.add_argument("--profile", type=Path, default=os.environ.get("STARWEALTH_PROFILE", str(config / "star.session.json")))
    p.add_argument("--credentials", type=Path, default=os.environ.get("STARWEALTH_CREDENTIALS", str(config / "star.credentials.json")))
    p.add_argument("--journal", type=Path, default=os.environ.get("STARWEALTH_JOURNAL", str(config / "orders.sqlite3")))
    p.add_argument("--inbox", type=Path, help="Defaults beside the journal; separate dry-run inbox")
    p.add_argument("--max-command-age", type=positive_seconds, default=300)
    p.add_argument("--max-notional", help="Optional maximum USD notional per order; market orders use a quote estimate")
    p.add_argument("--live", action="store_true", help="Execute incoming orders; default prepares only")
    p.add_argument("--check-config", action="store_true", help="Validate local configuration without network connections")
    return p


def main():
    os.umask(0o077)
    args = parser().parse_args()
    args.consumer_group_id = args.consumer_group_id or ("star-24" if args.live else "star-24-dry-run")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.numerical_id < 1 or not args.string_id.strip():
        raise SystemExit("Positive numerical ID and nonempty string ID required")
    if args.max_notional is not None:
        from .orders import number
        number(args.max_notional, "max notional")
    args.inbox = args.inbox or args.journal.with_name("ktrader-live.sqlite3" if args.live else "ktrader-dry-run.sqlite3")
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    try:
        with profile_lock(args.profile):
            client = Client.load(args.profile, args.credentials)
            client.account_params()
            # Check reusable credentials locally, without login/unlock or printing them.
            client.auth._material("login", client.state["user"]["uin"], client.state["user"]["salt"])
            info = client.state["user"]["clientInfo"]
            client.auth._material("trade", info["crId"], info["salt"])
            if args.check_config:
                import confluent_kafka  # noqa: F401
                print(json.dumps({"configuration": "valid", "bootstrap_servers": args.bootstrap_servers,
                    "topic": args.topic, "consumer_group_id": args.consumer_group_id,
                    "account_id": args.string_id, "account_num_id": args.numerical_id,
                    "mode": "live" if args.live else "dry-run", "network_connections": 0}))
                return 0
            from confluent_kafka import Consumer
            journal, inbox, consumer = None, None, None
            try:
                journal = Journal(args.journal)
                inbox = Inbox(args.inbox)
                adapter = BrokerAdapter(client, journal, live=args.live, max_notional=args.max_notional)
                listener = Listener(adapter, inbox, string_id=args.string_id, numerical_id=args.numerical_id, max_age=args.max_command_age)
                consumer = Consumer({"bootstrap.servers": args.bootstrap_servers,
                    "group.id": args.consumer_group_id,
                    "client.id": f"starwealth-{args.numerical_id}",
                    "enable.auto.commit": False, "enable.auto.offset.store": False,
                    "auto.offset.reset": "latest", "allow.auto.create.topics": False,
                    "session.timeout.ms": 10000, "max.poll.interval.ms": 300000})
                consumer.subscribe([args.topic])
                LOG.info("Listening topic=%s group=%s account=%s account_num_id=%s mode=%s",
                    args.topic, args.consumer_group_id, args.string_id, args.numerical_id, "live" if args.live else "dry-run")
                consume(consumer, listener, stop)
            finally:
                if consumer is not None:
                    consumer.close()
                if inbox is not None:
                    inbox.close()
                if journal is not None:
                    journal.close()
    except Exception as exc:
        LOG.error("Listener stopped (%s); check private configuration, journal, and connectivity", type(exc).__name__)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
