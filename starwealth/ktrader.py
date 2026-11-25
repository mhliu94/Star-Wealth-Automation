"""Durable, single-account KTrader command listener.

The API journal guards financial writes; the inbox also deduplicates rejected,
delayed and control commands across Kafka redeliveries and process restarts.
"""
from starwealth import latency
from .kafka_security import connection_settings
import argparse
from contextlib import closing, contextmanager, nullcontext
from datetime import datetime, timezone
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
from .ktrader_broker import BrokerAdapter, intent_id, smart_child
from .reporting import validate_order
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

def nonnegative_seconds(value):
    result=float(value)
    if isinstance(value,bool) or not math.isfinite(result) or not 0<=result<=300:
        raise ValueError("Seconds must be finite and within 0..300")
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
        self.db.execute("""CREATE TABLE IF NOT EXISTS child_rejections (
            account TEXT, id TEXT, snapshot TEXT NOT NULL, PRIMARY KEY(account,id))""")
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

    def add(self, command, due, *, state="queued", rejection=None):
        payload = json.dumps(command, sort_keys=True, separators=(",", ":"), allow_nan=False)
        digest = hashlib.sha256(payload.encode()).hexdigest()
        account, cid = command["account_id"], command["command_id"]
        prior = self.db.execute("SELECT digest,state FROM commands WHERE account=? AND id=?", (account, cid)).fetchone()
        if prior:
            if prior[0] != digest:
                raise ValueError("command_id reused with different contents")
            return False
        with self.db:
            self.db.execute("INSERT INTO commands VALUES (?,?,?,?,?,?)", (account, cid, digest, payload, due, state))
            self._store_rejection(command, rejection)
        return True

    def _store_rejection(self, command, rejection):
        if rejection is not None:
            self.db.execute("INSERT OR IGNORE INTO child_rejections VALUES (?,?,?)",
                            (command["account_id"], command["command_id"], json.dumps(rejection)))

    def finish(self, command, state, *, rejection=None):
        with self.db:
            self.db.execute("UPDATE commands SET state=? WHERE account=? AND id=?", (state, command["account_id"], command["command_id"]))
            self._store_rejection(command, rejection)

    @staticmethod
    def queue_rejections(db, reporter):
        rows = db.execute("SELECT id,snapshot FROM child_rejections WHERE account=? LIMIT 100",
                          (reporter.account_id,)).fetchall()
        for cid, snapshot in rows:
            reporter.queue_order(json.loads(snapshot))
            with db:
                db.execute("DELETE FROM child_rejections WHERE account=? AND id=?", (reporter.account_id, cid))

    def due(self, account, now):
        row = self.db.execute("SELECT payload,due FROM commands WHERE account=? AND state='queued' AND due<=? ORDER BY due,rowid LIMIT 1", (account, now)).fetchone()
        return (json.loads(row[0]), row[1]) if row else None


class Listener:
    def __init__(self, adapter, inbox, *, string_id, numerical_id, max_age=300, clock=time.time):
        self.adapter, self.inbox = adapter, inbox
        self.string_id, self.numerical_id = string_id, numerical_id
        self.max_age, self.clock = max_age, clock

    def rejection(self, command, reason):
        if not smart_child(command) or not self.adapter.live:
            return None
        # An accepted, pending or ambiguous write must be resolved by broker evidence.
        prior = self.adapter.journal.get(intent_id(command))
        if prior and prior["state"] != "rejected":
            return None
        cid = command["command_id"]
        digest = hashlib.sha256((self.string_id + "\0" + cid).encode()).hexdigest()
        observed = max(self.clock(), timestamp(command["ts"]))
        try:
            return validate_order(dict(order_id="local-rejection:" + digest, command_id=cid,
                client_order_id=cid, created_at=command["ts"], updated_at=datetime.fromtimestamp(observed, timezone.utc).isoformat(),
                symbol=command["symbol"], side=command["side"], order_type="LIMIT", status="REJECTED",
                quantity=float(command["qty_shares"]), filled_quantity=0, average_fill_price=None,
                limit_price=float(command["limit_price"]), message=reason[:2000]))
        except (KeyError, ValueError, TypeError, OverflowError):
            LOG.warning("Invalid Smart order fields prevent correlated rejection feedback")
            return None

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
        stale = now - issued > self.max_age
        if now - issued < -30 or (stale and not smart_child(command)):
            raise ValueError("Command is stale or timestamp is in the future")
        if not isinstance(command.get("type"), str):
            raise ValueError("Command type is required")
        due = now
        if command["type"] == "DELAYED_MARKET_ORDER":
            due = timestamp(command["execute_at"]) if command.get("execute_at") else now + positive_seconds(command.get("delay_seconds"))
            due = max(now, due)
        state, rejection = "queued", None
        if stale:
            state, rejection = "rejected", self.rejection(command, "Command expired before execution")
        elif command["type"] not in {"SET_TRADING_ENABLED", "CANCEL_OPEN_ORDERS", "CANCEL_ORDER"} and not self.inbox.enabled(self.string_id):
            state, rejection = "disabled", self.rejection(command, "Trading disabled before execution")
        return state if self.inbox.add(command, due, state=state, rejection=rejection) else "duplicate"

    def execute_due(self):
        item = self.inbox.due(self.string_id, self.clock())
        if item is None:
            return False
        command, due = item
        # Log a digest, never arbitrary input, broker response, or credentials.
        tag = hashlib.sha256(command["command_id"].encode()).hexdigest()[:12]
        self.inbox.finish(command, "running")
        execution_start = time.perf_counter_ns()
        rejection = None
        LOG.info("Command starting command=%s type=%s start_ms=%d", tag, command["type"], time.time_ns()//1_000_000)
        try:
            if self.clock() - due > self.max_age:
                raise ValueError("Scheduled command is overdue; not executed after downtime")
            if command["type"] == "SET_TRADING_ENABLED":
                enabled = command.get("trading_enabled")
                if type(enabled) is not bool:
                    raise ValueError("trading_enabled must be a JSON boolean")
                self.inbox.set_enabled(self.string_id, enabled)
                state = "enabled" if enabled else "disabled"
            elif command["type"] not in {"CANCEL_OPEN_ORDERS", "CANCEL_ORDER"} and not self.inbox.enabled(self.string_id):
                state = "disabled"
                rejection = self.rejection(command, "Trading disabled before execution")
            else:
                with latency.span("command_execution", command["type"], command):
                    result = self.adapter.execute(command)
                state = result["state"]
        except OutcomeUnknown:
            state = "unknown"
            LOG.error("command=%s outcome unknown; reconcile broker orders and journal before another intent", tag)
        except (StarWealthError, ValueError) as exc:
            state = "rejected"
            rejection = self.rejection(command, str(exc))
            # Library validation messages contain no secrets; don't log arbitrary exceptions.
            LOG.warning("command=%s rejected: %s", tag, exc)
        except Exception as exc:
            state = "unknown"
            LOG.error("command=%s stopped with %s; inspect journal before another intent", tag, type(exc).__name__)
        self.inbox.finish(command, state, rejection=rejection)
        LOG.info("command=%s state=%s account=%s account_num_id=%s finished_ms=%d elapsed_ms=%.3f", tag, state,
                 self.string_id, self.numerical_id, time.time_ns()//1_000_000, (time.perf_counter_ns()-execution_start)/1_000_000)
        return True


def consume(consumer, listener, stop, reports=None, session_lock=None, reconcile=None, interval=30):
    from confluent_kafka import KafkaError
    next_reconcile = time.monotonic() + interval
    while not stop.is_set():
        msg = consumer.poll(1.0)
        received_ms = time.time_ns() // 1_000_000
        if msg is not None:
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    LOG.warning("Kafka error code=%s", msg.error().code())
            else:
                try:
                    command = parse_message(msg.value())
                    outcome = listener.accept(command)
                    if outcome != "ignored":
                        latency.received(command, msg, received_ms)
                        LOG.info("Kafka partition=%s offset=%s state=%s", msg.partition(), msg.offset(), outcome)
                except (ValueError, UnicodeError, TypeError, OverflowError):
                    LOG.warning("Rejected invalid command partition=%s offset=%s", msg.partition(), msg.offset())
                # The durable inbox is written before committing delivery; DB failures
                # terminate the listener without advancing Kafka's offset.
                consumer.commit(message=msg, asynchronous=False)
        if not stop.is_set():
            with session_lock if session_lock is not None else nullcontext():
                executed = listener.execute_due()
                if reconcile is not None and time.monotonic() >= next_reconcile:
                    next_reconcile = time.monotonic() + interval
                    try:
                        reconcile()
                    except Exception as exc:
                        LOG.error("Pending IOC reconciliation incomplete (%s)", type(exc).__name__)
            if executed and reports is not None:
                reports.after_command()


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
    p.add_argument("--fok-submission-delay-seconds", type=nonnegative_seconds,
                   default=os.environ.get("STARWEALTH_FOK_SUBMISSION_DELAY_SECONDS", "0"))
    p.add_argument("--max-notional", help="Optional maximum USD notional per order; market orders use a quote estimate")
    p.add_argument("--account-topic", default="account-details")
    p.add_argument("--order-topic", default="order-status")
    p.add_argument("--report-interval", type=positive_seconds, default=30)
    p.add_argument("--report-lookback-days", type=int, default=90)
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
    if args.report_lookback_days < 1:
        raise SystemExit("Report lookback days must be positive")
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
            from confluent_kafka import Consumer, Producer
            from .reporting import Reporter
            from .report_loop import ReportLoop, BackgroundReports
            journal, inbox, consumer, adapter, reports = None, None, None, None, None
            try:
                journal = Journal(args.journal)
                inbox = Inbox(args.inbox)
                execution_path = args.inbox.with_name(args.inbox.stem + "-execution.sqlite3")
                adapter = BrokerAdapter(client, journal, live=args.live, max_notional=args.max_notional,
                    execution_path=execution_path, stop_requested=stop.is_set,
                    fok_submission_delay_seconds=args.fok_submission_delay_seconds)
                listener = Listener(adapter, inbox, string_id=args.string_id, numerical_id=args.numerical_id, max_age=args.max_command_age)
                reports = None
                if args.live:
                    producer = Producer({**connection_settings(), "bootstrap.servers": args.bootstrap_servers,
                        "client.id": f"starwealth-reports-{args.numerical_id}",
                        "enable.idempotence": True, "acks": "all", "delivery.timeout.ms": 10000})
                    @contextmanager
                    def open_reports():
                        with closing(Journal(args.journal)) as reader, closing(sqlite3.connect(
                                args.inbox.resolve().as_uri() + "?mode=rw", uri=True)) as settings, closing(Reporter(
                                producer, args.inbox.with_name("ktrader-reports.sqlite3"), account_id=args.string_id,
                                account_num_id=args.numerical_id, account_topic=args.account_topic,
                                order_topic=args.order_topic, delivery_timeout=3)) as reporter:
                            def enabled():
                                row = settings.execute("SELECT enabled FROM settings WHERE account=?", (args.string_id,)).fetchone()
                                return bool(row[0]) if row else True
                            yield ReportLoop(client, reader, reporter, enabled, interval=args.report_interval,
                                queue_rejections=lambda: Inbox.queue_rejections(settings, reporter))
                    adapter.recover()
                    reports = BackgroundReports(open_reports, stop)
                    reports.start()
                consumer = Consumer({**connection_settings(), "bootstrap.servers": args.bootstrap_servers,
                    "group.id": args.consumer_group_id,
                    "client.id": f"starwealth-{args.numerical_id}",
                    "enable.auto.commit": False, "enable.auto.offset.store": False,
                    "auto.offset.reset": "latest", "allow.auto.create.topics": False,
                    "session.timeout.ms": 10000, "max.poll.interval.ms": 900000})
                consumer.subscribe([args.topic])
                LOG.info("Listening topic=%s group=%s account=%s account_num_id=%s mode=%s",
                    args.topic, args.consumer_group_id, args.string_id, args.numerical_id, "live" if args.live else "dry-run")
                consume(consumer, listener, stop, reports, client.session_lock,
                        adapter.recover if args.live else None, args.report_interval)
            finally:
                if consumer is not None:
                    consumer.close()
                if reports is not None:
                    reports.close()
                if adapter is not None:
                    adapter.close()
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
