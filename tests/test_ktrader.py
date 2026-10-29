"""Offline delivery and restart guarantees for the KTrader listener."""
from datetime import datetime, timezone
import json
import sqlite3
import threading

import pytest

from starwealth.errors import OutcomeUnknown
from starwealth.ktrader import Inbox, Listener, consume, parse_message, timestamp


NOW = 1793232000.0


def iso(value=NOW):
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


def command(**changes):
    return {"account_id": "STAR-DONG", "account_num_id": 24,
            "command_id": "command-1", "ts": iso(), "type": "MARKET_ORDER",
            "symbol": "AAPL", "side": "BUY", "qty_shares": 1, **changes}


class Adapter:
    def __init__(self, error=None):
        self.calls = []
        self.error = error

    def execute(self, payload):
        self.calls.append(payload)
        if self.error:
            raise self.error
        return {"state": "acknowledged"}


def listener(inbox, adapter=None, clock=lambda: NOW):
    return Listener(adapter or Adapter(), inbox, string_id="STAR-DONG", numerical_id=24, clock=clock)


@pytest.fixture
def inbox(tmp_path):
    result = Inbox(tmp_path / "inbox.sqlite3")
    yield result
    result.close()


def recorded_state(inbox, cid="command-1"):
    row = inbox.db.execute("SELECT state FROM commands WHERE account=? AND id=?",
                           ("STAR-DONG", cid)).fetchone()
    return row[0] if row else None


def test_command_survives_restart_and_duplicate_does_not_execute_twice(tmp_path):
    path = tmp_path / "inbox.sqlite3"
    first = Inbox(path)
    payload = command()
    assert listener(first).accept(payload) == "queued"
    first.close()

    second = Inbox(path)
    adapter = Adapter()
    resumed = listener(second, adapter)
    assert resumed.accept(dict(reversed(list(payload.items())))) == "duplicate"
    assert resumed.execute_due() is True
    assert resumed.execute_due() is False
    assert resumed.accept(payload) == "duplicate"
    assert recorded_state(second) == "acknowledged"
    assert adapter.calls == [payload]
    second.close()


def test_duplicate_id_with_changed_contents_is_rejected(inbox):
    worker = listener(inbox)
    worker.accept(command())
    with pytest.raises(ValueError, match="different contents"):
        worker.accept(command(qty_shares=2))
    assert inbox.due("STAR-DONG", NOW)[0]["qty_shares"] == 1


def test_crash_during_execution_is_unknown_and_never_retried(tmp_path):
    path = tmp_path / "inbox.sqlite3"
    first = Inbox(path)
    payload = command()
    listener(first).accept(payload)
    first.finish(payload, "running")
    first.close()

    second = Inbox(path)
    adapter = Adapter()
    resumed = listener(second, adapter)
    assert recorded_state(second) == "unknown"
    assert resumed.accept(payload) == "duplicate"
    assert resumed.execute_due() is False
    assert adapter.calls == []
    second.close()


@pytest.mark.parametrize("error", [OutcomeUnknown("uncertain"), RuntimeError("unclassified failure")])
def test_uncertain_outcome_is_persisted_without_replay(inbox, error):
    adapter = Adapter(error)
    worker = listener(inbox, adapter)
    worker.accept(command())
    assert worker.execute_due() is True
    assert recorded_state(inbox) == "unknown"
    assert worker.accept(command()) == "duplicate"
    assert worker.execute_due() is False
    assert len(adapter.calls) == 1


@pytest.mark.parametrize("bad_ts", [None, "not-a-time", "2026-10-29T12:00:00", iso(NOW - 301), iso(NOW + 31)])
def test_invalid_stale_or_future_timestamp_never_queues(inbox, bad_ts):
    with pytest.raises(ValueError):
        listener(inbox).accept(command(ts=bad_ts))
    assert inbox.due("STAR-DONG", NOW + 1000) is None


def test_timestamp_accepts_explicit_offsets_and_z():
    assert timestamp("2026-10-29T00:00:00Z") == timestamp("2026-10-29T08:00:00+08:00")


@pytest.mark.parametrize("account_id", ["SOMEONE-ELSE", "24", 24, None])
def test_account_string_routes_before_validation(inbox, account_id):
    worker = listener(inbox)
    assert worker.accept({"account_id": account_id}) == "ignored"
    assert worker.execute_due() is False


@pytest.mark.parametrize("num_id", [25, "24", 24.0, True, None])
def test_numeric_id_mismatch_never_queues(inbox, num_id):
    with pytest.raises(ValueError, match="account_num_id"):
        listener(inbox).accept(command(account_num_id=num_id))
    assert inbox.due("STAR-DONG", NOW) is None


def test_numeric_id_optional_in_standard_ktrader_command(inbox):
    payload = command()
    del payload["account_num_id"]
    assert listener(inbox).accept(payload) == "queued"


def test_delayed_schedule_survives_restart_and_preserves_due_time(tmp_path):
    path = tmp_path / "inbox.sqlite3"
    payload = command(type="DELAYED_MARKET_ORDER", delay_seconds=120)
    first = Inbox(path)
    assert listener(first).accept(payload) == "queued"
    first.close()

    second = Inbox(path)
    adapter = Adapter()
    now = [NOW + 60]
    resumed = listener(second, adapter, clock=lambda: now[0])
    assert resumed.accept(payload) == "duplicate"
    assert resumed.execute_due() is False
    now[0] = NOW + 120
    assert resumed.execute_due() is True
    assert adapter.calls == [payload]
    second.close()


def test_execute_at_takes_precedence_over_relative_delay(inbox):
    worker = listener(inbox)
    worker.accept(command(type="DELAYED_MARKET_ORDER", execute_at=iso(NOW + 60), delay_seconds=600))
    assert inbox.due("STAR-DONG", NOW + 59) is None
    assert inbox.due("STAR-DONG", NOW + 60)[1] == NOW + 60


@pytest.mark.parametrize("delay", [None, 0, -1, True, "nan", "inf", "invalid"])
def test_invalid_relative_delay_is_rejected(inbox, delay):
    with pytest.raises(ValueError):
        listener(inbox).accept(command(type="DELAYED_MARKET_ORDER", delay_seconds=delay))


def test_queued_order_overdue_after_downtime_is_not_sent(tmp_path):
    path = tmp_path / "inbox.sqlite3"
    first = Inbox(path)
    listener(first).accept(command(type="DELAYED_MARKET_ORDER", delay_seconds=60))
    first.close()
    second = Inbox(path)
    adapter = Adapter()
    resumed = listener(second, adapter, clock=lambda: NOW + 361)
    assert resumed.execute_due() is True
    assert recorded_state(second) == "rejected"
    assert adapter.calls == []
    second.close()


def test_disable_before_due_prevents_delayed_execution(inbox):
    now = [NOW]
    adapter = Adapter()
    worker = listener(inbox, adapter, clock=lambda: now[0])
    worker.accept(command(type="DELAYED_MARKET_ORDER", delay_seconds=60))
    worker.accept(command(command_id="disable", type="SET_TRADING_ENABLED", trading_enabled=False))
    assert worker.execute_due() is True
    assert inbox.enabled("STAR-DONG") is False
    now[0] += 60
    assert worker.execute_due() is True
    assert recorded_state(inbox) == "disabled"
    assert adapter.calls == []


def test_disabled_arrival_is_not_resurrected_after_enable(inbox):
    inbox.set_enabled("STAR-DONG", False)
    worker = listener(inbox)
    payload = command(type="DELAYED_MARKET_ORDER", delay_seconds=60)
    assert worker.accept(payload) == "disabled"
    inbox.set_enabled("STAR-DONG", True)
    assert worker.accept(payload) == "duplicate"
    assert inbox.due("STAR-DONG", NOW + 120) is None


def test_trading_disable_persists_across_restart(tmp_path):
    path = tmp_path / "inbox.sqlite3"
    first = Inbox(path)
    worker = listener(first)
    worker.accept(command(type="SET_TRADING_ENABLED", trading_enabled=False))
    worker.execute_due()
    first.close()
    second = Inbox(path)
    assert second.enabled("STAR-DONG") is False
    second.close()


@pytest.mark.parametrize("value", ["false", "true", 0, 1, None])
def test_trading_enabled_requires_json_boolean(inbox, value):
    worker = listener(inbox)
    worker.accept(command(type="SET_TRADING_ENABLED", trading_enabled=value))
    worker.execute_due()
    assert recorded_state(inbox) == "rejected"
    assert inbox.enabled("STAR-DONG") is True


@pytest.mark.parametrize("raw", [b"", b"null", b"[]", b"1", b'"text"', b'{"a":1,"a":2}',
    b'{"nested":{"a":1,"a":2}}', b'{"qty_shares":NaN}', b'{"qty_shares":Infinity}',
    b'{"qty_shares":-Infinity}', b'{"qty_shares":1e999}', b'{"nested":[1e999]}', b'{"a":', b"\xff"])
def test_parse_message_rejects_ambiguous_malformed_or_nonfinite_json(raw):
    with pytest.raises((ValueError, UnicodeError)):
        parse_message(raw)


def test_message_size_limit():
    with pytest.raises(ValueError, match="oversized"):
        parse_message(b'{"a":"' + b"x" * 65536 + b'"}')


class Message:
    def __init__(self, raw):
        self.raw = raw

    def value(self):
        return self.raw

    def error(self):
        return None

    def partition(self):
        return 0

    def offset(self):
        return 12


class Consumer:
    def __init__(self, raw, stop, on_commit=lambda: None):
        self.message = Message(raw)
        self.stop = stop
        self.on_commit = on_commit
        self.commits = []

    def poll(self, timeout):
        self.stop.set()
        return self.message

    def commit(self, **kwargs):
        self.on_commit()
        self.commits.append(kwargs)


def test_consumer_commits_only_after_inbox_is_durable(tmp_path):
    path = tmp_path / "inbox.sqlite3"
    inbox = Inbox(path)
    adapter = Adapter()
    stop = threading.Event()

    def assert_persisted_before_commit():
        with sqlite3.connect(path) as db:
            assert db.execute("SELECT state FROM commands WHERE id='command-1'").fetchone() == ("queued",)
        assert adapter.calls == []

    consumer = Consumer(json.dumps(command()).encode(), stop, assert_persisted_before_commit)
    consume(consumer, listener(inbox, adapter), stop)
    assert consumer.commits == [{"message": consumer.message, "asynchronous": False}]
    assert adapter.calls == []
    assert recorded_state(inbox) == "queued"
    inbox.close()
    resumed = Inbox(path)
    assert listener(resumed, adapter).execute_due() is True
    assert adapter.calls == [command()]
    assert recorded_state(resumed) == "acknowledged"
    resumed.close()


def test_database_failure_does_not_commit_delivery(inbox, monkeypatch):
    def fail_add(*_):
        raise sqlite3.OperationalError("disk full")

    monkeypatch.setattr(inbox, "add", fail_add)
    stop = threading.Event()
    consumer = Consumer(json.dumps(command()).encode(), stop)
    worker = listener(inbox)
    with pytest.raises(sqlite3.OperationalError):
        consume(consumer, worker, stop)
    assert consumer.commits == []
    assert worker.adapter.calls == []


def test_invalid_delivery_is_committed_without_broker_call(inbox):
    stop = threading.Event()
    consumer = Consumer(b'{"type":"MARKET_ORDER","type":"LIMIT_ORDER"}', stop)
    worker = listener(inbox)
    consume(consumer, worker, stop)
    assert len(consumer.commits) == 1
    assert worker.adapter.calls == []
