"""Kafka reporting must never fabricate fills or turn retries into new events."""
import json

import pytest

from starwealth.reporting import Reporter, ReportingError, validate_order


def order(**changes):
    return {"order_id": "9007199254740993123", "created_at": "2026-10-29T12:00:00Z",
            "updated_at": "2026-10-29T12:00:01Z", "symbol": "AAPL", "side": "BUY",
            "order_type": "LIMIT", "status": "NEW", "quantity": 10,
            "filled_quantity": 0, "average_fill_price": None, "limit_price": 100,
            "command_id": "command-1", **changes}


def account(**changes):
    return {"cash": 150.5, "cash_by_currency": {"USD": 150.5, "HKD": 12},
            "available_cash_by_currency": {"USD": 100}, "positions": [
                {"symbol": "AAPL", "qty": 10, "avg_price": 90, "available_qty": 8}],
            "ts": "2026-10-29T12:00:00Z", "trading_enabled": True, **changes}


class Producer:
    def __init__(self, outcomes=()):
        self.outcomes = list(outcomes)
        self.sent = []
        self.callbacks = []

    def produce(self, topic, *, key, value, on_delivery):
        self.sent.append((topic, key, value))
        self.callbacks.append(on_delivery)

    def flush(self, timeout):
        outcome = self.outcomes.pop(0) if self.outcomes else "ok"
        callbacks, self.callbacks = self.callbacks, []
        if outcome == "timeout":
            return 1
        if outcome == "missing-callback":
            return 0
        for callback in callbacks:
            callback(None if outcome == "ok" else RuntimeError("delivery failed"), object())
        return 0


def reporter(tmp_path, producer=None):
    return Reporter(producer or Producer(), tmp_path / "reports.sqlite3",
                    account_id="STAR-DONG", account_num_id=24)


def events(producer):
    return [json.loads(item[2]) for item in producer.sent]


def test_account_and_order_partition_keys_and_schema(tmp_path):
    producer = Producer()
    report = reporter(tmp_path, producer)
    report.publish_account(account())
    report.queue_order(order())
    assert producer.sent == []
    assert report.flush() == 2
    assert producer.sent[0][:2] == ("account-details", b"STAR-DONG")
    assert producer.sent[1][:2] == ("order-status", b'["STAR-DONG","9007199254740993123"]')
    cash, status = events(producer)
    assert cash["account_id"] == status["account_id"] == "STAR-DONG"
    assert cash["account_num_id"] == status["account_num_id"] == 24
    assert cash["cash_by_currency"] == {"USD": 150.5, "HKD": 12}
    assert status["schema_version"] == 1 and status["sequence"] == 0
    assert status["order_id"] == "9007199254740993123"
    assert status["command_id"] == "command-1"
    assert status["status"] == "NEW" and status["filled_quantity"] == 0
    report.close()


@pytest.mark.parametrize("failure", ["error", "timeout", "missing-callback"])
def test_failed_delivery_retries_identical_payload_after_restart(tmp_path, failure):
    first_producer = Producer([failure])
    first = reporter(tmp_path, first_producer)
    first.queue_order(order())
    with pytest.raises(ReportingError):
        first.flush()
    first.close()
    second_producer = Producer()
    second = reporter(tmp_path, second_producer)
    assert second.flush() == 1
    assert second_producer.sent == first_producer.sent
    assert second.flush() == 0
    assert second.queue_order(order()) is False
    second.close()


def test_crash_after_kafka_ack_before_local_commit_replays_exact_event(tmp_path):
    first_producer = Producer()
    first = reporter(tmp_path, first_producer)
    first.queue_order(order())
    topic, key, payload = first.db.execute("SELECT topic,message_key,payload FROM report_outbox").fetchone()
    first._deliver(topic, key, payload)
    first.close()
    second_producer = Producer()
    second = reporter(tmp_path, second_producer)
    assert second.flush() == 1
    assert second_producer.sent == first_producer.sent
    second.close()


def test_sequence_increments_across_restart_and_unchanged_replay_retains_sequence(tmp_path):
    first = reporter(tmp_path)
    assert first.queue_order(order()) is True
    assert first.flush() == 1
    assert first.queue_order(order(updated_at="2026-10-29T12:00:09Z")) is False
    first.close()

    producer = Producer()
    second = reporter(tmp_path, producer)
    assert second.queue_order(order(updated_at="2026-10-29T12:00:10Z")) is True
    assert second.flush() == 1
    assert events(producer)[0]["sequence"] == 0
    assert events(producer)[0]["updated_at"] == "2026-10-29T12:00:01Z"
    assert second.queue_order(order(status="PARTIALLY_FILLED", filled_quantity=3, average_fill_price=99)) is True
    assert second.flush() == 1
    assert events(producer)[1]["sequence"] == 1
    assert second.queue_order(order(status="FILLED", filled_quantity=10, average_fill_price=99)) is True
    assert second.flush() == 1
    assert events(producer)[2]["sequence"] == 2
    second.close()


def test_pending_failure_preserves_sequence_order_but_other_entities_publish(tmp_path):
    producer = Producer(["ok", "error", "ok"])
    report = reporter(tmp_path, producer)
    report.queue_order(order())
    report.queue_order(order(status="PARTIALLY_FILLED", filled_quantity=3, average_fill_price=99))
    report.queue_order(order(order_id="independent-order"))
    report.publish_account(account())
    with pytest.raises(ReportingError, match="2 delivered"):
        report.flush()
    assert [event.get("order_id") for event in events(producer)] == [
        None, "9007199254740993123", "independent-order"]
    assert report.flush() == 2
    assert [event["sequence"] for event in events(producer)[-2:]] == [0, 1]
    report.close()


def test_account_delivery_failure_does_not_suppress_orders(tmp_path):
    producer = Producer(["error", "ok"])
    report = reporter(tmp_path, producer)
    report.publish_account(account())
    report.queue_order(order())
    with pytest.raises(ReportingError, match="1 delivered"):
        report.flush()
    assert len(producer.sent) == 2
    assert report.flush() == 1
    assert producer.sent[2] == producer.sent[0]
    report.close()


def test_acknowledged_order_succeeds_with_unrelated_producer_messages_pending(tmp_path):
    class BackloggedProducer(Producer):
        def flush(self, timeout):
            super().flush(timeout)
            return 1

    report = reporter(tmp_path, BackloggedProducer())
    report.queue_order(order())
    assert report.flush() == 1
    assert report.flush() == 0
    report.close()


def test_changed_creation_or_regressed_fill_is_rejected_without_new_event(tmp_path):
    report = reporter(tmp_path)
    report.queue_order(order(status="PARTIALLY_FILLED", filled_quantity=4, average_fill_price=99))
    with pytest.raises(ValueError, match="creation"):
        report.queue_order(order(created_at="2026-10-29T11:59:00Z", filled_quantity=4))
    with pytest.raises(ValueError, match="regressed"):
        report.queue_order(order(filled_quantity=3))
    assert report.flush() == 1
    report.close()


def test_update_timestamp_never_moves_backward(tmp_path):
    producer = Producer()
    report = reporter(tmp_path, producer)
    report.queue_order(order(updated_at="2026-10-29T12:00:10Z"))
    report.queue_order(order(status="PARTIALLY_FILLED", filled_quantity=3, average_fill_price=99,
                             updated_at="2026-10-29T12:00:05Z"))
    assert report.flush() == 2
    assert events(producer)[1]["updated_at"] == "2026-10-29T12:00:10Z"
    report.close()


@pytest.mark.parametrize("changes", [
    {"status": "FILLED", "filled_quantity": 0},
    {"status": "FILLED", "filled_quantity": 9},
    {"status": "PARTIALLY_FILLED", "filled_quantity": 0},
    {"status": "PARTIALLY_FILLED", "filled_quantity": 10},
    {"filled_quantity": 11}, {"filled_quantity": -1},
    {"quantity": True}, {"quantity": 0}, {"quantity": float("nan")},
    {"average_fill_price": float("inf")}, {"limit_price": None},
    {"order_id": 9007199254740993123}, {"created_at": "2026-10-29T12:00:00"},
    {"updated_at": "2026-10-29T11:59:59Z"}, {"status": "MadeUpStatus"},
    {"extra_broker_data": "must not publish"}, {"quantity": None},
    {"schema_version": 1}, {"sequence": 77}, {"account_id": "other"},
    {"side": []}, {"order_type": {}}, {"status": []},
])
def test_invalid_order_data_cannot_claim_fill_or_enter_outbox(tmp_path, changes):
    report = reporter(tmp_path)
    with pytest.raises(ValueError):
        report.queue_order(order(**changes))
    assert report.flush() == 0
    report.close()


def test_amount_order_requires_positive_notional_and_preserves_unknown_quantity():
    value = validate_order(order(quantity=None, notional=1000, order_type="MARKET"))
    assert value["quantity"] is None
    assert value["filled_quantity"] == 0
    assert value["average_fill_price"] is None


def test_partial_cancel_retains_actual_fill_instead_of_claiming_full_fill(tmp_path):
    producer = Producer()
    report = reporter(tmp_path, producer)
    report.queue_order(order(status="CANCELED", filled_quantity=3, average_fill_price=99))
    report.flush()
    assert events(producer)[0]["status"] == "CANCELED"
    assert events(producer)[0]["filled_quantity"] == 3
    report.close()


@pytest.mark.parametrize("changes", [
    {"cash": None}, {"cash": float("inf")}, {"trading_enabled": "true"},
    {"cash_by_currency": {"USD": float("nan")}}, {"ts": "not a date"},
    {"positions": [{"symbol": "AAPL", "qty": 5}]},
    {"positions": [{"symbol": "AAPL", "qty": 5, "avg_price": float("inf")}]},
    {"account_id": "OTHER"}, {"account_num_id": 25},
])
def test_invalid_balances_or_positions_do_not_publish_fabricated_snapshot(tmp_path, changes):
    report = reporter(tmp_path)
    with pytest.raises(ValueError):
        report.publish_account(account(**changes))
    assert report.flush() == 0
    report.close()


def test_pending_order_can_publish_without_being_present_in_next_broker_list(tmp_path):
    first = reporter(tmp_path, Producer(["timeout"]))
    first.queue_order(order())
    with pytest.raises(ReportingError):
        first.flush()
    first.close()
    producer = Producer()
    second = reporter(tmp_path, producer)
    second.publish_account(account(positions=[]))
    assert second.flush() == 2
    assert events(producer)[1]["status"] == "NEW"
    second.close()


def test_bounded_flush_leaves_remaining_events_durable_across_restart(tmp_path):
    first_producer = Producer()
    first = reporter(tmp_path, first_producer)
    for index in range(5):
        first.queue_order(order(order_id=f"order-{index}"))
    assert first.flush(max_records=2) == 2
    assert len(first_producer.sent) == 2
    first.close()
    second_producer = Producer()
    second = reporter(tmp_path, second_producer)
    assert second.flush(max_records=2) == 2
    assert second.flush(max_records=2) == 1
    assert [item["order_id"] for item in events(second_producer)] == ["order-2", "order-3", "order-4"]
    second.close()


def test_bounded_flush_counts_actual_attempts_and_services_independent_streams(tmp_path):
    producer = Producer(["error"])
    report = reporter(tmp_path, producer)
    report.queue_order(order())
    report.queue_order(order(status="PARTIALLY_FILLED", filled_quantity=3, average_fill_price=99))
    report.queue_order(order(order_id="independent-order"))
    with pytest.raises(ReportingError):
        report.flush(max_records=2)
    assert len(producer.sent) == 2
    assert events(producer)[1]["order_id"] == "independent-order"
    assert report.db.execute("SELECT COUNT(*) FROM report_outbox").fetchone()[0] == 2
    assert report.flush(max_records=2) == 1
    assert report.flush(max_records=2) == 1
    assert [event["sequence"] for event in events(producer)[-2:]] == [0, 1]
    report.close()


@pytest.mark.parametrize("maximum", [0, -1, True, 2.5, "20"])
def test_bounded_flush_rejects_invalid_limit(tmp_path, maximum):
    report = reporter(tmp_path)
    with pytest.raises(ValueError):
        report.flush(max_records=maximum)
    report.close()


def test_account_outage_coalesces_to_latest_snapshot_without_dropping_order_events(tmp_path):
    first = reporter(tmp_path, Producer(["timeout"]))
    first.publish_account(account())
    with pytest.raises(ReportingError):
        first.flush()
    first.queue_order(order())
    first.queue_order(order(status="PARTIALLY_FILLED", filled_quantity=3, average_fill_price=99))
    for second in range(1, 20):
        first.publish_account(account(cash=100 + second, cash_by_currency={"USD": 100 + second},
                                      ts=f"2026-10-29T12:00:{second:02d}Z"))
    assert first.db.execute("SELECT COUNT(*) FROM report_outbox").fetchone()[0] == 3
    first.close()
    producer = Producer()
    resumed = reporter(tmp_path, producer)
    assert resumed.flush() == 3
    assert [event["sequence"] for event in events(producer) if "sequence" in event] == [0, 1]
    accounts = [event for event in events(producer) if "cash" in event]
    assert len(accounts) == 1 and accounts[0]["cash"] == 119
    resumed.close()


def test_older_account_observation_cannot_replace_pending_fresh_snapshot(tmp_path):
    producer = Producer()
    report = reporter(tmp_path, producer)
    report.publish_account(account(ts="2026-10-29T12:01:00Z"))
    with pytest.raises(ValueError, match="newer"):
        report.publish_account(account(ts="2026-10-29T12:00:00Z", cash=0))
    assert report.flush() == 1
    assert events(producer)[0]["cash"] == 150.5
    report.close()


def test_large_failed_order_backlog_cannot_starve_current_account_or_other_order(tmp_path):
    producer = Producer(["ok", "error", "ok"])
    report = reporter(tmp_path, producer)
    for quantity in range(25):
        report.queue_order(order(quantity=100, filled_quantity=quantity,
                                 status="PARTIALLY_FILLED" if quantity else "NEW",
                                 average_fill_price=99 if quantity else None))
    report.queue_order(order(order_id="independent-order"))
    report.publish_account(account())
    with pytest.raises(ReportingError, match="2 delivered"):
        report.flush(max_records=20)
    delivered = events(producer)
    assert len(delivered) == 3
    assert delivered[0]["cash"] == 150.5
    assert delivered[1]["order_id"] == "9007199254740993123"
    assert delivered[1]["sequence"] == 0
    assert delivered[2]["order_id"] == "independent-order"
    assert report.db.execute("SELECT COUNT(*) FROM report_outbox").fetchone()[0] == 25
    assert report.flush() == 25
    assert [event["sequence"] for event in events(producer)[3:]] == list(range(25))
    report.close()


def test_tracked_open_orders_persist_across_restart_until_terminal_observation(tmp_path):
    first = reporter(tmp_path)
    first.queue_order(order(order_id="open-order"))
    first.queue_order(order(order_id="partial-order", status="PARTIALLY_FILLED",
                            filled_quantity=3, average_fill_price=99))
    first.queue_order(order(order_id="unknown-order", status="UNKNOWN"))
    for status in ("FILLED", "CANCELED", "REJECTED", "EXPIRED"):
        first.queue_order(order(order_id=status, status=status,
                                filled_quantity=10 if status == "FILLED" else 0))
    first.flush()
    first.close()
    second = reporter(tmp_path)
    tracked = second.tracked_open_orders()
    assert [value["order_id"] for value in tracked] == ["open-order", "partial-order", "unknown-order"]
    assert all(value["account_id"] == "STAR-DONG" for value in tracked)
    assert tracked[1]["filled_quantity"] == 3
    second.queue_order(order(order_id="partial-order", status="CANCELED",
                             filled_quantity=3, average_fill_price=99))
    assert [value["order_id"] for value in second.tracked_open_orders()] == ["open-order", "unknown-order"]
    # The latest normalized observation governs reconciliation even before delivery.
    assert second.db.execute("SELECT COUNT(*) FROM report_outbox").fetchone()[0] == 1
    second.close()
