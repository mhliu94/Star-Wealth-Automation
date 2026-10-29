from copy import deepcopy

import pytest

from starwealth.errors import OutcomeUnknown
from starwealth.fok import FokExecutor, SubmissionRejected


COMMAND = {"type": "LIMIT_ORDER_FOK", "command_id": "command-1", "account_id": "STAR-DONG",
           "symbol": "AAPL", "side": "BUY", "qty_shares": 10, "limit_price": "100.00"}


def view(status="NEW", filled="0", **overrides):
    result = {"order_id": "broker-1", "symbol": "AAPL", "side": "BUY", "quantity": "10",
              "limit_price": "100.00", "order_type": "LIMIT", "status": status,
              "filled_quantity": filled, "cancelable": status in {"NEW", "PARTIALLY_FILLED"}}
    result.update(overrides)
    return result


class Broker:
    def __init__(self, views):
        self.views = list(views)
        self.events = []
        self.submit_error = self.cancel_error = None
        self.now = 100

    def submit(self, command, identity):
        self.events.append(("submit", identity))
        if self.submit_error:
            raise self.submit_error
        return "broker-1"

    def detail(self, order_id):
        self.events.append(("detail", order_id))
        response = self.views.pop(0) if len(self.views) > 1 else self.views[0]
        if isinstance(response, BaseException):
            raise response
        return deepcopy(response)

    def cancel(self, order_id, identity):
        self.events.append(("cancel", order_id, identity))
        if self.cancel_error:
            raise self.cancel_error

    def sleep(self, seconds):
        self.events.append(("sleep", seconds))
        self.now += seconds

    def executor(self, path, **kwargs):
        return FokExecutor(path, submit=self.submit, detail=self.detail, cancel=self.cancel,
                           clock=lambda: self.now, sleep=self.sleep, **kwargs)


def actions(broker, name):
    return [item for item in broker.events if item[0] == name]


def test_immediate_fill_never_cancels_and_replay_never_resubmits(tmp_path):
    broker = Broker([view("FILLED", "10")])
    executor = broker.executor(tmp_path / "fok.db")
    result = executor.execute(COMMAND, "intent")
    assert result == {"state": "acknowledged", "order_id": "broker-1", "status": "FILLED"}
    assert broker.events == [("sleep", 5), ("submit", "intent"), ("detail", "broker-1")]
    assert executor.execute(COMMAND, "intent") == result
    assert len(actions(broker, "submit")) == 1
    assert executor.pending() == []
    executor.close()


def test_fill_during_two_second_wait_never_cancels(tmp_path):
    broker = Broker([view(), view("FILLED", "10")])
    executor = broker.executor(tmp_path / "fok.db")
    assert executor.execute(COMMAND, "intent")["status"] == "FILLED"
    assert actions(broker, "sleep") == [("sleep", 5), ("sleep", 2)]
    assert not actions(broker, "cancel")
    executor.close()


def test_partial_fill_cancels_remainder_and_waits_for_confirmation(tmp_path):
    broker = Broker([view(), view("PARTIALLY_FILLED", "4"),
                     view("PENDING_CANCEL", "4"), view("CANCELED", "4")])
    executor = broker.executor(tmp_path / "fok.db")
    assert executor.execute(COMMAND, "intent")["status"] == "CANCELED"
    assert actions(broker, "cancel") == [("cancel", "broker-1", executor.cancel_intent_id("intent"))]
    assert actions(broker, "sleep") == [("sleep", 5), ("sleep", 2), ("sleep", 1)]
    executor.close()


def test_fill_wins_cancel_race(tmp_path):
    broker = Broker([view(), view(), view("FILLED", "10")])
    executor = broker.executor(tmp_path / "fok.db")
    assert executor.execute(COMMAND, "intent")["status"] == "FILLED"
    assert len(actions(broker, "cancel")) == 1
    executor.close()


@pytest.mark.parametrize("status", ["REJECTED", "EXPIRED"])
def test_broker_rejected_or_expired_not_reported_as_filled(tmp_path, status):
    broker = Broker([view(status)])
    executor = broker.executor(tmp_path / "fok.db")
    assert executor.execute(COMMAND, "intent")["state"] == "rejected"
    assert not actions(broker, "cancel")
    executor.close()


def test_cancel_timeout_then_confirmed_cancel_is_success(tmp_path):
    broker = Broker([view(), view(), view("CANCELED")])
    broker.cancel_error = TimeoutError()
    executor = broker.executor(tmp_path / "fok.db")
    assert executor.execute(COMMAND, "intent")["status"] == "CANCELED"
    assert len(actions(broker, "cancel")) == 1
    executor.close()


def test_unknown_cancellation_stays_unknown_and_never_retries_after_restart(tmp_path):
    path = tmp_path / "fok.db"
    broker = Broker([view()])
    executor = broker.executor(path)
    with pytest.raises(OutcomeUnknown):
        executor.execute(COMMAND, "intent")
    assert actions(broker, "sleep") == [("sleep", 5), ("sleep", 2), ("sleep", 1), ("sleep", 1), ("sleep", 2)]
    assert executor.pending()[0]["cancel_started"] is True
    executor.close()
    executor = broker.executor(path)
    result = executor.recover_pending()
    assert result[0]["state"] == "unknown"
    assert len(actions(broker, "submit")) == 1
    assert len(actions(broker, "cancel")) == 1
    broker.views = [view("CANCELED")]
    assert executor.recover_pending()[0]["status"] == "CANCELED"
    assert executor.pending() == []
    executor.close()


def test_unknown_submit_blocks_all_new_intents_and_never_resubmits(tmp_path):
    path = tmp_path / "fok.db"
    broker = Broker([view()])
    broker.submit_error = TimeoutError()
    executor = broker.executor(path)
    with pytest.raises(OutcomeUnknown):
        executor.execute(COMMAND, "intent")
    executor.close()
    executor = broker.executor(path)
    broker.submit_error = None
    for identity in ("intent", "another-intent"):
        with pytest.raises(OutcomeUnknown):
            executor.execute(COMMAND, identity)
    assert len(actions(broker, "submit")) == 1
    assert not actions(broker, "cancel")
    executor.close()


def test_verified_submission_rejection_does_not_block_new_intent(tmp_path):
    broker = Broker([view("FILLED", "10")])
    broker.submit_error = SubmissionRejected("No financial write occurred")
    executor = broker.executor(tmp_path / "fok.db")
    assert executor.execute(COMMAND, "rejected-intent") == {
        "state": "rejected", "order_id": None, "status": "REJECTED"}
    assert executor.pending() == []
    broker.submit_error = None
    assert executor.execute(COMMAND, "new-intent")["status"] == "FILLED"
    assert len(actions(broker, "submit")) == 2
    assert not actions(broker, "cancel")
    executor.close()


def test_verified_submission_rejection_survives_restart_without_retry(tmp_path):
    path = tmp_path / "fok.db"
    broker = Broker([view()])
    broker.submit_error = SubmissionRejected("Broker explicitly rejected")
    executor = broker.executor(path)
    executor.execute(COMMAND, "intent")
    executor.close()
    executor = broker.executor(path)
    assert executor.execute(COMMAND, "intent")["status"] == "REJECTED"
    assert executor.recover_pending() == []
    assert len(actions(broker, "submit")) == 1
    executor.close()


def test_journal_resolves_absent_write_and_releases_submission_gate(tmp_path):
    path = tmp_path / "fok.db"
    broker = Broker([view("FILLED", "10")])
    broker.submit_error = KeyboardInterrupt()
    executor = broker.executor(path)
    with pytest.raises(KeyboardInterrupt):
        executor.execute(COMMAND, "interrupted-intent")
    executor.close()
    executor = broker.executor(path, resolve_submit=lambda identity: False)
    assert executor.recover_pending() == [{"intent_id": "interrupted-intent", "state": "rejected",
                                            "order_id": None, "status": "REJECTED"}]
    broker.submit_error = None
    assert executor.execute(COMMAND, "new-intent")["status"] == "FILLED"
    assert executor.pending() == []
    executor.close()


def test_unresolved_journal_does_not_release_submission_gate(tmp_path):
    broker = Broker([view()])
    broker.submit_error = TimeoutError()
    executor = broker.executor(tmp_path / "fok.db", resolve_submit=lambda identity: None)
    with pytest.raises(OutcomeUnknown):
        executor.execute(COMMAND, "intent")
    assert executor.recover_pending()[0]["state"] == "unknown"
    with pytest.raises(OutcomeUnknown):
        executor.execute(COMMAND, "another-intent")
    assert len(actions(broker, "submit")) == 1
    executor.close()


def test_crash_before_submit_is_conservatively_unresolved(tmp_path):
    path = tmp_path / "fok.db"
    broker = Broker([view()])
    executor = broker.executor(path)
    def interrupted(_):
        raise KeyboardInterrupt()
    executor.sleep = interrupted
    with pytest.raises(KeyboardInterrupt):
        executor.execute(COMMAND, "intent")
    executor.close()
    executor = broker.executor(path)
    assert executor.pending()[0]["state"] == "pending_submit"
    with pytest.raises(OutcomeUnknown):
        executor.execute(COMMAND, "intent")
    assert not actions(broker, "submit")
    executor.close()


def test_journal_resolves_crash_after_submit_before_id_save(tmp_path):
    path = tmp_path / "fok.db"
    broker = Broker([view("FILLED", "10")])
    broker.submit_error = KeyboardInterrupt()
    executor = broker.executor(path)
    with pytest.raises(KeyboardInterrupt):
        executor.execute(COMMAND, "intent")
    executor.close()
    executor = broker.executor(path, resolve_submit=lambda identity: "broker-1")
    assert executor.recover_pending()[0]["status"] == "FILLED"
    assert len(actions(broker, "submit")) == 1
    assert not actions(broker, "cancel")
    executor.close()


def test_recovery_known_id_can_send_first_cancel(tmp_path):
    path = tmp_path / "fok.db"
    broker = Broker([KeyboardInterrupt()])
    executor = broker.executor(path)
    with pytest.raises(KeyboardInterrupt):
        executor.execute(COMMAND, "intent")
    executor.close()
    broker.views = [view(), view("CANCELED")]
    executor = broker.executor(path)
    assert executor.recover_pending()[0]["status"] == "CANCELED"
    assert len(actions(broker, "submit")) == 1
    assert len(actions(broker, "cancel")) == 1
    executor.close()


def test_crash_inside_cancel_never_sends_a_second_cancel(tmp_path):
    path = tmp_path / "fok.db"
    broker = Broker([view()])
    broker.cancel_error = KeyboardInterrupt()
    executor = broker.executor(path)
    with pytest.raises(KeyboardInterrupt):
        executor.execute(COMMAND, "intent")
    executor.close()
    broker.cancel_error = None
    broker.views = [view("CANCELED")]
    executor = broker.executor(path)
    assert executor.recover_pending()[0]["status"] == "CANCELED"
    assert len(actions(broker, "cancel")) == 1
    executor.close()


def test_cancel_crash_before_reservation_allows_first_actual_cancel_on_recovery(tmp_path):
    path = tmp_path / "fok.db"
    broker = Broker([view()])
    broker.cancel_error = KeyboardInterrupt()
    executor = broker.executor(path)
    with pytest.raises(KeyboardInterrupt):
        executor.execute(COMMAND, "intent")
    executor.close()
    broker.cancel_error = None
    broker.views = [view(), view("CANCELED")]
    checked = []
    def resolve_cancel(identity):
        checked.append(identity)
        return False  # No durable cancel journal reservation: no broker write.
    executor = broker.executor(path, resolve_cancel=resolve_cancel)
    assert executor.recover_pending()[0]["status"] == "CANCELED"
    assert checked == [executor.cancel_intent_id("intent")]
    # First callback crashed before writing. Recovery invokes the first real write.
    assert len(actions(broker, "cancel")) == 2
    assert len(actions(broker, "submit")) == 1
    executor.close()


@pytest.mark.parametrize("resolved", [True, None])
def test_pending_or_uncertain_cancel_journal_never_allows_resend(tmp_path, resolved):
    path = tmp_path / "fok.db"
    broker = Broker([view()])
    broker.cancel_error = KeyboardInterrupt()
    executor = broker.executor(path)
    with pytest.raises(KeyboardInterrupt):
        executor.execute(COMMAND, "intent")
    executor.close()
    broker.cancel_error = None
    executor = broker.executor(path, resolve_cancel=lambda _: resolved)
    assert executor.recover_pending()[0]["state"] == "unknown"
    assert len(actions(broker, "cancel")) == 1
    executor.close()


def test_cancel_auth_failure_does_not_loop_within_one_polling_cycle(tmp_path):
    broker = Broker([view()])
    broker.cancel_error = RuntimeError("Authentication failed before reservation")
    resolver_calls = []
    def resolve_cancel(identity):
        resolver_calls.append(identity)
        return False
    executor = broker.executor(tmp_path / "fok.db", resolve_cancel=resolve_cancel)
    with pytest.raises(OutcomeUnknown):
        executor.execute(COMMAND, "intent")
    assert len(actions(broker, "cancel")) == 1
    assert resolver_calls == []
    executor.close()


def test_newly_cancelable_order_is_cancelled_during_polling_without_extra_wait(tmp_path):
    broker = Broker([view(cancelable=False), view(cancelable=False), view(), view("CANCELED")])
    executor = broker.executor(tmp_path / "fok.db")
    assert executor.execute(COMMAND, "intent")["status"] == "CANCELED"
    assert len(actions(broker, "cancel")) == 1
    assert actions(broker, "sleep") == [("sleep", 5), ("sleep", 2), ("sleep", 1)]
    assert broker.events.index(actions(broker, "cancel")[0]) > broker.events.index(("sleep", 2))
    executor.close()


@pytest.mark.parametrize("patch", [
    {"order_id": "another-order"}, {"symbol": "MSFT"}, {"side": "SELL"},
    {"quantity": "11"}, {"limit_price": "99"}, {"order_type": "MARKET"},
    {"cancelable": 1}, {"status": "UNRECOGNIZED"}, {"filled_quantity": "11"},
    {"filled_quantity": "-1"}, {"filled_quantity": "NaN"},
    {"status": "FILLED", "filled_quantity": "4"},
])
def test_unverified_order_never_cancelled(tmp_path, patch):
    broker = Broker([view(**patch)])
    executor = broker.executor(tmp_path / "fok.db")
    with pytest.raises(OutcomeUnknown):
        executor.execute(COMMAND, "intent")
    assert not actions(broker, "cancel")
    executor.close()


def test_fills_cannot_regress(tmp_path):
    broker = Broker([view("PARTIALLY_FILLED", "4"), view("PARTIALLY_FILLED", "3")])
    executor = broker.executor(tmp_path / "fok.db")
    with pytest.raises(OutcomeUnknown):
        executor.execute(COMMAND, "intent")
    assert not actions(broker, "cancel")
    executor.close()


@pytest.mark.parametrize("status,cancelable", [("NEW", False), ("UNKNOWN", True), ("PENDING_CANCEL", True)])
def test_cancel_requires_both_open_status_and_cancelability(tmp_path, status, cancelable):
    broker = Broker([view(status, cancelable=cancelable)])
    executor = broker.executor(tmp_path / "fok.db")
    with pytest.raises(OutcomeUnknown):
        executor.execute(COMMAND, "intent")
    assert not actions(broker, "cancel")
    executor.close()


def test_conflicting_intent_payload_rejected(tmp_path):
    broker = Broker([view("FILLED", "10")])
    executor = broker.executor(tmp_path / "fok.db")
    executor.execute(COMMAND, "intent")
    with pytest.raises(ValueError, match="different contents"):
        executor.execute({**COMMAND, "qty_shares": 11}, "intent")
    assert len(actions(broker, "submit")) == 1
    executor.close()


def test_manual_reconciliation_mismatch_retains_unknown_submit_gate(tmp_path):
    broker = Broker([view(symbol="MSFT")])
    broker.submit_error = TimeoutError()
    executor = broker.executor(tmp_path / "fok.db")
    with pytest.raises(OutcomeUnknown):
        executor.execute(COMMAND, "intent")
    with pytest.raises(OutcomeUnknown):
        executor.reconcile_submission("intent", "broker-1")
    assert executor.pending()[0]["order_id"] is None
    with pytest.raises(OutcomeUnknown):
        executor.execute(COMMAND, "new-intent")
    assert not actions(broker, "cancel")
    executor.close()


@pytest.mark.parametrize("patch", [
    {"qty_shares": 1.5}, {"qty_shares": 0}, {"limit_price": 0}, {"side": "buy"},
    {"qty_shares": True}, {"limit_price": "Infinity"}, {"notional_usd": 10},
    {"type": "MARKET_ORDER"},
])
def test_invalid_commands_rejected_before_any_operation(tmp_path, patch):
    broker = Broker([view()])
    executor = broker.executor(tmp_path / "fok.db")
    with pytest.raises(ValueError):
        executor.execute({**COMMAND, **patch}, "intent")
    assert broker.events == []
    executor.close()
