from unittest.mock import Mock, call

from starwealth.report_loop import ReportLoop


def configured(monkeypatch):
    now = [1000]
    client = Mock()
    client.state = {"account": {"subAcctId": "broker-account"}}
    client.order_history.return_value = [{"orderId": "order-1", "marketCode": "US"}]
    journal = Mock()
    journal.order_correlations.return_value = {"order-1": {"command_id": "command-1"}}
    reporter = Mock(account_id="STAR-DONG", account_num_id=24)
    reporter.flush.return_value = 2
    reporter.tracked_open_orders.return_value = []
    account = Mock(return_value={"cash": 10})
    order = Mock(side_effect=lambda *args, **kwargs: {"order_id": "order-1", "cancelable": True})
    monkeypatch.setattr("starwealth.report_loop.account_snapshot", account)
    monkeypatch.setattr("starwealth.report_loop.normalize_order", order)
    loop = ReportLoop(client, journal, reporter, lambda: True, interval=30, clock=lambda: now[0])
    return loop, client, journal, reporter, account, order, now


def test_reports_orders_and_cash_with_command_correlation(monkeypatch):
    loop, client, _, reporter, account, order, _ = configured(monkeypatch)
    loop.tick()
    account.assert_called_once_with(client, "STAR-DONG", 24, True)
    assert client.order_history.call_args_list == [call(lookback_days=90),
        call(from_date="2000-01-01", statuses=[10, 20, 21, 40, 60])]
    assert order.call_args.kwargs["command_id"] == "command-1"
    assert order.call_args.kwargs["sub_account_id"] == "broker-account"
    reporter.queue_order.assert_called_once_with({"order_id": "order-1"})
    reporter.flush.assert_called_once()


def test_failed_cash_read_does_not_block_order_reports_or_outbox(monkeypatch, caplog):
    loop, _, _, reporter, account, _, _ = configured(monkeypatch)
    account.side_effect = RuntimeError("sensitive broker content")
    loop.tick()
    reporter.publish_account.assert_not_called()
    reporter.queue_order.assert_called_once()
    reporter.flush.assert_called_once()
    assert "sensitive broker content" not in caplog.text


def test_failed_order_read_does_not_block_cash_or_outbox(monkeypatch):
    loop, client, _, reporter, _, _, _ = configured(monkeypatch)
    client.order_history.side_effect = RuntimeError()
    loop.tick()
    reporter.publish_account.assert_called_once()
    reporter.queue_order.assert_not_called()
    reporter.flush.assert_called_once()


def test_malformed_order_does_not_prevent_other_order_reports(monkeypatch):
    loop, client, _, reporter, _, order, _ = configured(monkeypatch)
    client.order_history.return_value.append({"orderId": "order-2", "marketCode": "US"})
    order.side_effect = [ValueError("invalid row"), {"order_id": "order-2", "cancelable": False}]
    loop.tick()
    reporter.queue_order.assert_called_once_with({"order_id": "order-2"})
    reporter.flush.assert_called_once()


def test_interval_command_refresh_and_delivery_retry(monkeypatch):
    loop, client, _, reporter, _, _, now = configured(monkeypatch)
    reporter.flush.side_effect = [RuntimeError("offline"), 2]
    loop.tick()
    assert loop.next_refresh == 1030
    now[0] = 1001
    loop.tick()
    assert client.order_history.call_count == 2
    loop.after_command()
    assert loop.next_refresh == 1003
    now[0] = 1003
    loop.tick()
    assert reporter.flush.call_count == 2
    assert client.order_history.call_count == 4


def test_old_gtc_missing_from_recent_and_active_is_reconciled_by_symbol(monkeypatch):
    loop, client, _, reporter, _, order, _ = configured(monkeypatch)
    reporter.tracked_open_orders.return_value = [{"order_id": "old-order", "symbol": "AAPL",
                                                  "created_at": "2025-01-02T12:00:00Z", "status": "NEW"}]
    terminal = {"orderId": "old-order", "marketCode": "US", "orderStatus": 70}
    client.order_history.side_effect = [[], [], [{"orderId": "unrelated-order"}, terminal]]
    order.side_effect = lambda row, **kwargs: {"order_id": row["orderId"], "status": "CANCELED", "cancelable": False}
    loop.tick()
    assert client.order_history.call_args_list[-1] == call(from_date="2025-01-01", stock_code="AAPL")
    assert order.call_args.args[0] == terminal
    reporter.queue_order.assert_called_once_with({"order_id": "old-order", "status": "CANCELED"})
    reporter.flush.assert_called_once_with(max_records=20)


def test_missing_tracked_order_is_not_inferred_canceled(monkeypatch, caplog):
    loop, client, _, reporter, _, order, _ = configured(monkeypatch)
    reporter.tracked_open_orders.return_value = [{"order_id": "old-order", "symbol": "AAPL",
                                                  "created_at": "2025-01-02T12:00:00Z", "status": "NEW"}]
    client.order_history.return_value = []
    loop.tick()
    order.assert_not_called()
    reporter.queue_order.assert_not_called()
    assert "retaining its last reported state" in caplog.text
    reporter.flush.assert_called_once_with(max_records=20)


def test_periodic_reconcile_runs_with_reporting_interval(monkeypatch):
    loop, _, _, reporter, _, _, now = configured(monkeypatch)
    loop.reconcile = Mock(side_effect=[[{"state": "unknown"}], [{"state": "acknowledged"}]])
    loop.tick()
    now[0] = 1001
    loop.tick()
    assert loop.reconcile.call_count == 1
    now[0] = 1030
    loop.tick()
    assert loop.reconcile.call_count == 2
    assert reporter.publish_account.call_count == 2
    assert reporter.flush.call_count == 2


def test_reconciliation_error_does_not_block_account_reports(monkeypatch, caplog):
    loop, _, _, reporter, _, _, _ = configured(monkeypatch)
    loop.reconcile = Mock(side_effect=RuntimeError("private exception content"))
    loop.tick()
    reporter.publish_account.assert_called_once()
    reporter.queue_order.assert_called_once()
    reporter.flush.assert_called_once()
    assert "private exception content" not in caplog.text
