"""Serialized broker reads and independent Kafka snapshot delivery."""
from datetime import datetime, timezone
import logging
import threading
from concurrent.futures import Future
import time

from .account_data import account_snapshot
from .order_data import ACTIVE, exact_order_row, normalize_order

LOG = logging.getLogger("starwealth.reporting")


class ReportLoop:
    def __init__(self, client, journal, reporter, enabled, *, interval=30, lookback_days=90, clock=time.time, reconcile=None, queue_rejections=None):
        self.client, self.journal, self.reporter, self.enabled = client, journal, reporter, enabled
        self.interval, self.lookback_days, self.clock = interval, lookback_days, clock
        self.reconcile = reconcile
        self.queue_rejections = queue_rejections
        self.next_refresh = 0
        self.next_account = 0
        self.next_reconcile = 0
        self.refresh_until = 0
        from .account_delta import OrderChanges
        self.order_changes = OrderChanges()

    def after_command(self):
        self.next_refresh = 0
        self.next_account = 0
        self.refresh_until = self.clock() + 10

    def tick(self, *, force=False):
        if not force and self.clock() < self.next_refresh:
            return
        started = self.clock()
        elapsed_start = time.perf_counter_ns()
        account_ok, order_count = False, 0
        observations = []
        try:
            if self.queue_rejections:
                try:
                    self.queue_rejections()
                except Exception as exc:
                    LOG.error("Child rejection delivery deferred (%s)", type(exc).__name__)
            if self.reconcile is not None and self.clock() >= self.next_reconcile:
                self.next_reconcile = self.clock() + self.interval
                try:
                    for outcome in self.reconcile():
                        LOG.info("FOK reconciliation state=%s", outcome["state"])
                except Exception as exc:
                    LOG.error("FOK reconciliation incomplete (%s)", type(exc).__name__)
            try:
                # The broker filters the discovery query to working orders, including GTC.
                try:
                    active = self.client.order_history(from_date="2000-01-01", statuses=sorted(ACTIVE))
                except Exception as exc:
                    LOG.error("Working-order discovery failed (%s); checking tracked tickets", type(exc).__name__)
                    active = []
                by_id = {str(row["orderId"]): row for row in active if isinstance(row, dict) and row.get("orderId") is not None}
                correlations = self.journal.order_correlations()
                for identifier in sorted(self.reporter.unresolved_order_ids(correlations) - by_id.keys()):
                    try:
                        by_id[identifier] = exact_order_row(self.client, identifier)
                    except Exception as exc:
                        LOG.warning("Order detail unavailable ticket=%s (%s); retaining its last reported state",
                                    identifier, type(exc).__name__)
                rows = sorted(by_id.values(), key=lambda row: str(row.get("orderId")) not in correlations)
                observed_at = datetime.fromtimestamp(self.clock(), timezone.utc)
                for row in rows:
                    if str(row.get("marketCode", "")).upper() != "US":
                        continue
                    try:
                        correlation = correlations.get(str(row.get("orderId")), {})
                        snapshot = normalize_order(row, sub_account_id=self.client.state["account"]["subAcctId"],
                            observed_at=observed_at, **correlation)
                        snapshot.pop("cancelable")
                        observations.append(snapshot)
                        self.reporter.queue_order(snapshot)
                        order_count += 1
                    except Exception as exc:
                        LOG.error("Order snapshot rejected (%s); no status inferred", type(exc).__name__)
            except Exception as exc:
                LOG.error("Order reporting read failed (%s)", type(exc).__name__)
            if self.order_changes.observe(observations):
                self.refresh_until = self.clock() + 10
                self.next_account = 0
            account_due = force or self.clock() >= self.next_account
            if account_due:
                # Publish freshly observed and previously queued order evidence before the
                # serial holdings/quote/cash reads can delay the controller's next order.
                try:
                    self.reporter.flush_orders(max_records=20)
                except Exception as exc:
                    LOG.error("Order publication deferred (%s); durable outbox will retry", type(exc).__name__)
                try:
                    snapshot = account_snapshot(self.client, self.reporter.account_id,
                        self.reporter.account_num_id, self.enabled())
                    self.reporter.publish_account(snapshot)
                    account_ok = True
                except Exception as exc:
                    LOG.error("Account reporting failed (%s); keeping the previous snapshot", type(exc).__name__)
                self.next_account = self.clock() + (1 if self.clock() < self.refresh_until else min(30, self.interval))
            try:
                sent = self.reporter.flush(max_records=20, only_topic=self.reporter.account_topic if account_due else None)
                LOG.info("Reporting account_read=%s orders_read=%s snapshots_delivered=%s elapsed_ms=%.3f",
                         account_ok, order_count, sent, (time.perf_counter_ns()-elapsed_start)/1_000_000)
            except Exception as exc:
                LOG.error("Snapshot delivery incomplete (%s); durable outbox will retry", type(exc).__name__)
        finally:
            self.next_refresh = max(started + 1.0, self.clock() + .01)


class BackgroundReports:
    """The reporting thread owns its SQLite handles and Kafka outbox."""
    def __init__(self, open_loop, stop):
        self.open_loop = open_loop
        self.stop = stop
        self.wake = threading.Event()
        self.ready = Future()
        self.thread = threading.Thread(target=self._run, name="broker-reporting")

    def start(self):
        self.thread.start()
        self.ready.result()

    def after_command(self):
        self.wake.set()

    def close(self):
        self.stop.set()
        self.wake.set()
        self.thread.join()

    def _run(self):
        try:
            with self.open_loop() as loop:
                self.ready.set_result(None)
                while not self.stop.is_set():
                    loop.tick()
                    if self.wake.wait(max(0, loop.next_refresh - time.time())):
                        self.wake.clear()
                        loop.after_command()
        except Exception as exc:
            self.stop.set()
            if not self.ready.done():
                self.ready.set_exception(exc)
            else:
                LOG.exception("Reporting worker stopped")
