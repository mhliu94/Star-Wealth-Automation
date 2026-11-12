"""Serialized broker reads and independent Kafka snapshot delivery."""
from datetime import datetime, timedelta, timezone
import logging
import time

from .account_data import account_snapshot
from .order_data import normalize_order

LOG = logging.getLogger("starwealth.reporting")


class ReportLoop:
    def __init__(self, client, journal, reporter, enabled, *, interval=30, lookback_days=90, clock=time.time, reconcile=None):
        self.client, self.journal, self.reporter, self.enabled = client, journal, reporter, enabled
        self.interval, self.lookback_days, self.clock = interval, lookback_days, clock
        self.reconcile = reconcile
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
        account_ok, order_count = False, 0
        observations = []
        try:
            if self.reconcile is not None and self.clock() >= self.next_reconcile:
                self.next_reconcile = self.clock() + self.interval
                try:
                    for outcome in self.reconcile():
                        LOG.info("FOK reconciliation state=%s", outcome["state"])
                except Exception as exc:
                    LOG.error("FOK reconciliation incomplete (%s)", type(exc).__name__)
            try:
                rows = self.client.order_history(lookback_days=self.lookback_days)
                # Include older open/GTC orders even beyond the history window.
                active = self.client.order_history(from_date="2000-01-01", statuses=[10, 20, 21, 40, 60])
                by_id = {str(row.get("orderId")): row for row in rows}
                by_id.update({str(row.get("orderId")): row for row in active})
                tracked = self.reporter.tracked_open_orders() if hasattr(self.reporter, "tracked_open_orders") else []
                for previous in tracked:
                    if previous["order_id"] in by_id:
                        continue
                    start = (datetime.fromisoformat(previous["created_at"].replace("Z", "+00:00")) - timedelta(days=1)).date().isoformat()
                    older = self.client.order_history(from_date=start, stock_code=previous["symbol"])
                    for row in older:
                        if str(row.get("orderId")) == previous["order_id"]:
                            by_id[previous["order_id"]] = row
                            break
                    else:
                        LOG.warning("A previously open order is missing from broker history; retaining its last reported state")
                rows = list(by_id.values())
                correlations = self.journal.order_correlations()
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
            if force or self.clock() >= self.next_account:
                try:
                    snapshot = account_snapshot(self.client, self.reporter.account_id,
                        self.reporter.account_num_id, self.enabled())
                    self.reporter.publish_account(snapshot)
                    account_ok = True
                except Exception as exc:
                    LOG.error("Account reporting failed (%s); keeping the previous snapshot", type(exc).__name__)
                self.next_account = self.clock() + (1 if self.clock() < self.refresh_until else min(30, self.interval))
            try:
                sent = self.reporter.flush(max_records=20)
                LOG.info("Reporting account_read=%s orders_read=%s snapshots_delivered=%s", account_ok, order_count, sent)
            except Exception as exc:
                LOG.error("Snapshot delivery incomplete (%s); durable outbox will retry", type(exc).__name__)
        finally:
            self.next_refresh = max(started + 1.0, self.clock() + .01)
