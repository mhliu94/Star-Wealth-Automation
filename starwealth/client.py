from datetime import date, datetime, timedelta
import time
from zoneinfo import ZoneInfo
from .auth import Auth
from .errors import AuthenticationRequired, BrokerError, USER_SESSION_ERRORS, TRADE_SESSION_ERRORS, OutcomeUnknown, ProtocolError
from .orders import Order, StaleQuoteError, build_order, symbol
from .quotes import freshness_report
from .storage import load_private, save_private
from .transport import Transport


class Client:
    def __init__(self, state, credentials=None, transport=None, persist=lambda: None):
        self.state = state
        self.transport = transport or Transport(state["device"])
        self.auth = Auth(state, credentials or {}, self.transport, persist)

    @classmethod
    def load(cls, profile, credentials=None):
        state = load_private(profile)
        secrets = load_private(credentials) if credentials else {}
        return cls(state, secrets, persist=lambda: save_private(profile, state))

    def account_params(self):
        a = self.state["account"]
        return {"uin": self.state["user"]["uin"], "clientId": a["cltId"],
                "accountId": a["acctId"], "subAccountId": a["subAcctId"]}

    def read(self, operation, params):
        if operation not in {"orders", "holdings", "assets", "asset_summary", "bid_ask", "detail", "user_info", "market_status", "quote"}:
            raise ValueError("Not a read operation")
        self.auth.ensure_user()
        for attempt in range(2):
            try:
                return self.transport.call(operation, params, user=self.state["user"])
            except BrokerError as exc:
                if exc.code not in USER_SESSION_ERRORS or attempt:
                    raise
                self.auth.login()

    def orders(self, date=None, start=0, count=100):
        date = date or datetime.now(ZoneInfo("America/New_York")).date().isoformat()
        return self.read("orders", {**self.account_params(), "market": ["us"], "fromDate": date,
                     "toDate": date, "start": start, "count": count, "sortBy": "createTime", "sort": "desc"})

    def asset_summary(self):
        """Native currency cash breakdown, unlike TotalAssets' converted totals."""
        return self.read("asset_summary", self.account_params())

    def holdings(self, *, page_size=100, max_pages=1000):
        """Return every US holding, or fail if pagination cannot be verified."""
        return self._read_pages("holdings", {**self.account_params(), "market": ["us"]},
                                page_size=page_size, max_pages=max_pages)

    def order_history(self, *, lookback_days=90, from_date=None, to_date=None,
                      statuses=None, stock_code=None, page_size=100, max_pages=1000):
        """Read a complete, bounded date range; the default includes today and 89 prior days."""
        if type(lookback_days) is not int or lookback_days < 1:
            raise ValueError("lookback_days must be a positive integer")
        try:
            end = date.fromisoformat(to_date) if to_date is not None else datetime.now(ZoneInfo("America/New_York")).date()
            start = date.fromisoformat(from_date) if from_date is not None else end - timedelta(days=lookback_days - 1)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("Order history requires valid ISO calendar dates") from exc
        if start > end:
            raise ValueError("from_date must not follow to_date")
        params = {**self.account_params(), "market": ["us"], "fromDate": start.isoformat(),
                  "toDate": end.isoformat(), "sortBy": "createTime", "sort": "asc"}
        if stock_code is not None:
            params["stockCode"] = symbol(stock_code)
        if statuses is not None:
            allowed = {10, 20, 21, 40, 50, 60, 70, 80, 90, 100}
            if (not isinstance(statuses, (list, tuple)) or not statuses
                    or any(type(value) is not int or value not in allowed for value in statuses)):
                raise ValueError("statuses must contain supported integer order states")
            params["statusArr"] = list(statuses)
        return self._read_pages("orders", params, page_size=page_size, max_pages=max_pages)

    def _read_pages(self, operation, params, *, page_size, max_pages):
        if type(page_size) is not int or not 1 <= page_size <= 100:
            raise ValueError("page_size must be between 1 and 100")
        if type(max_pages) is not int or max_pages < 1:
            raise ValueError("max_pages must be a positive integer")
        records, seen = [], set()
        expected_total = None
        for _ in range(max_pages):
            offset = len(records)
            page = self.read(operation, {**params, "start": offset, "count": page_size})
            if not isinstance(page, dict):
                raise ProtocolError("Account pagination response must be an object")
            start, count, total = (page.get(key) for key in ("start", "count", "total"))
            if any(type(value) is not int or value < 0 for value in (start, count, total)):
                raise ProtocolError("Account pagination metadata is missing or invalid")
            if start != offset or count > page_size:
                raise ProtocolError("Account pagination metadata is inconsistent")
            if expected_total is not None and total != expected_total:
                raise ProtocolError("Account records changed during pagination; refresh the snapshot")
            expected_total = total
            if "list" not in page:
                raise ProtocolError("Account page has no record list")
            batch = page["list"]
            if batch is None and total == 0:
                batch = []
            # Native endpoints echo the requested capacity in count; the public
            # OpenAPI wrapper documents count as the number actually returned.
            # Both shapes are verified. Advance and bound using actual records.
            if (not isinstance(batch, list) or count not in (page_size, len(batch))
                    or len(batch) > count or start + len(batch) > total):
                raise ProtocolError("Account page count does not match its records")
            for row in batch:
                if not isinstance(row, dict):
                    raise ProtocolError("Account page contains a malformed record")
                fields = ("orderId",) if operation == "orders" else ("marketCode", "stockCode")
                identity = tuple(row.get(key) for key in fields)
                if any(not isinstance(value, str) or not value.strip() for value in identity):
                    raise ProtocolError("Account page record identity is missing")
                if identity in seen:
                    raise ProtocolError("Account pagination repeated a record; refresh the snapshot")
                seen.add(identity)
            records.extend(batch)
            if len(records) == total:
                return records
            if not batch:
                raise ProtocolError("Account pagination stopped before returning every record")
        raise ProtocolError("Account pagination exceeded max_pages; no partial snapshot returned")

    def bid_ask(self, symbol, order_type=3, session=2, direction=1):
        params = {**self.account_params(), "marketCode": "US", "stockCode": symbol,
                  "orderType": order_type, "direction": direction}
        if session is not None:
            params["timeInForce"] = session
        return self.read("bid_ask", params)

    def quote(self, stock):
        return self.read("quote", {"code": "us" + symbol(stock), "obCount": 1})

    def market_status(self):
        return self.read("market_status", {})

    def quote_check(self, stock):
        stock = symbol(stock)
        market = self.market_status()
        return freshness_report(stock, market, self.quote(stock))

    def prepare(self, order: Order, *, max_notional=None):
        order.validate()
        for attempt in range(3):
            market = self.market_status()
            quote = self.quote(order.symbol)
            try:
                return build_order(order, market, quote, self.account_params(), max_notional=max_notional)
            except StaleQuoteError:
                if attempt == 2:
                    raise
                # Only refresh read-only snapshots. The five-second age limit stays in force.
                time.sleep(0.4)

    def order_detail(self, order_id):
        return self.read("detail", {**self.account_params(), "orderId": str(order_id),
            "orderSubAccountId": self.state["account"]["subAcctId"], "showFee": True})

    def submit(self, order, *, journal, intent_id, live=False, max_notional=None):
        if not live:
            return {"dry_run": True, **self.prepare(order, max_notional=max_notional)}
        if max_notional is None:
            raise ValueError("Set max_notional for live submission")
        order.validate()
        journal.assert_new(intent_id)
        plan = self._prepare_live(order, max_notional)
        return self._write("create", plan["params"], journal, intent_id,
            refresh=lambda: self._prepare_live(order, max_notional)["params"])

    def _prepare_live(self, order, max_notional):
        for _ in range(2):
            self.auth.validate_trade()
            # Reads can themselves trigger re-login and clear trading access.
            plan = self.prepare(order, max_notional=max_notional)
            if self.auth._fresh(self.state.get("trade"), "session2"):
                return plan
        raise AuthenticationRequired("Session changed repeatedly while preparing; no order sent")

    def cancel(self, order_id, *, journal, intent_id, live=False):
        if not isinstance(order_id, str) or not order_id.strip():
            raise ValueError("order_id required")
        params = {**self.account_params(), "orderId": order_id}
        if not live:
            return {"dry_run": True, "operation": "cancel", "params": params}
        journal.assert_new(intent_id)
        self.auth.validate_trade()
        return self._write("cancel", params, journal, intent_id)

    def _write(self, operation, params, journal, intent_id, refresh=None):
        rid = journal.reserve(intent_id, operation, params)
        for attempt in range(2):
            try:
                result = self.transport.call(operation, params, user=self.state["user"],
                                             trade=self.state["trade"], request_id=rid)
            except BrokerError as exc:
                if not attempt and exc.code in USER_SESSION_ERRORS | TRADE_SESSION_ERRORS:
                    try:
                        if exc.code in USER_SESSION_ERRORS:
                            self.auth.login()
                        self.auth.unlock()
                        self.auth.validate_trade()
                        if refresh:
                            params = refresh()
                        rid = journal.retry_authenticated(intent_id, params, exc.code)
                    except Exception:
                        journal.finish(intent_id, "rejected", broker_code=exc.code)
                        raise
                    continue
                journal.finish(intent_id, "rejected", broker_code=exc.code)
                raise
            except Exception as exc:
                journal.finish(intent_id, "unknown")
                raise OutcomeUnknown("Write response is uncertain; inspect broker orders before any new attempt") from exc
            # Preserve acknowledgement, never describe it as an execution/fill.
            if not isinstance(result, dict) or not result.get("orderId"):
                journal.finish(intent_id, "unknown", result=result)
                raise OutcomeUnknown("No broker order id in acknowledgement; reconcile before retrying")
            journal.finish(intent_id, "acknowledged", result=result)
            return result
