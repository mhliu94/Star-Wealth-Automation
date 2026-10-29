import copy
import pytest
from starwealth.client import Client
from starwealth.orders import Order, StaleQuoteError
from test_orders import snapshot


def configured(state, monkeypatch, stale_reads):
    now, market, fresh = snapshot("NIGHT", 20)
    stale = copy.deepcopy(fresh)
    now += 10
    fresh["orderbook"]["qtTime"] += 10000
    reads = []
    client = Client(state)
    client.market_status = lambda: market
    def quote(stock):
        reads.append(stock)
        return stale if len(reads) <= stale_reads else fresh
    client.quote = quote
    monkeypatch.setattr("starwealth.orders.time.time", lambda: now)
    monkeypatch.setattr("starwealth.client.time.sleep", lambda _: None)
    return client, reads


def test_refreshes_stale_quote_before_preparing(state, monkeypatch):
    client, reads = configured(state, monkeypatch, 1)
    plan = client.prepare(Order("AAPL", "buy", "1", kind="marketable-limit",
                               session="overnight", price_bound="101"))
    assert len(reads) == 2
    assert plan["params"]["price"] == "100.02"


def test_stale_refresh_is_bounded(state, monkeypatch):
    client, reads = configured(state, monkeypatch, 100)
    with pytest.raises(StaleQuoteError):
        client.prepare(Order("AAPL", "buy", "1", kind="marketable-limit",
                             session="overnight", price_bound="101"))
    assert len(reads) == 3


def test_explicit_limit_does_not_depend_on_book_timestamp(state, monkeypatch):
    client, reads = configured(state, monkeypatch, 100)
    plan = client.prepare(Order("AAPL", "buy", "1", session="overnight", price="99"))
    assert len(reads) == 1
    assert plan["params"]["price"] == "99"


def test_price_bound_failure_is_not_retried(state, monkeypatch):
    client, reads = configured(state, monkeypatch, 0)
    with pytest.raises(ValueError, match="price bound"):
        client.prepare(Order("AAPL", "buy", "1", kind="marketable-limit",
                             session="overnight", price_bound="90"))
    assert len(reads) == 1
