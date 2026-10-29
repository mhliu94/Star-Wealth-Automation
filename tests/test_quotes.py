import pytest
from starwealth.orders import Order, StaleQuoteError, build_order
from starwealth.quotes import freshness_report
from test_orders import snapshot


def test_night_report_separates_regular_close_from_active_trade():
    now, market, response = snapshot("NIGHT", 20)
    response["quote"].update(qtDate=20261028, qtTime=160000000,
                             nightQuote={"qtDate": 20261028, "qtTime": 195959000})
    report = freshness_report("AAPL", market, response, now=now)
    assert report["book_age_seconds"] == 0
    assert report["active_last_trade_age_seconds"] == 1
    assert report["regular_last_trade_age_seconds"] == 14400
    assert report["active_last_trade_field"] == "quote.nightQuote"
    plan = build_order(Order("AAPL", "buy", "1", kind="marketable-limit",
                            session="overnight", price_bound="101"), market, response, {}, now=now)
    assert plan["params"]["price"] == "100.02"


def test_fifteen_minute_old_book_is_rejected_even_if_delay_flags_are_false():
    now, market, response = snapshot()
    report = freshness_report("AAPL", market, response, now=now+900)
    assert report["realtime_indicated"]
    assert report["book_age_seconds"] == 900
    assert not report["book_timestamp_fresh"]
    with pytest.raises(StaleQuoteError):
        build_order(Order("AAPL", "buy", "1", kind="market", session="regular"),
                    market, response, {}, now=now+900)


@pytest.mark.parametrize("top,inner,expected", [(False,False,True), (True,False,False),
    (False,True,False), (None,None,False), (False,None,True)])
def test_realtime_flags_cannot_override_an_explicit_delay(top, inner, expected):
    now, market, response = snapshot()
    response["delay"], response["quote"]["delay"] = top, inner
    assert freshness_report("AAPL", market, response, now=now)["realtime_indicated"] is expected


def test_missing_book_does_not_fall_back_to_last_trade():
    now, market, response = snapshot()
    response["quote"].update(qtDate=20261028, qtTime=120000000)
    response.pop("orderbook")
    report = freshness_report("AAPL", market, response, now=now)
    assert report["regular_last_trade_age_seconds"] == 0
    assert report["book_age_seconds"] is None
    assert not report["book_timestamp_fresh"]
