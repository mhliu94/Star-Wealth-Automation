from datetime import datetime
from zoneinfo import ZoneInfo
import pytest
from starwealth.orders import Order, build_order, quote_timestamp


def snapshot(status="OPEN", hour=12):
    now = datetime(2026, 10, 28, hour, 0, 0, tzinfo=ZoneInfo("America/New_York")).timestamp()
    quote = {"delay": False, "quote": {"mkt": "us", "rawSymbol": "AAPL", "mktStatus": status,
             "tradable": 1, "tradeStatus": 0, "nightTrading": True, "delay": False},
             "orderbook": {"qtDate": 20261028, "qtTime": hour*10000000, "power": 4,
                           "buyOrders": [{"p": 1000000, "v": 10}], "sellOrders": [{"p": 1000200, "v": 10}]}}
    return now, {"status": status}, quote


@pytest.mark.parametrize("session,status,flag", [("regular","OPEN",None), ("extended","PRE_OPEN",2),
    ("extended","AFTER_OPEN",2), ("extended","OPEN",2), ("overnight","NIGHT",4)])
def test_limit_session_flags(session, status, flag):
    now, market, quote = snapshot(status)
    plan = build_order(Order("AAPL", "buy", "1", session=session, price="100.00"), market, quote, {}, now=now)
    assert plan["params"].get("timeInForce") == flag
    assert plan["params"]["expType"] == 1
    assert plan["params"]["orderType"] == 3


@pytest.mark.parametrize("status", ["PRE_OPEN","AFTER_OPEN","NIGHT","CLOSE","WEEKEND_CLOSE","HOLIDAY_CLOSE"])
def test_native_market_cannot_run_outside_regular(status):
    now, market, quote = snapshot(status)
    with pytest.raises(ValueError):
        build_order(Order("AAPL","buy","1",kind="market",session="regular"),market,quote,{},now=now)


@pytest.mark.parametrize("session", ["extended","overnight"])
def test_no_implicit_market_conversion(session):
    with pytest.raises(ValueError, match="Native"):
        Order("AAPL","buy","1",kind="market",session=session).validate()


def test_native_us_market_enum_is_nine_not_manual_order_seven():
    now, market, quote = snapshot()
    plan = build_order(Order("AAPL","buy","1",kind="market",session="regular"),market,quote,{},now=now)
    assert plan["params"]["orderType"] == 9
    assert "price" not in plan["params"]
    assert "timeInForce" not in plan["params"]


@pytest.mark.parametrize("side,bound,price", [("buy","100.03","100.02"),("sell","99.00","100")])
def test_marketable_limit_uses_opposite_side(side, bound, price):
    now, market, quote = snapshot("NIGHT",20)
    plan = build_order(Order("AAPL",side,"1",kind="marketable-limit",session="overnight",price_bound=bound),market,quote,{},now=now)
    assert plan["params"]["price"] == price
    assert plan["params"]["orderType"] == 3


@pytest.mark.parametrize("change", ["stale","crossed","delayed","wrong_symbol","ineligible","halted","empty_book"])
def test_unusable_quote_blocks_order(change):
    now, market, quote = snapshot("NIGHT",20)
    if change == "stale": now += 6
    if change == "crossed": quote["orderbook"]["buyOrders"][0]["p"] = 2000000
    if change == "delayed": quote["delay"] = True
    if change == "wrong_symbol": quote["quote"]["rawSymbol"] = "SPY"
    if change == "ineligible": quote["quote"]["nightTrading"] = False
    if change == "halted": quote["quote"]["tradeStatus"] = 1
    if change == "empty_book": quote["orderbook"]["sellOrders"] = []
    with pytest.raises(ValueError):
        build_order(Order("AAPL","buy","1",kind="marketable-limit",session="overnight",price_bound="100.03"),market,quote,{},now=now)


@pytest.mark.parametrize("value", ["0", "-1", "NaN", "Infinity", "1.5", 1.5, True])
def test_invalid_quantity(value):
    with pytest.raises(ValueError):
        Order("AAPL","buy",value,price="10").validate()


def test_limits_and_precision():
    now, market, quote = snapshot()
    with pytest.raises(ValueError, match="max_notional"):
        build_order(Order("AAPL","buy","2",price="100"),market,quote,{},now=now,max_notional="199")
    with pytest.raises(ValueError, match="precision"):
        build_order(Order("AAPL","buy","1",price="100.001"),market,quote,{},now=now)


@pytest.mark.parametrize("session", ["overnight"])
def test_night_day_validity(session):
    with pytest.raises(ValueError):
        Order("AAPL","buy","1",session=session,validity="gtc",price="100").validate()


@pytest.mark.parametrize("fold", [0,1])
def test_fall_dst_ambiguous_hour(fold):
    now=datetime(2026,11,1,1,30,tzinfo=ZoneInfo("America/New_York"),fold=fold).timestamp()
    assert quote_timestamp({"qtDate":20261101,"qtTime":13000000},now) == now


def test_all_session_requires_unverified_account_routing():
    with pytest.raises(ValueError, match="routing is not verified"):
        Order("AAPL", "buy", "1", session="all", price="100").validate()
