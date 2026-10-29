import copy
import pytest
from conftest import NOW
from starwealth.client import Client
from starwealth.auth import Auth
from starwealth.errors import AuthenticationRequired, BrokerError


class Fake:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def call(self, operation, params, **kwargs):
        self.calls.append((operation, params, copy.deepcopy(kwargs)))
        expected, response = self.responses.pop(0)
        assert expected == operation
        if isinstance(response, Exception):
            raise response
        return response


def login_sequence(state):
    return [("lookup", {"uin": 123, "salt": "login-salt"}),
            ("login", {"uin": 123, "session": "NEW", "expire": (NOW + 5000) * 1000}),
            ("user_info", {"uin": 123, "clientInfo": state["user"]["clientInfo"], "permission": {}})]


@pytest.mark.parametrize("code", [100008, 100031, 100034])
def test_user_recovery_retries_read_once(state, credentials, code):
    fake = Fake([("orders", BrokerError(code)), *login_sequence(state), ("orders", {"list": []})])
    client = Client(state, credentials, fake)
    client.auth.clock = lambda: NOW
    assert client.orders("2026-10-28") == {"list": []}
    assert state["user"]["session"] == "NEW"
    assert len(fake.calls) == 5
    assert "trade" not in state


def test_expired_session_authenticates_before_read(state, credentials):
    state["user"]["expire"] = 0
    fake = Fake([*login_sequence(state), ("orders", {"list": []})])
    client = Client(state, credentials, fake)
    client.auth.clock = lambda: NOW
    client.orders("2026-10-28")
    assert fake.calls[0][0] == "lookup"


def test_repeated_kick_does_not_loop(state, credentials):
    fake = Fake([("orders", BrokerError(100034)), *login_sequence(state), ("orders", BrokerError(100034))])
    client = Client(state, credentials, fake); client.auth.clock = lambda: NOW
    with pytest.raises(BrokerError):
        client.orders("2026-10-28")
    assert sum(op == "login" for op, *_ in fake.calls) == 1


def test_password_rejection_latches_without_second_attempt(state, credentials):
    fake = Fake(login_sequence(state)[:1] + [("login", BrokerError(110013))])
    auth = Auth(state, credentials, fake, clock=lambda: NOW)
    with pytest.raises(AuthenticationRequired):
        auth.login()
    assert state["auth_blocked"] == "login:110013"
    # Material gate prevents another password ticket even in a later call.
    with pytest.raises(AuthenticationRequired):
        auth._material("login", 123, "login-salt")
    assert len(fake.calls) == 2


def test_login_challenge_is_not_reported_as_success(state, credentials):
    fake = Fake(login_sequence(state)[:1] + [("login", {"uin": 123, "_2faId": "challenge"})])
    auth = Auth(state, credentials, fake, clock=lambda: NOW)
    with pytest.raises(AuthenticationRequired, match="challenge"):
        auth.login()
    assert state["auth_blocked"] == "login_challenge"
    assert state["user"]["session"] == "user-session"


def test_changed_salt_requires_new_password_setup(state, credentials):
    fake = Fake([("lookup", {"uin": 123, "salt": "changed"})])
    auth = Auth(state, credentials, fake, clock=lambda: NOW)
    with pytest.raises(AuthenticationRequired):
        auth.login()
    assert len(fake.calls) == 1


@pytest.mark.parametrize("code", [100030, 100032])
def test_trade_session_rejection_unlocks_then_renews(state, credentials, code):
    state["trade"] = {"session2": "OLD", "rndkey": "b"*32, "expire": (NOW+3000)*1000}
    reply = {"session2": "NEWTRADE", "expire": (NOW+3000)*1000, "timestamp": NOW*1000}
    fake = Fake([("extend_trade", BrokerError(code)), ("unlock", reply), ("extend_trade", reply)])
    auth = Auth(state, credentials, fake, clock=lambda: NOW)
    auth.validate_trade()
    assert [c[0] for c in fake.calls] == ["extend_trade", "unlock", "extend_trade"]
    assert fake.calls[1][1]["cltId"] == 789
    assert fake.calls[1][1]["type"] == 10
    assert fake.calls[1][1]["mode"] == 0


def test_expired_trade_unlocks_proactively(state, credentials):
    state["trade"] = {"session2": "expired", "expire": 0}
    fake = Fake([("unlock", {"session2": "new", "expire": (NOW+3600)*1000, "timestamp": NOW*1000})])
    auth = Auth(state, credentials, fake, clock=lambda: NOW)
    auth.ensure_trade()
    assert state["trade"]["session2"] == "new"


def test_ambiguous_session_renewal_discards_key(state, credentials):
    state["trade"] = {"session2": "old", "rndkey": "b"*32, "expire": (NOW+3000)*1000}
    auth = Auth(state, credentials, Fake([("extend_trade", TimeoutError())]), clock=lambda: NOW)
    with pytest.raises(TimeoutError):
        auth.validate_trade()
    assert "trade" not in state
