import copy
import pytest

NOW = 1793203200.0


@pytest.fixture
def state():
    return {"user": {"uin": 123, "session": "user-session", "rndkey": "a" * 32,
                     "salt": "login-salt", "expire": (NOW + 3600) * 1000,
                     "clientInfo": {"crId": 789, "salt": "trade-salt", "accts": [{"acctId": 456, "subAcctId": "S"}]}},
            "account": {"cltId": 789, "acctId": 456, "subAcctId": "S"},
            "lookup": {"value": "12345", "type": 20},
            "device": {"id": "test-device", "info": "test-device-info", "language": "en"}}


@pytest.fixture
def credentials(state):
    from starwealth.setup import prepare_credentials
    return prepare_credentials(state, "test-login", "test-trade")
