import pytest
from conftest import NOW
from starwealth.client import Client
from starwealth.journal import Journal
from starwealth.errors import BrokerError, DuplicateIntent, OutcomeUnknown


@pytest.mark.parametrize("failure", [TimeoutError(), ConnectionResetError(), ValueError("bad response")])
def test_uncertain_write_is_never_resent(tmp_path, state, failure):
    class Transport:
        count = 0
        def call(self, *args, **kwargs):
            self.count += 1
            raise failure
    transport=Transport(); state["trade"]={}
    client=Client(state,transport=transport); journal=Journal(tmp_path/"journal.db")
    try:
        with pytest.raises(OutcomeUnknown):
            client._write("create", {"stockCode":"AAPL"}, journal, "intent-1")
        assert journal.get("intent-1")["state"] == "unknown"
        with pytest.raises(DuplicateIntent):
            client._write("create", {"stockCode":"AAPL"}, journal, "intent-1")
        assert transport.count == 1
    finally: journal.close()


def test_pending_intent_survives_restart(tmp_path):
    path=tmp_path/"journal.db"
    journal=Journal(path); journal.reserve("one","create",{}); journal.close()
    reopened=Journal(path)
    try:
        with pytest.raises(DuplicateIntent): reopened.reserve("one","create",{})
        assert reopened.get("one")["state"] == "pending"
    finally: reopened.close()


def test_auth_rejection_refreshes_then_retries_once(tmp_path,state):
    class Transport:
        calls=[]
        def call(self, op, params, **kwargs):
            self.calls.append(dict(params))
            if len(self.calls)==1: raise BrokerError(100032)
            return {"orderId":"accepted"}
    transport=Transport();state["trade"]={}
    client=Client(state,transport=transport)
    steps=[]
    client.auth.unlock=lambda: steps.append("unlock")
    client.auth.validate_trade=lambda: steps.append("validate")
    journal=Journal(tmp_path/"journal.db")
    try:
        reply=client._write("create",{"price":"10"},journal,"one",refresh=lambda:{"price":"11"})
        assert reply["orderId"]=="accepted"
        assert transport.calls==[{"price":"10"},{"price":"11"}]
        assert steps==["unlock","validate"]
    finally:journal.close()


def test_rejection_is_not_treated_as_auth_failure(tmp_path,state):
    class Transport:
        def call(self,*args,**kwargs):raise BrokerError(210001)
    state["trade"]={};client=Client(state,transport=Transport());journal=Journal(tmp_path/"journal.db")
    try:
        with pytest.raises(BrokerError):client._write("create",{},journal,"one")
        assert journal.get("one")["state"]=="rejected"
    finally:journal.close()
