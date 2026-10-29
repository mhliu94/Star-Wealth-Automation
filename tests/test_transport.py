import json
import pytest
from starwealth.crypto import compact, decrypt, encrypt, signature
from starwealth.transport import Transport
from starwealth.errors import ProtocolError, BrokerError


def test_trade_uses_user_hmac_and_trade_body_key(state):
    user=state["user"]
    trade={"rndkey":"b"*32,"session2":"trade-session"}
    transport=Transport(state["device"])
    def post(host,path,headers,body,method):
        assert headers["X-sign"] == signature(user["rndkey"].encode(),host,path,headers,method)
        assert headers["X-session"] == "session=user-session;session2=trade-session"
        decoded=json.loads(decrypt(b"b"*32,body))
        assert decoded["params"] == {"sample":1}
        with pytest.raises(ProtocolError): decrypt(b"a"*32,body)
        return 200,"text/enc;ver=1;charset=utf-8",encrypt(b"b"*32,compact({"id":"test-id","result":{"orderId":"order-1"}})).encode()
    transport.post=post
    assert transport.call("create",{"sample":1},user=user,trade=trade,request_id="test-id")["orderId"] == "order-1"


@pytest.mark.parametrize("response", [b"[]", b"not JSON", b'{"id":"wrong","result":{}}'])
def test_bad_envelope_rejected(state,response):
    transport=Transport(state["device"])
    transport.post=lambda *args:(200,"application/json",response)
    with pytest.raises(ProtocolError): transport.call("orders",{},user=state["user"],request_id="expected")


def test_unsigned_lookup_success_rejected(state):
    transport=Transport(state["device"])
    transport.post=lambda *args:(200,"application/json",b'{"id":"test","result":{"uin":123}}')
    with pytest.raises(ProtocolError,match="signed"):
        transport.call("lookup",{},request_id="test")


def test_plain_auth_rejection_is_propagated(state):
    transport=Transport(state["device"])
    transport.post=lambda *args:(200,"application/json",b'{"id":"test","error":{"code":100034}}')
    with pytest.raises(BrokerError) as error:
        transport.call("orders",{},user=state["user"],request_id="test")
    assert error.value.code==100034
