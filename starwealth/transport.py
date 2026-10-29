import http.client
import json
import ssl
import time
import uuid
from .crypto import TemporarySession, compact, encrypt, decrypt, signature
from .errors import BrokerError, ProtocolError

USER_HOST = "uapi.fosunhanig.com"
TRADE_HOST = "tapi.fosunhanig.com"
# Explicit operation registry. No caller-supplied hosts or arbitrary RPC forwarding.
OPERATIONS = {
    "lookup": (USER_HOST, "/uc/v1/UinQuery", "temporary"),
    "login": (USER_HOST, "/uc/v1/Login", "plain"),
    "unlock": (USER_HOST, "/uc/v1/TradeLogin", "user"),
    "extend_trade": (USER_HOST, "/uc/v1/Session2Extend", "trade"),
    "user_info": (USER_HOST, "/uc/v1/UserDetail", "user"),
    "orders": (TRADE_HOST, "/trade/v5/OrderList", "user"),
    "holdings": (TRADE_HOST, "/portfolio/v6/Holdings", "user"),
    "assets": (TRADE_HOST, "/portfolio/v6/TotalAssets", "user"),
    "bid_ask": (TRADE_HOST, "/trade/v6/BidAskInfo", "user"),
    "create": (TRADE_HOST, "/trade/v4/OrderCreate", "trade"),
    "cancel": (TRADE_HOST, "/trade/v4/OrderCancel", "trade"),
    "detail": (TRADE_HOST, "/trade/v4/OrderDetail", "user"),
    "market_status": ("mktdata.fosunhanig.com", "/market/v2/exchange/status?mkt=us", "quote"),
    "quote": ("mktdata.fosunhanig.com", "/market/v2/group/quote", "quote"),
}


class Transport:
    def __init__(self, device, timeout=20):
        self.device = device
        self.timeout = timeout

    def call(self, operation, params, user=None, trade=None, request_id=None):
        host, path, mode = OPERATIONS[operation]
        method = "GET" if operation in {"user_info", "market_status"} else "POST"
        rid = request_id or uuid.uuid4().hex
        request = {"method": path, "id": rid, "jsonrpc": "2.0", "params": params}
        headers = {"X-request-id": rid, "X-timestamp": str(int(time.time() * 1000)),
                   "X-source": "100", "X-product": "product=app;version=5.1.2",
                   "X-device-info": self.device["info"], "X-device-id": self.device["id"],
                   "X-net": "wifi", "X-lang": self.device.get("language", "en"),
                   "Accept-Encoding": "identity"}
        temporary = None
        key = None
        if mode in {"plain", "quote"}:
            body = compact(request) if method == "POST" else None
            headers["Content-Type"] = "application/json;charset=utf-8"
            if mode == "quote" and user and user.get("session"):
                headers["X-uin"] = str(user["uin"])
                headers["X-session"] = "session=" + user["session"]
                token = user.get("permission", {}).get("token")
                if token:
                    headers["X-session"] += ";session3=" + token
        elif mode == "temporary":
            temporary = TemporarySession()
            body = compact(temporary.wrap(request))
            headers["Content-Type"] = "application/json;charset=utf-8"
        else:
            if not user or not user.get("session"):
                raise ProtocolError("Missing user session")
            key = (trade if mode == "trade" else user)["rndkey"].encode()
            headers["Content-Type"] = "text/enc;ver=1;charset=utf-8"
            headers["X-uin"] = str(user["uin"])
            session = "session=" + user["session"]
            if mode == "trade":
                session += ";session2=" + trade["session2"]
            token = user.get("permission", {}).get("token")
            if token:
                session += ";session3=" + token
            headers["X-session"] = session
            headers["X-sign"] = signature(user["rndkey"].encode(), host, path, headers, method)
            body = encrypt(key, compact(request)).encode() if method == "POST" else None
        status, kind, raw = self.post(host, path, headers, body, method)
        if kind.lower().startswith("text/enc;ver=1"):
            if key is None:
                raise ProtocolError("Unexpected encrypted temporary response")
            raw = decrypt(key, raw)
        try:
            envelope = json.loads(raw)
        except Exception as exc:
            raise ProtocolError(f"Non-JSON broker response (HTTP {status})") from exc
        if not isinstance(envelope, dict):
            raise ProtocolError("RPC response must be an object")
        verified = False
        if temporary and isinstance(envelope.get("result"), dict) and "cdata" in envelope["result"]:
            envelope = temporary.unwrap(envelope["result"])
            verified = True
        if status != 200:
            raise ProtocolError(f"Broker HTTP {status}")
        if not isinstance(envelope, dict) or envelope.get("id") != rid:
            raise ProtocolError("RPC response id mismatch")
        error = envelope.get("error")
        if error:
            raise BrokerError(error["code"], error.get("message", ""))
        if temporary and not verified:
            raise ProtocolError("Missing signed temporary-session response")
        if "result" not in envelope:
            raise ProtocolError("Missing RPC result")
        if temporary and not isinstance(envelope.get("result"), dict):
            raise ProtocolError("Invalid authentication response")
        return envelope["result"]

    def post(self, host, path, headers, body, method="POST"):
        connection = http.client.HTTPSConnection(host, timeout=self.timeout, context=ssl.create_default_context())
        try:
            connection.request(method, path, headers=headers, body=body)
            response = connection.getresponse()
            raw = response.read(8 * 1024 * 1024 + 1)
            if len(raw) > 8 * 1024 * 1024:
                raise ProtocolError("Broker response exceeds size limit")
            return response.status, response.getheader("Content-Type", ""), raw
        finally:
            connection.close()
