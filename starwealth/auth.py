import os
import time
from .crypto import ticket
from .errors import AuthenticationRequired, BrokerError, ProtocolError, USER_SESSION_ERRORS, TRADE_SESSION_ERRORS


class Auth:
    def __init__(self, state, credentials, transport, persist=lambda: None, clock=time.time):
        self.state, self.credentials, self.transport = state, credentials, transport
        self.persist, self.clock = persist, clock

    def _material(self, name, identity, salt):
        self._check_gate()
        material = self.credentials.get(name)
        if not material or material["identity"] != str(identity) or material["salt"] != salt:
            raise AuthenticationRequired(f"Run password setup for {name}; missing or changed credentials")
        return material

    def _block(self, reason):
        self.state["auth_blocked"] = reason
        self.persist()

    def _check_gate(self):
        if self.state.get("auth_blocked"):
            raise AuthenticationRequired("Authentication is paused; rerun password setup to clear it")
        if self.state.get("auth_retry_after", 0) > self.clock():
            raise AuthenticationRequired("Broker authentication is cooling down; try again after 60 seconds")

    def _rejected(self, name, code):
        if code in {110011, 110012, 110013, 110019, 220001}:
            self._block(f"{name}:{code}")
        else:
            self.state["auth_retry_after"] = self.clock() + 60
            self.persist()

    def login(self):
        self._check_gate()
        old = self.state["user"]
        lookup = self.transport.call("lookup", self.state["lookup"])
        if lookup.get("uin") != old["uin"]:
            raise AuthenticationRequired("Login identity changed; re-export the intended account")
        material = self._material("login", lookup["uin"], lookup["salt"])
        random_key = os.urandom(16).hex()
        timestamp = int(self.clock() * 1000)
        params = {"id": str(lookup["uin"]), "salt": lookup["salt"],
                  "ticket": ticket(material, timestamp, random_key)}
        try:
            result = self.transport.call("login", params)
        except BrokerError as exc:
            self._rejected("login", exc.code)
            raise AuthenticationRequired(f"Login rejected ({exc.code}); no automatic password retry") from exc
        if not result.get("session"):
            self._block("login_challenge")
            raise AuthenticationRequired("Broker requires an additional login challenge; complete it in the app and re-export")
        if result.get("uin") != old["uin"]:
            self._block("identity_mismatch")
            raise ProtocolError("Login response identity mismatch")
        user = {**old, "session": result["session"], "rndkey": random_key,
                "salt": lookup["salt"], "expire": result.get("expire", 0)}
        user["permission"] = {"token": result["session3"]} if result.get("session3") else {}
        self.state["user"] = user
        self.state.pop("trade", None)
        self.state["metadata_pending"] = True
        self.persist()
        # Refresh the broker's trading-password salt and account details after a new login.
        self.refresh_user()
        return result

    def refresh_user(self):
        user = self.state["user"]
        detail = self.transport.call("user_info", {}, user=user)
        if detail.get("uin") != user["uin"]:
            self._block("identity_mismatch")
            raise ProtocolError("User-detail response identity mismatch")
        client = detail["clientInfo"]
        expected = self.state["account"]
        matches = [a for a in client.get("accts", []) if a.get("acctId") == expected["acctId"] and a.get("subAcctId") == expected["subAcctId"]]
        if len(matches) != 1:
            self._block("account_mismatch")
            raise AuthenticationRequired("Selected trading account is no longer available")
        self.state["user"]["clientInfo"] = client
        self.state["user"]["permission"] = detail.get("permission", {})
        self.state["account"] = {**expected, **matches[0]}
        self.state.pop("metadata_pending", None)
        self.persist()

    def unlock(self):
        self.ensure_user()
        for attempt in range(2):
            user = self.state["user"]
            client = user["clientInfo"]
            material = self._material("trade", client["crId"], client["salt"])
            random_key = os.urandom(16).hex()
            timestamp = int(self.clock() * 1000)
            params = {"uin": user["uin"], "cltId": self.state["account"]["cltId"],
                      "ticket": ticket(material, timestamp, random_key), "type": 10, "mode": 0}
            try:
                result = self.transport.call("unlock", params, user=user)
            except BrokerError as exc:
                if exc.code in USER_SESSION_ERRORS and attempt == 0:
                    self.login()
                    continue
                self._rejected("trade", exc.code)
                raise AuthenticationRequired(f"Trading unlock rejected ({exc.code}); no automatic password retry") from exc
            if not result.get("session2"):
                raise ProtocolError("Trading unlock returned no session")
            self.state["trade"] = {**result, "rndkey": random_key, "obtained_at": timestamp}
            self.persist()
            return result
        raise AuthenticationRequired("Repeated user-session invalidation")

    def _fresh(self, session, field):
        if not session or not session.get(field):
            return False
        # Broker expiry values are absolute milliseconds, as used by Android UserManagerKt.
        now = int(self.clock() * 1000)
        if field == "session2" and "timestamp" in session and "obtained_at" in session:
            now = int(session["timestamp"]) + (now - int(session["obtained_at"]))
        return int(session.get("expire", 0)) > now + 30000

    def ensure_user(self):
        self._check_gate()
        if not self._fresh(self.state["user"], "session"):
            self.login()
        elif self.state.get("metadata_pending"):
            self.refresh_user()

    def ensure_trade(self):
        self.ensure_user()
        if not self._fresh(self.state.get("trade"), "session2"):
            self.unlock()

    def validate_trade(self):
        """Renew/verify session2 without submitting a financial transaction."""
        self.ensure_trade()
        for attempt in range(2):
            random_key = os.urandom(16).hex()
            timestamp = int(self.clock() * 1000)
            try:
                result = self.transport.call("extend_trade", {"rndkey": random_key},
                    user=self.state["user"], trade=self.state["trade"])
            except BrokerError as exc:
                if attempt:
                    raise
                if exc.code in USER_SESSION_ERRORS:
                    self.login()
                    self.unlock()
                elif exc.code in TRADE_SESSION_ERRORS:
                    self.unlock()
                else:
                    raise
                continue
            except Exception:
                # An interrupted renewal may have changed the broker key. Discard it locally.
                self.state.pop("trade", None)
                self.persist()
                raise
            if not result.get("session2"):
                self.state.pop("trade", None)
                self.persist()
                raise ProtocolError("Trading renewal returned no session")
            self.state["trade"] = {**result, "rndkey": random_key, "obtained_at": timestamp}
            self.persist()
            return result

