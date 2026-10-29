import hashlib
from pathlib import Path
import sqlite3
import time
import uuid
from .crypto import compact
from .errors import DuplicateIntent


class Journal:
    def __init__(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.db = sqlite3.connect(path, timeout=10)
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("""CREATE TABLE IF NOT EXISTS intents (
            intent TEXT PRIMARY KEY, digest TEXT NOT NULL, operation TEXT NOT NULL,
            request_id TEXT NOT NULL, state TEXT NOT NULL, updated REAL NOT NULL,
            result TEXT, broker_code INTEGER)""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS attempts (
            intent TEXT NOT NULL, request_id TEXT PRIMARY KEY, params TEXT NOT NULL,
            created REAL NOT NULL, auth_rejection INTEGER)""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS command_metadata (
            intent TEXT PRIMARY KEY, command_id TEXT NOT NULL, client_order_id TEXT)""")
        self.db.commit()

    def close(self):
        self.db.close()

    def associate(self, intent, command_id, client_order_id=None):
        """Save command attribution before submission, including across a crash."""
        with self.db:
            row = self.db.execute("SELECT command_id,client_order_id FROM command_metadata WHERE intent=?", (intent,)).fetchone()
            if row and row != (command_id, client_order_id):
                raise DuplicateIntent("Intent attribution changed")
            self.db.execute("INSERT OR IGNORE INTO command_metadata VALUES (?,?,?)", (intent, command_id, client_order_id))

    def acknowledged_order(self, intent):
        import json
        row = self.db.execute("SELECT result FROM intents WHERE intent=? AND operation='create' AND state='acknowledged'", (intent,)).fetchone()
        result = json.loads(row[0]) if row and row[0] else {}
        value = result.get("orderId") if isinstance(result, dict) else None
        return str(value) if value is not None else None

    def order_correlations(self):
        import json
        result = {}
        for payload, command_id, client_order_id in self.db.execute("""SELECT i.result,m.command_id,m.client_order_id
                FROM intents i JOIN command_metadata m ON i.intent=m.intent
                WHERE i.operation='create' AND i.state='acknowledged' AND i.result IS NOT NULL"""):
            identifier = json.loads(payload).get("orderId")
            if identifier is not None:
                result[str(identifier)] = {"command_id": command_id}
                if client_order_id is not None:
                    result[str(identifier)]["client_order_id"] = client_order_id
        return result

    def get(self, intent):
        row = self.db.execute("SELECT intent,operation,request_id,state,updated,result,broker_code FROM intents WHERE intent=?", (intent,)).fetchone()
        if not row:
            return None
        result = dict(zip(("intent", "operation", "request_id", "state", "updated", "result", "broker_code"), row))
        result["attempts"] = [dict(zip(("request_id", "params", "created", "auth_rejection"), attempt)) for attempt in
            self.db.execute("SELECT request_id,params,created,auth_rejection FROM attempts WHERE intent=? ORDER BY created", (intent,))]
        return result

    def assert_new(self, intent):
        if not isinstance(intent, str) or not 1 <= len(intent) <= 128:
            raise ValueError("A stable intent id (1-128 characters) is required")
        prior = self.get(intent)
        if prior:
            raise DuplicateIntent(f"Intent already recorded as {prior['state']}; inspect journal and broker orders")

    def reserve(self, intent, operation, params):
        self.assert_new(intent)
        rid = uuid.uuid4().hex
        digest = hashlib.sha256(compact(params)).hexdigest()
        try:
            with self.db:
                self.db.execute("INSERT INTO intents VALUES(?,?,?,?,?,?,?,?)",
                    (intent, digest, operation, rid, "pending", time.time(), None, None))
                self.db.execute("INSERT INTO attempts VALUES(?,?,?,?,NULL)", (intent, rid, compact(params).decode(), time.time()))
        except sqlite3.IntegrityError as exc:
            raise DuplicateIntent("Intent was claimed by another process") from exc
        return rid

    def retry_authenticated(self, intent, params, code):
        """Only used after an explicit authentication rejection, never a timeout."""
        from .errors import USER_SESSION_ERRORS, TRADE_SESSION_ERRORS
        if code not in USER_SESSION_ERRORS | TRADE_SESSION_ERRORS:
            raise ValueError("Retry requires an explicit authentication rejection")
        prior = self.get(intent)
        if not prior or prior["state"] != "pending" or len(prior["attempts"]) != 1:
            raise DuplicateIntent("Authentication retry already used")
        rid = uuid.uuid4().hex
        with self.db:
            self.db.execute("UPDATE attempts SET auth_rejection=? WHERE request_id=?", (code, prior["request_id"]))
            self.db.execute("INSERT INTO attempts VALUES(?,?,?,?,NULL)", (intent, rid, compact(params).decode(), time.time()))
            self.db.execute("UPDATE intents SET digest=?,request_id=?,updated=? WHERE intent=?",
                (hashlib.sha256(compact(params)).hexdigest(), rid, time.time(), intent))
        return rid

    def finish(self, intent, state, result=None, broker_code=None):
        with self.db:
            self.db.execute("UPDATE intents SET state=?,updated=?,result=?,broker_code=? WHERE intent=?",
                (state, time.time(), compact(result).decode() if result is not None else None, broker_code, intent))
