"""One-time rooted Android export. The trading client itself never calls ADB."""
import base64
import hashlib
import json
import subprocess
import xml.etree.ElementTree as ET
from .keys import PREFERENCES_SEED
from .crypto import decrypt
from .storage import save_private

PACKAGE = "com.fosunhani.stock"


def export_phone(adb, serial, destination, uin=None, account_id=None):
    def read(command):
        return subprocess.run([str(adb), "-s", serial, "exec-out", "su", "-c", command],
                              check=True, capture_output=True, timeout=30).stdout

    def prefs(name):
        return {e.get("name"): e.text for e in ET.fromstring(read(
            f"cat /data/data/{PACKAGE}/shared_prefs/{name}.xml"))}

    info = prefs("user_info_record")
    seed = bytes.fromhex(PREFERENCES_SEED)
    key = bytes(v ^ b"user_info_record"[i % 16] for i, v in enumerate(seed))
    record = hashlib.md5(b"key_logged_on_user_info_list").hexdigest()
    if record not in info:
        raise ValueError("App's logged-in user record not found")
    users = json.loads(decrypt(key, info[record]))
    users = [u for u in users if u.get("session") and (uin is None or u["uin"] == uin)]
    if len(users) != 1:
        raise ValueError("Specify the intended --uin; no unique logged-in user")
    u = users[0]
    accounts = [a for a in u["clientInfo"]["accts"] if account_id is None or str(a["acctId"]) == str(account_id)]
    if len(accounts) != 1:
        raise ValueError("Specify --account-id; multiple trading accounts are available")
    app = prefs("fh_shared_preferences_utils")
    model = read("getprop ro.product.model").decode().strip()
    os_version = read("getprop ro.build.version.release").decode().strip()
    user = {k: u[k] for k in ("uin", "session", "rndkey", "salt", "expire", "clientInfo", "permission") if k in u}
    state = {"schema": 1, "device": {"id": app["key_device_id"], "language": u.get("language", "en"),
             "info": f"platform=Android;osver={os_version};romver=;model={model};lang=en;channel=1000"},
             "user": user, "account": accounts[0], "lookup": {"value": str(u["hlId"]), "type": 20}}
    save_private(destination, state)
    return {"exported": True, "accounts_selected": 1, "trade_password_required": True}
