import copy
import hashlib
import json
import xml.etree.ElementTree as ET
import pytest
from conftest import NOW
from starwealth.auth import Auth
from starwealth.client import Client
from starwealth.crypto import encrypt, compact
from starwealth.errors import AuthenticationRequired, BrokerError
from starwealth.storage import save_private, load_private, profile_lock
from starwealth.keys import PREFERENCES_SEED
from test_auth import Fake, login_sequence


def test_transient_auth_error_has_cooldown_not_credential_lock(state,credentials):
    fake=Fake(login_sequence(state)[:1]+[("login",BrokerError(100029))])
    auth=Auth(state,credentials,fake,clock=lambda:NOW)
    with pytest.raises(AuthenticationRequired):auth.login()
    assert "auth_blocked" not in state
    assert state["auth_retry_after"] == NOW+60
    with pytest.raises(AuthenticationRequired,match="cooling"):
        auth.login()
    assert len(fake.calls)==2


def test_metadata_failure_recovers_without_repeating_password_login(state,credentials):
    sequence=login_sequence(state)
    fake=Fake(sequence[:2]+[("user_info",TimeoutError()),sequence[2]])
    auth=Auth(state,credentials,fake,clock=lambda:NOW)
    with pytest.raises(TimeoutError):auth.login()
    assert state["metadata_pending"] is True
    auth.ensure_user()
    assert "metadata_pending" not in state
    assert sum(op=="login" for op,*_ in fake.calls)==1


def test_locked_credentials_block_even_fresh_sessions(state,credentials):
    state["auth_blocked"]="trade:110013"
    auth=Auth(state,credentials,Fake([]),clock=lambda:NOW)
    with pytest.raises(AuthenticationRequired):auth.ensure_user()


def test_private_state_replace_and_lock(tmp_path):
    p=tmp_path/"session.json"
    save_private(p,{"version":1});save_private(p,{"version":2})
    assert load_private(p)=={"version":2}
    with profile_lock(p):
        with pytest.raises(OSError):
            with profile_lock(p):pass


def test_export_selects_logged_in_user_not_history(tmp_path,monkeypatch,state):
    from starwealth.export import export_phone
    import starwealth.export as module
    seed=bytes.fromhex(PREFERENCES_SEED)
    key=bytes(v ^ b"user_info_record"[i%16] for i,v in enumerate(seed))
    user=copy.deepcopy(state["user"])
    user.update(hlId=87654321,language="en")
    user["clientInfo"]["accts"]=[state["account"]]
    xml=ET.Element("map")
    ET.SubElement(xml,"string",name=hashlib.md5(b"key_logged_on_user_info_list").hexdigest()).text=encrypt(key,compact([user]))
    ET.SubElement(xml,"string",name=hashlib.md5(b"key_user_info_list").hexdigest()).text=encrypt(key,compact([{**user,"uin":999}]))
    class Result:
        def __init__(self,data):self.stdout=data
    def run(command,**kwargs):
        cmd=command[-1]
        if "user_info_record.xml" in cmd:return Result(ET.tostring(xml))
        if "fh_shared_preferences" in cmd:return Result(b'<map><string name="key_device_id">device</string></map>')
        return Result(b"Pixel" if "model" in cmd else b"16")
    monkeypatch.setattr(module.subprocess,"run",run)
    path=tmp_path/"export.json"
    export_phone("adb","serial",path)
    assert load_private(path)["user"]["uin"]==123
    assert load_private(path)["lookup"]=={"value":"87654321","type":20}
