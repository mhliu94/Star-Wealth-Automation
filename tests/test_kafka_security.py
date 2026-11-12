import json
import pytest
from starwealth.kafka_security import connection_settings

def test_authenticated_clients_preserve_tls_verification(tmp_path,monkeypatch):
    ca=tmp_path/'ca.crt';ca.write_text('test CA')
    p=tmp_path/'client.json'
    cfg={'security.protocol':'SASL_SSL','sasl.mechanism':'SCRAM-SHA-512',
         'sasl.username':'test','sasl.password':'synthetic-secret','ssl.ca.location':str(ca)}
    p.write_text(json.dumps(cfg));monkeypatch.setenv('KAFKA_CLIENT_CONFIG_FILE',str(p))
    assert connection_settings()==cfg
    py=connection_settings('kafka-python')
    assert py['security_protocol']=='SASL_SSL' and py['ssl_check_hostname'] is True
    assert py['sasl_plain_password']=='synthetic-secret'
    for bad in [{'enable.auto.commit':'true'},{'ssl.endpoint.identification.algorithm':''},{'security.protocol':'PLAINTEXT'}]:
        p.write_text(json.dumps({**cfg,**bad}))
        with pytest.raises(ValueError) as error:connection_settings()
        assert 'synthetic-secret' not in str(error.value)

def test_missing_optional_file_preserves_existing_plaintext_deployments(monkeypatch):
    monkeypatch.delenv('KAFKA_CLIENT_CONFIG_FILE',raising=False)
    assert connection_settings()=={}
