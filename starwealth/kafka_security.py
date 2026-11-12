"""Read private Kafka TLS/SASL settings without allowing consumer-policy overrides."""
import json
import os
from pathlib import Path

KEYS = {'security.protocol', 'sasl.mechanism', 'sasl.username', 'sasl.password',
        'ssl.ca.location', 'ssl.certificate.location', 'ssl.key.location',
        'ssl.key.password', 'ssl.endpoint.identification.algorithm'}

def connection_settings(backend='librdkafka'):
    filename = os.environ.get('KAFKA_CLIENT_CONFIG_FILE', '').strip()
    if not filename:
        return {}
    try:
        settings = json.loads(Path(filename).read_text(encoding='utf-8'))
        if not isinstance(settings, dict) or set(settings) - KEYS:
            raise ValueError()
        if any(not isinstance(v, str) or not v for v in settings.values()):
            raise ValueError()
        if settings.get('security.protocol') not in {'SSL', 'SASL_SSL'}:
            raise ValueError()
        if settings.get('ssl.endpoint.identification.algorithm', 'https') != 'https':
            raise ValueError()
        if not Path(settings['ssl.ca.location']).is_file():
            raise ValueError()
        if settings['security.protocol'] == 'SASL_SSL':
            if any(not settings.get(k) for k in ('sasl.mechanism','sasl.username','sasl.password')):
                raise ValueError()
    except (OSError, ValueError, KeyError, TypeError):
        raise ValueError('Invalid private Kafka TLS/SASL configuration') from None
    if backend == 'librdkafka':
        return settings
    if backend != 'kafka-python':
        raise ValueError('Unsupported Kafka client')
    mapping = {'security.protocol':'security_protocol', 'sasl.mechanism':'sasl_mechanism',
        'sasl.username':'sasl_plain_username', 'sasl.password':'sasl_plain_password',
        'ssl.ca.location':'ssl_cafile', 'ssl.certificate.location':'ssl_certfile',
        'ssl.key.location':'ssl_keyfile', 'ssl.key.password':'ssl_password'}
    return {**{mapping[k]:v for k,v in settings.items() if k in mapping}, 'ssl_check_hostname':True}
