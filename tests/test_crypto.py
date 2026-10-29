import base64
import hashlib
import hmac
import json
import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from starwealth import crypto
from starwealth.errors import ProtocolError


def test_aes_gcm_independent_known_vector():
    # NIST all-zero AES-256-GCM example (empty plaintext).
    encrypted = crypto.encrypt(bytes(32), b"", bytes(12))
    assert base64.b64decode(encrypted).hex() == "00" * 12 + "530f8afbc74536b9a963b4f1c4cb738b"
    assert crypto.decrypt(bytes(32), encrypted) == b""
    with pytest.raises(ProtocolError):
        crypto.decrypt(b"x" * 32, encrypted)


def test_password_ticket_cross_checked_with_web_formula():
    material = crypto.password_material("42", "example", "salt")
    p1 = hashlib.pbkdf2_hmac("sha256", b"example", b"salt", 10000, 32)
    p2 = hashlib.pbkdf2_hmac("sha256", bytes.fromhex(b"42".hex() + p1.hex()), b"salt", 10000, 32)
    decoded = crypto.decrypt(p2, crypto.ticket(material, 123456, "r" * 32)).decode()
    assert decoded == f"42 {base64.b64encode(p1).decode()} 123456 {'r'*32}"


def test_temporary_server_signature_and_ecdh(monkeypatch):
    signing_key = ec.generate_private_key(ec.SECP384R1())
    public_der = signing_key.public_key().public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
    monkeypatch.setattr(crypto, "VERIFICATION_PUBLIC_KEY", base64.b64encode(public_der).decode())
    client = crypto.TemporarySession()
    server = ec.generate_private_key(ec.SECP384R1())
    server_public = server.public_key().public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
    server_b64 = base64.b64encode(server_public).decode()
    message = (client.nonce + "server-nonce" + server_b64).encode()
    result = {"kver": 1, "serverPublicKey": server_b64, "serverNonce": "server-nonce",
              "signature": signing_key.sign(message, ec.ECDSA(hashes.SHA384())).hex(),
              "cdata": crypto.encrypt(crypto.shared_key(client.private, server_public), b'{"result":{"uin":42}}')}
    assert client.unwrap(result)["result"]["uin"] == 42
    result["serverNonce"] = "tampered"
    with pytest.raises(ProtocolError):
        client.unwrap(result)
