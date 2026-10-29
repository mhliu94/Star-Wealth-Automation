"""Wire cryptography extracted from libfh_safety in Android app 5.1.2."""
import base64
import hashlib
import hmac
import json
import os
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from .keys import ENCRYPTION_PUBLIC_KEY, VERIFICATION_PUBLIC_KEY
from .errors import ProtocolError


def compact(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def encrypt(key, plaintext, nonce=None):
    nonce = os.urandom(12) if nonce is None else nonce
    return base64.b64encode(nonce + AESGCM(key).encrypt(nonce, plaintext, None)).decode()


def decrypt(key, wire):
    try:
        data = base64.b64decode(wire, validate=True)
        if len(data) < 28:
            raise ValueError("short encrypted message")
        return AESGCM(key).decrypt(data[:12], data[12:], None)
    except Exception as exc:
        raise ProtocolError("Encrypted response failed authentication") from exc


def password_material(identity, password, salt):
    first = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 10000, 32)
    second = hashlib.pbkdf2_hmac("sha256", str(identity).encode() + first, salt.encode(), 10000, 32)
    return {"identity": str(identity), "salt": salt,
            "first": base64.b64encode(first).decode(), "second": base64.b64encode(second).decode()}


def ticket(material, timestamp, random_key):
    text = " ".join((material["identity"], material["first"], str(timestamp), random_key))
    return encrypt(base64.b64decode(material["second"], validate=True), text.encode())


def signature(key, host, path, headers, method="POST"):
    fields = [method, "https" + host + path, headers["X-uin"], headers["X-request-id"],
              headers["X-timestamp"], headers["X-device-info"], headers["X-device-id"],
              headers["X-net"], headers["X-lang"], headers["X-product"], "",
              headers["X-session"], headers["Content-Type"], headers["X-source"]]
    return hmac.new(key, "".join(fields).encode(), hashlib.sha256).hexdigest()


def shared_key(private_key, public_der):
    public = serialization.load_der_public_key(public_der)
    if not isinstance(public, ec.EllipticCurvePublicKey) or not isinstance(public.curve, ec.SECP384R1):
        raise ProtocolError("Unexpected key type")
    secret = private_key.exchange(ec.ECDH(), public)
    return HKDF(algorithm=hashes.SHA256(), length=32, salt=None, info=b"").derive(secret)


class TemporarySession:
    def __init__(self):
        self.private = ec.generate_private_key(ec.SECP384R1())
        self.nonce = os.urandom(16).hex()

    def wrap(self, request):
        public = self.private.public_key().public_bytes(serialization.Encoding.DER,
                                                       serialization.PublicFormat.SubjectPublicKeyInfo)
        key = shared_key(self.private, base64.b64decode(ENCRYPTION_PUBLIC_KEY))
        return {**request, "params": {"cdata": encrypt(key, compact(request)),
                "clientPublicKey": base64.b64encode(public).decode(), "clientNonce": self.nonce, "kver": 1}}

    def unwrap(self, result):
        try:
            if result.get("kver") != 1:
                raise ValueError("unsupported key version")
            public = result["serverPublicKey"]
            message = (self.nonce + result["serverNonce"] + public).encode()
            verifier = serialization.load_der_public_key(base64.b64decode(VERIFICATION_PUBLIC_KEY))
            verifier.verify(bytes.fromhex(result["signature"]), message, ec.ECDSA(hashes.SHA384()))
            key = shared_key(self.private, base64.b64decode(public, validate=True))
            return json.loads(decrypt(key, result["cdata"]))
        except Exception as exc:
            raise ProtocolError("Temporary-session response signature/decryption failed") from exc

