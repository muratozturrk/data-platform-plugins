"""One-shot: RSA key pair for the rsa-key-pair token broker (/secrets/public.key, /secrets/private.key)."""
import pathlib

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

out = pathlib.Path("/secrets")
key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
(out / "private.key").write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                                    serialization.NoEncryption()))
(out / "public.key").write_bytes(key.public_key().public_bytes(serialization.Encoding.PEM,
                                                               serialization.PublicFormat.SubjectPublicKeyInfo))
for f in out.iterdir():
    f.chmod(0o644)
print("keys written")
