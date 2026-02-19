from cryptography.fernet import Fernet

from apps.api.app.credentials_codec import CredentialsCodec


def test_codec_encrypt_decrypt_roundtrip() -> None:
    key = Fernet.generate_key().decode("utf-8")
    codec = CredentialsCodec(key)
    encrypted = codec.encrypt("secret-value")
    assert encrypted is not None
    assert encrypted.startswith("enc:v1:")
    assert codec.decrypt_maybe(encrypted) == "secret-value"


def test_codec_plaintext_compatibility() -> None:
    codec = CredentialsCodec("", require_encrypted=False)
    assert codec.decrypt_maybe("plain-secret") == "plain-secret"


def test_codec_rejects_plaintext_when_required() -> None:
    codec = CredentialsCodec("", require_encrypted=True)
    try:
        codec.decrypt_maybe("plain-secret")
    except RuntimeError as exc:
        assert "plaintext credential not allowed" in str(exc)
        return
    assert False, "expected RuntimeError for plaintext credential"
