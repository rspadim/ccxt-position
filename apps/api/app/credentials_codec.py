from cryptography.fernet import Fernet, InvalidToken


PREFIX = "enc:v1:"


class CredentialsCodec:
    def __init__(self, master_key: str, require_encrypted: bool = True) -> None:
        self.master_key = master_key.strip()
        self.require_encrypted = require_encrypted
        self.fernet = Fernet(self.master_key.encode("utf-8")) if self.master_key else None

    def decrypt_maybe(self, value: str | None) -> str | None:
        if value is None:
            return None
        text = str(value)
        if not text.startswith(PREFIX):
            if self.require_encrypted:
                raise RuntimeError(
                    "plaintext credential not allowed; expected enc:v1:* format"
                )
            return text
        if self.fernet is None:
            raise RuntimeError(
                "encrypted credential requires security.encryption_master_key"
            )
        token = text[len(PREFIX) :].encode("utf-8")
        try:
            return self.fernet.decrypt(token).decode("utf-8")
        except InvalidToken as exc:
            raise RuntimeError("invalid encrypted credential token") from exc

    def encrypt(self, value: str | None) -> str | None:
        if value is None:
            return None
        if self.fernet is None:
            raise RuntimeError("encryption requires security.encryption_master_key")
        token = self.fernet.encrypt(str(value).encode("utf-8")).decode("utf-8")
        return f"{PREFIX}{token}"
