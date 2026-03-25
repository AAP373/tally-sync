"""
Server-side crypto — mirrors agent/crypto.py exactly.
The server decrypts whatever the agent encrypted.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import ClassVar

from cryptography.fernet import Fernet, InvalidToken


@dataclass
class CryptoContext:
    key: bytes
    _PREFIX: ClassVar[bytes] = b"tally-platform::"

    @classmethod
    def from_base64(cls, key_b64: str) -> "CryptoContext":
        return cls(key_b64.encode("ascii"))

    @classmethod
    def from_env(cls, *, env_var: str = "AGENT_FERNET_KEY") -> "CryptoContext":
        key_b64 = os.environ.get(env_var)
        if not key_b64:
            raise RuntimeError(
                f"Environment variable {env_var!r} is not set. "
                "Set it to the same Fernet key the agent was started with."
            )
        return cls.from_base64(key_b64)

    @classmethod
    def from_env_or_generate(cls, *, env_var: str = "AGENT_FERNET_KEY") -> "CryptoContext":
        key_b64 = os.environ.get(env_var)
        if key_b64:
            return cls.from_base64(key_b64)
        new_key = Fernet.generate_key()
        import logging
        logging.getLogger(__name__).warning(
            "No %s set — generated a throwaway key. Payloads from the agent WILL fail to decrypt "
            "unless both sides share the same key.",
            env_var,
        )
        return cls(new_key)

    def _fernet(self) -> Fernet:
        return Fernet(self.key)

    def decrypt_text(self, ciphertext: str) -> str:
        try:
            data = self._fernet().decrypt(ciphertext.encode("ascii"))
        except InvalidToken as exc:
            raise ValueError("Decryption failed — wrong key or corrupted payload") from exc
        if not data.startswith(self._PREFIX):
            raise ValueError("Invalid ciphertext prefix — payload may be tampered")
        return data[len(self._PREFIX):].decode("utf-8")

    def encrypt_text(self, plaintext: str) -> str:
        token = self._fernet().encrypt(self._PREFIX + plaintext.encode("utf-8"))
        return token.decode("ascii")