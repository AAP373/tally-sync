"""
Crypto helpers for securing data between agent and server.

Architecture notes
------------------
- The agent may run on untrusted or partially managed machines.
- All sensitive accounting data should be:
    - Encrypted at rest in the local queue.
    - Encrypted in transit (e.g. HTTPS + payload-level encryption, if required).
- This module centralizes key management and encryption/decryption logic so
  that the rest of the agent (`sync_worker.py`, `persistence.py`) can be
  implemented without crypto details leaking everywhere.
"""

from __future__ import annotations

import base64
import os
from dataclasses import dataclass
from typing import ClassVar

from cryptography.fernet import Fernet


@dataclass
class CryptoContext:
    """
    Small wrapper around Fernet for symmetric encryption.

    In a full implementation, key provisioning would likely be tied to
    the SME's tenant identity and rotated by the server.
    """

    key: bytes
    _PREFIX: ClassVar[bytes] = b"tally-platform::"

    @classmethod
    def from_base64(cls, key_b64: str) -> "CryptoContext":
        # Fernet expects the *base64-encoded* 32-byte key, as url-safe base64 bytes.
        # Do not decode it here; just store the original Fernet key bytes.
        return cls(key_b64.encode("ascii"))

    @classmethod
    def generate(cls) -> "CryptoContext":
        """
        Convenience method for bootstrapping a new agent during development.
        """
        return cls(Fernet.generate_key())

    @classmethod
    def from_env_or_generate(cls, *, env_var: str = "AGENT_FERNET_KEY") -> "CryptoContext":
        """
        Load a persistent key from environment; otherwise generate a new one.

        For real deployments, key provisioning should be explicit and stable
        across restarts. For prototype/dev, this helper keeps behaviour simple.
        """
        key_b64 = os.environ.get(env_var)
        if key_b64:
            return cls.from_base64(key_b64)
        return cls.generate()

    @property
    def key_b64(self) -> str:
        # `self.key` is already the Fernet base64 key bytes.
        return self.key.decode("ascii")

    def _fernet(self) -> Fernet:
        return Fernet(self.key)

    def encrypt_text(self, plaintext: str) -> str:
        """
        Encrypt outbound JSON payloads destined for the server.
        """
        token = self._fernet().encrypt(self._PREFIX + plaintext.encode("utf-8"))
        return token.decode("ascii")

    def decrypt_text(self, ciphertext: str) -> str:
        """
        Decrypt responses or commands issued by the server.
        """
        data = self._fernet().decrypt(ciphertext.encode("ascii"))
        if not data.startswith(self._PREFIX):  # simple framing check
            raise ValueError("Invalid ciphertext prefix")
        return data[len(self._PREFIX) :].decode("utf-8")


__all__ = ["CryptoContext"]

