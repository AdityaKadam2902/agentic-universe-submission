"""PII encryption and phone HMAC service.

Provides authenticated encryption for PII fields and deterministic
HMAC-SHA256 phone matching with workflow/domain separation.

Keys are read from environment variables, never hardcoded.
"""

from __future__ import annotations

import hashlib
import hmac
import os
from typing import Optional, Tuple

from cryptography.fernet import Fernet, InvalidToken

from solution.config import get_fernet_key, get_hmac_key, ConfigurationError

# Key caching to avoid repeated env lookups
_fernet_instance: Optional[Fernet] = None
_hmac_key_bytes: Optional[bytes] = None


def _get_fernet() -> Fernet:
    """Get or create cached Fernet instance."""
    global _fernet_instance
    if _fernet_instance is None:
        _fernet_instance = Fernet(get_fernet_key())
    return _fernet_instance


def _get_hmac_key() -> bytes:
    """Get or create cached HMAC key."""
    global _hmac_key_bytes
    if _hmac_key_bytes is None:
        _hmac_key_bytes = get_hmac_key()
    return _hmac_key_bytes


def encrypt_pii(plaintext: str) -> bytes:
    """
    Encrypt a PII string using authenticated encryption (Fernet).

    Returns ciphertext bytes.
    Raises ConfigurationError if encryption key is missing.
    """
    if not plaintext:
        raise ValueError("Cannot encrypt empty plaintext")
    fernet = _get_fernet()
    return fernet.encrypt(plaintext.encode("utf-8"))


def decrypt_pii(ciphertext: bytes) -> str:
    """
    Decrypt PII ciphertext back to plaintext.

    Returns plaintext string.
    Raises ConfigurationError if key is missing.
    Raises InvalidToken if ciphertext is invalid/tampered.
    """
    if not ciphertext:
        raise ValueError("Cannot decrypt empty ciphertext")
    fernet = _get_fernet()
    return fernet.decrypt(ciphertext).decode("utf-8")


def compute_phone_hmac(workflow_id: str, canonical_phone: str) -> str:
    """
    Compute a deterministic, keyed HMAC-SHA256 digest for phone matching.

    The HMAC input includes workflow/domain separation:
        workflow_id + "|" + canonical_phone

    Returns lowercase hex digest string.
    """
    if not workflow_id or not canonical_phone:
        raise ValueError("workflow_id and canonical_phone required")

    key = _get_hmac_key()
    message = f"{workflow_id}|{canonical_phone}".encode("utf-8")
    digest = hmac.new(key, message, hashlib.sha256).hexdigest()
    return digest


def verify_phone_hmac(workflow_id: str, canonical_phone: str, expected_hmac: str) -> bool:
    """
    Verify that a phone HMAC matches the expected value.

    Uses constant-time comparison to prevent timing attacks.
    """
    computed = compute_phone_hmac(workflow_id, canonical_phone)
    return hmac.compare_digest(computed, expected_hmac)


def mask_phone(canonical_phone: str) -> str:
    """
    Create a masked phone display string: +91******XXXX
    where XXXX are the last 4 digits.
    """
    if not canonical_phone:
        return ""
    # canonical_phone is +91XXXXXXXXXX (10 digits after +91)
    # Format: +91 followed by 10 digits = 13 characters total
    if len(canonical_phone) >= 13 and canonical_phone.startswith("+91"):
        return f"+91******{canonical_phone[-4:]}"
    return canonical_phone


# Key generation helper (for documentation/setup, not for production use)
def generate_fernet_key() -> bytes:
    """Generate a new Fernet key. For initial setup only."""
    return Fernet.generate_key()


def generate_hmac_key() -> bytes:
    """Generate a new high-entropy HMAC key. For initial setup only."""
    import secrets
    return secrets.token_bytes(32)
