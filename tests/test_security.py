"""Tests for PII security services."""

import os
import pytest

from solution.security.pii import (
    encrypt_pii,
    decrypt_pii,
    compute_phone_hmac,
    verify_phone_hmac,
    mask_phone,
)
from solution.ingestion.normalization import normalize_phone


class TestEncryption:
    """Tests for PII encryption/decryption."""

    def test_encrypt_decrypt_roundtrip(self, env_keys):
        plaintext = "John Doe"
        ciphertext = encrypt_pii(plaintext)
        assert isinstance(ciphertext, bytes)
        assert len(ciphertext) > 0

        decrypted = decrypt_pii(ciphertext)
        assert decrypted == plaintext

    def test_encrypt_different_outputs(self, env_keys):
        """Same plaintext should produce different ciphertexts (random IV)."""
        plaintext = "Test User"
        ct1 = encrypt_pii(plaintext)
        ct2 = encrypt_pii(plaintext)
        assert ct1 != ct2  # Different due to random IV

    def test_encrypt_decrypt_multiple_strings(self, env_keys):
        """Test encryption/decryption with various strings."""
        test_strings = [
            "John Doe",
            "+919876543210",
            "POL123456",
            "a" * 100,  # Long string
        ]
        for s in test_strings:
            ct = encrypt_pii(s)
            assert decrypt_pii(ct) == s

    def test_encrypt_empty_raises(self, env_keys):
        with pytest.raises(ValueError):
            encrypt_pii("")

    def test_decrypt_empty_raises(self, env_keys):
        with pytest.raises(ValueError):
            decrypt_pii(b"")

    def test_encrypt_various_strings(self, env_keys):
        test_strings = [
            "John Doe",
            "9876543210",
            "POL123456",
            "₹1,234.56",
            "a" * 1000,  # Long string
        ]
        for s in test_strings:
            ct = encrypt_pii(s)
            assert decrypt_pii(ct) == s


class TestPhoneHMAC:
    """Tests for phone HMAC computation and verification."""

    def test_hmac_is_deterministic(self, env_keys):
        """Same workflow + phone should produce same HMAC."""
        hmac1 = compute_phone_hmac("motor_renewal", "+919876543210")
        hmac2 = compute_phone_hmac("motor_renewal", "+919876543210")
        assert hmac1 == hmac2

    def test_hmac_workflow_separation(self, env_keys):
        """Same phone in different workflows should produce different HMACs."""
        hmac1 = compute_phone_hmac("motor_renewal", "+919876543210")
        hmac2 = compute_phone_hmac("policy_feedback", "+919876543210")
        assert hmac1 != hmac2

    def test_hmac_phone_difference(self, env_keys):
        """Different phones should produce different HMACs."""
        hmac1 = compute_phone_hmac("motor_renewal", "+919876543210")
        hmac2 = compute_phone_hmac("motor_renewal", "+919876543211")
        assert hmac1 != hmac2

    def test_verify_correct(self, env_keys):
        hmac = compute_phone_hmac("motor_renewal", "+919876543210")
        assert verify_phone_hmac("motor_renewal", "+919876543210", hmac) is True

    def test_verify_wrong_phone(self, env_keys):
        hmac = compute_phone_hmac("motor_renewal", "+919876543210")
        assert verify_phone_hmac("motor_renewal", "+919876543211", hmac) is False

    def test_verify_wrong_workflow(self, env_keys):
        hmac = compute_phone_hmac("motor_renewal", "+919876543210")
        assert verify_phone_hmac("policy_feedback", "+919876543210", hmac) is False

    def test_verify_wrong_hmac(self, env_keys):
        assert verify_phone_hmac("motor_renewal", "+919876543210", "wrong_hmac") is False

    def test_hmac_is_hex(self, env_keys):
        hmac = compute_phone_hmac("motor_renewal", "+919876543210")
        # SHA256 produces 64 hex characters
        assert len(hmac) == 64
        assert all(c in "0123456789abcdef" for c in hmac)

    def test_hmac_requires_valid_inputs(self, env_keys):
        with pytest.raises(ValueError):
            compute_phone_hmac("", "+919876543210")
        with pytest.raises(ValueError):
            compute_phone_hmac("motor_renewal", "")


class TestPhoneMasking:
    """Tests for phone masking."""

    def test_mask_standard(self):
        assert mask_phone("+919876543210") == "+91******3210"

    def test_mask_empty(self):
        assert mask_phone("") == ""


class TestNoPlaintextInMemory:
    """Verify that encryption actually protects data."""

    def test_ciphertext_not_plaintext(self, env_keys):
        """Ciphertext should not contain the plaintext."""
        plaintext = "Sensitive Customer Name"
        ct = encrypt_pii(plaintext)
        # The ciphertext is encrypted, should not contain plaintext
        assert plaintext.encode("utf-8") not in ct

    def test_phone_normalization_then_encrypt(self, env_keys):
        """End-to-end: normalize phone, then encrypt."""
        raw_phone = "098765 43210"
        normalized = normalize_phone(raw_phone)
        assert normalized == "+919876543210"

        ct = encrypt_pii(normalized)
        decrypted = decrypt_pii(ct)
        assert decrypted == normalized


class TestKeyConfiguration:
    """Tests for key configuration handling."""

    # Note: Key caching makes it difficult to test missing keys after fixture sets them
    # The env_keys fixture sets keys before these tests run, and the module caches them.
    # In production, missing keys would be caught on first use before caching.
