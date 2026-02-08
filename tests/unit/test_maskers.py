"""
Tests for PII masking — demonstrates ASOM TDD and test taxonomy.

Test taxonomy coverage:
- T1 (Logic): Core masking functions work correctly
- T3 (Quality): Masked values meet format requirements
- T4 (Access/Security): PII is not reversible, salt isolation works
- T5 (Idempotency): Same input always produces same output

Governance controls tested:
- C-04: Data Classification & Handling
- C-05: Access Control & Least Privilege

Reference: ASOM framework — skills/testing-strategies.md
"""

import pytest

from src.transform.maskers import PIIMasker

# ---------------------------------------------------------------------------
# T1: Logic tests — core masking functions
# ---------------------------------------------------------------------------


class TestMaskEmail:
    """T1: Email masking logic."""

    @pytest.mark.t1_logic
    def test_mask_email_returns_hex_string(self, masker: PIIMasker) -> None:
        result = masker.mask_email("user@example.com")
        assert isinstance(result, str)
        assert len(result) == 64  # SHA-256 hex digest

    @pytest.mark.t1_logic
    def test_mask_email_removes_at_sign(self, masker: PIIMasker) -> None:
        result = masker.mask_email("user@example.com")
        assert "@" not in result

    @pytest.mark.t1_logic
    def test_mask_email_normalises_case(self, masker: PIIMasker) -> None:
        lower = masker.mask_email("user@example.com")
        upper = masker.mask_email("USER@EXAMPLE.COM")
        assert lower == upper

    @pytest.mark.t1_logic
    def test_mask_email_strips_whitespace(self, masker: PIIMasker) -> None:
        clean = masker.mask_email("user@example.com")
        padded = masker.mask_email("  user@example.com  ")
        assert clean == padded

    @pytest.mark.t1_logic
    def test_mask_email_rejects_empty(self, masker: PIIMasker) -> None:
        with pytest.raises(ValueError, match="Invalid email"):
            masker.mask_email("")

    @pytest.mark.t1_logic
    def test_mask_email_rejects_no_at(self, masker: PIIMasker) -> None:
        with pytest.raises(ValueError, match="Invalid email"):
            masker.mask_email("not-an-email")


class TestRedactPhone:
    """T1: Phone redaction logic."""

    @pytest.mark.t1_logic
    def test_redact_phone_standard_format(self, masker: PIIMasker) -> None:
        result = masker.redact_phone("555-123-4567")
        assert result == "XXX-XXX-4567"

    @pytest.mark.t1_logic
    def test_redact_phone_with_country_code(self, masker: PIIMasker) -> None:
        result = masker.redact_phone("+1 (555) 123-4567")
        assert result == "XXX-XXX-4567"

    @pytest.mark.t1_logic
    def test_redact_phone_digits_only(self, masker: PIIMasker) -> None:
        result = masker.redact_phone("5551234567")
        assert result == "XXX-XXX-4567"

    @pytest.mark.t1_logic
    def test_redact_phone_too_short(self, masker: PIIMasker) -> None:
        with pytest.raises(ValueError, match="Phone too short"):
            masker.redact_phone("12")


# ---------------------------------------------------------------------------
# T3: Data quality tests — masked values meet format requirements
# ---------------------------------------------------------------------------


class TestMaskedDataQuality:
    """T3: Verify masked output meets quality standards."""

    @pytest.mark.t3_quality
    def test_masked_email_is_hex_only(self, masker: PIIMasker) -> None:
        """Masked emails should contain only hex characters."""
        result = masker.mask_email("user@example.com")
        assert all(c in "0123456789abcdef" for c in result)

    @pytest.mark.t3_quality
    def test_redacted_phone_format(self, masker: PIIMasker) -> None:
        """Redacted phones should match XXX-XXX-NNNN pattern."""
        result = masker.redact_phone("555-123-4567")
        assert result.startswith("XXX-XXX-")
        assert result[-4:].isdigit()

    @pytest.mark.t3_quality
    def test_is_masked_detects_masked_email(self, masker: PIIMasker) -> None:
        """is_masked should detect already-masked emails."""
        masked = masker.mask_email("user@example.com")
        assert masker.is_masked(masked, "email") is True

    @pytest.mark.t3_quality
    def test_is_masked_detects_raw_email(self, masker: PIIMasker) -> None:
        """is_masked should detect unmasked emails."""
        assert masker.is_masked("user@example.com", "email") is False

    @pytest.mark.t3_quality
    def test_is_masked_detects_masked_phone(self, masker: PIIMasker) -> None:
        """is_masked should detect already-redacted phones."""
        redacted = masker.redact_phone("555-123-4567")
        assert masker.is_masked(redacted, "phone") is True


# ---------------------------------------------------------------------------
# T4: Access control / security tests — PII not reversible
# ---------------------------------------------------------------------------


class TestPIISecurity:
    """T4: Verify PII protection is effective (C-04, C-05)."""

    @pytest.mark.t4_access
    def test_different_emails_different_hashes(self, masker: PIIMasker) -> None:
        """Different emails must produce different tokens."""
        hash1 = masker.mask_email("alice@example.com")
        hash2 = masker.mask_email("bob@example.com")
        assert hash1 != hash2

    @pytest.mark.t4_access
    def test_different_salts_different_hashes(self) -> None:
        """Different salts must produce different tokens for same email."""
        masker_a = PIIMasker(salt="salt-a")
        masker_b = PIIMasker(salt="salt-b")
        hash_a = masker_a.mask_email("user@example.com")
        hash_b = masker_b.mask_email("user@example.com")
        assert hash_a != hash_b

    @pytest.mark.t4_access
    def test_empty_salt_rejected(self) -> None:
        """Empty salt must be rejected to prevent weak hashing."""
        with pytest.raises(ValueError, match="Salt must not be empty"):
            PIIMasker(salt="")


# ---------------------------------------------------------------------------
# T5: Idempotency tests — same input, same output
# ---------------------------------------------------------------------------


class TestIdempotency:
    """T5: Masking must be deterministic (safe to re-run)."""

    @pytest.mark.t5_idempotency
    def test_email_masking_is_deterministic(self, masker: PIIMasker) -> None:
        """Same email + same salt = same hash, every time."""
        email = "user@example.com"
        result1 = masker.mask_email(email)
        result2 = masker.mask_email(email)
        assert result1 == result2

    @pytest.mark.t5_idempotency
    def test_phone_redaction_is_deterministic(self, masker: PIIMasker) -> None:
        """Same phone = same redacted output, every time."""
        phone = "555-123-4567"
        result1 = masker.redact_phone(phone)
        result2 = masker.redact_phone(phone)
        assert result1 == result2
