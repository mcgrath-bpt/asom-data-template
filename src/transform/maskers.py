"""
PII Masker — deterministic masking for email and phone fields.

Governance controls:
- C-04: Data Classification & Handling (PII protection)
- C-05: Access Control & Least Privilege (masking enforcement)

Reference: ASOM framework — docs/ASOM_CONTROLS.md
"""

import hashlib


class PIIMasker:
    """Mask PII fields using deterministic hashing.

    Supports:
    - Email masking via salted SHA-256 (deterministic for joins)
    - Phone redaction to last 4 digits
    - Masked-state detection for data quality validation
    """

    def __init__(self, salt: str = "default-salt"):
        if not salt:
            raise ValueError("Salt must not be empty")
        self._salt = salt

    def mask_email(self, email: str) -> str:
        """Mask email with deterministic SHA-256 hash.

        Args:
            email: Valid email address containing '@'

        Returns:
            64-character hex digest (SHA-256)

        Raises:
            ValueError: If email is empty or missing '@'
        """
        if not email or "@" not in email:
            raise ValueError(f"Invalid email: {email!r}")
        normalised = email.strip().lower()
        data = f"{normalised}{self._salt}"
        return hashlib.sha256(data.encode("utf-8")).hexdigest()

    def redact_phone(self, phone: str) -> str:
        """Redact phone number to last 4 digits.

        Args:
            phone: Phone number string (any format)

        Returns:
            String in format 'XXX-XXX-NNNN'

        Raises:
            ValueError: If phone has fewer than 4 digits
        """
        digits = "".join(c for c in phone if c.isdigit())
        if len(digits) < 4:
            raise ValueError(f"Phone too short: {phone!r}")
        return f"XXX-XXX-{digits[-4:]}"

    def is_masked(self, value: str, field_type: str) -> bool:
        """Check whether a value appears to be already masked.

        Args:
            value: The value to check
            field_type: Either 'email' or 'phone'

        Returns:
            True if the value matches masked patterns
        """
        if field_type == "email":
            return "@" not in value and len(value) == 64
        if field_type == "phone":
            return value.startswith("XXX-XXX-")
        return False
