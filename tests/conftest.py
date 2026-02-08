"""
Shared pytest configuration and fixtures for ASOM data projects.

Fixtures:
- masker: PIIMasker with deterministic test salt
- local_db: In-memory DuckDB for fast isolated tests
- local_sqlite: In-memory SQLite for minimal-dependency tests

Markers align with ASOM test taxonomy (T1-T8):
    @pytest.mark.t1_logic        — Unit/logic tests
    @pytest.mark.t2_contract     — Schema/contract tests
    @pytest.mark.t3_quality      — Data quality tests
    @pytest.mark.t4_access       — Access control/security tests
    @pytest.mark.t5_idempotency  — Re-run safety tests
    @pytest.mark.t6_performance  — Performance/cost tests
    @pytest.mark.t7_observability — Observability tests
    @pytest.mark.t8_integration  — Integration/E2E tests

Reference: ASOM framework — skills/testing-strategies.md
"""

import pytest

from src.connector import DuckDBConnector, SQLiteConnector
from src.transform.maskers import PIIMasker

# ---------------------------------------------------------------------------
# PII Masker fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def masker() -> PIIMasker:
    """PIIMasker with a deterministic test salt."""
    return PIIMasker(salt="test-salt")


# ---------------------------------------------------------------------------
# Database fixtures (in-memory — fast, isolated, no cleanup needed)
# ---------------------------------------------------------------------------


@pytest.fixture
def local_db() -> DuckDBConnector:
    """In-memory DuckDB for fast isolated tests."""
    conn = DuckDBConnector(database=":memory:")
    yield conn  # type: ignore[misc]
    conn.close()


@pytest.fixture
def local_sqlite() -> SQLiteConnector:
    """In-memory SQLite for minimal-dependency tests."""
    conn = SQLiteConnector(database=":memory:")
    yield conn  # type: ignore[misc]
    conn.close()
