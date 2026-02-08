# Developer Guide

Your Day 1 guide to working in an ASOM data engineering project. From zero to productive in under 10 minutes — no Snowflake access needed.

---

## Prerequisites

- **Python 3.9+** (`python3 --version`)
- **make** (pre-installed on macOS/Linux; optional on Windows — just run the commands manually)
- **Git**

That's it. No Snowflake account, no cloud credentials, no Docker.

---

## Quick Start (5 minutes)

```bash
# 1. Clone the repo
git clone <repo-url> && cd <repo-name>

# 2. Set up local environment
make setup

# 3. Activate the virtual environment
source .venv/bin/activate

# 4. Generate fixture data
python scripts/generate_fixtures.py

# 5. Run tests
make test
```

All tests should pass. You're ready to write code.

---

## Environment Modes

The project uses `ASOM_ENV` to control which database backend is active. You progress from left to right as your work matures:

```
local (DuckDB)  →  local-sqlite  →  dev (Snowflake DEV)  →  qa  →  prod
     ▲                                     ▲
     │                                     │
  Start here                        When you need
  (no dependencies)                 real data
```

| Mode | Backend | When to Use |
|------|---------|-------------|
| `local` (default) | DuckDB in-memory/file | Day-to-day TDD, local development |
| `local-sqlite` | SQLite3 | Minimal fallback, no pip dependencies beyond stdlib |
| `dev` | Snowflake DEV | Integration testing with real data and schemas |
| `qa` | Snowflake QA | Pre-production validation (CI/CD only) |
| `prod` | Snowflake PROD | Production (CI/CD only, never used locally) |

Switch modes:
```bash
# Default — DuckDB (no env var needed)
make test

# Explicit DuckDB
ASOM_ENV=local make test

# SQLite fallback
ASOM_ENV=local-sqlite make test

# Snowflake DEV (requires credentials in .env)
ASOM_ENV=dev make test-integration
```

### Configuring Snowflake DEV

When you're ready to test against real Snowflake:

1. Copy `.env.example` to `.env`
2. Fill in your Snowflake credentials
3. Set `ASOM_ENV=dev`
4. Install the Snowflake connector: `pip install -e ".[snowflake]"`

```bash
cp .env.example .env
# Edit .env with your Snowflake account details
ASOM_ENV=dev make test-integration
```

---

## The TDD Loop

All code in ASOM follows **RED → GREEN → REFACTOR**. This is not optional.

### 1. RED — Write a failing test

```python
# tests/unit/test_my_transformer.py
import pytest
from src.transform.my_transformer import calculate_revenue

@pytest.mark.t1_logic
def test_calculate_revenue_basic():
    result = calculate_revenue(quantity=10, unit_price=5.0)
    assert result == 50.0
```

```bash
make test-unit
# ❌ FAILED — ImportError: cannot import 'calculate_revenue'
# This is correct. The test fails because the code doesn't exist yet.
```

### 2. GREEN — Write minimum code to pass

```python
# src/transform/my_transformer.py
def calculate_revenue(quantity: int, unit_price: float) -> float:
    return quantity * unit_price
```

```bash
make test-unit
# ✅ PASSED — 1 passed
```

### 3. REFACTOR — Improve quality

```python
# src/transform/my_transformer.py
def calculate_revenue(quantity: int, unit_price: float, discount: float = 0.0) -> float:
    """Calculate line item revenue with optional discount.

    Args:
        quantity: Number of units sold
        unit_price: Price per unit
        discount: Discount rate (0.0 to 1.0)

    Returns:
        Net revenue after discount
    """
    if quantity < 0:
        raise ValueError(f"Quantity must be non-negative, got {quantity}")
    if not 0.0 <= discount <= 1.0:
        raise ValueError(f"Discount must be 0.0-1.0, got {discount}")
    return quantity * unit_price * (1 - discount)
```

```bash
make test-unit
# ✅ PASSED — all tests still green after refactoring
```

### Commit Pattern

Commit after each phase so the TDD history is visible in git:

```bash
git commit -m "test(transform): add revenue calculation test (RED)"
git commit -m "feat(transform): implement revenue calculation (GREEN)"
git commit -m "refactor(transform): add validation and docstring (REFACTOR)"
```

---

## Working with Local Data

### Fixture Data

Small sample datasets live in `tests/fixtures/`. These are committed to git so every developer has the same test data.

Generate fixtures:
```bash
python scripts/generate_fixtures.py
```

### Local Database

Seed a persistent local DuckDB with fixture data:
```bash
make local-db
# Creates data/local.duckdb with sample tables
```

Query it interactively:
```python
from src.connector import get_connector

db = get_connector()  # Uses ASOM_ENV=local → DuckDB
rows = db.fetch_all("SELECT * FROM raw_customers LIMIT 5")
df = db.fetch_df("SELECT segment, COUNT(*) as n FROM raw_customers GROUP BY segment")
print(df)
db.close()
```

### Tests Use In-Memory Databases

Unit and integration tests use in-memory DuckDB/SQLite — no disk, no cleanup, fast:

```python
def test_my_query(local_db):
    """local_db fixture is an in-memory DuckDB, auto-cleaned after test."""
    local_db.execute("CREATE TABLE t (id INT, value REAL)")
    local_db.execute("INSERT INTO t VALUES (1, 3.14)")
    rows = local_db.fetch_all("SELECT * FROM t")
    assert rows[0]["value"] == 3.14
```

---

## Project Structure

```
project/
├── config/
│   ├── settings.py          # Pydantic Settings — environment-aware config
│   ├── local.yaml            # DuckDB config (default)
│   ├── local-sqlite.yaml     # SQLite config (fallback)
│   ├── dev.yaml              # Snowflake DEV config
│   ├── test.yaml             # CI/CD config
│   └── prod.yaml             # Production config (reference only)
│
├── src/
│   ├── connector.py          # DB abstraction (DuckDB / SQLite / Snowflake)
│   ├── extract/              # Data extraction logic (APIs, files, databases)
│   ├── transform/            # Business logic, cleaning, masking
│   │   └── maskers.py        # PII masking (reference implementation)
│   ├── load/                 # Loading to Snowflake / target systems
│   ├── quality/              # Data quality checks and validation
│   └── utils/                # Shared utilities (logging, config helpers)
│
├── sql/
│   ├── ddl/                  # CREATE TABLE / schema definitions
│   └── dml/                  # INSERT, MERGE, data manipulation
│
├── tests/
│   ├── conftest.py           # Shared fixtures and T1-T8 marker registration
│   ├── unit/                 # Fast, isolated tests (T1-T5)
│   ├── integration/          # Cross-component tests (T8)
│   └── fixtures/             # Sample data files (Parquet, JSON)
│
├── evidence/                 # Evidence ledger (CI-produced, not hand-written)
├── scripts/                  # Dev tools (gate check, DB seeding, fixture generation)
│
├── DEVELOPER.md              # This file
├── README.md                 # Project overview
├── Makefile                  # Dev commands
├── pyproject.toml            # Dependencies and tool config
├── .env.example              # Environment variable template
└── .gitignore                # Git exclusions
```

---

## Test Taxonomy (T1-T8)

ASOM uses 8 test categories aligned with governance controls. Mark your tests with the appropriate marker:

| Marker | Category | What It Tests | When Required |
|--------|----------|---------------|---------------|
| `@pytest.mark.t1_logic` | Logic | Functions, transformations, business rules | Always |
| `@pytest.mark.t2_contract` | Contract | Schema compliance, API contracts, data types | When schemas defined |
| `@pytest.mark.t3_quality` | Quality | Null rates, value ranges, completeness | When DQ thresholds exist |
| `@pytest.mark.t4_access` | Security | RBAC, PII masking, encryption | When PII or access controls apply |
| `@pytest.mark.t5_idempotency` | Idempotency | Re-run safety, MERGE correctness | When re-run safety matters |
| `@pytest.mark.t6_performance` | Performance | Query cost, row count limits, SLA compliance | When SLAs exist |
| `@pytest.mark.t7_observability` | Observability | Alert triggers, log format, metric emission | When alerts configured |
| `@pytest.mark.t8_integration` | Integration | End-to-end pipeline, cross-component | When components interact |

Run by category:
```bash
make test-taxonomy T=t1_logic      # Unit tests only
make test-taxonomy T=t4_access     # Security tests only
make test-taxonomy T=t8_integration # Integration tests only
```

**Coverage targets:**
- Overall: 80% minimum
- Critical paths (PII, security): 95%+
- New code: 100% (enforced via TDD)

---

## PR Workflow (G1 Gate)

### Branch Naming

```
feature/STORY-ID-brief-description
```

Examples:
- `feature/S001-customer-api-extraction`
- `feature/S003-pii-masking-curated-layer`
- `hotfix/fix-null-handling-phone`

### Before Creating a PR

```bash
make test        # All tests pass
make lint        # No lint errors
make coverage    # Check coverage meets targets
```

### PR Template

The `.github/pull_request_template.md` auto-populates a G1 gate checklist when you create a PR. Fill it out honestly — it maps to governance controls.

### Commit Messages

```
<type>(<scope>): <subject>

<body>

<footer>
```

Types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `chore`

Examples:
```
feat(extract): add customer API pagination support
test(transform): add PII masking tests (RED phase)
fix(load): handle null phone numbers in MERGE
```

---

## Connector Abstraction

The `src/connector.py` module provides a unified interface across three database backends. Your pipeline code uses `get_connector()` and doesn't know or care which backend is active.

### How It Works

```python
from src.connector import get_connector

# Reads ASOM_ENV to pick the right backend
db = get_connector()

# Same API regardless of backend
db.execute("CREATE TABLE t (id INT, name TEXT)")
db.execute("INSERT INTO t VALUES (1, 'alice')")
rows = db.fetch_all("SELECT * FROM t")     # list[dict]
df = db.fetch_df("SELECT * FROM t")        # polars.DataFrame
db.close()
```

### When to Use Which

| Situation | Backend | Why |
|-----------|---------|-----|
| Writing unit tests | DuckDB (in-memory) | Fast, isolated, no cleanup |
| Local development | DuckDB (file-backed) | Persistent, reads Parquet |
| Minimal environments | SQLite3 | No pip deps beyond stdlib |
| Integration testing | Snowflake DEV | Real schemas, real data |
| CI/CD pipeline | DuckDB (in-memory) | Fast, deterministic |

### SQL Compatibility Notes

DuckDB covers most Snowflake SQL, but not everything:

| Feature | DuckDB | SQLite | Snowflake |
|---------|--------|--------|-----------|
| `SELECT ... FROM 'file.parquet'` | ✅ | ❌ | ❌ (use COPY INTO) |
| `MERGE INTO` | ✅ | ❌ | ✅ |
| `VARIANT` type | ✅ (STRUCT/MAP) | ❌ | ✅ |
| Window functions | ✅ | ✅ | ✅ |
| CTEs | ✅ | ✅ | ✅ |
| `QUALIFY` | ✅ | ❌ | ✅ |

For SQL that must work across all backends, stick to standard SQL (SELECT, INSERT, CREATE TABLE with basic types).

---

## Makefile Reference

```bash
make help             # Show all available targets
make setup            # Create venv, install dependencies
make test             # Run all tests
make test-unit        # Run unit tests only (fast)
make test-integration # Run integration tests
make test-taxonomy T=t1_logic  # Run tests by ASOM taxonomy marker
make coverage         # Run tests with coverage report
make lint             # Check code style (ruff)
make format           # Auto-format code (ruff)
make local-db         # Seed local DuckDB with fixture data
make gate-check GATE=G3 CONTROLS=C-04,C-05  # Run gate check
make clean            # Remove all build artifacts and local DBs
```

---

## Troubleshooting

### `ModuleNotFoundError: No module named 'src'`

Make sure you installed in editable mode:
```bash
pip install -e ".[dev]"
```

And that `pythonpath = ["."]` is in `pyproject.toml` (it is by default).

### `make test` fails with import errors

Activate your venv first:
```bash
source .venv/bin/activate
```

### DuckDB `OperationalError` on file path

The `data/` directory is created automatically. If you get path errors, check:
```bash
ls -la data/
# Should exist after make local-db
```

### Snowflake connection fails

1. Check `.env` has correct credentials
2. Verify `ASOM_ENV=dev` is set
3. Install the Snowflake connector: `pip install -e ".[snowflake]"`
4. Check your Snowflake account is reachable

### Tests pass locally but fail in CI

CI uses `ASOM_ENV=test` (in-memory DuckDB). Check:
- No hardcoded file paths
- No dependency on local DuckDB state
- Tests use fixtures, not `make local-db` data

---

## ASOM Framework Reference

This project follows the [ASOM (Agentic Scrum Operating Model)](https://github.com/mcgrath-bpt/asom-framework) framework. Key references:

- **Controls (C-01 to C-11)**: Governance controls that every pipeline must satisfy
- **Gates (G1-G4)**: Promotion checkpoints (PR merge, DEV, QA, PROD)
- **Test taxonomy (T1-T8)**: Test categories aligned with governance evidence
- **TDD (RED → GREEN → REFACTOR)**: Mandatory development methodology
- **Evidence Ledger**: CI-produced audit trail (never hand-written)

> **Agents assist. Systems enforce. Humans approve.**
