# ASOM Data Project Template

A production-ready project template for data engineering teams using the [ASOM (Agentic Scrum Operating Model)](https://github.com/mcgrath-bpt/asom-framework) framework.

**Local-first development.** Write and test code on your laptop with DuckDB before ever touching Snowflake.

---

## What This Is

A starter project structure for Python + Snowflake data pipelines with:

- **Database abstraction** — DuckDB (local default), SQLite3 (fallback), Snowflake (DEV/QA/PROD)
- **TDD-ready test suite** — pytest with ASOM test taxonomy markers (T1-T8)
- **Environment-aware config** — Pydantic Settings, per-environment YAML, env vars
- **Governance tooling** — Gate check script, evidence ledger structure, PR template with G1 checklist
- **Reference implementation** — PII masker with full test coverage as a working example

## What This Is Not

- A copy of the ASOM framework (that lives in its own repo)
- An opinionated CI/CD pipeline (bring your own GitHub Actions / Jenkins / etc.)
- A Snowflake administration tool

---

## Quick Start

```bash
git clone <this-repo> my-data-project
cd my-data-project
make setup
source .venv/bin/activate
python scripts/generate_fixtures.py
make test
```

See [DEVELOPER.md](DEVELOPER.md) for the full guide.

---

## Project Structure

```
├── config/           # Environment-aware settings (local, dev, qa, prod)
├── src/
│   ├── connector.py  # DB abstraction (DuckDB / SQLite / Snowflake)
│   ├── extract/      # Data extraction
│   ├── transform/    # Business logic, PII masking
│   ├── load/         # Data loading
│   ├── quality/      # Data quality checks
│   └── utils/        # Logging, helpers
├── sql/              # DDL and DML scripts
├── tests/            # pytest with T1-T8 taxonomy markers
├── evidence/         # CI-produced evidence ledger
├── scripts/          # Gate check, DB seeding, fixture generation
├── DEVELOPER.md      # Developer guide (start here)
└── Makefile          # Dev commands
```

---

## Development Workflow

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   localhost   │ ──► │ Snowflake DEV│ ──► │ Snowflake QA │ ──► │ Snowflake    │
│   (DuckDB)   │     │              │     │   (G3 gate)  │     │   PROD       │
│              │     │              │     │              │     │   (G4 gate)  │
│  TDD here    │     │  Integration │     │  Human QA    │     │  Human       │
│  make test   │     │  testing     │     │  approval    │     │  approval    │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
      G1 gate ────────────►
      (PR merge)
```

---

## ASOM Framework

This project follows the ASOM framework for governance, testing, and delivery:

| Concept | What It Means |
|---------|---------------|
| **Controls (C-01 to C-11)** | Governance controls every pipeline must satisfy |
| **Gates (G1-G4)** | Promotion checkpoints — machine-enforced, human-approved |
| **Test taxonomy (T1-T8)** | 8 test categories aligned with governance evidence |
| **TDD** | RED → GREEN → REFACTOR for all code, always |
| **Evidence Ledger** | JSONL audit trail — CI-produced, never hand-written |

> **Agents assist. Systems enforce. Humans approve.**

Full framework: [asom-framework](https://github.com/mcgrath-bpt/asom-framework)

---

## Make Targets

| Command | Description |
|---------|-------------|
| `make setup` | Create venv and install dependencies |
| `make test` | Run all tests |
| `make test-unit` | Run unit tests only (fast) |
| `make test-taxonomy T=t1_logic` | Run tests by taxonomy marker |
| `make coverage` | Tests with coverage report |
| `make lint` | Check code style |
| `make format` | Auto-format code |
| `make local-db` | Seed local DuckDB with fixture data |
| `make clean` | Remove all build artifacts |
