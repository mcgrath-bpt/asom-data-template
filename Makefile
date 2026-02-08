.PHONY: setup test test-unit test-integration test-taxonomy coverage lint format local-db gate-check clean help

PYTHON ?= python3
VENV   := .venv
PIP    := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest
RUFF   := $(VENV)/bin/ruff

help:             ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup:            ## Create venv and install all dev dependencies
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev]"
	@echo ""
	@echo "✅ Setup complete. Activate with: source .venv/bin/activate"
	@echo "   Then run: make test"

test:             ## Run all tests
	$(PYTEST) -v --tb=short

test-unit:        ## Run unit tests only (fast, local)
	$(PYTEST) tests/unit/ -v --tb=short

test-integration: ## Run integration tests
	$(PYTEST) tests/integration/ -v --tb=short

test-taxonomy:    ## Run tests by ASOM taxonomy marker (usage: make test-taxonomy T=t1_logic)
	$(PYTEST) -v -m "$(T)" --tb=short

coverage:         ## Run tests with coverage report
	$(PYTEST) --cov=src --cov-report=term-missing --cov-report=html
	@echo ""
	@echo "📊 HTML report: htmlcov/index.html"

lint:             ## Run ruff linter and formatter check
	$(RUFF) check src/ tests/ config/
	$(RUFF) format --check src/ tests/ config/

format:           ## Auto-format code with ruff
	$(RUFF) format src/ tests/ config/
	$(RUFF) check --fix src/ tests/ config/

local-db:         ## Seed local DuckDB with fixture data
	ASOM_ENV=local $(VENV)/bin/python scripts/seed_local_db.py
	@echo ""
	@echo "✅ Local DuckDB seeded at data/local.duckdb"

gate-check:       ## Run gate check (usage: make gate-check GATE=G3 CONTROLS=C-04,C-05,C-06)
	$(VENV)/bin/python scripts/gate_check.py --gate $(GATE) --controls $(CONTROLS)

clean:            ## Remove build artifacts, caches, local databases
	rm -rf .venv/ .pytest_cache/ .mypy_cache/ .ruff_cache/ htmlcov/ .coverage
	rm -rf data/local.duckdb data/local.sqlite
	rm -rf build/ dist/ *.egg-info/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@echo "✅ Clean"
