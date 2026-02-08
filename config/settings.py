"""
Configuration management — Pydantic Settings with per-environment YAML.

Loads settings from (in priority order):
1. Environment variables (highest priority)
2. config/{ASOM_ENV}.yaml
3. Defaults defined below

Usage:
    from config.settings import get_settings

    settings = get_settings()           # Uses ASOM_ENV env var (default: "local")
    settings = get_settings("dev")      # Explicitly load dev config

Reference: ASOM framework — skills/python-data-engineering.md
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings

CONFIG_DIR = Path(__file__).parent


class Settings(BaseSettings):
    """Application settings — environment-aware configuration."""

    # --- Environment ---
    asom_env: str = Field(default="local", description="Environment: local | local-sqlite | dev | qa | prod")

    # --- Database ---
    db_type: str = Field(default="duckdb", description="Database backend: duckdb | sqlite | snowflake")
    db_path: str = Field(default="data/local.duckdb", description="Path for local database file")

    # --- Snowflake (only needed for dev/qa/prod) ---
    snowflake_account: str = Field(default="", description="Snowflake account identifier")
    snowflake_user: str = Field(default="", description="Snowflake username")
    snowflake_database: str = Field(default="", description="Snowflake database name")
    snowflake_schema: str = Field(default="RAW", description="Snowflake default schema")
    snowflake_warehouse: str = Field(default="", description="Snowflake warehouse")
    snowflake_role: str = Field(default="", description="Snowflake role")

    # --- Logging ---
    log_level: str = Field(default="INFO", description="Log level: DEBUG | INFO | WARNING | ERROR")
    log_format: str = Field(
        default="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        description="Python logging format string",
    )

    # --- Data paths ---
    raw_data_path: str = Field(default="data/raw", description="Path for raw/landing data")
    fixture_path: str = Field(default="tests/fixtures", description="Path for test fixture data")

    model_config = {"env_prefix": "ASOM_", "env_file": ".env", "extra": "ignore"}


def _load_yaml_config(env: str) -> dict[str, Any]:
    """Load environment-specific YAML config file."""
    config_file = CONFIG_DIR / f"{env}.yaml"
    if config_file.exists():
        with open(config_file) as f:
            return yaml.safe_load(f) or {}
    return {}


@lru_cache
def get_settings(env: str | None = None) -> Settings:
    """Get settings for the specified environment.

    Args:
        env: Environment name. If None, reads ASOM_ENV env var (default: "local").

    Returns:
        Settings instance with merged config (env vars > YAML > defaults).
    """
    env = env or os.getenv("ASOM_ENV", "local")
    yaml_config = _load_yaml_config(env)
    yaml_config["asom_env"] = env
    return Settings(**yaml_config)
