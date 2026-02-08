"""
Standard logging setup for ASOM data projects.

Usage:
    from src.utils.logging import setup_logging

    setup_logging()                    # Uses config defaults
    setup_logging(level="DEBUG")       # Override level

All modules use:
    import logging
    logger = logging.getLogger(__name__)
"""

from __future__ import annotations

import logging
import sys


def setup_logging(level: str | None = None, fmt: str | None = None) -> None:
    """Configure root logger with standard format.

    Args:
        level: Log level override. If None, reads from settings.
        fmt: Format string override. If None, reads from settings.
    """
    from config.settings import get_settings

    settings = get_settings()
    log_level = level or settings.log_level
    log_format = fmt or settings.log_format

    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format=log_format,
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )

    # Quiet noisy libraries
    logging.getLogger("snowflake.connector").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("duckdb").setLevel(logging.WARNING)
