"""
Helpers & Utilities
===================
Shared utility functions used across the application.
"""

import json
import logging
import sys
from datetime import datetime
from typing import Any


# ── Logging ───────────────────────────────────────────────
def setup_logger(name: str = "carbon_app", level: int = logging.INFO) -> logging.Logger:
    """
    Create and configure a logger with console output.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                "[%(asctime)s] %(levelname)s %(name)s — %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger


logger = setup_logger()


# ── JSON Helpers ──────────────────────────────────────────
class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder that handles datetime objects."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def to_json(data: Any, pretty: bool = False) -> str:
    """Serialize data to JSON, handling datetimes."""
    return json.dumps(data, cls=DateTimeEncoder, indent=2 if pretty else None)
