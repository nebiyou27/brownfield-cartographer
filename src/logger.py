"""
Centralised logging configuration for Brownfield Cartographer.

Usage in any module:
    from src.logger import get_logger
    logger = get_logger(__name__)
    logger.info("Something happened")
"""

import logging
import os


def get_logger(name: str) -> logging.Logger:
    """Return a logger for the given module name."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        level_name = os.getenv("LOG_LEVEL", "INFO").upper()
        level = getattr(logging, level_name, logging.INFO)
        handler.setLevel(level)
        fmt = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        )
        handler.setFormatter(fmt)
        logger.addHandler(handler)
        logger.setLevel(level)
        logger.propagate = False
    return logger
