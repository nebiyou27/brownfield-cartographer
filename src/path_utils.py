from __future__ import annotations

from pathlib import Path


def normalize_path_key(path: str) -> str:
    """Canonicalize file-like keys so artifacts are stable across OSes."""
    return Path(path).as_posix()


def with_path_aliases(store: dict[str, int], path: str, value: int) -> None:
    """
    Store a canonical POSIX key and a Windows-style alias when they differ.
    This keeps existing callers working while making POSIX keys the default.
    """
    canonical = normalize_path_key(path)
    store[canonical] = value
    legacy = canonical.replace("/", "\\")
    if legacy != canonical:
        store[legacy] = value
