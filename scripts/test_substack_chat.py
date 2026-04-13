#!/usr/bin/env python3
"""Convenience test entry point for substack-chat-archive."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path


TESTS_DIR = Path(__file__).resolve().parents[1] / "tests"
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.discover(str(TESTS_DIR), pattern="test_substack_chat.py")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(0 if result.wasSuccessful() else 1)
