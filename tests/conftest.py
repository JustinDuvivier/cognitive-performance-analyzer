"""
Test configuration for pytest.

This ensures the src/ directory is on sys.path so tests can import
project modules using standard imports without per-file path hacks.
"""

import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
SRC_PATH = os.path.join(PROJECT_ROOT, "src")

if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)


