"""Ensure required env vars exist before tests import application settings."""

from __future__ import annotations

import os

os.environ.setdefault("INTERNAL_API_KEY", "test-internal-key")
os.environ.setdefault("ENVIRONMENT", "development")
