from __future__ import annotations

import pytest

from app.core.config import (
    Settings,
    database_url_requires_ssl,
    validate_production_cors,
    validate_production_internal_api_key,
)


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("postgresql://u:p@host/db?sslmode=require", True),
        ("postgresql://u:p@host/db?sslmode=verify-full", True),
        ("postgresql://u:p@host/db?ssl=true", True),
        ("postgresql://u:p@host/db", False),
        ("postgresql://u:p@host/db?sslmode=disable", False),
    ],
)
def test_database_url_requires_ssl(url: str, expected: bool) -> None:
    assert database_url_requires_ssl(url) is expected


def test_production_requires_database_tls() -> None:
    with pytest.raises(ValueError, match="TLS"):
        Settings(
            environment="production",
            internal_api_key="x",
            database_url="postgresql://u:p@h/db",
            cors_origins="https://app.example.com",
        )


def test_production_cors_rejects_wildcard() -> None:
    s = Settings(
        environment="production",
        internal_api_key="x",
        database_url="postgresql://u:p@h/db?sslmode=require",
        cors_origins="*",
    )
    with pytest.raises(ValueError, match="wildcard"):
        validate_production_cors(s)


def test_production_settings_ok() -> None:
    s = Settings(
        environment="production",
        internal_api_key="x",
        database_url="postgresql://u:p@h/db?sslmode=require",
        cors_origins="https://app.example.com",
    )
    assert s.is_production is True
    validate_production_internal_api_key(s)
    validate_production_cors(s)


def test_production_private_api_requires_internal_key() -> None:
    s = Settings(
        environment="production",
        internal_api_key="",
        database_url="postgresql://u:p@h/db?sslmode=require",
        cors_origins="https://app.example.com",
    )
    with pytest.raises(ValueError, match="INTERNAL_API_KEY"):
        validate_production_internal_api_key(s)
