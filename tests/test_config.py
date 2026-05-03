from __future__ import annotations

import pytest

from app.core.config import Settings, database_url_requires_ssl, validate_production_internal_api_key


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
        )


def test_production_settings_ok() -> None:
    s = Settings(
        environment="production",
        internal_api_key="x",
        database_url="postgresql://u:p@h/db?sslmode=require",
    )
    assert s.is_production is True
    validate_production_internal_api_key(s)


def test_production_private_api_requires_internal_key() -> None:
    s = Settings(
        environment="production",
        internal_api_key="",
        database_url="postgresql://u:p@h/db?sslmode=require",
    )
    with pytest.raises(ValueError, match="INTERNAL_API_KEY"):
        validate_production_internal_api_key(s)


def test_default_hf_extraction_model_uses_qwen_coder_32b() -> None:
    assert (
        Settings.model_fields["hf_extraction_model_name"].default
        == "Qwen/Qwen2.5-Coder-32B-Instruct"
    )
