from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path
from typing import Literal

from dotenv import load_dotenv
from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(_env_path)


def database_url_requires_ssl(url: str) -> bool:
    """Return True if the DSN clearly requests TLS (libpq / asyncpg style query params)."""
    lower = url.lower()
    if "sslmode=require" in lower or "sslmode=verify-full" in lower or "sslmode=verify-ca" in lower:
        return True
    if "ssl=true" in lower or "ssl=require" in lower:
        return True
    from urllib.parse import parse_qsl, urlparse

    for key, value in parse_qsl(urlparse(url).query, keep_blank_values=True):
        k = key.lower()
        v = (value or "").lower()
        if k == "sslmode" and v in ("require", "verify-full", "verify-ca"):
            return True
        if k == "ssl" and v in ("true", "require", "1"):
            return True
    return False


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Reads env var ENVIRONMENT (pydantic-settings default for field name `environment`).
    environment: Literal["development", "production"] = "development"

    service_name: str = "processing-server"
    base_url: str = "http://127.0.0.1:8000"
    cors_origins: str = "*"

    # Required for the FastAPI private API in production; omit on workers (queue consumers).
    internal_api_key: str = ""

    database_url: str = "postgresql://postgres:postgres@localhost:5432/postgres"
    processing_schema: str = "processing"
    db_pool_min_size: int = 1
    db_pool_max_size: int = 10
    db_command_timeout_seconds: int = 30

    queue_redis_url: str = "redis://localhost:6379/0"
    queue_namespace: str = "processing:jobs"
    queue_pop_timeout_seconds: int = 5
    queue_promote_batch_size: int = 50

    worker_max_attempts: int = 3
    worker_retry_base_seconds: int = 10
    worker_retry_max_seconds: int = 300
    worker_idle_sleep_seconds: float = 1.0

    crawler_timeout: int = 30

    instagram_ua: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    instagram_locale: str = "ko-KR"
    instagram_navigation_timeout: int = 30
    instagram_og_wait_timeout_ms: int = 8000

    playwright_no_sandbox: bool = True
    playwright_disable_dev_shm_usage: bool = True

    # TODO(next stage): enable when embedding-based candidate extraction is implemented.
    extraction_max_candidates: int = 12
    extraction_max_sentence_length: int = 280

    # TODO(next stage): enable when Kakao Local enrichment is implemented.
    kakao_rest_api_key: str = ""
    kakao_base_url: str = "https://dapi.kakao.com"
    kakao_timeout_seconds: int = 5
    kakao_max_places_per_candidate: int = 5

    @field_validator("processing_schema")
    @classmethod
    def validate_schema_name(cls, value: str) -> str:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            raise ValueError("processing_schema must be a valid SQL identifier")
        return value

    @model_validator(mode="after")
    def validate_production_database_ssl(self) -> Settings:
        if self.environment != "production":
            return self

        if not database_url_requires_ssl(self.database_url):
            raise ValueError(
                "In production, DATABASE_URL must enable TLS "
                "(e.g. add sslmode=require or ssl=true to the connection string)."
            )
        return self

    @property
    def is_production(self) -> bool:
        return self.environment == "production"

    @property
    def openapi_enabled(self) -> bool:
        return self.environment == "development"

    @property
    def queue_ready_key(self) -> str:
        return f"{self.queue_namespace}:ready"

    @property
    def queue_delayed_key(self) -> str:
        return f"{self.queue_namespace}:delayed"

    @property
    def queue_processing_key(self) -> str:
        return f"{self.queue_namespace}:processing"


def validate_production_cors(settings: Settings) -> None:
    """Private HTTP API only: workers do not call this (they do not use CORS)."""
    if not settings.is_production:
        return
    raw = [o.strip() for o in settings.cors_origins.split(",") if o.strip()]
    if not raw or (len(raw) == 1 and raw[0] == "*"):
        raise ValueError(
            "In production, set CORS_ORIGINS to explicit comma-separated origins "
            "(wildcard is not allowed)."
        )


def validate_production_internal_api_key(settings: Settings) -> None:
    """Private HTTP API only: workers do not need INTERNAL_API_KEY."""
    if not settings.is_production:
        return
    if not settings.internal_api_key.strip():
        raise ValueError(
            "In production, INTERNAL_API_KEY must be set for the private HTTP API."
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
