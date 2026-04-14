from __future__ import annotations

import secrets
from typing import Annotated

from fastapi import Header, HTTPException, status

from app.core.config import get_settings


def _build_error(code: str, message: str) -> dict[str, str]:
    return {"code": code, "message": message}


def _api_keys_equal(provided: str | None, expected: str) -> bool:
    if provided is None:
        return False
    try:
        return secrets.compare_digest(provided.encode("utf-8"), expected.encode("utf-8"))
    except ValueError:
        return False


async def require_internal_api_key(
    x_internal_api_key: Annotated[str | None, Header(alias="X-Internal-Api-Key")] = None,
) -> None:
    settings = get_settings()
    expected = settings.internal_api_key.strip()
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=_build_error("INTERNAL_API_KEY_NOT_CONFIGURED", "Internal API key is not configured."),
        )
    if not _api_keys_equal(x_internal_api_key, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=_build_error("INVALID_INTERNAL_API_KEY", "Invalid internal API key."),
        )
