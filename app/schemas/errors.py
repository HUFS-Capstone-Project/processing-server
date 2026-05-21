from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class ApiErrorDetail(BaseModel):
    code: str = Field(..., description="Machine-readable error code.", examples=["INVALID_URL"])
    message: str = Field(..., description="Human-readable error message.")
    retryable: bool = Field(
        default=False,
        description="True when the client may retry the same request later.",
    )


class ApiErrorResponse(BaseModel):
    detail: ApiErrorDetail = Field(..., description="FastAPI HTTPException error payload.")


class InstagramRateLimitErrorDetail(BaseModel):
    code: Literal["INSTAGRAM_RATE_LIMITED"] = Field(
        default="INSTAGRAM_RATE_LIMITED",
        description="Instagram global cooldown is active.",
    )
    message: str = Field(
        ...,
        description="Human-readable cooldown message.",
        examples=[
            "Instagram crawling is temporarily rate-limited. Retry after 600 seconds."
        ],
    )
    retryable: Literal[True] = Field(
        default=True,
        description="The client may retry after cooldown_seconds elapses.",
    )
    cooldown_seconds: int = Field(
        ...,
        ge=0,
        description="Remaining Instagram global cooldown in seconds.",
        examples=[600],
    )


class InstagramRateLimitErrorResponse(BaseModel):
    detail: InstagramRateLimitErrorDetail = Field(
        ...,
        description="FastAPI HTTPException error payload for Instagram cooldown rejection.",
    )
