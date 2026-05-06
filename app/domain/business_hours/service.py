from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Protocol
from urllib.parse import urlparse, urlunparse
from uuid import UUID

from app.domain.business_hours.model import (
    BusinessHoursCreateOutcome,
    BusinessHoursJobRecord,
    BusinessHoursJobSubmission,
)


class InvalidBusinessHoursRequest(Exception):
    pass


class BusinessHoursEnqueueError(Exception):
    pass


class BusinessHoursRepositoryPort(Protocol):
    async def prepare_business_hours_job(
        self,
        *,
        kakao_place_id: str,
        place_url: str,
        place_name: str | None,
        stale_timeout_seconds: int,
    ) -> BusinessHoursCreateOutcome: ...

    async def mark_business_hours_enqueue_failed(
        self,
        *,
        job_id: UUID,
        error_message: str,
        expires_in_seconds: int,
    ) -> BusinessHoursCreateOutcome: ...

    async def get_business_hours_job(self, job_id: UUID) -> BusinessHoursJobRecord | None: ...


class BusinessHoursQueuePort(Protocol):
    async def enqueue(self, job_id: UUID) -> None: ...


@dataclass(slots=True)
class BusinessHoursService:
    repository: BusinessHoursRepositoryPort
    queue: BusinessHoursQueuePort
    stale_timeout_seconds: int
    enqueue_failed_ttl_seconds: int

    async def create_job(self, submission: BusinessHoursJobSubmission) -> BusinessHoursCreateOutcome:
        kakao_place_id = self._validate_kakao_place_id(submission.kakao_place_id)
        place_url = self._validate_place_url(submission.place_url, kakao_place_id)
        place_name = self._normalize_optional_text(submission.place_name)

        outcome = await self.repository.prepare_business_hours_job(
            kakao_place_id=kakao_place_id,
            place_url=place_url,
            place_name=place_name,
            stale_timeout_seconds=max(1, self.stale_timeout_seconds),
        )
        if not outcome.created or not outcome.job:
            return outcome

        try:
            await self.queue.enqueue(outcome.job.job_id)
        except Exception as exc:
            await self.repository.mark_business_hours_enqueue_failed(
                job_id=outcome.job.job_id,
                error_message=str(exc)[:1000],
                expires_in_seconds=max(60, self.enqueue_failed_ttl_seconds),
            )
            raise BusinessHoursEnqueueError(str(exc)) from exc

        return BusinessHoursCreateOutcome(
            job=outcome.job,
            detail=outcome.detail,
            created=True,
            enqueued=True,
            cache_hit=False,
        )

    @staticmethod
    def _validate_kakao_place_id(value: str) -> str:
        normalized = (value or "").strip()
        if not re.fullmatch(r"\d+", normalized):
            raise InvalidBusinessHoursRequest("kakaoPlaceId must be numeric.")
        return normalized

    @staticmethod
    def _validate_place_url(value: str, kakao_place_id: str) -> str:
        raw = (value or "").strip()
        parsed = urlparse(raw)
        if parsed.scheme not in {"http", "https"}:
            raise InvalidBusinessHoursRequest("placeUrl must use http or https.")
        if parsed.hostname != "place.map.kakao.com":
            raise InvalidBusinessHoursRequest("placeUrl must be a Kakao Place URL.")
        path_place_id = parsed.path.strip("/")
        if not re.fullmatch(r"\d+", path_place_id or ""):
            raise InvalidBusinessHoursRequest("placeUrl path must be a numeric Kakao place id.")
        if path_place_id != kakao_place_id:
            raise InvalidBusinessHoursRequest("placeUrl place id must match kakaoPlaceId.")
        return urlunparse(
            (
                "https",
                parsed.netloc,
                parsed.path,
                parsed.params,
                parsed.query,
                parsed.fragment,
            )
        )

    @staticmethod
    def _normalize_optional_text(value: str | None) -> str | None:
        normalized = (value or "").strip()
        return normalized or None
