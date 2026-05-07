from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol
from uuid import UUID

from app.core.config import Settings
from app.domain.business_hours import (
    BusinessHoursFetchStatus,
    BusinessHoursJobRecord,
    BusinessHoursJobStatus,
)
from app.services.business_hours import KakaoPlaceCrawlError, fetch_kakao_place_business_hours

logger = logging.getLogger("processing.business_hours.processor")


@dataclass(slots=True)
class BusinessHoursProcessOutcome:
    processed: bool
    succeeded: bool
    elapsed_ms: int
    queue_wait_ms: int | None = None
    total_elapsed_ms: int | None = None
    kakao_place_id: str | None = None
    status: str | None = None
    error_code: str | None = None


class BusinessHoursRepositoryPort(Protocol):
    async def claim_business_hours_job(
        self,
        job_id: UUID,
    ) -> tuple[BusinessHoursJobRecord, object] | None: ...

    async def complete_business_hours_job(self, **kwargs): ...


class BusinessHoursWorker:
    def __init__(
        self,
        *,
        repository: BusinessHoursRepositoryPort,
        settings: Settings,
    ) -> None:
        self._repository = repository
        self._settings = settings

    async def process_job(self, job_id: UUID) -> BusinessHoursProcessOutcome:
        started = time.monotonic()
        claimed = await self._repository.claim_business_hours_job(job_id)
        if not claimed:
            logger.info("business hours job skipped job_id=%s", job_id)
            elapsed_ms = self._elapsed_ms(started)
            return BusinessHoursProcessOutcome(
                False,
                False,
                elapsed_ms,
                total_elapsed_ms=elapsed_ms,
                status="SKIPPED",
            )

        job, _detail = claimed
        queue_wait_ms = self._queue_wait_ms(job)
        logger.info(
            "business hours job claimed job_id=%s kakao_place_id=%s queue_wait_ms=%s status=%s",
            job.job_id,
            job.kakao_place_id,
            queue_wait_ms,
            BusinessHoursJobStatus.PROCESSING.value,
        )

        try:
            parse_result = await fetch_kakao_place_business_hours(job.place_url, self._settings)
            final_job_status = self._job_status_for_detail_status(parse_result.status)
            error_code = None if final_job_status == BusinessHoursJobStatus.SUCCEEDED else parse_result.status.value
            await self._repository.complete_business_hours_job(
                job_id=job.job_id,
                detail_status=parse_result.status,
                job_status=final_job_status,
                business_hours=parse_result.business_hours,
                business_hours_raw=parse_result.raw_text,
                error_code=error_code,
                error_message=parse_result.error_message,
                expires_in_seconds=self._ttl_seconds(parse_result.status),
            )
            succeeded = final_job_status == BusinessHoursJobStatus.SUCCEEDED
            elapsed_ms = self._elapsed_ms(started)
            return BusinessHoursProcessOutcome(
                True,
                succeeded,
                elapsed_ms,
                queue_wait_ms=queue_wait_ms,
                total_elapsed_ms=queue_wait_ms + elapsed_ms,
                kakao_place_id=job.kakao_place_id,
                status=final_job_status.value,
                error_code=error_code,
            )
        except KakaoPlaceCrawlError as exc:
            await self._repository.complete_business_hours_job(
                job_id=job.job_id,
                detail_status=BusinessHoursFetchStatus.FAILED,
                job_status=BusinessHoursJobStatus.FAILED,
                business_hours=None,
                business_hours_raw=None,
                error_code="CRAWL_FAILED",
                error_message=str(exc)[:1000],
                expires_in_seconds=self._settings.business_hours_crawl_failed_ttl_seconds,
            )
            elapsed_ms = self._elapsed_ms(started)
            logger.exception(
                "business hours crawl failed job_id=%s kakao_place_id=%s error_code=%s",
                job.job_id,
                job.kakao_place_id,
                "CRAWL_FAILED",
            )
            return BusinessHoursProcessOutcome(
                True,
                False,
                elapsed_ms,
                queue_wait_ms=queue_wait_ms,
                total_elapsed_ms=queue_wait_ms + elapsed_ms,
                kakao_place_id=job.kakao_place_id,
                status=BusinessHoursJobStatus.FAILED.value,
                error_code="CRAWL_FAILED",
            )
        except Exception as exc:
            await self._repository.complete_business_hours_job(
                job_id=job.job_id,
                detail_status=BusinessHoursFetchStatus.FAILED,
                job_status=BusinessHoursJobStatus.FAILED,
                business_hours=None,
                business_hours_raw=None,
                error_code="PARSE_FAILED",
                error_message=str(exc)[:1000],
                expires_in_seconds=self._ttl_seconds(BusinessHoursFetchStatus.FAILED),
            )
            elapsed_ms = self._elapsed_ms(started)
            logger.exception(
                "business hours job failed job_id=%s kakao_place_id=%s error_code=%s",
                job.job_id,
                job.kakao_place_id,
                "PARSE_FAILED",
            )
            return BusinessHoursProcessOutcome(
                True,
                False,
                elapsed_ms,
                queue_wait_ms=queue_wait_ms,
                total_elapsed_ms=queue_wait_ms + elapsed_ms,
                kakao_place_id=job.kakao_place_id,
                status=BusinessHoursJobStatus.FAILED.value,
                error_code="PARSE_FAILED",
            )

    def _ttl_seconds(self, status: BusinessHoursFetchStatus) -> int:
        if status == BusinessHoursFetchStatus.SUCCEEDED:
            return self._settings.business_hours_success_ttl_seconds
        if status == BusinessHoursFetchStatus.NOT_FOUND:
            return self._settings.business_hours_not_found_ttl_seconds
        if status == BusinessHoursFetchStatus.FAILED:
            return self._settings.business_hours_parse_failed_ttl_seconds
        return self._settings.business_hours_parse_failed_ttl_seconds

    @staticmethod
    def _job_status_for_detail_status(
        status: BusinessHoursFetchStatus,
    ) -> BusinessHoursJobStatus:
        if status in {
            BusinessHoursFetchStatus.SUCCEEDED,
            BusinessHoursFetchStatus.NOT_FOUND,
        }:
            return BusinessHoursJobStatus.SUCCEEDED
        return BusinessHoursJobStatus.FAILED

    @staticmethod
    def _elapsed_ms(started: float) -> int:
        return int((time.monotonic() - started) * 1000)

    @staticmethod
    def _queue_wait_ms(job: BusinessHoursJobRecord) -> int:
        now = datetime.now(timezone.utc)
        created_at = job.created_at
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=now.tzinfo)
        return max(0, int((now - created_at).total_seconds() * 1000))
