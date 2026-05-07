from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

import asyncpg  # type: ignore[import-untyped]

from app.domain.business_hours import (
    BusinessHoursCreateOutcome,
    BusinessHoursPlaceCacheRecord,
    BusinessHoursFetchStatus,
    BusinessHoursJobRecord,
    BusinessHoursJobStatus,
)


class BusinessHoursRepository:
    def __init__(self, pool: asyncpg.Pool, schema: str) -> None:
        self._pool = pool
        self._schema = schema

    @property
    def _jobs_table(self) -> str:
        return f"{self._schema}.business_hours_jobs"

    @property
    def _details_table(self) -> str:
        return f"{self._schema}.business_hours_details"

    async def get_business_hours_job(self, job_id: UUID) -> BusinessHoursJobRecord | None:
        row = await self._pool.fetchrow(
            f"SELECT * FROM {self._jobs_table} WHERE job_id = $1",
            job_id,
        )
        return self._to_job_record(row) if row else None

    async def get_business_hours_detail(self, kakao_place_id: str) -> BusinessHoursPlaceCacheRecord | None:
        row = await self._pool.fetchrow(
            f"SELECT * FROM {self._details_table} WHERE kakao_place_id = $1",
            kakao_place_id,
        )
        return self._to_detail_record(row) if row else None

    async def get_business_hours_job_detail(
        self,
        job_id: UUID,
    ) -> BusinessHoursPlaceCacheRecord | None:
        row = await self._pool.fetchrow(
            f"""
            SELECT d.*
            FROM {self._details_table} d
            JOIN {self._jobs_table} j ON j.kakao_place_id = d.kakao_place_id
            WHERE j.job_id = $1
            """,
            job_id,
        )
        return self._to_detail_record(row) if row else None

    async def prepare_business_hours_job(
        self,
        *,
        kakao_place_id: str,
        place_url: str,
        place_name: str | None,
        stale_timeout_seconds: int,
    ) -> BusinessHoursCreateOutcome:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                detail_row = await conn.fetchrow(
                    f"SELECT * FROM {self._details_table} WHERE kakao_place_id = $1 FOR UPDATE",
                    kakao_place_id,
                )

                if detail_row:
                    detail = self._to_detail_record(detail_row)
                    existing_job = await self._active_or_cached_job(conn, detail)
                    now = datetime.now(timezone.utc)
                    if self._is_valid_cache(detail, now):
                        return BusinessHoursCreateOutcome(
                            job=existing_job,
                            place_cache=detail,
                            job_created=False,
                            enqueued=False,
                            cache_hit=True,
                        )

                    stale = self._is_stale_fetching(detail, now, stale_timeout_seconds)
                    if detail.business_hours_status in {
                        BusinessHoursFetchStatus.PENDING,
                        BusinessHoursFetchStatus.FETCHING,
                    } and not stale:
                        return BusinessHoursCreateOutcome(
                            job=existing_job,
                            place_cache=detail,
                            job_created=False,
                            enqueued=False,
                            cache_hit=False,
                        )

                    if stale and detail.business_hours_job_id:
                        await conn.execute(
                            f"""
                            UPDATE {self._jobs_table}
                            SET
                                status = 'FAILED',
                                error_code = 'STALE_FETCHING_TIMEOUT',
                                error_message = 'Business hours fetching timed out.'
                            WHERE job_id = $1 AND status = 'PROCESSING'
                            """,
                            detail.business_hours_job_id,
                        )

                job = await self._insert_job(conn, kakao_place_id, place_url)
                detail = await self._upsert_pending_detail(
                    conn,
                    kakao_place_id=kakao_place_id,
                    place_url=place_url,
                    place_name=place_name,
                    job_id=job.job_id,
                )
                return BusinessHoursCreateOutcome(
                    job=job,
                    place_cache=detail,
                    job_created=True,
                    enqueued=False,
                    cache_hit=False,
                )

    async def mark_business_hours_enqueue_failed(
        self,
        *,
        job_id: UUID,
        error_message: str,
        expires_in_seconds: int,
    ) -> BusinessHoursCreateOutcome:
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=max(60, expires_in_seconds))
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                job_row = await conn.fetchrow(
                    f"""
                    UPDATE {self._jobs_table}
                    SET
                        status = 'FAILED',
                        error_code = 'ENQUEUE_FAILED',
                        error_message = $2
                    WHERE job_id = $1
                    RETURNING *
                    """,
                    job_id,
                    error_message,
                )
                detail_row = await conn.fetchrow(
                    f"""
                    UPDATE {self._details_table}
                    SET
                        business_hours_status = 'FAILED',
                        business_hours_expires_at = $2,
                        last_error = $3,
                        version = version + 1
                    WHERE business_hours_job_id = $1
                    RETURNING *
                    """,
                    job_id,
                    expires_at,
                    error_message,
                )
                if job_row is None or detail_row is None:
                    raise RuntimeError("Failed to mark business hours enqueue failure")
                return BusinessHoursCreateOutcome(
                    job=self._to_job_record(job_row),
                    place_cache=self._to_detail_record(detail_row),
                    job_created=True,
                    enqueued=False,
                    cache_hit=False,
                )

    async def claim_business_hours_job(
        self,
        job_id: UUID,
    ) -> tuple[BusinessHoursJobRecord, BusinessHoursPlaceCacheRecord] | None:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                job_row = await conn.fetchrow(
                    f"""
                    UPDATE {self._jobs_table}
                    SET status = 'PROCESSING', error_code = NULL, error_message = NULL
                    WHERE job_id = $1 AND status = 'QUEUED'
                    RETURNING *
                    """,
                    job_id,
                )
                if job_row is None:
                    return None
                detail_row = await conn.fetchrow(
                    f"""
                    UPDATE {self._details_table}
                    SET
                        business_hours_status = 'FETCHING',
                        business_hours_job_id = $1,
                        last_error = NULL,
                        version = version + 1
                    WHERE kakao_place_id = $2
                    RETURNING *
                    """,
                    job_id,
                    job_row["kakao_place_id"],
                )
                if detail_row is None:
                    raise RuntimeError("Failed to claim business hours detail")
                return self._to_job_record(job_row), self._to_detail_record(detail_row)

    async def recover_stale_processing_jobs(self, stale_after_seconds: int) -> list[UUID]:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    f"""
                    WITH recovered AS (
                        UPDATE {self._jobs_table}
                        SET
                            status = 'QUEUED',
                            error_code = NULL,
                            error_message = NULL
                        WHERE status = 'PROCESSING'
                          AND updated_at <= NOW() - ($1 * INTERVAL '1 second')
                        RETURNING job_id, kakao_place_id
                    ),
                    updated_details AS (
                        UPDATE {self._details_table} d
                        SET
                            business_hours_status = 'PENDING',
                            last_error = NULL,
                            version = version + 1
                        FROM recovered r
                        WHERE d.kakao_place_id = r.kakao_place_id
                          AND d.business_hours_job_id = r.job_id
                          AND d.business_hours_status = 'FETCHING'
                        RETURNING d.kakao_place_id
                    )
                    SELECT job_id FROM recovered
                    """,
                    max(1, stale_after_seconds),
                )
        return [row["job_id"] for row in rows]

    async def complete_business_hours_job(
        self,
        *,
        job_id: UUID,
        detail_status: BusinessHoursFetchStatus,
        job_status: BusinessHoursJobStatus,
        business_hours: dict[str, Any] | None,
        business_hours_raw: str | None,
        error_code: str | None,
        error_message: str | None,
        expires_in_seconds: int,
    ) -> tuple[BusinessHoursJobRecord, BusinessHoursPlaceCacheRecord]:
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=max(60, expires_in_seconds))
        fetched_at = datetime.now(timezone.utc)
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                job_row = await conn.fetchrow(
                    f"""
                    UPDATE {self._jobs_table}
                    SET status = $2, error_code = $3, error_message = $4
                    WHERE job_id = $1
                    RETURNING *
                    """,
                    job_id,
                    job_status.value,
                    error_code,
                    error_message,
                )
                detail_row = await conn.fetchrow(
                    f"""
                    UPDATE {self._details_table}
                    SET
                        business_hours = $2::jsonb,
                        business_hours_raw = $3,
                        business_hours_status = $4,
                        business_hours_fetched_at = $5,
                        business_hours_expires_at = $6,
                        business_hours_source = 'kakao_place_crawl',
                        last_error = $7,
                        version = version + 1
                    WHERE business_hours_job_id = $1
                    RETURNING *
                    """,
                    job_id,
                    json.dumps(business_hours) if business_hours is not None else None,
                    business_hours_raw,
                    detail_status.value,
                    fetched_at,
                    expires_at,
                    error_message,
                )
                if job_row is None or detail_row is None:
                    raise RuntimeError("Failed to complete business hours job")
                return self._to_job_record(job_row), self._to_detail_record(detail_row)

    async def _insert_job(
        self,
        conn,
        kakao_place_id: str,
        place_url: str,
    ) -> BusinessHoursJobRecord:
        row = await conn.fetchrow(
            f"""
            INSERT INTO {self._jobs_table}
                (job_id, kakao_place_id, place_url, status)
            VALUES
                ($1, $2, $3, 'QUEUED')
            RETURNING *
            """,
            uuid4(),
            kakao_place_id,
            place_url,
        )
        if row is None:
            raise RuntimeError("Failed to create business hours job")
        return self._to_job_record(row)

    async def _upsert_pending_detail(
        self,
        conn,
        *,
        kakao_place_id: str,
        place_url: str,
        place_name: str | None,
        job_id: UUID,
    ) -> BusinessHoursPlaceCacheRecord:
        row = await conn.fetchrow(
            f"""
            INSERT INTO {self._details_table} AS d
                (
                    kakao_place_id,
                    place_url,
                    place_name,
                    business_hours_status,
                    business_hours_job_id
                )
            VALUES
                ($1, $2, $3, 'PENDING', $4)
            ON CONFLICT (kakao_place_id)
            DO UPDATE SET
                place_url = EXCLUDED.place_url,
                place_name = COALESCE(EXCLUDED.place_name, d.place_name),
                business_hours_status = 'PENDING',
                business_hours_job_id = EXCLUDED.business_hours_job_id,
                last_error = NULL,
                version = d.version + 1
            RETURNING *
            """,
            kakao_place_id,
            place_url,
            place_name,
            job_id,
        )
        if row is None:
            raise RuntimeError("Failed to upsert business hours detail")
        return self._to_detail_record(row)

    async def _active_or_cached_job(
        self,
        conn,
        detail: BusinessHoursPlaceCacheRecord,
    ) -> BusinessHoursJobRecord | None:
        if detail.business_hours_job_id is None:
            return None
        row = await conn.fetchrow(
            f"SELECT * FROM {self._jobs_table} WHERE job_id = $1",
            detail.business_hours_job_id,
        )
        return self._to_job_record(row) if row else None

    @staticmethod
    def _is_valid_cache(detail: BusinessHoursPlaceCacheRecord, now: datetime) -> bool:
        return (
            detail.business_hours_expires_at is not None
            and detail.business_hours_expires_at > now
            and detail.business_hours_status
            not in {
                BusinessHoursFetchStatus.PENDING,
                BusinessHoursFetchStatus.FETCHING,
            }
        )

    @staticmethod
    def _is_stale_fetching(
        detail: BusinessHoursPlaceCacheRecord,
        now: datetime,
        stale_timeout_seconds: int,
    ) -> bool:
        if detail.business_hours_status != BusinessHoursFetchStatus.FETCHING:
            return False
        return detail.updated_at <= now - timedelta(seconds=max(1, stale_timeout_seconds))

    def _to_job_record(self, row: asyncpg.Record) -> BusinessHoursJobRecord:
        return BusinessHoursJobRecord(
            job_id=row["job_id"],
            kakao_place_id=row["kakao_place_id"],
            place_url=row["place_url"],
            status=BusinessHoursJobStatus(row["status"]),
            error_code=row["error_code"],
            error_message=row["error_message"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _to_detail_record(self, row: asyncpg.Record) -> BusinessHoursPlaceCacheRecord:
        return BusinessHoursPlaceCacheRecord(
            kakao_place_id=row["kakao_place_id"],
            place_url=row["place_url"],
            place_name=row["place_name"],
            business_hours=self._json_to_dict(row["business_hours"]),
            business_hours_raw=row["business_hours_raw"],
            business_hours_status=BusinessHoursFetchStatus(row["business_hours_status"]),
            business_hours_fetched_at=row["business_hours_fetched_at"],
            business_hours_expires_at=row["business_hours_expires_at"],
            business_hours_source=row["business_hours_source"],
            business_hours_job_id=row["business_hours_job_id"],
            last_error=row["last_error"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            version=int(row["version"]),
        )

    @staticmethod
    def _json_to_dict(value: Any) -> dict[str, Any] | None:
        if value is None:
            return None
        if isinstance(value, str):
            return json.loads(value)
        if isinstance(value, dict):
            return value
        return dict(value)
