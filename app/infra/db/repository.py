from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import asyncpg

from app.domain.job.model import JobRecord, JobResultRecord, JobStatus


class JobRepository:
    def __init__(self, pool: asyncpg.Pool, schema: str) -> None:
        self._pool = pool
        self._schema = schema

    @property
    def _jobs_table(self) -> str:
        return f"{self._schema}.jobs"

    @property
    def _results_table(self) -> str:
        return f"{self._schema}.job_results"

    async def create_job(
        self,
        *,
        job_id: UUID,
        source_url: str,
        source_url_hash: str,
        idempotency_key: str | None,
        source: str | None,
        room_id: str | None,
        max_attempts: int,
    ) -> tuple[JobRecord, bool]:
        if idempotency_key:
            existing = await self.get_job_by_idempotency_key(idempotency_key)
            if existing:
                return existing, False

        sql = f"""
        INSERT INTO {self._jobs_table}
            (job_id, source_url, source_url_hash, idempotency_key, source, room_id, status, attempt, max_attempts, queued_at)
        VALUES
            ($1, $2, $3, $4, $5, $6, 'QUEUED', 0, $7, NOW())
        RETURNING *
        """

        try:
            row = await self._pool.fetchrow(
                sql,
                job_id,
                source_url,
                source_url_hash,
                idempotency_key,
                source,
                room_id,
                max_attempts,
            )
            if row is None:
                raise RuntimeError("Failed to create job")
            return self._to_job_record(row), True
        except asyncpg.UniqueViolationError:
            if not idempotency_key:
                raise
            row = await self._pool.fetchrow(
                f"SELECT * FROM {self._jobs_table} WHERE idempotency_key = $1",
                idempotency_key,
            )
            if row is None:
                raise RuntimeError("Idempotency conflict occurred but existing job was not found")
            return self._to_job_record(row), False

    async def get_job_by_idempotency_key(self, idempotency_key: str) -> JobRecord | None:
        row = await self._pool.fetchrow(
            f"SELECT * FROM {self._jobs_table} WHERE idempotency_key = $1",
            idempotency_key,
        )
        return self._to_job_record(row) if row else None

    async def get_job(self, job_id: UUID) -> JobRecord | None:
        row = await self._pool.fetchrow(
            f"SELECT * FROM {self._jobs_table} WHERE job_id = $1",
            job_id,
        )
        return self._to_job_record(row) if row else None

    async def get_job_result(self, job_id: UUID) -> JobResultRecord | None:
        row = await self._pool.fetchrow(
            f"SELECT * FROM {self._results_table} WHERE job_id = $1",
            job_id,
        )
        return self._to_job_result_record(row) if row else None

    async def claim_job(self, job_id: UUID) -> JobRecord | None:
        sql = f"""
        UPDATE {self._jobs_table}
        SET
            status = 'PROCESSING',
            attempt = attempt + 1,
            processing_started_at = NOW(),
            next_retry_at = NULL,
            error_code = NULL,
            error_message = NULL,
            updated_at = NOW()
        WHERE
            job_id = $1
            AND status = 'QUEUED'
            AND attempt < max_attempts
            AND (next_retry_at IS NULL OR next_retry_at <= NOW())
        RETURNING *
        """
        row = await self._pool.fetchrow(sql, job_id)
        return self._to_job_record(row) if row else None

    async def mark_for_retry(self, job_id: UUID, error_code: str, error_message: str, delay_seconds: int) -> JobRecord | None:
        next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=max(1, delay_seconds))
        sql = f"""
        UPDATE {self._jobs_table}
        SET
            status = 'QUEUED',
            error_code = $2,
            error_message = $3,
            queued_at = NOW(),
            next_retry_at = $4,
            updated_at = NOW()
        WHERE job_id = $1
        RETURNING *
        """
        row = await self._pool.fetchrow(sql, job_id, error_code, error_message, next_retry_at)
        return self._to_job_record(row) if row else None

    async def mark_failed(self, job_id: UUID, error_code: str, error_message: str) -> JobRecord | None:
        sql = f"""
        UPDATE {self._jobs_table}
        SET
            status = 'FAILED',
            error_code = $2,
            error_message = $3,
            completed_at = NOW(),
            next_retry_at = NULL,
            updated_at = NOW()
        WHERE job_id = $1
        RETURNING *
        """
        row = await self._pool.fetchrow(sql, job_id, error_code, error_message)
        return self._to_job_record(row) if row else None

    async def mark_succeeded(self, job_id: UUID) -> JobRecord | None:
        sql = f"""
        UPDATE {self._jobs_table}
        SET
            status = 'SUCCEEDED',
            error_code = NULL,
            error_message = NULL,
            completed_at = NOW(),
            next_retry_at = NULL,
            updated_at = NOW()
        WHERE job_id = $1
        RETURNING *
        """
        row = await self._pool.fetchrow(sql, job_id)
        return self._to_job_record(row) if row else None

    async def mark_job_enqueue_failed(self, job_id: UUID, error_message: str) -> None:
        await self.mark_failed(
            job_id,
            "QUEUE_ENQUEUE_FAILED",
            error_message[:500],
        )

    async def upsert_job_result(
        self,
        *,
        job_id: UUID,
        media_type: str | None,
        caption: str | None,
        instagram_meta: dict[str, Any] | None,
        raw_candidates: list[dict[str, Any]],
        places: list[dict[str, Any]],
        kakao_raw: dict[str, Any] | None,
    ) -> JobResultRecord:
        sql = f"""
        INSERT INTO {self._results_table}
            (job_id, media_type, caption, instagram_meta, raw_candidates, places, kakao_raw)
        VALUES
            ($1, $2, $3, $4::jsonb, $5::jsonb, $6::jsonb, $7::jsonb)
        ON CONFLICT (job_id)
        DO UPDATE SET
            media_type = EXCLUDED.media_type,
            caption = EXCLUDED.caption,
            instagram_meta = EXCLUDED.instagram_meta,
            raw_candidates = EXCLUDED.raw_candidates,
            places = EXCLUDED.places,
            kakao_raw = EXCLUDED.kakao_raw,
            updated_at = NOW()
        RETURNING *
        """
        row = await self._pool.fetchrow(
            sql,
            job_id,
            media_type,
            caption,
            json.dumps(instagram_meta or {}),
            json.dumps(raw_candidates),
            json.dumps(places),
            json.dumps(kakao_raw or {}),
        )
        if row is None:
            raise RuntimeError("Failed to upsert job result")
        return self._to_job_result_record(row)

    def _to_job_record(self, row: asyncpg.Record) -> JobRecord:
        return JobRecord(
            job_id=row["job_id"],
            source_url=row["source_url"],
            source_url_hash=row["source_url_hash"],
            status=JobStatus(row["status"]),
            attempt=row["attempt"],
            max_attempts=row["max_attempts"],
            idempotency_key=row["idempotency_key"],
            source=row["source"],
            room_id=row["room_id"],
            error_code=row["error_code"],
            error_message=row["error_message"],
            queued_at=row["queued_at"],
            processing_started_at=row["processing_started_at"],
            completed_at=row["completed_at"],
            next_retry_at=row["next_retry_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _to_job_result_record(self, row: asyncpg.Record) -> JobResultRecord:
        return JobResultRecord(
            job_id=row["job_id"],
            media_type=row["media_type"],
            caption=row["caption"],
            instagram_meta=self._json_to_dict(row["instagram_meta"]),
            raw_candidates=self._json_to_list(row["raw_candidates"]),
            places=self._json_to_list(row["places"]),
            kakao_raw=self._json_to_dict(row["kakao_raw"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
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

    @staticmethod
    def _json_to_list(value: Any) -> list[dict[str, Any]]:
        if value is None:
            return []
        if isinstance(value, str):
            loaded = json.loads(value)
            return loaded if isinstance(loaded, list) else []
        if isinstance(value, list):
            return value
        return []
