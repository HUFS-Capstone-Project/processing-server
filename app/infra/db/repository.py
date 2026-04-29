from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg  # type: ignore[import-untyped]

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
        room_id: UUID,
        source_url: str,
    ) -> JobRecord:
        sql = f"""
        INSERT INTO {self._jobs_table}
            (job_id, room_id, source_url, status)
        VALUES
            ($1, $2, $3, 'QUEUED')
        RETURNING *
        """
        row = await self._pool.fetchrow(sql, job_id, room_id, source_url)
        if row is None:
            raise RuntimeError("Failed to create job")
        return self._to_job_record(row)

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
            error_message = NULL,
            updated_at = NOW()
        WHERE
            job_id = $1
            AND status = 'QUEUED'
        RETURNING *
        """
        row = await self._pool.fetchrow(sql, job_id)
        return self._to_job_record(row) if row else None

    async def mark_failed(self, job_id: UUID, error_message: str) -> JobRecord | None:
        sql = f"""
        UPDATE {self._jobs_table}
        SET
            status = 'FAILED',
            error_message = $2,
            updated_at = NOW()
        WHERE job_id = $1
        RETURNING *
        """
        row = await self._pool.fetchrow(sql, job_id, error_message)
        return self._to_job_record(row) if row else None

    async def mark_succeeded(self, job_id: UUID) -> JobRecord | None:
        sql = f"""
        UPDATE {self._jobs_table}
        SET
            status = 'SUCCEEDED',
            error_message = NULL,
            updated_at = NOW()
        WHERE job_id = $1
        RETURNING *
        """
        row = await self._pool.fetchrow(sql, job_id)
        return self._to_job_record(row) if row else None

    async def mark_job_enqueue_failed(self, job_id: UUID, error_message: str) -> None:
        await self.mark_failed(job_id, error_message[:500])

    async def upsert_job_result(
        self,
        *,
        job_id: UUID,
        caption: str | None,
        instagram_meta: dict[str, Any] | None,
        extraction_result: dict[str, Any] | None = None,
        place_candidates: list[dict[str, Any]] | None = None,
        selected_place: dict[str, Any] | None = None,
    ) -> JobResultRecord:
        sql = f"""
        INSERT INTO {self._results_table}
            (job_id, caption, instagram_meta, extraction_result, place_candidates, selected_place)
        VALUES
            ($1, $2, $3::jsonb, $4::jsonb, $5::jsonb, $6::jsonb)
        ON CONFLICT (job_id)
        DO UPDATE SET
            caption = EXCLUDED.caption,
            instagram_meta = EXCLUDED.instagram_meta,
            extraction_result = EXCLUDED.extraction_result,
            place_candidates = EXCLUDED.place_candidates,
            selected_place = EXCLUDED.selected_place,
            updated_at = NOW()
        RETURNING *
        """
        row = await self._pool.fetchrow(
            sql,
            job_id,
            caption,
            json.dumps(instagram_meta or {}),
            json.dumps(extraction_result) if extraction_result is not None else None,
            json.dumps(place_candidates or []),
            json.dumps(selected_place) if selected_place is not None else None,
        )
        if row is None:
            raise RuntimeError("Failed to upsert job result")
        return self._to_job_result_record(row)

    def _to_job_record(self, row: asyncpg.Record) -> JobRecord:
        return JobRecord(
            job_id=row["job_id"],
            room_id=row["room_id"],
            source_url=row["source_url"],
            status=JobStatus(row["status"]),
            error_message=row["error_message"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _to_job_result_record(self, row: asyncpg.Record) -> JobResultRecord:
        return JobResultRecord(
            job_id=row["job_id"],
            caption=row["caption"],
            instagram_meta=self._json_to_dict(row["instagram_meta"]),
            extraction_result=self._json_to_dict(row["extraction_result"]),
            place_candidates=self._json_to_list(row["place_candidates"]),
            selected_place=self._json_to_dict(row["selected_place"]),
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
            value = json.loads(value)
        if isinstance(value, list):
            return value
        return list(value)
