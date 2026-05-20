from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import UUID

import asyncpg  # type: ignore[import-untyped]

from app.domain.job.model import (
    CrawledContentRecord,
    JobRecord,
    JobResultRecord,
    JobStatus,
    LinkStatsRecord,
)


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

    @property
    def _crawled_contents_table(self) -> str:
        return f"{self._schema}.crawled_contents"

    @property
    def _link_stats_table(self) -> str:
        return f"{self._schema}.link_stats"

    async def create_job(
        self,
        *,
        job_id: UUID,
        room_id: UUID,
        source_url: str,
    ) -> JobRecord:
        normalized_source_url = self.normalize_source_url(source_url)
        existing = await self._pool.fetchrow(
            f"""
            SELECT j.*
            FROM {self._jobs_table} j
            LEFT JOIN {self._results_table} r ON r.job_id = j.job_id
            WHERE j.room_id = $1
              AND COALESCE(j.normalized_source_url, j.source_url) = $2
              AND (
                    j.status IN ('QUEUED', 'PROCESSING')
                    OR (j.status = 'SUCCEEDED' AND r.job_id IS NOT NULL)
              )
            ORDER BY j.created_at DESC
            LIMIT 1
            """,
            room_id,
            normalized_source_url,
        )
        if existing:
            return self._to_job_record(existing)

        row = await self._pool.fetchrow(
            f"""
            INSERT INTO {self._jobs_table}
                (job_id, room_id, source_url, normalized_source_url, status)
            VALUES
                ($1, $2, $3, $4, 'QUEUED')
            RETURNING *
            """,
            job_id,
            room_id,
            source_url,
            normalized_source_url,
        )
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

    async def get_crawled_content(self, job_id: UUID) -> CrawledContentRecord | None:
        row = await self._pool.fetchrow(
            f"SELECT * FROM {self._crawled_contents_table} WHERE job_id = $1",
            job_id,
        )
        return self._to_crawled_content_record(row) if row else None

    async def get_link_stats(self, job_id: UUID) -> LinkStatsRecord | None:
        row = await self._pool.fetchrow(
            f"SELECT * FROM {self._link_stats_table} WHERE job_id = $1",
            job_id,
        )
        return self._to_link_stats_record(row) if row else None

    async def claim_job(self, job_id: UUID) -> JobRecord | None:
        row = await self._pool.fetchrow(
            f"""
            UPDATE {self._jobs_table}
            SET
                status = 'PROCESSING',
                error_message = NULL,
                error_code = NULL,
                processing_started_at = NOW(),
                last_heartbeat_at = NOW(),
                attempt_count = attempt_count + 1,
                updated_at = NOW()
            WHERE
                job_id = $1
                AND status = 'QUEUED'
                AND (next_retry_at IS NULL OR next_retry_at <= NOW())
                AND attempt_count < max_attempts
            RETURNING *
            """,
            job_id,
        )
        return self._to_job_record(row) if row else None

    async def mark_failed(
        self,
        job_id: UUID,
        error_message: str,
        error_code: str | None = None,
    ) -> JobRecord | None:
        row = await self._pool.fetchrow(
            f"""
            UPDATE {self._jobs_table}
            SET
                status = 'FAILED',
                error_message = $2,
                error_code = $3,
                failed_at = NOW(),
                updated_at = NOW()
            WHERE job_id = $1
            RETURNING *
            """,
            job_id,
            error_message,
            error_code,
        )
        return self._to_job_record(row) if row else None

    async def mark_succeeded(self, job_id: UUID) -> JobRecord | None:
        row = await self._pool.fetchrow(
            f"""
            UPDATE {self._jobs_table}
            SET
                status = 'SUCCEEDED',
                error_message = NULL,
                error_code = NULL,
                completed_at = NOW(),
                processing_started_at = NULL,
                last_heartbeat_at = NULL,
                updated_at = NOW()
            WHERE job_id = $1
            RETURNING *
            """,
            job_id,
        )
        return self._to_job_record(row) if row else None

    async def mark_job_enqueue_failed(self, job_id: UUID, error_message: str) -> None:
        await self.mark_failed(job_id, error_message[:500], error_code="ENQUEUE_FAILED")

    async def schedule_retry(
        self,
        job_id: UUID,
        *,
        error_code: str | None,
        error_message: str | None,
        delay_seconds: int,
    ) -> JobRecord | None:
        next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=max(1, delay_seconds))
        row = await self._pool.fetchrow(
            f"""
            UPDATE {self._jobs_table}
            SET
                status = 'QUEUED',
                error_code = $2,
                error_message = $3,
                next_retry_at = $4,
                processing_started_at = NULL,
                last_heartbeat_at = NULL,
                updated_at = NOW()
            WHERE job_id = $1 AND attempt_count < max_attempts
            RETURNING *
            """,
            job_id,
            error_code,
            (error_message or "")[:1000],
            next_retry_at,
        )
        return self._to_job_record(row) if row else None

    async def recover_stale_processing_jobs(self, stale_after_seconds: int) -> list[UUID]:
        rows = await self._pool.fetch(
            f"""
            UPDATE {self._jobs_table}
            SET
                status = CASE WHEN attempt_count < max_attempts THEN 'QUEUED' ELSE 'FAILED' END,
                error_code = CASE
                    WHEN attempt_count < max_attempts THEN 'STALE_PROCESSING_RECOVERED'
                    ELSE 'STALE_PROCESSING_MAX_ATTEMPTS'
                END,
                error_message = CASE
                    WHEN attempt_count < max_attempts THEN 'Recovered stale processing job.'
                    ELSE 'Stale processing job exceeded max attempts.'
                END,
                failed_at = CASE WHEN attempt_count < max_attempts THEN failed_at ELSE NOW() END,
                next_retry_at = CASE WHEN attempt_count < max_attempts THEN NOW() ELSE next_retry_at END,
                processing_started_at = NULL,
                last_heartbeat_at = NULL,
                updated_at = NOW()
            WHERE status = 'PROCESSING'
              AND COALESCE(last_heartbeat_at, processing_started_at, updated_at)
                    <= NOW() - ($1::int * INTERVAL '1 second')
            RETURNING job_id
            """,
            max(1, stale_after_seconds),
        )
        return [row["job_id"] for row in rows]

    async def converge_completed_results(self) -> list[UUID]:
        rows = await self._pool.fetch(
            f"""
            UPDATE {self._jobs_table} j
            SET status = 'SUCCEEDED',
                error_code = NULL,
                error_message = NULL,
                completed_at = NOW(),
                processing_started_at = NULL,
                last_heartbeat_at = NULL,
                updated_at = NOW()
            FROM {self._results_table} r
            WHERE r.job_id = j.job_id
              AND j.status = 'PROCESSING'
            RETURNING j.job_id
            """
        )
        return [row["job_id"] for row in rows]

    async def upsert_job_result(
        self,
        *,
        job_id: UUID,
        extraction_result: dict[str, Any] | None = None,
        place_candidates: list[dict[str, Any]] | None = None,
        resolved_places: list[dict[str, Any]] | None = None,
    ) -> JobResultRecord:
        row = await self._pool.fetchrow(
            f"""
            INSERT INTO {self._results_table}
                (
                    job_id,
                    extraction_result,
                    place_candidates,
                    resolved_places
                )
            VALUES
                ($1, $2::jsonb, $3::jsonb, $4::jsonb)
            ON CONFLICT (job_id)
            DO UPDATE SET
                extraction_result = EXCLUDED.extraction_result,
                place_candidates = EXCLUDED.place_candidates,
                resolved_places = EXCLUDED.resolved_places,
                updated_at = NOW()
            RETURNING *
            """,
            job_id,
            json.dumps(extraction_result) if extraction_result is not None else None,
            json.dumps(place_candidates or []),
            json.dumps(resolved_places or []),
        )
        if row is None:
            raise RuntimeError("Failed to upsert job result")
        return self._to_job_result_record(row)

    async def upsert_crawled_content(
        self,
        *,
        job_id: UUID,
        source_url: str,
        source_type: str,
        content_text: str,
        extraction_method: str | None = None,
        raw_metadata: dict[str, Any] | None = None,
    ) -> CrawledContentRecord:
        row = await self._pool.fetchrow(
            f"""
            INSERT INTO {self._crawled_contents_table}
                (
                    job_id,
                    source_url,
                    source_type,
                    content_text,
                    extraction_method,
                    raw_metadata
                )
            VALUES
                ($1, $2, $3, $4, $5, $6::jsonb)
            ON CONFLICT (job_id)
            DO UPDATE SET
                source_url = EXCLUDED.source_url,
                source_type = EXCLUDED.source_type,
                content_text = EXCLUDED.content_text,
                extraction_method = EXCLUDED.extraction_method,
                raw_metadata = EXCLUDED.raw_metadata,
                updated_at = NOW()
            RETURNING *
            """,
            job_id,
            source_url,
            source_type,
            content_text,
            extraction_method,
            json.dumps(raw_metadata or {}),
        )
        if row is None:
            raise RuntimeError("Failed to upsert crawled content")
        return self._to_crawled_content_record(row)

    async def upsert_link_stats(
        self,
        *,
        job_id: UUID,
        source_url: str,
        source_type: str,
        like_count: int | None = None,
        comment_count: int | None = None,
        posted_at: str | None = None,
        collected_at: datetime | None = None,
        stats_source: str,
        confidence: str,
        unavailable_reason: str | None = None,
        raw_stats: dict[str, Any] | None = None,
    ) -> LinkStatsRecord:
        row = await self._pool.fetchrow(
            f"""
            INSERT INTO {self._link_stats_table}
                (
                    job_id,
                    source_url,
                    source_type,
                    like_count,
                    comment_count,
                    posted_at,
                    collected_at,
                    stats_source,
                    confidence,
                    unavailable_reason,
                    raw_stats
                )
            VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)
            ON CONFLICT (job_id)
            DO UPDATE SET
                source_url = EXCLUDED.source_url,
                source_type = EXCLUDED.source_type,
                like_count = EXCLUDED.like_count,
                comment_count = EXCLUDED.comment_count,
                posted_at = EXCLUDED.posted_at,
                collected_at = EXCLUDED.collected_at,
                stats_source = EXCLUDED.stats_source,
                confidence = EXCLUDED.confidence,
                unavailable_reason = EXCLUDED.unavailable_reason,
                raw_stats = EXCLUDED.raw_stats,
                updated_at = NOW()
            RETURNING *
            """,
            job_id,
            source_url,
            source_type,
            like_count,
            comment_count,
            posted_at,
            collected_at,
            stats_source,
            confidence,
            unavailable_reason,
            json.dumps(raw_stats or {}),
        )
        if row is None:
            raise RuntimeError("Failed to upsert link stats")
        return self._to_link_stats_record(row)

    def _to_job_record(self, row: asyncpg.Record) -> JobRecord:
        return JobRecord(
            job_id=row["job_id"],
            room_id=row["room_id"],
            source_url=row["source_url"],
            status=JobStatus(row["status"]),
            error_message=self._row_get(row, "error_message"),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            attempt_count=int(self._row_get(row, "attempt_count", default=0) or 0),
            max_attempts=int(self._row_get(row, "max_attempts", default=3) or 3),
            error_code=self._row_get(row, "error_code"),
            next_retry_at=self._row_get(row, "next_retry_at"),
            processing_started_at=self._row_get(row, "processing_started_at"),
            last_heartbeat_at=self._row_get(row, "last_heartbeat_at"),
            failed_at=self._row_get(row, "failed_at"),
            completed_at=self._row_get(row, "completed_at"),
            normalized_source_url=self._row_get(row, "normalized_source_url"),
        )

    def _to_job_result_record(self, row: asyncpg.Record) -> JobResultRecord:
        return JobResultRecord(
            job_id=row["job_id"],
            extraction_result=self._json_to_dict(row["extraction_result"]),
            place_candidates=self._json_to_list(row["place_candidates"]),
            resolved_places=self._json_to_list(row["resolved_places"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _to_crawled_content_record(self, row: asyncpg.Record) -> CrawledContentRecord:
        return CrawledContentRecord(
            job_id=row["job_id"],
            source_url=row["source_url"],
            source_type=row["source_type"],
            content_text=row["content_text"],
            extraction_method=self._row_get(row, "extraction_method"),
            raw_metadata=self._json_to_dict(row["raw_metadata"]) or {},
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _to_link_stats_record(self, row: asyncpg.Record) -> LinkStatsRecord:
        return LinkStatsRecord(
            job_id=row["job_id"],
            source_url=row["source_url"],
            source_type=row["source_type"],
            like_count=self._optional_int(self._row_get(row, "like_count")),
            comment_count=self._optional_int(self._row_get(row, "comment_count")),
            posted_at=self._row_get(row, "posted_at"),
            collected_at=self._row_get(row, "collected_at"),
            stats_source=row["stats_source"],
            confidence=row["confidence"],
            unavailable_reason=self._row_get(row, "unavailable_reason"),
            raw_stats=self._json_to_dict(row["raw_stats"]) or {},
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def normalize_source_url(source_url: str) -> str:
        parsed = urlsplit((source_url or "").strip())
        query = urlencode(sorted(parse_qsl(parsed.query, keep_blank_values=True)))
        path = parsed.path.rstrip("/") or "/"
        return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), path, query, ""))

    @staticmethod
    def _row_get(row: Any, key: str, fallback_key: str | None = None, default: Any = None) -> Any:
        try:
            return row[key]
        except (KeyError, IndexError):
            if fallback_key is not None:
                try:
                    return row[fallback_key]
                except (KeyError, IndexError):
                    return default
            return default

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

    @staticmethod
    def _optional_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

