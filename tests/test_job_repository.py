from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from app.infra.db.repository import JobRepository

if hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def _can_create_event_loop() -> bool:
    try:
        loop = asyncio.new_event_loop()
        loop.close()
        return True
    except OSError:
        return False


EVENT_LOOP_AVAILABLE = _can_create_event_loop()


def _run(coro):
    try:
        return asyncio.run(coro)
    except OSError as exc:
        pytest.skip(f"Event loop creation is blocked in this environment: {exc}")


class FakePool:
    def __init__(self, row: dict | None = None) -> None:
        self.row = row
        self.sql: str | None = None
        self.args: tuple | None = None

    async def fetchrow(self, sql: str, *args):
        self.sql = sql
        self.args = args
        if self.row is not None:
            return self.row

        now = datetime.now(timezone.utc)
        return {
            "job_id": args[0],
            "caption": args[1],
            "instagram_meta": args[2],
            "extraction_result": args[3],
            "created_at": now,
            "updated_at": now,
        }


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_upsert_job_result_persists_extraction_result() -> None:
    pool = FakePool()
    repository = JobRepository(pool, "processing")
    job_id = uuid4()
    extraction_result = {
        "store_name": "Common Mansion",
        "address": "1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
        "store_name_evidence": "Common Mansion",
        "address_evidence": "1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
        "certainty": "high",
    }

    record = _run(
        repository.upsert_job_result(
            job_id=job_id,
            caption="Common Mansion review",
            instagram_meta={"media_type": "reel"},
            extraction_result=extraction_result,
        )
    )

    assert pool.sql is not None
    assert "extraction_result" in pool.sql
    assert "extraction_result = EXCLUDED.extraction_result" in pool.sql
    assert pool.args == (
        job_id,
        "Common Mansion review",
        json.dumps({"media_type": "reel"}),
        json.dumps(extraction_result),
    )
    assert record.extraction_result == extraction_result


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_get_job_result_maps_extraction_result() -> None:
    job_id = uuid4()
    now = datetime.now(timezone.utc)
    extraction_result = {
        "store_name": None,
        "address": None,
        "store_name_evidence": None,
        "address_evidence": None,
        "certainty": "low",
    }
    pool = FakePool(
        {
            "job_id": job_id,
            "caption": "caption",
            "instagram_meta": json.dumps({"caption": "caption"}),
            "extraction_result": json.dumps(extraction_result),
            "created_at": now,
            "updated_at": now,
        }
    )
    repository = JobRepository(pool, "processing")

    record = _run(repository.get_job_result(job_id))

    assert record is not None
    assert record.extraction_result == extraction_result
