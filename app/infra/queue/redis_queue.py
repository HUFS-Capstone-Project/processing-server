from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from redis.asyncio import Redis

from app.core.config import Settings


class RedisJobQueue:
    def __init__(
        self,
        client: Redis,
        *,
        ready_key: str,
        delayed_key: str,
        processing_key: str,
    ) -> None:
        self._client = client
        self._ready_key = ready_key
        self._delayed_key = delayed_key
        self._processing_key = processing_key

    @classmethod
    def from_settings(cls, settings: Settings) -> "RedisJobQueue":
        client = Redis.from_url(settings.queue_redis_url, decode_responses=True)
        return cls(
            client,
            ready_key=settings.queue_ready_key,
            delayed_key=settings.queue_delayed_key,
            processing_key=settings.queue_processing_key,
        )

    @classmethod
    def from_business_hours_settings(cls, settings: Settings) -> "RedisJobQueue":
        client = Redis.from_url(settings.queue_redis_url, decode_responses=True)
        return cls(
            client,
            ready_key=settings.business_hours_queue_ready_key,
            delayed_key=settings.business_hours_queue_delayed_key,
            processing_key=settings.business_hours_queue_processing_key,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def enqueue(self, job_id: UUID) -> None:
        await self._client.rpush(self._ready_key, str(job_id))

    async def enqueue_delayed(self, job_id: UUID, delay_seconds: int) -> None:
        score = int(datetime.now(tz=timezone.utc).timestamp()) + max(1, delay_seconds)
        await self._client.zadd(self._delayed_key, {str(job_id): score})

    async def dequeue(self, timeout_seconds: int) -> UUID | None:
        item = await self._client.blpop(self._ready_key, timeout=max(1, timeout_seconds))
        if not item:
            return None
        _, value = item
        return UUID(value)

    async def promote_delayed(self, batch_size: int) -> int:
        now_ts = int(datetime.now(tz=timezone.utc).timestamp())
        due_ids = await self._client.zrangebyscore(
            self._delayed_key,
            min=0,
            max=now_ts,
            start=0,
            num=max(1, batch_size),
        )
        if not due_ids:
            return 0

        moved = 0
        async with self._client.pipeline(transaction=True) as pipe:
            for job_id in due_ids:
                pipe.zrem(self._delayed_key, job_id)
                pipe.rpush(self._ready_key, job_id)
            results = await pipe.execute()

        # zrem/rpush pair per id. Count successful zrem only.
        for idx in range(0, len(results), 2):
            if int(results[idx]) > 0:
                moved += 1
        return moved
