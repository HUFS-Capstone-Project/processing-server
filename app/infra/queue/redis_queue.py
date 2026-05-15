from __future__ import annotations

import logging
from datetime import datetime, timezone
from uuid import UUID

from redis.asyncio import Redis

from app.core.config import Settings

logger = logging.getLogger("processing.queue.redis")


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

    @property
    def ready_key(self) -> str:
        return self._ready_key

    @property
    def processing_key(self) -> str:
        return self._processing_key

    @property
    def delayed_key(self) -> str:
        return self._delayed_key

    async def close(self) -> None:
        await self._client.aclose()

    async def enqueue(self, job_id: UUID) -> None:
        await self._client.rpush(self._ready_key, str(job_id))
        logger.info("queue action=enqueue job_id=%s ready_key=%s", job_id, self._ready_key)

    async def dequeue(self, timeout_seconds: int) -> UUID | None:
        return await self.dequeue_for_processing(timeout_seconds)

    async def dequeue_for_processing(self, timeout_seconds: int) -> UUID | None:
        if timeout_seconds <= 0:
            raw = await self._dequeue_nowait()
        else:
            try:
                raw = await self._client.blmove(
                    self._ready_key,
                    self._processing_key,
                    timeout=max(1, timeout_seconds),
                    src="LEFT",
                    dest="RIGHT",
                )
            except AttributeError:
                raw = await self._client.brpoplpush(
                    self._ready_key,
                    self._processing_key,
                    timeout=max(1, timeout_seconds),
                )
        if not raw:
            return None
        job_id = self._parse_job_id(raw)
        stamped = self._processing_item(job_id)
        if raw != stamped:
            await self._client.lrem(self._processing_key, 1, raw)
            await self._client.rpush(self._processing_key, stamped)
        logger.info("queue action=dequeue job_id=%s processing_key=%s", job_id, self._processing_key)
        return job_id

    async def _dequeue_nowait(self) -> str | None:
        try:
            return await self._client.lmove(
                self._ready_key,
                self._processing_key,
                src="LEFT",
                dest="RIGHT",
            )
        except AttributeError:
            raw = await self._client.lpop(self._ready_key)
            if raw:
                await self._client.rpush(self._processing_key, raw)
            return raw

    async def ack(self, job_id: UUID) -> None:
        removed = await self._remove_processing_item(job_id)
        logger.info("queue action=ack job_id=%s removed=%s", job_id, removed)

    async def retry_later(self, job_id: UUID, delay_seconds: int) -> None:
        await self._remove_processing_item(job_id)
        score = int(datetime.now(tz=timezone.utc).timestamp()) + max(1, delay_seconds)
        await self._client.zadd(self._delayed_key, {str(job_id): score})
        logger.info(
            "queue action=retry_later job_id=%s delay_seconds=%s delayed_key=%s",
            job_id,
            delay_seconds,
            self._delayed_key,
        )

    async def enqueue_delayed(self, job_id: UUID, delay_seconds: int) -> None:
        await self.retry_later(job_id, delay_seconds)

    async def promote_delayed(self, batch_size: int) -> int:
        return await self.promote_due_jobs(batch_size)

    async def promote_due_jobs(self, batch_size: int) -> int:
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

        for idx in range(0, len(results), 2):
            if int(results[idx]) > 0:
                moved += 1
        logger.info("queue action=promote_due_jobs moved=%s delayed_key=%s", moved, self._delayed_key)
        return moved

    async def recover_stale_processing_jobs(self, stale_after_seconds: int) -> int:
        now_ts = int(datetime.now(tz=timezone.utc).timestamp())
        items = await self._client.lrange(self._processing_key, 0, -1)
        moved = 0
        for item in items:
            job_id, started_at = self._parse_processing_item(item)
            if started_at and started_at > now_ts - max(1, stale_after_seconds):
                continue
            removed = await self._client.lrem(self._processing_key, 1, item)
            if int(removed) <= 0:
                continue
            await self._client.rpush(self._ready_key, str(job_id))
            moved += 1
        if moved:
            logger.warning(
                "queue action=recover_stale_processing_jobs moved=%s stale_after_seconds=%s",
                moved,
                stale_after_seconds,
            )
        return moved

    async def _remove_processing_item(self, job_id: UUID) -> int:
        removed = await self._client.lrem(self._processing_key, 0, str(job_id))
        for item in await self._client.lrange(self._processing_key, 0, -1):
            parsed_id, _ = self._parse_processing_item(item)
            if parsed_id == job_id:
                removed += await self._client.lrem(self._processing_key, 1, item)
        return int(removed)

    @staticmethod
    def _processing_item(job_id: UUID) -> str:
        return f"{job_id}|{int(datetime.now(tz=timezone.utc).timestamp())}"

    @staticmethod
    def _parse_job_id(raw: str) -> UUID:
        return UUID(str(raw).split("|", 1)[0])

    @classmethod
    def _parse_processing_item(cls, raw: str) -> tuple[UUID, int | None]:
        parts = str(raw).split("|", 1)
        job_id = UUID(parts[0])
        if len(parts) == 1:
            return job_id, None
        try:
            return job_id, int(parts[1])
        except ValueError:
            return job_id, None

