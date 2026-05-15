from __future__ import annotations

import asyncio
from uuid import uuid4

from app.infra.queue.redis_queue import RedisJobQueue


def _run(coro):
    return asyncio.run(coro)


class FakePipeline:
    def __init__(self, redis: FakeRedis) -> None:
        self.redis = redis
        self.ops = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    def zrem(self, key, value):
        self.ops.append(("zrem", key, value))

    def rpush(self, key, value):
        self.ops.append(("rpush", key, value))

    async def execute(self):
        results = []
        for op, key, value in self.ops:
            if op == "zrem":
                results.append(await self.redis.zrem(key, value))
            else:
                results.append(await self.redis.rpush(key, value))
        return results


class FakeRedis:
    def __init__(self) -> None:
        self.lists = {}
        self.zsets = {}

    async def rpush(self, key, value):
        self.lists.setdefault(key, []).append(str(value))
        return len(self.lists[key])

    async def blmove(self, source, destination, timeout, src="LEFT", dest="RIGHT"):
        if not self.lists.get(source):
            return None
        value = self.lists[source].pop(0)
        self.lists.setdefault(destination, []).append(value)
        return value

    async def lmove(self, source, destination, src="LEFT", dest="RIGHT"):
        if not self.lists.get(source):
            return None
        value = self.lists[source].pop(0)
        self.lists.setdefault(destination, []).append(value)
        return value

    async def lpop(self, key):
        if not self.lists.get(key):
            return None
        return self.lists[key].pop(0)

    async def lrem(self, key, count, value):
        values = self.lists.get(key, [])
        removed = 0
        remaining = []
        for item in values:
            if item == value and (count == 0 or removed < abs(count)):
                removed += 1
                continue
            remaining.append(item)
        self.lists[key] = remaining
        return removed

    async def lrange(self, key, start, end):
        return list(self.lists.get(key, []))

    async def zadd(self, key, mapping):
        self.zsets.setdefault(key, {}).update(mapping)
        return len(mapping)

    async def zrangebyscore(self, key, min, max, start=0, num=1):
        due = [member for member, score in self.zsets.get(key, {}).items() if score <= max]
        return due[start : start + num]

    async def zrem(self, key, value):
        return 1 if self.zsets.get(key, {}).pop(str(value), None) is not None else 0

    def pipeline(self, transaction=True):
        return FakePipeline(self)


def test_dequeue_moves_ready_to_processing_and_ack_removes() -> None:
    redis = FakeRedis()
    queue = RedisJobQueue(redis, ready_key="q:ready", processing_key="q:processing", delayed_key="q:delayed")
    job_id = uuid4()

    _run(queue.enqueue(job_id))
    dequeued = _run(queue.dequeue(1))

    assert dequeued == job_id
    assert redis.lists["q:ready"] == []
    assert redis.lists["q:processing"][0].startswith(str(job_id))

    _run(queue.ack(job_id))
    assert redis.lists["q:processing"] == []


def test_dequeue_nowait_moves_ready_to_processing_without_blocking() -> None:
    redis = FakeRedis()
    queue = RedisJobQueue(redis, ready_key="q:ready", processing_key="q:processing", delayed_key="q:delayed")
    job_id = uuid4()

    _run(queue.enqueue(job_id))
    dequeued = _run(queue.dequeue(0))

    assert dequeued == job_id
    assert redis.lists["q:ready"] == []
    assert redis.lists["q:processing"][0].startswith(str(job_id))


def test_retry_later_moves_processing_to_delayed() -> None:
    redis = FakeRedis()
    queue = RedisJobQueue(redis, ready_key="q:ready", processing_key="q:processing", delayed_key="q:delayed")
    job_id = uuid4()

    _run(queue.enqueue(job_id))
    _run(queue.dequeue(1))
    _run(queue.retry_later(job_id, 10))

    assert redis.lists["q:processing"] == []
    assert str(job_id) in redis.zsets["q:delayed"]


def test_recover_stale_processing_jobs_requeues_unstamped_item() -> None:
    redis = FakeRedis()
    queue = RedisJobQueue(redis, ready_key="q:ready", processing_key="q:processing", delayed_key="q:delayed")
    job_id = uuid4()
    redis.lists["q:processing"] = [str(job_id)]

    moved = _run(queue.recover_stale_processing_jobs(60))

    assert moved == 1
    assert redis.lists["q:ready"] == [str(job_id)]
    assert redis.lists["q:processing"] == []


def test_recover_stale_processing_jobs_requeues_old_stamped_item() -> None:
    redis = FakeRedis()
    queue = RedisJobQueue(redis, ready_key="q:ready", processing_key="q:processing", delayed_key="q:delayed")
    job_id = uuid4()
    redis.lists["q:processing"] = [f"{job_id}|1"]

    moved = _run(queue.recover_stale_processing_jobs(60))

    assert moved == 1
    assert redis.lists["q:ready"] == [str(job_id)]
    assert redis.lists["q:processing"] == []

