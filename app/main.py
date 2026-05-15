from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.v1.router import api_router
from app.core.config import get_settings, validate_production_internal_api_key
from app.domain.business_hours import BusinessHoursService
from app.domain.job import JobService
from app.infra.db import BusinessHoursRepository, JobRepository, create_db_pool
from app.infra.queue import RedisJobQueue
from app.services.crawler.playwright_service import shutdown_crawler_runtime


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    db_pool = await create_db_pool(settings)
    queue = RedisJobQueue.from_settings(settings)
    business_hours_queue = RedisJobQueue.from_business_hours_settings(settings)
    repository = JobRepository(db_pool, settings.processing_schema)
    business_hours_repository = BusinessHoursRepository(db_pool, settings.processing_schema)
    service = JobService(repository, queue)
    business_hours_service = BusinessHoursService(
        repository=business_hours_repository,
        queue=business_hours_queue,
        stale_timeout_seconds=settings.business_hours_fetching_stale_timeout_seconds,
        enqueue_failed_ttl_seconds=settings.business_hours_enqueue_failed_ttl_seconds,
    )

    app.state.db_pool = db_pool
    app.state.job_queue = queue
    app.state.business_hours_queue = business_hours_queue
    app.state.job_repository = repository
    app.state.business_hours_repository = business_hours_repository
    app.state.job_service = service
    app.state.business_hours_service = business_hours_service

    try:
        yield
    finally:
        await shutdown_crawler_runtime()
        await business_hours_queue.close()
        await queue.close()
        await db_pool.close()


def create_app() -> FastAPI:
    settings = get_settings()
    validate_production_internal_api_key(settings)
    openapi_enabled = settings.openapi_enabled
    application = FastAPI(
        title="processing-private-service",
        description="Processing subsystem private API for Spring Boot integration",
        version="0.2.0",
        lifespan=lifespan,
        docs_url="/docs" if openapi_enabled else None,
        redoc_url="/redoc" if openapi_enabled else None,
        openapi_url="/openapi.json" if openapi_enabled else None,
        openapi_tags=[
            {"name": "health", "description": "Service health endpoints"},
            {"name": "jobs", "description": "Job lifecycle endpoints"},
            {"name": "business-hours", "description": "Business hours crawling endpoints"},
        ],
    )

    application.include_router(api_router, prefix="/api/v1")
    return application


app = create_app()
