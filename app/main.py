from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.v1.router import api_router
from app.core.config import get_settings, validate_production_internal_api_key
from app.domain.job import JobService
from app.infra.db import JobRepository, create_db_pool
from app.infra.queue import RedisJobQueue


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    db_pool = await create_db_pool(settings)
    queue = RedisJobQueue.from_settings(settings)
    repository = JobRepository(db_pool, settings.processing_schema)
    service = JobService(repository, queue, max_attempts=settings.worker_max_attempts)

    app.state.db_pool = db_pool
    app.state.job_queue = queue
    app.state.job_repository = repository
    app.state.job_service = service

    try:
        yield
    finally:
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
        ],
    )

    application.include_router(api_router, prefix="/api/v1")
    return application


app = create_app()
