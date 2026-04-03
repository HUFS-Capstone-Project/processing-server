from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.router import api_router
from app.core.config import get_settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


def create_app() -> FastAPI:
    settings = get_settings()
    application = FastAPI(
        title="udidura-ai",
        description="SNS 콘텐츠 크롤링·데이터 처리 API",
        version="0.1.0",
        lifespan=lifespan,
        openapi_tags=[
            {"name": "health", "description": "헬스 체크"},
            {"name": "analyze", "description": "URL 크롤링"},
        ],
    )

    raw = [o.strip() for o in settings.cors_origins.split(",") if o.strip()]
    use_all = len(raw) == 1 and raw[0] == "*"
    application.add_middleware(
        CORSMiddleware,
        allow_origins=["*"] if use_all else raw,
        allow_credentials=not use_all,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    application.include_router(api_router, prefix="/api/v1")
    return application


app = create_app()
