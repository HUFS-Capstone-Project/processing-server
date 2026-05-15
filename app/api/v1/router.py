from fastapi import APIRouter

from app.api.v1.endpoints import business_hours, health, jobs

api_router = APIRouter()
api_router.include_router(health.router, tags=["health"])
api_router.include_router(jobs.router, tags=["jobs"])
api_router.include_router(business_hours.router, tags=["business-hours"])
