from fastapi import APIRouter

router = APIRouter()


@router.get(
    "/health",
    summary="Health check",
    description="Returns UP when the private service is running.",
    response_description="Service liveness status.",
)
async def health() -> dict[str, str]:
    return {"status": "UP"}
