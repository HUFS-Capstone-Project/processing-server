from fastapi import APIRouter

router = APIRouter()


@router.get(
    "/health",
    summary="헬스 체크",
    description=(
        "애플리케이션 프로세스가 응답할 수 있는지 확인합니다. "
    ),
    response_description="`status`가 `UP`이면 정상 기동 상태입니다.",
)
async def health() -> dict[str, str]:
    return {"status": "UP"}
