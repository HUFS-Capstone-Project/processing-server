from fastapi import APIRouter

from app.schemas.analyze import AnalyzeRequest, AnalyzeResponse
from app.services.pipeline.analyze_pipeline import run_analyze

router = APIRouter()


@router.post(
    "/analyze",
    response_model=AnalyzeResponse,
    summary="콘텐츠 URL 크롤링",
    description=(
        "요청 본문의 `url`에 접속해 콘텐츠를 가져옵니다.\n\n"
        "**동작 요약**\n"
        "- 일반 사이트: `html` + `text`(본문 텍스트)\n"
        "- Instagram 릴스·게시글: `html`은 null, og 메타 기반 문자열을 파싱해 `instagram`·`media_type`을 채웁니다.\n\n"
        "인스타는 로그인·지역·A/B 테스트에 따라 메타 형식이 달라질 수 있어, "
        "파싱에 실패하면 `instagram`은 비어 있고 `text`에 원문이 들어갈 수 있습니다."
    ),
    response_description="크롤·파싱 결과. `success`가 false이면 `error`를 확인합니다.",
)
async def analyze(payload: AnalyzeRequest) -> AnalyzeResponse:
    return await run_analyze(str(payload.url))
