from typing import Literal

from pydantic import BaseModel, Field, HttpUrl


class AnalyzeRequest(BaseModel):
    url: HttpUrl = Field(
        ...,
        description="크롤링할 페이지의 URL입니다.",
        examples=["https://www.example.com/", "https://www.instagram.com/reel/xxxxx/"],
    )


class InstagramOgMeta(BaseModel):
    likes: int = Field(..., description="좋아요")
    comments: int = Field(..., description="댓글")
    username: str = Field(..., description="작성자")
    posted_at: str = Field(..., description="게시 일시(원문)")
    caption: str = Field(..., description="캡션")


class AnalyzeResponse(BaseModel):
    url: str = Field(..., description="요청에 넣은 URL 문자열")
    success: bool = Field(..., description="크롤·파싱이 예외 없이 끝났으면 true")
    text: str | None = Field(
        None,
        description=(
            "일반 URL: `body`에서 추출한 텍스트. "
            "Instagram이고 og 형식 파싱에 성공한 경우: 캡션만. "
            "Instagram인데 파싱 실패 시: 메타 원문 전체가 들어갈 수 있음."
        ),
    )
    html: str | None = Field(
        None,
        description="일반 URL일 때만 전체 HTML. Instagram 미디어 URL이면 수집하지 않아 null입니다.",
    )
    error: str | None = Field(
        None,
        description="크롤링 등 처리 중 예외가 나면 메시지. 성공 시 null입니다.",
    )
    media_type: Literal["reel", "post"] | None = Field(
        None,
        description="Instagram 전용. 경로가 릴스면 `reel`, 피드 게시글이면 `post`. Instagram이 아니면 null.",
    )
    instagram: InstagramOgMeta | None = Field(
        None,
        description=(
            "Instagram URL이고, og 설명 문자열이 `likes, comments - user - date: \"…\"` 형태로 "
            "파싱된 경우에만 채워집니다."
        ),
    )


InstagramReelMeta = InstagramOgMeta
