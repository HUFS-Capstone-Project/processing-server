from __future__ import annotations

from app.core.config import Settings
from app.infra.kakao import KakaoLocalClient
from app.infra.llm import HFExtractionClient
from app.worker.runner import build_extraction_client, build_place_search_client


def test_build_extraction_client_returns_none_without_endpoint() -> None:
    settings = Settings(
        hf_extraction_endpoint_url="",
        hf_extraction_api_token="test-token",
    )

    assert build_extraction_client(settings) is None


def test_build_extraction_client_returns_none_without_token() -> None:
    settings = Settings(
        hf_extraction_endpoint_url="https://router.huggingface.co/v1/chat/completions",
        hf_extraction_api_token="",
    )

    assert build_extraction_client(settings) is None


def test_build_extraction_client_returns_hf_client_when_configured() -> None:
    settings = Settings(
        hf_extraction_endpoint_url="https://router.huggingface.co/v1/chat/completions",
        hf_extraction_api_token="test-token",
    )

    assert isinstance(build_extraction_client(settings), HFExtractionClient)


def test_build_place_search_client_returns_none_without_key() -> None:
    settings = Settings(kakao_rest_api_key="")

    assert build_place_search_client(settings) is None


def test_build_place_search_client_returns_kakao_client_when_configured() -> None:
    settings = Settings(kakao_rest_api_key="test-kakao-key")

    assert isinstance(build_place_search_client(settings), KakaoLocalClient)
