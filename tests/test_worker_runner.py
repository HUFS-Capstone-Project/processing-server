from __future__ import annotations

from app.core.config import Settings
from app.infra.llm import HFExtractionClient
from app.worker.runner import build_extraction_client


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
