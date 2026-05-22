from __future__ import annotations

from app.main import create_app


def _schema(name: str) -> dict:
    return create_app().openapi()["components"]["schemas"][name]


def _post_responses(path: str) -> dict:
    return create_app().openapi()["paths"][path]["post"]["responses"]


def _get_responses(path: str) -> dict:
    return create_app().openapi()["paths"][path]["get"]["responses"]


def test_openapi_api_error_response_uses_detail_wrapper() -> None:
    schema = _schema("ApiErrorResponse")

    assert "detail" in schema["properties"]
    assert schema["properties"]["detail"]["$ref"] == "#/components/schemas/ApiErrorDetail"


def test_openapi_instagram_rate_limit_error_includes_cooldown_seconds() -> None:
    schema = _schema("InstagramRateLimitErrorResponse")
    detail = _schema("InstagramRateLimitErrorDetail")

    assert schema["properties"]["detail"]["$ref"] == "#/components/schemas/InstagramRateLimitErrorDetail"
    assert "cooldown_seconds" in detail["properties"]
    assert detail["properties"]["code"]["const"] == "INSTAGRAM_RATE_LIMITED"
    assert detail["properties"]["retryable"]["const"] is True


def test_openapi_create_job_documents_instagram_rate_limit_response() -> None:
    responses = _post_responses("/api/v1/jobs")

    assert "429" in responses
    assert (
        responses["429"]["content"]["application/json"]["schema"]["$ref"]
        == "#/components/schemas/InstagramRateLimitErrorResponse"
    )


def test_openapi_job_result_documents_retryable_field() -> None:
    schema = _schema("JobResultResponse")

    assert "retryable" in schema["properties"]
    assert schema["properties"]["retryable"]["default"] is False
    assert "INSTAGRAM_RATE_LIMITED" in schema["properties"]["error_code"]["description"]
    assert "UNSUPPORTED_PLATFORM_URL" in schema["properties"]["error_code"]["description"]


def test_openapi_get_job_result_documents_conflict_and_not_found() -> None:
    responses = _get_responses("/api/v1/jobs/{jobId}/result")

    assert "404" in responses
    assert "409" in responses
    assert (
        responses["404"]["content"]["application/json"]["schema"]["$ref"]
        == "#/components/schemas/ApiErrorResponse"
    )
    assert (
        responses["409"]["content"]["application/json"]["schema"]["$ref"]
        == "#/components/schemas/ApiErrorResponse"
    )
