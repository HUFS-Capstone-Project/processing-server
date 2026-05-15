from app.domain.source_classifier import classify_source_url


def test_classify_source_url_keeps_existing_response_values() -> None:
    assert classify_source_url("https://www.instagram.com/reel/abc/") == "instagram"
    assert classify_source_url("https://example.com/post") == "web"

