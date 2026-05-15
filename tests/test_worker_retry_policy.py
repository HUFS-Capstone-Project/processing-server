from app.core.config import Settings
from app.worker.runner import retry_delay_seconds


def test_retry_delay_uses_exponential_backoff_caps() -> None:
    settings = Settings(
        worker_retry_initial_delay=5,
        worker_retry_backoff_multiplier=2,
        worker_retry_max_delay=12,
    )

    assert retry_delay_seconds(settings, 1) == 5
    assert retry_delay_seconds(settings, 2) == 10
    assert retry_delay_seconds(settings, 3) == 12


def test_reliable_queue_key_names_are_namespace_based() -> None:
    settings = Settings(
        queue_namespace="queue:link-analysis",
        business_hours_queue_namespace="queue:business-hours",
    )

    assert settings.queue_ready_key == "queue:link-analysis:ready"
    assert settings.queue_processing_key == "queue:link-analysis:processing"
    assert settings.queue_delayed_key == "queue:link-analysis:delayed"
    assert settings.business_hours_queue_ready_key == "queue:business-hours:ready"
    assert settings.business_hours_queue_processing_key == "queue:business-hours:processing"
    assert settings.business_hours_queue_delayed_key == "queue:business-hours:delayed"

