from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest

from app.core.config import Settings
from app.domain.job import (
    CrawlArtifact,
    ExtractionCertainty,
    ExtractionResult,
    JobRecord,
    JobStatus,
    PlaceCandidate,
)
from app.worker.processor import JobProcessor

if hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def _can_create_event_loop() -> bool:
    try:
        loop = asyncio.new_event_loop()
        loop.close()
        return True
    except OSError:
        return False


EVENT_LOOP_AVAILABLE = _can_create_event_loop()


def _run(coro):
    try:
        return asyncio.run(coro)
    except OSError as exc:
        pytest.skip(f"Event loop creation is blocked in this environment: {exc}")


class FakeRepository:
    def __init__(self, job: JobRecord) -> None:
        self._job = job
        self.saved_result: dict | None = None
        self.succeeded = False
        self.failed: str | None = None

    async def claim_job(self, job_id: UUID) -> JobRecord | None:
        if job_id != self._job.job_id:
            return None
        return self._job

    async def upsert_job_result(self, **kwargs):
        self.saved_result = kwargs
        return None

    async def mark_succeeded(self, job_id: UUID):
        self.succeeded = True
        return self._job

    async def mark_failed(self, job_id: UUID, error_message: str):
        self.failed = error_message
        return self._job


class FakeExtractionClient:
    def __init__(self, result: ExtractionResult | None) -> None:
        self.result = result
        self.calls: list[dict[str, object]] = []

    async def extract(
        self,
        *,
        text: str,
        source_url: str,
        media_type: str | None,
    ) -> ExtractionResult | None:
        self.calls.append(
            {
                "text": text,
                "source_url": source_url,
                "media_type": media_type,
            }
        )
        return self.result


class FailingExtractionClient:
    async def extract(
        self,
        *,
        text: str,
        source_url: str,
        media_type: str | None,
    ) -> ExtractionResult | None:
        raise RuntimeError("endpoint unavailable")


@dataclass
class FakePlaceSearchResult:
    places: list[PlaceCandidate]


class FakePlaceSearchClient:
    def __init__(self, places: list[PlaceCandidate]) -> None:
        self.places = places
        self.calls: list[dict[str, object]] = []

    async def search_places(self, candidate, location_hints: list[str]) -> FakePlaceSearchResult:
        self.calls.append(
            {
                "keyword": candidate.keyword,
                "source_keyword": candidate.source_keyword,
                "location_hints": location_hints,
            }
        )
        return FakePlaceSearchResult(self.places)


class HintAwarePlaceSearchClient:
    def __init__(self, places_by_hint: dict[tuple[str, ...], list[PlaceCandidate]]) -> None:
        self.places_by_hint = places_by_hint
        self.calls: list[list[str]] = []

    async def search_places(self, candidate, location_hints: list[str]) -> FakePlaceSearchResult:
        self.calls.append(location_hints)
        return FakePlaceSearchResult(self.places_by_hint.get(tuple(location_hints), []))


class FailingPlaceSearchClient:
    async def search_places(self, candidate, location_hints: list[str]) -> FakePlaceSearchResult:
        raise RuntimeError("kakao unavailable")


def _new_job() -> JobRecord:
    now = datetime.now(timezone.utc)
    return JobRecord(
        job_id=uuid4(),
        room_id=uuid4(),
        source_url="https://www.instagram.com/reel/example/",
        status=JobStatus.QUEUED,
        error_message=None,
        created_at=now,
        updated_at=now,
    )


def _place_candidate(*, confidence: float = 0.95) -> PlaceCandidate:
    return PlaceCandidate(
        kakao_place_id="123",
        place_name="Common Mansion",
        category_name="Food > Cafe",
        category_group_code="CE7",
        category_group_name="Cafe",
        phone="02-0000-0000",
        address_name="Seoul Jongno-gu Sinmunro 2-ga 1-102",
        road_address_name="Seoul Jongno-gu Saemunan-ro 1",
        x="126.970000",
        y="37.570000",
        place_url="https://place.map.kakao.com/123",
        confidence=confidence,
        source_keyword="Common Mansion",
        source_sentence="Common Mansion 1-102 Sinmunro 2-ga",
        raw_candidate="Common Mansion",
    )


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_processor_success(monkeypatch) -> None:
    job = _new_job()
    repo = FakeRepository(job)
    settings = Settings()

    async def fake_crawl(url: str, _settings: Settings) -> CrawlArtifact:
        return CrawlArtifact(
            url=url,
            html=None,
            text="#yeonnamcafe review",
            media_type="reel",
            caption="#yeonnamcafe review",
            instagram_meta={"caption": "#yeonnamcafe review"},
        )

    monkeypatch.setattr("app.worker.processor.crawl_and_parse", fake_crawl)

    processor = JobProcessor(
        repository=repo,
        settings=settings,
    )

    _run(processor.process_job(job.job_id))

    assert repo.succeeded is True
    assert repo.saved_result is not None
    assert repo.saved_result["caption"] == "#yeonnamcafe review"
    assert repo.saved_result["extraction_result"] is None
    assert repo.failed is None


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_processor_passes_caption_to_extraction_client(monkeypatch) -> None:
    job = _new_job()
    repo = FakeRepository(job)
    settings = Settings()
    extractor = FakeExtractionClient(
        ExtractionResult(
            store_name="Common Mansion",
            address="1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
            store_name_evidence="Common Mansion",
            address_evidence="1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
            certainty=ExtractionCertainty.HIGH,
        )
    )

    async def fake_crawl(url: str, _settings: Settings) -> CrawlArtifact:
        return CrawlArtifact(
            url=url,
            html=None,
            text="Common Mansion 1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
            media_type="reel",
            caption="Common Mansion 1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
            instagram_meta=None,
        )

    monkeypatch.setattr("app.worker.processor.crawl_and_parse", fake_crawl)

    processor = JobProcessor(
        repository=repo,
        settings=settings,
        extraction_client=extractor,
    )

    _run(processor.process_job(job.job_id))

    assert extractor.calls == [
        {
            "text": "Common Mansion 1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
            "source_url": job.source_url,
            "media_type": "reel",
        }
    ]
    assert repo.succeeded is True
    assert repo.saved_result is not None
    assert repo.saved_result["extraction_result"] == {
        "store_name": "Common Mansion",
        "address": "1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
        "store_name_evidence": "Common Mansion",
        "address_evidence": "1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
        "certainty": "high",
    }
    assert repo.failed is None


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_processor_tries_broader_location_hints_before_keyword_only(monkeypatch) -> None:
    job = _new_job()
    repo = FakeRepository(job)
    settings = Settings(kakao_min_place_confidence=0.7)
    extractor = FakeExtractionClient(
        ExtractionResult(
            store_name="Geumdonok",
            address="서울 서초구 방배로 23길 31-6",
            store_name_evidence="Geumdonok",
            address_evidence="서울 서초구 방배로 23길 31-6",
            certainty=ExtractionCertainty.HIGH,
        )
    )
    place = _place_candidate(confidence=0.95)
    place_search = HintAwarePlaceSearchClient(
        {
            ("서울 서초구",): [place],
            tuple(): [_place_candidate(confidence=0.85)],
        }
    )

    async def fake_crawl(url: str, _settings: Settings) -> CrawlArtifact:
        return CrawlArtifact(
            url=url,
            html=None,
            text="Geumdonok 서울 서초구 방배로 23길 31-6",
            media_type="reel",
            caption="Geumdonok 서울 서초구 방배로 23길 31-6",
            instagram_meta=None,
        )

    monkeypatch.setattr("app.worker.processor.crawl_and_parse", fake_crawl)

    processor = JobProcessor(
        repository=repo,
        settings=settings,
        extraction_client=extractor,
        place_search_client=place_search,
    )

    _run(processor.process_job(job.job_id))

    assert place_search.calls == [
        ["서울 서초구 방배로 23길 31-6"],
        ["서울 서초구"],
    ]
    assert repo.saved_result is not None
    assert repo.saved_result["selected_place"]["confidence"] == 0.95


def test_build_location_hints_from_korean_address() -> None:
    assert JobProcessor._build_location_hints("서울 서초구 방배로 23길 31-6") == [
        "서울 서초구 방배로 23길 31-6",
        "서울 서초구",
        "서울 서초구 방배로23길",
    ]
    assert JobProcessor._build_location_hints("서울 강남구 도곡동 954-17") == [
        "서울 강남구 도곡동 954-17",
        "서울 강남구",
        "서울 강남구 도곡동",
    ]


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_processor_enriches_place_from_extraction_result(monkeypatch) -> None:
    job = _new_job()
    repo = FakeRepository(job)
    settings = Settings()
    extractor = FakeExtractionClient(
        ExtractionResult(
            store_name="Common Mansion",
            address="1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
            store_name_evidence="Common Mansion",
            address_evidence="1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
            certainty=ExtractionCertainty.HIGH,
        )
    )
    place_search = FakePlaceSearchClient(
        [
            _place_candidate(confidence=0.75),
            _place_candidate(confidence=0.95),
        ]
    )

    async def fake_crawl(url: str, _settings: Settings) -> CrawlArtifact:
        return CrawlArtifact(
            url=url,
            html=None,
            text="Common Mansion 1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
            media_type="reel",
            caption="Common Mansion 1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
            instagram_meta=None,
        )

    monkeypatch.setattr("app.worker.processor.crawl_and_parse", fake_crawl)

    processor = JobProcessor(
        repository=repo,
        settings=settings,
        extraction_client=extractor,
        place_search_client=place_search,
    )

    _run(processor.process_job(job.job_id))

    assert place_search.calls == [
        {
            "keyword": "Common Mansion",
            "source_keyword": "Common Mansion",
            "location_hints": ["1-102 Sinmunro 2-ga, Jongno-gu, Seoul"],
        }
    ]
    assert repo.succeeded is True
    assert repo.saved_result is not None
    assert len(repo.saved_result["place_candidates"]) == 2
    assert repo.saved_result["selected_place"]["confidence"] == 0.95
    assert repo.saved_result["selected_place"]["kakao_place_id"] == "123"
    assert repo.failed is None


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_processor_succeeds_when_place_search_fails(monkeypatch) -> None:
    job = _new_job()
    repo = FakeRepository(job)
    settings = Settings()
    extractor = FakeExtractionClient(
        ExtractionResult(
            store_name="Common Mansion",
            address="1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
            store_name_evidence="Common Mansion",
            address_evidence="1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
            certainty=ExtractionCertainty.HIGH,
        )
    )

    async def fake_crawl(url: str, _settings: Settings) -> CrawlArtifact:
        return CrawlArtifact(
            url=url,
            html=None,
            text="Common Mansion 1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
            media_type="reel",
            caption="Common Mansion 1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
            instagram_meta=None,
        )

    monkeypatch.setattr("app.worker.processor.crawl_and_parse", fake_crawl)

    processor = JobProcessor(
        repository=repo,
        settings=settings,
        extraction_client=extractor,
        place_search_client=FailingPlaceSearchClient(),
    )

    _run(processor.process_job(job.job_id))

    assert repo.succeeded is True
    assert repo.saved_result is not None
    assert repo.saved_result["place_candidates"] == []
    assert repo.saved_result["selected_place"] is None
    assert repo.failed is None


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_processor_drops_low_confidence_place_candidates(monkeypatch) -> None:
    job = _new_job()
    repo = FakeRepository(job)
    settings = Settings(kakao_min_place_confidence=0.7)
    extractor = FakeExtractionClient(
        ExtractionResult(
            store_name="Common Mansion",
            address="1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
            store_name_evidence="Common Mansion",
            address_evidence="1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
            certainty=ExtractionCertainty.HIGH,
        )
    )

    async def fake_crawl(url: str, _settings: Settings) -> CrawlArtifact:
        return CrawlArtifact(
            url=url,
            html=None,
            text="Common Mansion 1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
            media_type="reel",
            caption="Common Mansion 1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
            instagram_meta=None,
        )

    monkeypatch.setattr("app.worker.processor.crawl_and_parse", fake_crawl)

    processor = JobProcessor(
        repository=repo,
        settings=settings,
        extraction_client=extractor,
        place_search_client=FakePlaceSearchClient([_place_candidate(confidence=0.55)]),
    )

    _run(processor.process_job(job.job_id))

    assert repo.succeeded is True
    assert repo.saved_result is not None
    assert repo.saved_result["place_candidates"] == []
    assert repo.saved_result["selected_place"] is None
    assert repo.failed is None


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_processor_succeeds_when_extraction_client_fails(monkeypatch) -> None:
    job = _new_job()
    repo = FakeRepository(job)
    settings = Settings()

    async def fake_crawl(url: str, _settings: Settings) -> CrawlArtifact:
        return CrawlArtifact(
            url=url,
            html=None,
            text="Common Mansion 1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
            media_type="reel",
            caption="Common Mansion 1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
            instagram_meta=None,
        )

    monkeypatch.setattr("app.worker.processor.crawl_and_parse", fake_crawl)

    processor = JobProcessor(
        repository=repo,
        settings=settings,
        extraction_client=FailingExtractionClient(),
    )

    _run(processor.process_job(job.job_id))

    assert repo.succeeded is True
    assert repo.saved_result is not None
    assert repo.saved_result["caption"] == "Common Mansion 1-102 Sinmunro 2-ga, Jongno-gu, Seoul"
    assert repo.saved_result["extraction_result"] is None
    assert repo.failed is None


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_processor_marks_failed_on_error(monkeypatch) -> None:
    job = _new_job()
    repo = FakeRepository(job)
    settings = Settings()

    async def fake_crawl(url: str, _settings: Settings) -> CrawlArtifact:
        raise RuntimeError("temporary timeout while crawling")

    monkeypatch.setattr("app.worker.processor.crawl_and_parse", fake_crawl)

    processor = JobProcessor(
        repository=repo,
        settings=settings,
    )

    _run(processor.process_job(job.job_id))

    assert repo.failed is not None
