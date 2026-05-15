from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest

from app.core.config import Settings
from app.domain.job import (
    CrawlArtifact,
    ExtractedPlace,
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
                "keyword": candidate.query,
                "query": candidate.query,
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


class KeywordAwarePlaceSearchClient:
    def __init__(self, places_by_keyword: dict[str, list[PlaceCandidate]]) -> None:
        self.places_by_keyword = places_by_keyword
        self.calls: list[dict[str, object]] = []

    async def search_places(self, candidate, location_hints: list[str]) -> FakePlaceSearchResult:
        self.calls.append(
            {
                "keyword": candidate.query,
                "query": candidate.query,
                "location_hints": location_hints,
            }
        )
        return FakePlaceSearchResult(self.places_by_keyword.get(candidate.query, []))


class KeywordAndHintAwarePlaceSearchClient:
    def __init__(
        self,
        places_by_call: dict[tuple[str, tuple[str, ...]], list[PlaceCandidate]],
    ) -> None:
        self.places_by_call = places_by_call
        self.calls: list[dict[str, object]] = []

    async def search_places(self, candidate, location_hints: list[str]) -> FakePlaceSearchResult:
        self.calls.append(
            {
                "keyword": candidate.query,
                "query": candidate.query,
                "location_hints": location_hints,
            }
        )
        return FakePlaceSearchResult(
            self.places_by_call.get((candidate.query, tuple(location_hints)), [])
        )


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


def _place_candidate(
    *,
    confidence: float = 0.95,
    kakao_place_id: str = "123",
    place_name: str = "Common Mansion",
    query: str = "Common Mansion",
    address_name: str = "Seoul Jongno-gu Sinmunro 2-ga 1-102",
    road_address_name: str = "Seoul Jongno-gu Saemunan-ro 1",
) -> PlaceCandidate:
    return PlaceCandidate(
        kakao_place_id=kakao_place_id,
        place_name=place_name,
        category_name="Food > Cafe",
        category_group_code="CE7",
        category_group_name="Cafe",
        phone="02-0000-0000",
        address_name=address_name,
        road_address_name=road_address_name,
        x="126.970000",
        y="37.570000",
        place_url=f"https://place.map.kakao.com/{kakao_place_id}",
        confidence=confidence,
        query=query,
        evidence_text=f"{query} 1-102 Sinmunro 2-ga",
        original_text=query,
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
        "places": [
            {
                "store_name": "Common Mansion",
                "address": "1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
                "store_name_evidence": "Common Mansion",
                "address_evidence": "1-102 Sinmunro 2-ga, Jongno-gu, Seoul",
                "certainty": "high",
            }
        ],
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
    assert "selected_place" not in repo.saved_result
    assert repo.saved_result["resolved_places"][0]["confidence"] == 0.95


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_processor_falls_back_to_address_only_search(monkeypatch) -> None:
    job = _new_job()
    repo = FakeRepository(job)
    settings = Settings(kakao_min_place_confidence=0.7)
    address = "경북 경주시 내남면 포석로 110-32"
    extractor = FakeExtractionClient(
        ExtractionResult(
            store_name="수뢰뫼",
            address=address,
            store_name_evidence="수뢰뫼",
            address_evidence=f"📍위치 : {address}",
            certainty=ExtractionCertainty.HIGH,
        )
    )
    place = _place_candidate(
        confidence=0.8,
        kakao_place_id="456",
        place_name="수뢰뫼",
        query="수뢰뫼",
        address_name="경북 경주시 내남면 용장리 114-3",
        road_address_name=address,
    )
    place_search = KeywordAndHintAwarePlaceSearchClient(
        {
            (address, (address,)): [place],
        }
    )

    async def fake_crawl(url: str, _settings: Settings) -> CrawlArtifact:
        return CrawlArtifact(
            url=url,
            html=None,
            text=f"수뢰뫼\n📍위치 : {address}",
            media_type="reel",
            caption=f"수뢰뫼\n📍위치 : {address}",
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

    assert [
        {
            "keyword": call["keyword"],
            "location_hints": call["location_hints"],
        }
        for call in place_search.calls
    ] == [
        {"keyword": "수뢰뫼", "location_hints": [address]},
        {"keyword": "수뢰뫼", "location_hints": []},
        {"keyword": address, "location_hints": [address]},
    ]
    assert repo.saved_result is not None
    assert repo.saved_result["resolved_places"][0]["place_name"] == "수뢰뫼"
    assert repo.saved_result["resolved_places"][0]["query"] == "수뢰뫼"


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
            _place_candidate(confidence=0.75, kakao_place_id="122"),
            _place_candidate(confidence=0.95, kakao_place_id="123"),
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
            "query": "Common Mansion",
            "location_hints": ["1-102 Sinmunro 2-ga, Jongno-gu, Seoul"],
        }
    ]
    assert repo.succeeded is True
    assert repo.saved_result is not None
    assert len(repo.saved_result["place_candidates"]) == 2
    assert "selected_place" not in repo.saved_result
    assert repo.saved_result["resolved_places"][0]["confidence"] == 0.95
    assert repo.saved_result["resolved_places"][0]["kakao_place_id"] == "123"
    assert repo.failed is None


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_processor_enriches_multiple_places_from_extraction_result(monkeypatch) -> None:
    job = _new_job()
    repo = FakeRepository(job)
    settings = Settings()
    extracted_places = [
        ("플루밍", "서울 마포구 연남로13길 9 1층 101호"),
        ("누크녹", "서울 마포구 성미산로 190-31 2층"),
        ("예챠", "서울 마포구 망원로7길 31-18 1층 102호"),
        ("라뚜셩트", "서울 서초구 방배로25길 50 1층"),
        ("코이크", "서울 마포구 동교로39길 8 1-2층"),
        ("카페토요", "서울 영등포구 도림로 436-7 1층"),
    ]
    extractor = FakeExtractionClient(
        ExtractionResult(
            store_name="플루밍",
            address="서울 마포구 연남로13길 9 1층 101호",
            store_name_evidence="❶ 플루밍",
            address_evidence="📍서울 마포구 연남로13길 9 1층 101호",
            certainty=ExtractionCertainty.HIGH,
            places=[
                ExtractedPlace(
                    store_name=name,
                    address=address,
                    store_name_evidence=name,
                    address_evidence=address,
                    certainty=ExtractionCertainty.HIGH,
                )
                for name, address in extracted_places
            ],
        )
    )
    place_search = KeywordAwarePlaceSearchClient(
        {
            name: [
                _place_candidate(
                    kakao_place_id=str(index),
                    place_name=name,
                    query=name,
                    address_name=address,
                    road_address_name=address,
                )
            ]
            for index, (name, address) in enumerate(extracted_places, start=1)
        }
    )

    async def fake_crawl(url: str, _settings: Settings) -> CrawlArtifact:
        return CrawlArtifact(
            url=url,
            html=None,
            text="서울에서 만나는 비주얼 디저트 카페들",
            media_type="reel",
            caption="서울에서 만나는 비주얼 디저트 카페들",
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

    assert repo.succeeded is True
    assert repo.saved_result is not None
    assert [call["keyword"] for call in place_search.calls] == [
        name for name, _ in extracted_places
    ]
    assert len(repo.saved_result["place_candidates"]) == 6
    assert [place["place_name"] for place in repo.saved_result["resolved_places"]] == [
        name for name, _ in extracted_places
    ]
    assert "selected_place" not in repo.saved_result
    assert repo.saved_result["resolved_places"][0]["place_name"] == "플루밍"
    assert [
        place["store_name"]
        for place in repo.saved_result["extraction_result"]["places"]
    ] == [name for name, _ in extracted_places]
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
    assert "selected_place" not in repo.saved_result
    assert repo.saved_result["resolved_places"] == []
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
    assert "selected_place" not in repo.saved_result
    assert repo.saved_result["resolved_places"] == []
    assert repo.failed is None


@pytest.mark.skipif(not EVENT_LOOP_AVAILABLE, reason="Event loop creation is blocked in this environment")
def test_processor_succeeds_when_extraction_client_fails(monkeypatch) -> None:
    job = _new_job()
    repo = FakeRepository(job)
    settings = Settings(extraction_failure_retry_enabled=False)

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
