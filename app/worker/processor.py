from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from app.core.config import Settings
from app.domain.crawl import crawl_and_parse
from app.domain.job import (
    CrawlArtifact,
    ExtractedCandidate,
    ExtractionResult,
    JobRecord,
    PlaceCandidate,
    as_extraction_result_dict,
    as_place_dict,
)
from app.infra.kakao import KakaoNonRetryableError

logger = logging.getLogger("processing.worker.processor")


@dataclass(slots=True)
class JobProcessOutcome:
    processed: bool
    succeeded: bool
    timed_out: bool
    elapsed_ms: int


class JobRepositoryPort(Protocol):
    async def claim_job(self, job_id: UUID) -> JobRecord | None: ...

    async def upsert_job_result(self, **kwargs): ...

    async def mark_succeeded(self, job_id: UUID): ...

    async def mark_failed(self, job_id: UUID, error_message: str): ...


class ExtractionPort(Protocol):
    async def extract(
        self,
        *,
        text: str,
        source_url: str,
        media_type: str | None,
    ) -> ExtractionResult | None: ...


class PlaceSearchResultPort(Protocol):
    places: list[PlaceCandidate]


class PlaceSearchPort(Protocol):
    async def search_places(
        self,
        candidate: ExtractedCandidate,
        location_hints: list[str],
    ) -> PlaceSearchResultPort: ...


class JobProcessor:
    def __init__(
        self,
        *,
        repository: JobRepositoryPort,
        settings: Settings,
        extraction_client: ExtractionPort | None = None,
        place_search_client: PlaceSearchPort | None = None,
    ) -> None:
        self._repository = repository
        self._settings = settings
        self._extraction_client = extraction_client
        self._place_search_client = place_search_client

    async def process_job(self, job_id: UUID) -> JobProcessOutcome:
        started = time.monotonic()
        job = await self._repository.claim_job(job_id)
        if not job:
            logger.info("job skipped (not found or already claimed) job_id=%s", job_id)
            return JobProcessOutcome(
                processed=False,
                succeeded=False,
                timed_out=False,
                elapsed_ms=int((time.monotonic() - started) * 1000),
            )
        logger.info("job claimed job_id=%s source_url=%s", job.job_id, job.source_url)

        try:
            crawl_artifact = await crawl_and_parse(job.source_url, self._settings)
            extraction_result = await self._extract_result(job.source_url, crawl_artifact)
            place_candidates, selected_place = await self._enrich_place(
                extraction_result,
                crawl_artifact,
            )
            logger.info(
                "job crawl completed job_id=%s caption_len=%s place_candidates=%s",
                job.job_id,
                len(crawl_artifact.caption or ""),
                len(place_candidates),
            )

            await self._repository.upsert_job_result(
                job_id=job.job_id,
                caption=crawl_artifact.caption,
                instagram_meta=crawl_artifact.instagram_meta,
                extraction_result=(
                    as_extraction_result_dict(extraction_result) if extraction_result else None
                ),
                place_candidates=place_candidates,
                selected_place=selected_place,
            )
            await self._repository.mark_succeeded(job.job_id)
            elapsed_ms = int((time.monotonic() - started) * 1000)
            logger.info("job succeeded job_id=%s elapsed_ms=%s", job.job_id, elapsed_ms)
            return JobProcessOutcome(
                processed=True,
                succeeded=True,
                timed_out=False,
                elapsed_ms=elapsed_ms,
            )
        except Exception as exc:
            logger.exception("job processing failed job_id=%s", job_id)
            await self._repository.mark_failed(job_id, f"{exc.__class__.__name__}: {exc}")
            elapsed_ms = int((time.monotonic() - started) * 1000)
            timed_out = isinstance(exc, (asyncio.TimeoutError, TimeoutError)) or (
                exc.__class__.__name__ == "PlaywrightTimeoutError"
            )
            return JobProcessOutcome(
                processed=True,
                succeeded=False,
                timed_out=timed_out,
                elapsed_ms=elapsed_ms,
            )

    async def _extract_result(
        self,
        source_url: str,
        crawl_artifact: CrawlArtifact,
    ) -> ExtractionResult | None:
        if not self._extraction_client or not crawl_artifact.caption:
            return None

        try:
            return await self._extraction_client.extract(
                text=crawl_artifact.caption,
                source_url=source_url,
                media_type=crawl_artifact.media_type,
            )
        except Exception:
            logger.exception("extraction failed source_url=%s", source_url)
            return None

    async def _enrich_place(
        self,
        extraction_result: ExtractionResult | None,
        crawl_artifact: CrawlArtifact,
    ) -> tuple[list[dict[str, object]], dict[str, object] | None]:
        if not self._place_search_client or not extraction_result:
            return [], None

        store_name = (extraction_result.store_name or "").strip()
        if not store_name:
            return [], None

        candidate = ExtractedCandidate(
            keyword=store_name,
            source_keyword=store_name,
            source_sentence=(
                extraction_result.store_name_evidence
                or extraction_result.address_evidence
                or crawl_artifact.caption
                or ""
            ),
            raw_candidate=store_name,
        )
        location_hints = self._build_location_hints(extraction_result.address)

        try:
            places = await self._search_places_by_hints(candidate, location_hints)
        except KakaoNonRetryableError:
            logger.error("kakao enrichment non-retryable failure", exc_info=True)
            return [], None
        except Exception:
            logger.exception("kakao enrichment failed")
            return [], None

        places = sorted(places, key=lambda place: place.confidence, reverse=True)
        places = [
            place
            for place in places
            if place.confidence >= self._settings.kakao_min_place_confidence
        ]
        place_candidates = [as_place_dict(place) for place in places]
        selected_place = place_candidates[0] if place_candidates else None
        return place_candidates, selected_place

    async def _search_places(
        self,
        candidate: ExtractedCandidate,
        location_hints: list[str],
    ) -> list[PlaceCandidate]:
        if not self._place_search_client:
            return []
        result = await self._place_search_client.search_places(candidate, location_hints)
        return result.places

    async def _search_places_by_hints(
        self,
        candidate: ExtractedCandidate,
        location_hints: list[str],
    ) -> list[PlaceCandidate]:
        for hint in location_hints:
            places = await self._search_places(candidate, [hint])
            qualified = self._qualified_places(places)
            if qualified:
                return qualified

        places = await self._search_places(candidate, [])
        return self._qualified_places(places)

    def _qualified_places(self, places: list[PlaceCandidate]) -> list[PlaceCandidate]:
        return [
            place
            for place in places
            if place.confidence >= self._settings.kakao_min_place_confidence
        ]

    @staticmethod
    def _build_location_hints(address: str | None) -> list[str]:
        raw = (address or "").strip()
        if not raw:
            return []

        tokens = [token.strip(",") for token in re.split(r"\s+", raw) if token.strip(",")]
        hints = [raw]

        district_suffixes = ("\uad6c", "\uad70")
        locality_suffixes = ("\ub3d9", "\uc74d", "\uba74", "\ub9ac", "\uac00")

        district_idx = next(
            (idx for idx, token in enumerate(tokens) if token.endswith(district_suffixes)),
            None,
        )
        if district_idx is not None:
            hints.append(" ".join(tokens[: district_idx + 1]))

            locality_idx = next(
                (
                    idx
                    for idx in range(district_idx + 1, len(tokens))
                    if tokens[idx].endswith(locality_suffixes)
                ),
                None,
            )
            if locality_idx is not None:
                hints.append(" ".join(tokens[: locality_idx + 1]))

            road_hint = JobProcessor._build_road_hint(tokens, district_idx)
            if road_hint:
                hints.append(road_hint)

        deduped: list[str] = []
        for hint in hints:
            if hint and hint not in deduped:
                deduped.append(hint)
        return deduped

    @staticmethod
    def _build_road_hint(tokens: list[str], district_idx: int) -> str | None:
        prefix = tokens[: district_idx + 1]
        rest = tokens[district_idx + 1 :]
        if not prefix or not rest:
            return None

        for idx, token in enumerate(rest):
            if token.endswith("\uae38"):
                return " ".join(prefix + [token])
            if token.endswith("\ub85c"):
                if idx + 1 < len(rest) and re.fullmatch(r"\d+\uae38", rest[idx + 1]):
                    return " ".join(prefix + [f"{token}{rest[idx + 1]}"])
                return " ".join(prefix + [token])
        return None
