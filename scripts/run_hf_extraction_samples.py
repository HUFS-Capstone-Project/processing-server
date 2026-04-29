from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.core.config import Settings  # noqa: E402
from app.domain.job import as_extraction_result_dict  # noqa: E402
from app.infra.llm import HFExtractionClient  # noqa: E402

DEFAULT_INPUT_PATH = ROOT / "artifacts" / "hf_extraction_sample_inputs.json"
DEFAULT_OUTPUT_PATH = ROOT / "artifacts" / "hf_extraction_samples.json"


def _load_samples(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as file:
        raw = json.load(file)

    if not isinstance(raw, list):
        raise ValueError("Input JSON must be a list of captions or sample objects.")

    samples: list[dict[str, Any]] = []
    for index, item in enumerate(raw, start=1):
        if isinstance(item, str):
            samples.append({"id": index, "caption": item})
            continue
        if isinstance(item, dict) and isinstance(item.get("caption"), str):
            samples.append(
                {
                    "id": item.get("id", index),
                    "caption": item["caption"],
                    "source_url": item.get("source_url"),
                    "media_type": item.get("media_type", "reel"),
                }
            )
            continue
        raise ValueError(f"Sample #{index} must be a string or an object with a caption field.")
    return samples


async def _run_samples(input_path: Path, output_path: Path) -> None:
    settings = Settings()
    extractor = HFExtractionClient(settings)
    samples = _load_samples(input_path)
    results: list[dict[str, Any]] = []

    for sample in samples:
        sample_id = sample["id"]
        caption = sample["caption"]
        print(f"[{sample_id}] extracting...", flush=True)

        try:
            prediction = await extractor.extract(
                text=caption,
                source_url=sample.get("source_url") or f"https://www.instagram.com/reel/sample-{sample_id}/",
                media_type=sample.get("media_type") or "reel",
            )
            results.append(
                {
                    "id": sample_id,
                    "caption": caption,
                    "prediction": as_extraction_result_dict(prediction) if prediction else None,
                    "error": None,
                }
            )
        except Exception as exc:  # noqa: BLE001
            results.append(
                {
                    "id": sample_id,
                    "caption": caption,
                    "prediction": None,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False, indent=2)
        file.write("\n")
    print(f"Wrote {len(results)} results to {output_path}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run HF extraction against local caption samples.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()

    asyncio.run(_run_samples(args.input, args.output))


if __name__ == "__main__":
    main()
