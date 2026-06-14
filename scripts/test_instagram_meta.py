from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception:
    pass

from app.core.config import Settings
from app.services.crawler.instagram_http_meta import (
    fetch_instagram_http_meta,
    public_debug_dict,
)


async def _main() -> None:
    parser = argparse.ArgumentParser(description="Test Instagram HTTP OG/meta extraction.")
    parser.add_argument("url", help="Instagram media URL")
    args = parser.parse_args()

    result = await fetch_instagram_http_meta(args.url, Settings())
    print(json.dumps(public_debug_dict(result), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_main())
