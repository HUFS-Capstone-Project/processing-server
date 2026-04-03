from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

_env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(_env_path)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    base_url: str = "http://127.0.0.1:8000"
    cors_origins: str = "*"

    crawler_timeout: int = 30

    instagram_ua: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    instagram_locale: str = "ko-KR"
    instagram_navigation_timeout: int = 30
    instagram_og_wait_timeout_ms: int = 8000


@lru_cache
def get_settings() -> Settings:
    return Settings()
