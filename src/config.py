from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class VideoURLModel(BaseModel):
    url: HttpUrl


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).parent.parent / ".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
    )

    # === Project paths ===
    DATA_DIR: Path = Path("./data")

    # === PostgreSQL Configuration ===
    POSTGRES_DB: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_HOST: str
    POSTGRES_PORT: int

    # === MinIO Configuration ===
    MINIO_ROOT_USER: str
    MINIO_ROOT_PASSWORD: str
    MINIO_DOMAIN: str
    MINIO_AUDIO_BUCKET: str = "standup-project"  # bucket name in MinIO
    MINIO_AUDIO_PATH: str = "data/audio"  # prefix (folder) inside the bucket

    # === yt-dlp settings ===
    YDL_DOWNLOAD_OPTS: dict = {
        "format": "bestaudio/best",
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "opus",
            }
        ],
        "postprocessor_args": [
            "-c:a",
            "libopus",  # Opus codec
            "-b:a",
            "16k",  # bitrate
        ],
        "cookiesfrombrowser": ("safari", None, None, None),
        "quiet": True,
    }

    YDL_PLAYLIST_OPTS: dict = {
        "skip_download": True,
        "extract_flat": "in_playlist",
        "cookiesfrombrowser": ("safari", None, None, None),
        "quiet": True,
    }

    # === Sound analysis settings ===
    WINDOW_DURATION_SECONDS: int = 1
    PREFERRED_TIMESCALE: int = 600
    CONFIDENCE_THRESHOLD: float = 0.5
    OVERLAP_FACTOR: float = 0.8

    # === Gemini Configuration ===
    GEMINI_MODEL: str = "gemini-2.5-pro"
    # GEMINI_MODEL: str = "gemini-2.5-flash"
    # GEMINI_MODEL: str = "gemini-flash-latest"


@lru_cache
def _load_settings() -> Settings:
    return Settings()


def get_settings(*, refresh: bool = False, **overrides) -> Settings:
    """Return application settings with optional runtime overrides."""
    if refresh:
        _load_settings.cache_clear()
    settings = _load_settings()
    if overrides:
        return settings.model_copy(update=overrides)
    return settings
