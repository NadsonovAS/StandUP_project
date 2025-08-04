from pathlib import Path

from pydantic import BaseModel, HttpUrl
from pydantic_settings import BaseSettings


class VideoURLModel(BaseModel):
    url: HttpUrl


class Settings(BaseSettings):
    # Project paths
    AUDIO_DIR: Path = Path("./data/audio")

    # PostgreSQL Configuration
    POSTGRES_DB: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_HOST: str
    POSTGRES_PORT: int

    # MinIO Configuration
    MINIO_ROOT_USER: str
    MINIO_ROOT_PASSWORD: str
    MINIO_DOMAIN: str
    MINIO_AUDIO_BUCKET: str = "standup-project"  # имя бакета в MinIO
    MINIO_AUDIO_PATH: str = "data/audio"  # префикс (папка) внутри бакета

    # API Keys
    GEMINI_API_KEY: str

    # Proxy URL
    PROXY_URL: str

    # LLM and Transcription Models
    WHISPER_MODEL: str = "mlx-community/whisper-large-v3-turbo"
    GEMINI_MODEL_FLASH: str = "gemini-2.5-flash"

    # MLX-Whisper params
    TRANSCRIBE_PARAMS: dict = {
        "temperature": 0,
        "compression_ratio_threshold": 2.0,
        "logprob_threshold": -0.5,
        "no_speech_threshold": 0.2,
        "condition_on_previous_text": False,
        "hallucination_silence_threshold": 2.0,
        "initial_prompt": "Стендап комедия и юмористическое шоу. Ведущие: комики, актеры. Разговорная речь с жаргонизмами, игрой слов, каламбурами. Реакция аудитории: смех, аплодисменты, возгласы.",
        "language": "ru",
    }

    # yt-dlp settings
    YDL_OPTS: dict = {
        "format": "m4a/bestaudio/best",
        "postprocessors": [{"key": "FFmpegExtractAudio", "preferredcodec": "m4a"}],
        "cookiesfrombrowser": ("safari", None, None, None),
        "quiet": True,
    }

    YDL_OPTS_PLAYLIST: dict = {
        "skip_download": True,
        "extract_flat": "in_playlist",
        "quiet": True,
    }

    # Sound analysis settings
    WINDOW_DURATION_SECONDS: str = "1"
    PREFERRED_TIMESCALE: str = "600"
    CONFIDENCE_THRESHOLD: str = "0.5"
    OVERLAP_FACTOR: str = "0.9"

    class Config:
        env_file = Path(__file__).parent.parent / ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


settings = Settings()
