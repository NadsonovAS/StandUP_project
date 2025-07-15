import os
from pathlib import Path

# Определение корневой директории проекта
PROJECT_ROOT = Path(__file__).parent.parent.resolve()

# Директории для каждого типа объекта
DATA_DIR = PROJECT_ROOT / "data"
AUDIO_DIR = DATA_DIR / "audio"
LLM_BLOCK_DIR = DATA_DIR / "llm_block_timestamp"
TRANSCRIPTS_DIR = DATA_DIR / "transcripts_with_timestamp"
LAUGHTER_DIR = DATA_DIR / "laughter_segmentation_json"

# Создание директорий, если они не существуют
for directory in [DATA_DIR, AUDIO_DIR, LLM_BLOCK_DIR, TRANSCRIPTS_DIR, LAUGHTER_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Загрузка ключа API из переменных окружения
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

if not GEMINI_API_KEY:
    print("Предупреждение: переменная окружения GEMINI_API_KEY не установлена.")

# Объявление моделей для LLM и транскрибации
WHISPER_MODEL = "mlx-community/whisper-large-v3-turbo"
GEMINI_MODEL = "gemini-2.5-flash"

# Настройки для yt-dlp
YDL_OPTS = {
    "format": "bestaudio/best",
    "postprocessors": [
        {
            "key": "FFmpegExtractAudio",
            "preferredcodec": "wav",
            "preferredquality": "0",
        }
    ],
    "postprocessor_args": [
        "-ar",
        "16000",
    ],
    "outtmpl": str(AUDIO_DIR / "%(title)s.%(ext)s"),
    "cookiesfrombrowser": ("safari", None, None, None),
    "quiet": True,
}

# Настройки детекции смеха
WINDOW_DURATION_SECONDS = "1"  # 0.5 - 15
PREFERRED_TIMESCALE = "600"  # WINDOW_DURATION_SECONDS / PREFERRED_TIMESCALE
CONFIDENCE_THRESHOLD = "0.5"  # 0 - 1
OVERLAP_FACTOR = "0.9"  # 0 - 1
