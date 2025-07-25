import os
from pathlib import Path

# Определение корневой директории проекта
PROJECT_ROOT = Path(__file__).parent.parent.resolve()

# Директории для каждого типа объекта
DATA_DIR = PROJECT_ROOT / "data"
AUDIO_DIR = DATA_DIR / "audio"
LLM_BLOCK_DIR = DATA_DIR / "llm_block_timestamp"
TRANSCRIPTS_DIR = DATA_DIR / "transcripts_with_timestamp"
LAUGHTER_DIR = DATA_DIR / "laughter_segmentation"

# Создание директорий, если они не существуют
for directory in [DATA_DIR, AUDIO_DIR, LLM_BLOCK_DIR, TRANSCRIPTS_DIR, LAUGHTER_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Настройки для подключения к БД
CONN_PARAMS = {
    "dbname": "standup",
    "user": "standup",
    "password": "standup",
    "host": "localhost",
    "port": "5432",
}

# Загрузка ключей API из переменных окружения
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

if not GEMINI_API_KEY:
    print("Предупреждение: переменная окружения не установлена.")

# Объявление моделей для LLM и транскрибации
WHISPER_MODEL = "mlx-community/whisper-large-v3-turbo"
GEMINI_MODEL_FLASH = "gemini-2.5-flash"

# Настройки для yt-dlp
YDL_OPTS = {
    "format": "m4a/bestaudio/best",
    "postprocessors": [{"key": "FFmpegExtractAudio", "preferredcodec": "m4a"}],
    "outtmpl": str(AUDIO_DIR / "%(title)s.%(ext)s"),
    "cookiesfrombrowser": ("safari", None, None, None),
    "quiet": True,
}

YDL_OPTS_PLAYLIST = {
    "skip_download": True,
    "extract_flat": "in_playlist",  # Получить только базовую информацию о плейлисте
    "quiet": True,
}

# Настройки детекции смеха для sound analysis (SoundFileClassifier_app)
WINDOW_DURATION_SECONDS = "1"  # 0.5 - 15
PREFERRED_TIMESCALE = "600"  # WINDOW_DURATION_SECONDS / PREFERRED_TIMESCALE
CONFIDENCE_THRESHOLD = "0.5"  # 0 - 1
OVERLAP_FACTOR = "0.9"  # 0 - 0.99
