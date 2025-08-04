import mlx_whisper

from config import settings
from utils import try_except_with_log


@try_except_with_log("Запуск транскрибации аудио")
def transcribe_audio(audio_path) -> str:
    """
    Транскрибирует аудиофайл с использованием mlx-whisper.
    """

    result = mlx_whisper.transcribe(
        str(audio_path),
        path_or_hf_repo=settings.WHISPER_MODEL,
        **settings.TRANSCRIBE_PARAMS,
    )

    # Фильтрация нужных ключей
    filter = {"id", "text", "start", "end"}
    filtered_result = [{k: d[k] for k in filter if k in d} for d in result["segments"]]

    return filtered_result
