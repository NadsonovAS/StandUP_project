import logging

import mlx_whisper

import config

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def transcribe_audio(audio_file_path) -> str | None:
    """
    Транскрибирует аудиофайл с использованием mlx-whisper.

    Args:
        audio_file_path: Путь к аудиофайлу.

    Returns:
        Транскрибированный текст или None в случае ошибки.
    """

    try:
        result = mlx_whisper.transcribe(
            str(audio_file_path), path_or_hf_repo=config.WHISPER_MODEL
        )

        filtered_result = {
            "text": result["text"],
            "segments": [
                {
                    "id": segment[0],
                    "start": segment[1],
                    "end": segment[2],
                    "text": segment[3],
                }
                for segment in result.get("segments", [])
            ],
        }

        if filtered_result and filtered_result.get("text"):
            logging.info("Транскрибация успешно завершена.")
            return filtered_result["text"]
        else:
            logging.warning("Транскрибация не успешна")
            return None
    except Exception as e:
        logging.error(f"Ошибка во время транскрибации {audio_file_path}: {e}")
        return None
