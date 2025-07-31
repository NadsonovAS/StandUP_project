import json
import logging
import subprocess

from config import settings


def get_sound_classifier(audio_path):
    try:
        logging.info("Запуск детекции смеха")
        cmd = [
            "./src/sound_classifier",
            str(audio_path),
            str(settings.WINDOW_DURATION_SECONDS),
            str(settings.PREFERRED_TIMESCALE),
            str(settings.CONFIDENCE_THRESHOLD),
            str(settings.OVERLAP_FACTOR),
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
        )

        sound_classifier = json.loads(result.stdout)
        logging.info("Работа модуля детекции смеха завершена")
        return sound_classifier

    except Exception as e:
        logging.error(f"Ошибка при работе модуля детекции смеха: {e}")
