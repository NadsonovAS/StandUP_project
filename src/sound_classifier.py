import json
import subprocess

from config import settings
from utils import try_except_with_log


@try_except_with_log("Запуск детекции смеха")
def get_sound_classifier(audio_path):
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

    return sound_classifier
