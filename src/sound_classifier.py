import json
import subprocess
from typing import Any

from config import settings
from utils import try_except_with_log


def build_command(audio_path: str) -> list[str]:
    return [
        "./src/sound_classifier",
        str(audio_path),
        str(settings.WINDOW_DURATION_SECONDS),
        str(settings.PREFERRED_TIMESCALE),
        str(settings.CONFIDENCE_THRESHOLD),
        str(settings.OVERLAP_FACTOR),
    ]


def run_command(cmd: list[str]) -> str:
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


@try_except_with_log("Starting laughter detection")
def get_sound_classifier(audio_path: str) -> Any:
    cmd = build_command(audio_path)
    output = run_command(cmd)
    sound_classifier = json.loads(output)
    return sound_classifier
