import json
import subprocess
from typing import Any, Callable, Sequence

from config import Settings, get_settings
from utils import try_except_with_log


def build_classifier_command(audio_path: str, settings: Settings) -> list[str]:
    return [
        "./src/sound_classifier",
        str(audio_path),
        str(settings.WINDOW_DURATION_SECONDS),
        str(settings.PREFERRED_TIMESCALE),
        str(settings.CONFIDENCE_THRESHOLD),
        str(settings.OVERLAP_FACTOR),
    ]


def run_command_default(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=True,
    )


class SoundClassifierClient:
    """Encapsulates invocation of the Swift laughter detector binary."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        runner: Callable[
            [Sequence[str]], subprocess.CompletedProcess[str]
        ] = run_command_default,
        command_builder: Callable[
            [str, Settings], list[str]
        ] = build_classifier_command,
    ) -> None:
        self._settings = settings or get_settings()
        self._runner = runner
        self._command_builder = command_builder

    @try_except_with_log("Starting laughter detection")
    def classify_audio(self, audio_path: str) -> Any:
        command = self._command_builder(audio_path, self._settings)
        completed_process = self._runner(command)
        return json.loads(completed_process.stdout)
