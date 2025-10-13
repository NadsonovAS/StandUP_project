from typing import Any, Dict

from parakeet_mlx import from_pretrained

from utils import try_except_with_log


class ParakeetTranscriber:
    """Facade for transcribing audio with lazy model loading."""

    def __init__(
        self,
        model_loader=lambda: from_pretrained("mlx-community/parakeet-tdt-0.6b-v3"),
        *,
        chunk_duration: float = 60.0,
        overlap_duration: float = 15.0,
    ) -> None:
        self._model_loader = model_loader
        self._chunk_duration = chunk_duration
        self._overlap_duration = overlap_duration
        self._model: Any | None = None

    def load_model_if_needed(self) -> Any:
        if self._model is None:
            self._model = self._model_loader()
        return self._model

    @try_except_with_log("Starting audio transcription")
    def transcribe_audio(self, audio_path: str) -> Dict[int, Dict[str, Any]]:
        model = self.load_model_if_needed()
        result = model.transcribe(
            audio_path,
            chunk_duration=self._chunk_duration,
            overlap_duration=self._overlap_duration,
        )
        return {
            i: {"text": s.text, "start": round(s.start, 2), "end": round(s.end, 2)}
            for i, s in enumerate(result.sentences)
        }
