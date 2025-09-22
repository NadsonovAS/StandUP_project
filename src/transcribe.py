from typing import Any, Callable, Dict, Iterable, Protocol

from parakeet_mlx import from_pretrained

from utils import try_except_with_log


class TranscriptionSentence(Protocol):
    text: str
    start: float
    end: float


def load_default_model() -> Any:
    """Default loader for the Parakeet transcription model."""
    return from_pretrained("mlx-community/parakeet-tdt-0.6b-v3")


def sentences_to_dict(sentences: Iterable[TranscriptionSentence]) -> Dict[int, Dict[str, Any]]:
    """Convert model sentences to a serializable dictionary."""
    result: Dict[int, Dict[str, Any]] = {}
    for index, sentence in enumerate(sentences):
        result[index] = {
            "text": sentence.text,
            "start": round(sentence.start, 2),
            "end": round(sentence.end, 2),
        }
    return result


class ParakeetTranscriber:
    """Facade for transcribing audio with dependency injection hooks."""

    def __init__(
        self,
        model_loader: Callable[[], Any] = load_default_model,
        *,
        chunk_duration: float = 60.0,
        overlap_duration: float = 15.0,
    ) -> None:
        self._model_loader = model_loader
        self._chunk_duration = chunk_duration
        self._overlap_duration = overlap_duration
        self._model: Any | None = None

    def _ensure_model(self) -> Any:
        if self._model is None:
            self._model = self._model_loader()
        return self._model

    @try_except_with_log("Starting audio transcription")
    def transcribe(self, audio_path: str) -> Dict[int, Dict[str, Any]]:
        model = self._ensure_model()
        result = model.transcribe(
            audio_path,
            chunk_duration=self._chunk_duration,
            overlap_duration=self._overlap_duration,
        )
        return sentences_to_dict(result.sentences)


def transcribe_audio(audio_path: str) -> Dict[int, Dict[str, Any]]:
    """Backward-compatible helper using the default transcriber."""
    return ParakeetTranscriber().transcribe(audio_path)
