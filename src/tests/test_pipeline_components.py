import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, List

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1].parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from config import Settings
from llm import GeminiClient
from main import (
    _ensure_and_update_db_field,
    ensure_audio_and_transcription,
    run_llm_tasks,
)
from models import ProcessVideo
from sound_classifier import SoundClassifierClient
from transcribe import ParakeetTranscriber, sentences_to_dict
from utils import try_except_with_log
from youtube_downloader import build_audio_artifacts


class FakeRepository:
    def __init__(self) -> None:
        self.calls: List[tuple[str, str, Any, bool]] = []

    def update_video_column(
        self, video_id: str, column: str, value: Any, *, json_type: bool = False
    ) -> None:
        self.calls.append((video_id, column, value, json_type))


class FakeDownloader:
    def __init__(self, audio_path: Path) -> None:
        self.audio_path = audio_path
        self.calls: List[tuple[str, str]] = []

    def download_audio(self, storage_client: Any, video_url: str, video_title: str) -> Path:
        self.calls.append((video_url, video_title))
        return self.audio_path

    def extract_video_info(self, video_url: str) -> dict[str, int]:
        return {"duration": 10}

    def extract_playlist_info(self, youtube_url: str) -> list[ProcessVideo]:
        raise NotImplementedError


class FakeStorageClient:
    def stat_object(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover - noop
        raise Exception("not expected")

    def fget_object(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover - noop
        raise Exception("not expected")

    def fput_object(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover - noop
        pass


class FakeTranscriber:
    def __init__(self) -> None:
        self.calls: List[str] = []

    def transcribe(self, audio_path: str) -> dict[int, dict[str, Any]]:
        self.calls.append(audio_path)
        return {0: {"text": "line", "start": 0.0, "end": 1.0}}


class FakeSoundClassifier:
    def __init__(self) -> None:
        self.calls: List[str] = []

    def classify(self, audio_path: str) -> dict[str, Any]:
        self.calls.append(audio_path)
        return {"laughs": []}


class FakeGeminiClient:
    def __init__(self) -> None:
        self.prompts: List[str] = []

    def request(self, prompt: str) -> dict[str, Any]:
        self.prompts.append(prompt)
        if len(self.prompts) == 1:
            return {"chapters": [{"id": 0, "theme": "Test", "summary": "Text"}]}
        return {
            "classifications": [
                {"id": 0, "main_category": "Test", "subcategory": "Test", "reason": "r"}
            ]
        }


@pytest.fixture
def repository() -> FakeRepository:
    return FakeRepository()


@pytest.fixture
def commit_tracker() -> tuple[Callable[[], None], dict[str, int]]:
    counter = {"value": 0}

    def commit() -> None:
        counter["value"] += 1

    return commit, counter


def test_try_except_with_log_reraises_errors() -> None:
    call_count = {"count": 0}

    @try_except_with_log()
    def will_fail() -> None:
        call_count["count"] += 1
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        will_fail()
    assert call_count["count"] == 1


def test_ensure_and_update_db_field_updates_missing_value(
    repository: FakeRepository, commit_tracker: tuple[Callable[[], None], dict[str, int]]
) -> None:
    commit, counter = commit_tracker
    video = ProcessVideo(video_id="vid", video_url="http://example", video_meta_json=None)

    _ensure_and_update_db_field(
        video,
        repository,
        "video_meta_json",
        lambda: {"duration": 10},
        commit=commit,
    )

    assert video.video_meta_json == {"duration": 10}
    assert repository.calls[0] == ("vid", "video_meta_json", {"duration": 10}, True)
    assert counter["value"] == 1


def test_ensure_and_update_db_field_skips_none_value(
    repository: FakeRepository, commit_tracker: tuple[Callable[[], None], dict[str, int]]
) -> None:
    commit, counter = commit_tracker
    video = ProcessVideo(video_id="vid", video_url="http://example", video_meta_json=None)

    _ensure_and_update_db_field(
        video,
        repository,
        "video_meta_json",
        lambda: None,
        allow_none=False,
        commit=commit,
    )

    assert video.video_meta_json is None
    assert repository.calls == []
    assert counter["value"] == 0


def test_ensure_audio_and_transcription_uses_dependencies(
    repository: FakeRepository, commit_tracker: tuple[Callable[[], None], dict[str, int]]
) -> None:
    commit, counter = commit_tracker
    video = ProcessVideo(
        video_id="vid",
        video_url="http://example",
        video_title="title",
        transcribe_json=None,
        sound_classifier_json=None,
    )
    downloader = FakeDownloader(Path("/tmp/audio.opus"))
    transcriber = FakeTranscriber()
    sound_classifier = FakeSoundClassifier()

    ensure_audio_and_transcription(
        video,
        repository,
        downloader,
        transcriber,
        sound_classifier,
        FakeStorageClient(),
        commit,
    )

    assert video.audio_path == str(Path("/tmp/audio.opus"))
    assert len(repository.calls) == 2
    assert counter["value"] == 2
    assert transcriber.calls == [str(Path("/tmp/audio.opus"))]
    assert sound_classifier.calls == [str(Path("/tmp/audio.opus"))]


def test_run_llm_tasks_updates_fields(
    repository: FakeRepository, commit_tracker: tuple[Callable[[], None], dict[str, int]]
) -> None:
    commit, counter = commit_tracker
    video = ProcessVideo(
        video_id="vid",
        transcribe_json={"0": {"text": "hello"}},
        llm_chapter_json=None,
        llm_classifier_json=None,
    )
    client = FakeGeminiClient()

    run_llm_tasks(video, repository, client, commit)

    assert len(repository.calls) == 2
    assert counter["value"] == 2
    assert video.llm_chapter_json["chapters"][0]["end_id"] == 0
    assert video.llm_classifier_json["classifications"][0]["id"] == 0


def test_sound_classifier_client_allows_fake_runner() -> None:
    completed = subprocess.CompletedProcess(
        args=["sound"], returncode=0, stdout='{"laughs": []}', stderr=""
    )
    client = SoundClassifierClient(
        settings=Settings(
            POSTGRES_DB="db",
            POSTGRES_USER="user",
            POSTGRES_PASSWORD="pass",
            POSTGRES_HOST="host",
            POSTGRES_PORT=5432,
            MINIO_ROOT_USER="root",
            MINIO_ROOT_PASSWORD="root",
            MINIO_DOMAIN="domain",
        ),
        runner=lambda command: completed,
    )

    result = client.classify("/tmp/audio.opus")
    assert result == {"laughs": []}


def test_parakeet_transcriber_uses_custom_loader() -> None:
    class FakeSentence:
        def __init__(self) -> None:
            self.text = "hello"
            self.start = 0.5
            self.end = 1.5

    class FakeModel:
        def __init__(self) -> None:
            self.calls: List[str] = []

        def transcribe(self, audio_path: str, *, chunk_duration: float, overlap_duration: float):
            self.calls.append(audio_path)
            return type("Result", (), {"sentences": [FakeSentence()]})()

    loader_calls: List[int] = []

    def loader() -> FakeModel:
        loader_calls.append(1)
        return FakeModel()

    transcriber = ParakeetTranscriber(model_loader=loader)
    result = transcriber.transcribe("/tmp/audio.opus")
    assert result[0]["text"] == "hello"
    assert loader_calls.count(1) == 1


def test_sentences_to_dict_supports_sequence() -> None:
    class FakeSentence:
        def __init__(self, text: str, start: float, end: float) -> None:
            self.text = text
            self.start = start
            self.end = end

    sentences = [FakeSentence("a", 0.0, 1.0), FakeSentence("b", 1.0, 2.0)]
    result = sentences_to_dict(sentences)
    assert result[0]["text"] == "a"
    assert result[1]["end"] == 2.0


def test_build_audio_artifacts_uses_settings_paths() -> None:
    settings = Settings(
        POSTGRES_DB="db",
        POSTGRES_USER="user",
        POSTGRES_PASSWORD="pass",
        POSTGRES_HOST="host",
        POSTGRES_PORT=5432,
        MINIO_ROOT_USER="root",
        MINIO_ROOT_PASSWORD="root",
        MINIO_DOMAIN="domain",
        MINIO_AUDIO_BUCKET="bucket",
        MINIO_AUDIO_PATH="prefix",
        DATA_DIR=Path("/tmp"),
    )

    local_path, object_name, template = build_audio_artifacts("video", settings)
    assert local_path == Path("/tmp/video.opus")
    assert object_name == "prefix/video.opus"
    assert template == str(Path("/tmp/video"))


def test_gemini_client_accepts_fake_runner() -> None:
    responses = [
        subprocess.CompletedProcess(
            args=["gemini"],
            returncode=0,
            stdout='```json\n{"chapters": []}\n```',
            stderr="",
        )
    ]

    def runner(command: list[str]) -> subprocess.CompletedProcess[str]:
        return responses.pop(0)

    client = GeminiClient(run_command=runner)
    result = client.request("prompt")
    assert result == {"chapters": []}
