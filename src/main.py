import argparse
import logging
import subprocess
from pathlib import Path
from typing import Any, Callable

from minio import Minio
from pydantic import ValidationError
from yt_dlp.utils import DownloadError, ExtractorError

from config import Settings, VideoURLModel, get_settings
from database import ProcessVideoRepository, get_db_connection
from llm import GeminiClient, llm_classifier, llm_summary
from models import ProcessVideo
from sound_classifier import SoundClassifierClient
from transcribe import ParakeetTranscriber
from utils import remove_audio_cache
from youtube_downloader import ObjectStorageClient, YoutubeDownloader

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def ensure_and_update_db_field(
    video_row: ProcessVideo,
    repository: ProcessVideoRepository,
    column_name: str,
    value_generator_func: Callable[[], Any],
    *,
    json_type: bool = True,
    allow_none: bool = False,
    commit: Callable[[], None],
) -> None:
    """Populate a missing column by generating and persisting the value."""
    if getattr(video_row, column_name) is not None:
        return

    if video_row.video_id is None:
        raise ValueError("Cannot update database without a video_id")

    new_value = value_generator_func()
    if new_value is None and not allow_none:
        return

    setattr(video_row, column_name, new_value)
    repository.update_video_column(
        video_row.video_id,
        column_name,
        new_value,
        json_type=json_type,
    )
    commit()


def ensure_video_metadata(
    video_row: ProcessVideo,
    repository: ProcessVideoRepository,
    downloader: YoutubeDownloader,
    commit: Callable[[], None],
) -> None:
    if not video_row.video_url:
        return

    ensure_and_update_db_field(
        video_row,
        repository,
        "video_meta_json",
        lambda: downloader.extract_video_info(video_row.video_url),
        commit=commit,
    )


def ensure_audio_and_transcription(
    video_row: ProcessVideo,
    repository: ProcessVideoRepository,
    downloader: YoutubeDownloader,
    transcriber: ParakeetTranscriber,
    sound_classifier: SoundClassifierClient,
    storage_client: ObjectStorageClient,
    commit: Callable[[], None],
) -> None:
    if not video_row.video_url or not video_row.video_title:
        return

    needs_audio = (
        video_row.transcribe_json is None or video_row.sound_classifier_json is None
    )
    if not needs_audio:
        return

    audio_path = downloader.download_audio(
        storage_client, video_row.video_url, video_row.video_title
    )
    video_row.audio_path = str(audio_path)

    ensure_and_update_db_field(
        video_row,
        repository,
        "transcribe_json",
        lambda: transcriber.transcribe(str(audio_path)),
        commit=commit,
    )

    ensure_and_update_db_field(
        video_row,
        repository,
        "sound_classifier_json",
        lambda: sound_classifier.classify(str(audio_path)),
        commit=commit,
    )


def run_llm_tasks(
    video_row: ProcessVideo,
    repository: ProcessVideoRepository,
    llm_client: GeminiClient,
    commit: Callable[[], None],
) -> None:
    if not video_row.transcribe_json:
        return

    ensure_and_update_db_field(
        video_row,
        repository,
        "llm_chapter_json",
        lambda: llm_summary(video_row.transcribe_json, client=llm_client),
        allow_none=False,
        commit=commit,
    )

    if not video_row.llm_chapter_json:
        return

    ensure_and_update_db_field(
        video_row,
        repository,
        "llm_classifier_json",
        lambda: llm_classifier(video_row.llm_chapter_json, client=llm_client),
        allow_none=False,
        commit=commit,
    )


def update_status(
    video_row: ProcessVideo,
    repository: ProcessVideoRepository,
    commit: Callable[[], None],
) -> None:
    completed = all(
        [
            video_row.video_meta_json,
            video_row.transcribe_json,
            video_row.llm_chapter_json,
            video_row.sound_classifier_json,
        ]
    )
    if completed and video_row.video_id:
        video_row.process_status = "finished"
        repository.update_video_column(
            video_row.video_id,
            "process_status",
            video_row.process_status,
            json_type=False,
        )
        commit()


def trigger_dbt_pipeline_after_video(
    dbt_project_dir: Path | None,
    video: ProcessVideo,
) -> None:
    if dbt_project_dir is None:
        return

    video_identifier = video.video_id or video.video_title or "<unknown>"

    command = (
        "uv",
        "run",
        "dbt",
        "run",
        "--project-dir",
        str(dbt_project_dir),
    )

    try:
        subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception as exc:
        logging.error(
            "Unexpected error when running dbt for video %s: %s",
            video_identifier,
            exc,
        )
        return

    logging.info(
        "DBT pipeline completed successfully",
        video_identifier,
    )


def process_single_video(
    video_info: ProcessVideo,
    repository: ProcessVideoRepository,
    *,
    downloader: YoutubeDownloader,
    transcriber: ParakeetTranscriber,
    sound_classifier_client: SoundClassifierClient,
    llm_client: GeminiClient,
    storage_client: ObjectStorageClient,
    commit: Callable[[], None],
    settings: Settings,
) -> None:
    try:
        if video_info.video_id is None:
            return

        row_from_db = repository.fetch_pending_video(video_info.video_id)

        if not row_from_db:
            return

        logging.info("=" * 42)
        logging.info("Starting processing for - %s", video_info.video_title)

        ensure_video_metadata(row_from_db, repository, downloader, commit)
        ensure_audio_and_transcription(
            row_from_db,
            repository,
            downloader,
            transcriber,
            sound_classifier_client,
            storage_client,
            commit,
        )
        run_llm_tasks(row_from_db, repository, llm_client, commit)
        update_status(row_from_db, repository, commit)
        remove_audio_cache(settings=settings)

    except (DownloadError, ExtractorError) as exc:
        logging.warning("Skipping unavailable video %s: %s", video_info.video_id, exc)
    except Exception as exc:  # noqa: BLE001
        logging.error("Error processing video %s: %s", video_info.video_id, exc)
        raise


def process_playlist(
    youtube_url: str,
    repository: ProcessVideoRepository,
    *,
    downloader: YoutubeDownloader,
    transcriber: ParakeetTranscriber,
    sound_classifier_client: SoundClassifierClient,
    llm_client: GeminiClient,
    storage_client: ObjectStorageClient,
    commit: Callable[[], None],
    settings: Settings,
    dbt_project_dir: Path | None = None,
) -> None:
    playlist_info = downloader.extract_playlist_info(youtube_url)

    repository.insert_new_videos(playlist_info)
    commit()

    for video in playlist_info:
        process_single_video(
            video,
            repository,
            downloader=downloader,
            transcriber=transcriber,
            sound_classifier_client=sound_classifier_client,
            llm_client=llm_client,
            storage_client=storage_client,
            commit=commit,
            settings=settings,
        )
        trigger_dbt_pipeline_after_video(dbt_project_dir, video)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="URL link to a YouTube video or playlist"
    )
    parser.add_argument("argument", help="URL of the video or playlist")
    return parser.parse_args()


def main() -> None:
    """Main entry point for the video processing pipeline."""
    connection = None
    try:
        args = parse_args()
        youtube_url = VideoURLModel(url=args.argument)

        settings = get_settings()
        connection = get_db_connection(settings=settings)
        repository = ProcessVideoRepository(connection)

        downloader = YoutubeDownloader(settings=settings)
        transcriber = ParakeetTranscriber()
        sound_classifier_client = SoundClassifierClient(settings=settings)
        llm_client = GeminiClient()

        project_root = Path(__file__).resolve().parent.parent
        dbt_project_dir: Path | None = project_root / "standup_project"
        if not dbt_project_dir.exists():
            logging.warning(
                "dbt project directory %s not found. Skipping dbt integration.",
                dbt_project_dir,
            )
            dbt_project_dir = None

        minio_client = Minio(
            settings.MINIO_DOMAIN,
            access_key=settings.MINIO_ROOT_USER,
            secret_key=settings.MINIO_ROOT_PASSWORD,
            secure=False,
        )

        process_playlist(
            str(youtube_url.url),
            repository,
            downloader=downloader,
            transcriber=transcriber,
            sound_classifier_client=sound_classifier_client,
            llm_client=llm_client,
            storage_client=minio_client,
            commit=connection.commit,
            settings=settings,
            dbt_project_dir=dbt_project_dir,
        )

    except ValidationError as exc:
        logging.error("URL validation error: %s", exc)
    except KeyboardInterrupt:
        logging.warning("Operation interrupted by user (KeyboardInterrupt)")
    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    main()
