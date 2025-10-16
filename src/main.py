import argparse
import logging
import subprocess

# import time
from datetime import date
from typing import Any, Callable

from minio import Minio
from pydantic import ValidationError
from yt_dlp.utils import DownloadError, ExtractorError

from config import Settings, VideoURLModel, get_settings
from database import ProcessVideoRepository, get_db_connection
from llm import GeminiClient, request_llm_classification, request_llm_summary
from models import ProcessVideo
from sound_classifier import SoundClassifierClient
from transcribe import ParakeetTranscriber
from utils import remove_audio_cache
from youtube_downloader import YoutubeDownloader

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def update_field_if_missing(
    video_row: ProcessVideo,
    repository: ProcessVideoRepository,
    column_name: str,
    value_generator_func: Callable[[], Any],
    *,
    json_type: bool = True,
    allow_none: bool = False,
    commit: Callable[[], None],
    force_update: bool = False,
) -> None:
    """Populate a missing column by generating and persisting the value."""
    if getattr(video_row, column_name) is not None and not force_update:
        return

    if video_row.video_id is None:
        raise ValueError("Cannot update database without a video_id")

    new_value = value_generator_func()
    if new_value is None and not allow_none:
        return

    setattr(video_row, column_name, new_value)
    repository.update_video_field(
        video_row.video_id,
        column_name,
        new_value,
        json_type=json_type,
    )
    commit()


def update_video_metadata(
    video_row: ProcessVideo,
    repository: ProcessVideoRepository,
    downloader: YoutubeDownloader,
    commit: Callable[[], None],
) -> None:
    if not video_row.video_url:
        return
    if video_row.video_meta_json is None:
        logging.info(
            "Starting video metadata download",
        )
        update_field_if_missing(
            video_row,
            repository,
            "video_meta_json",
            lambda: downloader.extract_video_info(video_row.video_url),
            commit=commit,
        )
    elif (
        not video_row.meta_updated_at or video_row.meta_updated_at.date() < date.today()
    ):
        logging.info(
            "Starting video metadata update",
        )
        update_field_if_missing(
            video_row,
            repository,
            "video_meta_json",
            lambda: downloader.extract_video_info(video_row.video_url),
            commit=commit,
            force_update=True,
        )
        # time.sleep(1)


def process_audio_and_transcription(
    video_row: ProcessVideo,
    repository: ProcessVideoRepository,
    downloader: YoutubeDownloader,
    transcriber: ParakeetTranscriber,
    sound_classifier: SoundClassifierClient,
    storage_client: Minio,
    commit: Callable[[], None],
) -> None:
    if not video_row.video_url:
        return

    needs_transcription = video_row.transcribe_json is None
    needs_sound_classifier = video_row.sound_classifier_json is None
    needs_laugh_events = video_row.laugh_events_json is None

    if not (needs_transcription or needs_sound_classifier or needs_laugh_events):
        return

    audio_path_str: str | None = None
    if needs_transcription or needs_sound_classifier:
        audio_path = downloader.download_audio(
            storage_client, video_row.video_url, video_row.video_id
        )
        audio_path_str = str(audio_path)
        video_row.audio_path = audio_path_str

    if needs_transcription and audio_path_str:
        update_field_if_missing(
            video_row,
            repository,
            "transcribe_json",
            lambda: transcriber.transcribe_audio(audio_path_str),
            commit=commit,
        )

    if needs_sound_classifier and audio_path_str:
        update_field_if_missing(
            video_row,
            repository,
            "sound_classifier_json",
            lambda: sound_classifier.classify_audio(audio_path_str),
            commit=commit,
        )

    if needs_laugh_events:
        update_field_if_missing(
            video_row,
            repository,
            "laugh_events_json",
            lambda: sound_classifier.build_laugh_events_payload(
                video_row.sound_classifier_json
            ),
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

    needs_llm = (
        video_row.llm_chapter_json is None or video_row.llm_classifier_json is None
    )
    if not needs_llm:
        return

    update_field_if_missing(
        video_row,
        repository,
        "llm_chapter_json",
        lambda: request_llm_summary(video_row.transcribe_json, client=llm_client),
        allow_none=False,
        commit=commit,
    )

    if not video_row.llm_chapter_json:
        return

    update_field_if_missing(
        video_row,
        repository,
        "llm_classifier_json",
        lambda: request_llm_classification(
            video_row.llm_chapter_json, client=llm_client
        ),
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
            video_row.llm_classifier_json,
            video_row.sound_classifier_json,
            video_row.laugh_events_json,
        ]
    )
    if completed:
        video_row.process_status = "finished"
        repository.update_video_field(
            video_row.video_id,
            "process_status",
            video_row.process_status,
            json_type=False,
        )
        commit()


def run_dbt_pipeline() -> None:
    """Run the dbt pipeline."""

    command = ("uv", "run", "dbt", "run")

    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception as exc:
        logging.exception("Unexpected error when running dbt")
        raise RuntimeError("Failed to invoke dbt run") from exc

    if result.returncode != 0:
        logging.error("DBT pipeline failed with return code %s", result.returncode)
        if result.stdout:
            logging.error("dbt stdout:\n%s", result.stdout)
        if result.stderr:
            logging.error("dbt stderr:\n%s", result.stderr)
        raise RuntimeError("dbt run exited with a non-zero status")

    logging.info("DBT pipeline completed successfully")


def process_single_video(
    video_row: ProcessVideo,
    repository: ProcessVideoRepository,
    *,
    downloader: YoutubeDownloader,
    transcriber: ParakeetTranscriber,
    sound_classifier_client: SoundClassifierClient,
    llm_client: GeminiClient,
    storage_client: Minio,
    commit: Callable[[], None],
    settings: Settings,
) -> bool:
    """Process a single video; return True when work was performed."""
    try:
        if video_row.video_id is None:
            return False

        video_from_db = repository.get_video_by_id(video_row.video_id)
        if not video_from_db:
            return False

        logging.info("=" * 42)
        logging.info("Starting processing for - %s", video_row.video_title)

        update_video_metadata(video_from_db, repository, downloader, commit)
        process_audio_and_transcription(
            video_from_db,
            repository,
            downloader,
            transcriber,
            sound_classifier_client,
            storage_client,
            commit,
        )
        run_llm_tasks(video_from_db, repository, llm_client, commit)
        update_status(video_from_db, repository, commit)
        remove_audio_cache(settings=settings)

        return True

    except (DownloadError, ExtractorError) as exc:
        logging.warning("Skipping unavailable video %s: %s", video_row.video_id, exc)
        return False
    except Exception as exc:  # noqa: BLE001
        logging.error("Error processing video %s: %s", video_row.video_id, exc)
        raise
    return False


def process_playlist(
    youtube_url: str,
    repository: ProcessVideoRepository,
    *,
    downloader: YoutubeDownloader,
    transcriber: ParakeetTranscriber,
    sound_classifier_client: SoundClassifierClient,
    llm_client: GeminiClient,
    storage_client: Minio,
    commit: Callable[[], None],
    settings: Settings,
) -> None:
    playlist_info = downloader.extract_playlist_info(youtube_url)

    repository.create_videos(playlist_info)
    commit()

    processed_videos = 0
    for video in playlist_info:
        processed = process_single_video(
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
        if processed:
            processed_videos += 1

    if processed_videos:
        run_dbt_pipeline()


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Process a new YouTube playlist or resume unfinished videos from the"
            " database"
        )
    )
    parser.add_argument(
        "--new_playlist",
        dest="new_playlist",
        help="URL of the new playlist to ingest",
    )
    return parser.parse_args()


def main() -> None:
    """Main entry point for the video processing pipeline."""
    connection = None
    try:
        args = parse_args()
        settings = get_settings()
        connection = get_db_connection(settings=settings)
        repository = ProcessVideoRepository(connection)

        downloader = YoutubeDownloader(settings=settings)
        transcriber = ParakeetTranscriber()
        sound_classifier_client = SoundClassifierClient(settings=settings)
        llm_client = GeminiClient()

        minio_client = Minio(
            settings.MINIO_DOMAIN,
            access_key=settings.MINIO_ROOT_USER,
            secret_key=settings.MINIO_ROOT_PASSWORD,
            secure=False,
        )

        if args.new_playlist:
            youtube_url = VideoURLModel(url=args.new_playlist)
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
            )
            return

        pending_playlists = repository.get_playlist_ids()
        for playlist in pending_playlists:
            pending_playlists_url = (
                "https://www.youtube.com/playlist?list=" + playlist.playlist_id
            )
            youtube_url = VideoURLModel(url=pending_playlists_url)
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
