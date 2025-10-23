import argparse
import logging
import re
import subprocess
import sys
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
) -> bool:
    """Populate a missing column by generating and persisting the value."""
    if getattr(video_row, column_name) is not None and not force_update:
        return False

    if video_row.video_id is None:
        raise ValueError("Cannot update database without a video_id")

    new_value = value_generator_func()
    if new_value is None and not allow_none:
        return False

    setattr(video_row, column_name, new_value)
    repository.update_video_field(
        video_row.video_id,
        column_name,
        new_value,
        json_type=json_type,
    )
    commit()
    return True


def update_video_metadata(
    video_row: ProcessVideo,
    repository: ProcessVideoRepository,
    downloader: YoutubeDownloader,
    commit: Callable[[], None],
) -> bool:
    updated = False
    if not video_row.video_url:
        return updated
    if video_row.video_meta_json is None:
        if update_field_if_missing(
            video_row,
            repository,
            "video_meta_json",
            lambda: downloader.extract_video_info(video_row.video_url),
            commit=commit,
        ):
            updated = True
    elif (
        not video_row.meta_updated_at or video_row.meta_updated_at.date() < date.today()
    ):
        logging.info("Starting video metadata update")
        if update_field_if_missing(
            video_row,
            repository,
            "video_meta_json",
            lambda: downloader.extract_video_info(video_row.video_url),
            commit=commit,
            force_update=True,
        ):
            updated = True
    return updated


def process_audio_and_transcription(
    video_row: ProcessVideo,
    repository: ProcessVideoRepository,
    downloader: YoutubeDownloader,
    transcriber: ParakeetTranscriber,
    sound_classifier: SoundClassifierClient,
    storage_client: Minio,
    commit: Callable[[], None],
) -> bool:
    updated = False
    if not video_row.video_url:
        return updated

    needs_transcription = video_row.transcribe_json is None
    needs_sound_classifier = video_row.sound_classifier_json is None
    needs_laugh_events = video_row.laugh_events_json is None

    if not (needs_transcription or needs_sound_classifier or needs_laugh_events):
        return updated

    audio_path_str: str | None = None
    if needs_transcription or needs_sound_classifier:
        audio_path = downloader.download_audio(
            storage_client, video_row.video_url, video_row.video_id
        )
        audio_path_str = str(audio_path)
        video_row.audio_path = audio_path_str

    if needs_transcription and audio_path_str:
        if update_field_if_missing(
            video_row,
            repository,
            "transcribe_json",
            lambda: transcriber.transcribe_audio(audio_path_str),
            commit=commit,
        ):
            updated = True

    if needs_sound_classifier and audio_path_str:
        if update_field_if_missing(
            video_row,
            repository,
            "sound_classifier_json",
            lambda: sound_classifier.classify_audio(audio_path_str),
            commit=commit,
        ):
            updated = True

    if needs_laugh_events:
        if update_field_if_missing(
            video_row,
            repository,
            "laugh_events_json",
            lambda: sound_classifier.build_laugh_events_payload(
                video_row.sound_classifier_json
            ),
            commit=commit,
        ):
            updated = True
    return updated


def run_llm_tasks(
    video_row: ProcessVideo,
    repository: ProcessVideoRepository,
    llm_client: GeminiClient,
    commit: Callable[[], None],
) -> bool:
    updated = False
    if not video_row.transcribe_json:
        return updated

    needs_llm = (
        video_row.llm_chapter_json is None or video_row.llm_classifier_json is None
    )
    if not needs_llm:
        return updated

    if update_field_if_missing(
        video_row,
        repository,
        "llm_chapter_json",
        lambda: request_llm_summary(video_row.transcribe_json, client=llm_client),
        allow_none=False,
        commit=commit,
    ):
        updated = True

    if not video_row.llm_chapter_json:
        return updated

    if update_field_if_missing(
        video_row,
        repository,
        "llm_classifier_json",
        lambda: request_llm_classification(
            video_row.llm_chapter_json, client=llm_client
        ),
        allow_none=False,
        commit=commit,
    ):
        updated = True
    return updated


def update_status(
    video_row: ProcessVideo,
    repository: ProcessVideoRepository,
    commit: Callable[[], None],
) -> bool:
    updated = False
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
    if completed and video_row.process_status != "finished":
        video_row.process_status = "finished"
        repository.update_video_field(
            video_row.video_id,
            "process_status",
            video_row.process_status,
            json_type=False,
        )
        commit()
        updated = True
    return updated


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

    test_command = ("uv", "run", "dbt", "test")

    try:
        test_result = subprocess.run(
            test_command,
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception as exc:
        logging.exception("Unexpected error when running dbt tests")
        raise RuntimeError("Failed to invoke dbt test") from exc

    if test_result.returncode == 0:
        logging.info("DBT tests completed successfully")
        return

    if test_result.stdout:
        pattern = re.compile(r"(Failure in test.*?(?:\n\s*\n|$))", re.DOTALL)
        error_blocks = pattern.findall(test_result.stdout)
        if error_blocks:
            for block in error_blocks:
                for line in block.strip().splitlines():
                    logging.error(line)
        else:
            logging.error("No failure blocks found in dbt test output")

    if test_result.stderr:
        logging.error("dbt test stderr:\n%s", test_result.stderr)

    sys.exit(1)


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

        if (
            video_from_db.process_status != "finished"
            or video_from_db.meta_updated_at.date() < date.today()
        ):
            logging.info("<<" + "-" * 40)
            logging.info("Starting processing for - %s", video_from_db.video_title)

        any_updates = False

        if update_video_metadata(video_from_db, repository, downloader, commit):
            any_updates = True

        if process_audio_and_transcription(
            video_from_db,
            repository,
            downloader,
            transcriber,
            sound_classifier_client,
            storage_client,
            commit,
        ):
            any_updates = True

        if run_llm_tasks(video_from_db, repository, llm_client, commit):
            any_updates = True

        if update_status(video_from_db, repository, commit):
            any_updates = True

        remove_audio_cache(settings=settings)

        if any_updates:
            return True

    except (DownloadError, ExtractorError) as exc:
        logging.debug("Skipping unavailable video %s: %s", video_row.video_id, exc)
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
    logging.info("=" * 42)
    playlist_info = downloader.extract_playlist_info(youtube_url)
    logging.info("Starting playlist processing - %s", playlist_info[0].playlist_title)

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
        logging.info(
            "Processed %s video(s) with changes; running dbt pipeline",
            processed_videos,
        )
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
