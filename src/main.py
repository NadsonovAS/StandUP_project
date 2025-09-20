import argparse
import logging
from typing import Any, Callable

from minio import Minio
from pydantic import ValidationError

import config
import database
import llm
import sound_classifier
import transcribe
import utils
import youtube_downloader
from config import settings

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def _ensure_and_update_db_field(
    video_row: Any,
    conn,
    cursor,
    column_name: str,
    value_generator_func: Callable[[], Any],
    json_type: bool = True,
) -> None:
    """
    Ensures that a given column in the video_row is populated.
    If the column is None, generate the value, update the object,
    and persist it to the database.
    """
    if getattr(video_row, column_name) is not None:
        return

    new_value = value_generator_func()
    setattr(video_row, column_name, new_value)

    database.obj_to_db(
        conn,
        cursor,
        column=column_name,
        value=new_value,
        where=video_row.video_id,
        json_type=json_type,
    )


def ensure_video_metadata(video_row: Any, conn, cursor) -> None:
    """Ensure the video metadata is downloaded and stored in the database."""
    _ensure_and_update_db_field(
        video_row,
        conn,
        cursor,
        "video_meta_json",
        lambda: youtube_downloader.yt_video_extract_info(video_row.video_url),
    )


def ensure_audio_and_transcription(
    video_row: Any, conn, cursor, minio_client: Minio
) -> None:
    """Download audio, run transcription, and classify sound."""
    if video_row.transcribe_json is None or video_row.sound_classifier_json is None:
        video_row.audio_path = youtube_downloader.download_audio(
            minio_client, video_row.video_url, video_row.video_title
        )

        _ensure_and_update_db_field(
            video_row,
            conn,
            cursor,
            "transcribe_json",
            lambda: transcribe.transcribe_audio(video_row.audio_path),
        )

        _ensure_and_update_db_field(
            video_row,
            conn,
            cursor,
            "sound_classifier_json",
            lambda: sound_classifier.get_sound_classifier(video_row.audio_path),
        )


def run_llm_tasks(video_row: Any, conn, cursor) -> None:
    """Execute LLM tasks such as summarization and classification."""
    if video_row.transcribe_json is not None:
        _ensure_and_update_db_field(
            video_row,
            conn,
            cursor,
            "llm_chapter_json",
            lambda: llm.llm_summary(video_row.transcribe_json),
        )

        _ensure_and_update_db_field(
            video_row,
            conn,
            cursor,
            "llm_classifier_json",
            lambda: llm.llm_classifier(video_row.llm_chapter_json),
        )


def update_status(video_row: Any, conn, cursor) -> None:
    """Update the processing status of the video if all tasks are completed."""
    if all(
        [
            video_row.video_meta_json,
            video_row.transcribe_json,
            video_row.llm_chapter_json,
            video_row.sound_classifier_json,
        ]
    ):
        video_row.process_status = "finished"
        database.obj_to_db(
            conn,
            cursor,
            column="process_status",
            value=video_row.process_status,
            where=video_row.video_id,
        )


def process_single_video(video_info: Any, conn, cursor, minio_client: Minio) -> None:
    """Process a single video through the full pipeline."""
    try:
        row_from_db = database.get_row_from_db(video_info, cursor)

        if row_from_db:
            logging.info("=" * 42)
            logging.info("Starting processing for - %s", video_info.video_title)

            ensure_video_metadata(row_from_db, conn, cursor)
            ensure_audio_and_transcription(row_from_db, conn, cursor, minio_client)
            run_llm_tasks(row_from_db, conn, cursor)
            update_status(row_from_db, conn, cursor)
            utils.remove_audio_cache()

    except Exception as e:
        logging.error("Error processing video %s: %s", video_info.video_id, e)


def process_playlist(youtube_url: str, conn, cursor, minio_client: Minio) -> None:
    """
    Video processing pipeline for a playlist.
    Steps:
    - Download playlist metadata
    - Download video metadata
    - Download audio
    - Run transcription
    - Extract topics
    - Classify summary
    - Extract laughter
    """
    playlist_info = youtube_downloader.yt_playlist_extract_info(youtube_url)

    database.new_video_from_playlist_info_to_db(conn, cursor, playlist_info)

    for video in playlist_info:
        if video.video_title != "Private_video":
            process_single_video(video, conn, cursor, minio_client)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="URL link to a YouTube video or playlist"
    )
    parser.add_argument("argument", help="URL of the video or playlist")
    return parser.parse_args()


def main() -> None:
    """Main entry point for the video processing pipeline."""
    try:
        args = parse_args()
        youtube_url = config.VideoURLModel(url=args.argument)

        conn = database.get_db_connection()
        cursor = conn.cursor()

        minio_client = Minio(
            settings.MINIO_DOMAIN,
            access_key=settings.MINIO_ROOT_USER,
            secret_key=settings.MINIO_ROOT_PASSWORD,
            secure=False,
        )

        process_playlist(str(youtube_url.url), conn, cursor, minio_client)

    except ValidationError as e:
        logging.error("URL validation error: %s", e)
    except KeyboardInterrupt:
        logging.warning("Operation interrupted by user (KeyboardInterrupt)")
    finally:
        if "cursor" in locals() and cursor:
            cursor.close()
        if "conn" in locals() and conn:
            conn.close()


if __name__ == "__main__":
    main()
