"""Daily refresh of YouTube metadata and dbt sync."""

import logging
import subprocess
import time
from collections.abc import Sequence

import psycopg
from yt_dlp.utils import DownloadError, ExtractorError

from config import get_settings
from database import ProcessVideoRepository, get_db_connection
from youtube_downloader import YoutubeDownloader

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def run_dbt() -> None:
    """Execute the dbt model responsible for refreshing metadata marts."""
    command: Sequence[str] = (
        "uv",
        "run",
        "dbt",
        "run",
        "--select",
        "core_videos_meta",
    )
    logging.info("Running dbt command: %s", " ".join(command))
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        logging.error(
            "DBT run failed with exit code %s\nSTDOUT:\n%s\nSTDERR:\n%s",
            result.returncode,
            result.stdout,
            result.stderr,
        )
        raise subprocess.CalledProcessError(
            result.returncode,
            command,
            output=result.stdout,
            stderr=result.stderr,
        )

    logging.info("DBT model core_videos_meta finished successfully")
    if result.stdout:
        logging.info("DBT output:\n%s", result.stdout)


def fetch_videos_to_refresh(
    connection: psycopg.Connection,
) -> list[tuple[str, str]]:
    """Return rows that require metadata refresh."""
    query = """
        SELECT video_id, video_url
        FROM standup_raw.process_video
        WHERE video_url IS NOT NULL
          AND (video_meta_json IS NULL OR meta_updated_at::date < CURRENT_DATE)
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
    return [(row[0], row[1]) for row in rows]


def main() -> None:
    settings = get_settings()
    connection = get_db_connection(settings=settings)
    repository = ProcessVideoRepository(connection)
    downloader = YoutubeDownloader(settings=settings)

    try:
        rows = fetch_videos_to_refresh(connection)
        logging.info("Found %d videos to refresh metadata", len(rows))

        updated_count = 0
        for video_id, video_url in rows:
            if not video_id or not video_url:
                continue

            try:
                metadata = downloader.extract_video_info(video_url)
                repository.update_video_column(
                    video_id,
                    "video_meta_json",
                    metadata,
                    json_type=True,
                )
                connection.commit()
                updated_count += 1
                logging.info("Refreshed metadata for video %s", video_id)
                time.sleep(1)
            except (DownloadError, ExtractorError) as error:
                logging.warning(
                    "Skipping video %s due to YouTube extractor error: %s",
                    video_id,
                    error,
                )
                connection.rollback()
            except Exception as error:  # noqa: BLE001
                logging.error(
                    "Failed to refresh metadata for video %s: %s",
                    video_id,
                    error,
                )
                connection.rollback()

        logging.info("Metadata refreshed for %d videos", updated_count)
    finally:
        connection.close()

    run_dbt()


if __name__ == "__main__":
    main()
