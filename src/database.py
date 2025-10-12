import json
import logging
from typing import Any, Iterable, Optional, Sequence

import psycopg

from config import Settings, get_settings
from models import ProcessVideo
from utils import try_except_with_log


@try_except_with_log("Connecting to Postgres")
def get_db_connection(settings: Settings | None = None) -> psycopg.Connection:
    """Establish a connection to Postgres using provided settings."""
    resolved_settings = settings or get_settings()
    return psycopg.connect(
        host=resolved_settings.POSTGRES_HOST,
        dbname=resolved_settings.POSTGRES_DB,
        user=resolved_settings.POSTGRES_USER,
        password=resolved_settings.POSTGRES_PASSWORD,
        port=resolved_settings.POSTGRES_PORT,
    )


class ProcessVideoRepository:
    """Data access layer for the standup_raw.process_video table."""

    def __init__(self, connection: psycopg.Connection) -> None:
        self._connection = connection

    def _row_to_model(self, row: Sequence[Any], columns: Sequence[str]) -> ProcessVideo:
        payload = dict(zip(columns, row))
        return ProcessVideo.model_validate(payload)

    @try_except_with_log()
    def fetch_pending_video(self, video_id: str) -> Optional[ProcessVideo]:
        query = "SELECT * FROM standup_raw.process_video WHERE video_id = %s"
        with self._connection.cursor() as cursor:
            cursor.execute(query, (video_id,))
            record = cursor.fetchone()
            if record is None:
                return None
            columns = [desc[0] for desc in cursor.description]
            return self._row_to_model(record, columns)

    @try_except_with_log()
    def update_video_column(
        self,
        video_id: str,
        column: str,
        value: Any,
        *,
        json_type: bool = False,
    ) -> None:
        payload = json.dumps(value) if json_type else value
        with self._connection.cursor() as cursor:
            cursor.execute(
                f"UPDATE standup_raw.process_video SET {column} = %s WHERE video_id = %s",
                (payload, video_id),
            )

    @try_except_with_log()
    def insert_new_videos(self, playlist_info: Iterable[ProcessVideo]) -> int:
        videos = list(playlist_info)
        if not videos:
            return 0

        video_ids = [video.video_id for video in videos if video.video_id]
        if not video_ids:
            return 0

        with self._connection.cursor() as cursor:
            cursor.execute(
                "SELECT video_id FROM standup_raw.process_video WHERE video_id = ANY(%s)",
                (video_ids,),
            )
            existing_ids = {row[0] for row in cursor.fetchall()}

            new_videos = [
                video for video in videos if video.video_id not in existing_ids
            ]

            seen_ids: set[str] = set()
            unique_new_videos: list[ProcessVideo] = []
            for video in new_videos:
                if video.video_id is None:
                    continue
                if video.video_id in seen_ids:
                    continue
                seen_ids.add(video.video_id)
                unique_new_videos.append(video)

            logging.info(f"Number of new video - {len(unique_new_videos)}")

            for video in unique_new_videos:
                cursor.execute(
                    """
                    INSERT INTO standup_raw.process_video (
                        channel_id,
                        channel_name,
                        playlist_id,
                        playlist_title,
                        video_id,
                        video_title,
                        video_url
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (video_id) DO NOTHING
                    """,
                    (
                        video.channel_id,
                        video.channel_name,
                        video.playlist_id,
                        video.playlist_title,
                        video.video_id,
                        video.video_title,
                        video.video_url,
                    ),
                )
        return len(unique_new_videos)

    @try_except_with_log()
    def fetch_unfinished_videos(self) -> list[ProcessVideo]:
        """Return all videos that have not been fully processed."""

        query = (
            "SELECT * FROM standup_raw.process_video"
            # "WHERE process_status IS DISTINCT FROM %s"
        )

        with self._connection.cursor() as cursor:
            cursor.execute(query)
            records = cursor.fetchall() or []
            columns = (
                [desc[0] for desc in cursor.description] if cursor.description else []
            )

        return [self._row_to_model(record, columns) for record in records]
