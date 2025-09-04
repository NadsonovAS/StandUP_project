import json
import logging
from typing import Any, List, Optional

import psycopg

from config import settings
from models import ProcessVideo
from utils import try_except_with_log


@try_except_with_log("Connecting to Postgres")
def get_db_connection() -> psycopg.Connection:
    """
    Establish a connection to the Postgres database.

    Returns:
        psycopg.Connection: Active database connection.
    """
    conn = psycopg.connect(
        host=settings.POSTGRES_HOST,
        dbname=settings.POSTGRES_DB,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
        port=settings.POSTGRES_PORT,
    )
    return conn


@try_except_with_log()
def get_row_from_db(video_obj: ProcessVideo, cursor) -> Optional[ProcessVideo]:
    """
    Retrieve a row from the database for a given video if not already processed.

    Args:
        video_obj (ProcessVideo): Video object containing video_id.
        cursor: Database cursor.

    Returns:
        Optional[ProcessVideo]: A ProcessVideo object if found, otherwise None.
    """
    cursor.execute(
        """
        SELECT * FROM standup_raw.process_video
        WHERE video_id = %s AND process_status IS NULL
        """,
        (video_obj.video_id,),
    )
    row = cursor.fetchone()
    if row:
        columns = [desc[0] for desc in cursor.description]
        row_dict = dict(zip(columns, row))
        return ProcessVideo.model_validate(row_dict)
    return None


@try_except_with_log()
def obj_to_db(
    conn,
    cursor,
    column: str,
    value: Any,
    where: str,
    json_type: bool = False,
) -> None:
    """
    Update a specific column for a video row in the database.

    Args:
        conn: Active database connection.
        cursor: Database cursor.
        column (str): Column name to update.
        value (Any): Value to set.
        where (str): Video ID condition.
        json_type (bool, optional): If True, serialize value to JSON. Defaults to False.
    """
    if json_type:
        value = json.dumps(value)

    cursor.execute(
        f"""
        UPDATE standup_raw.process_video
        SET {column} = %s
        WHERE video_id = %s
        """,
        (value, where),
    )
    conn.commit()


@try_except_with_log()
def new_video_from_playlist_info_to_db(
    conn, cursor, playlist_info: List[ProcessVideo]
) -> None:
    """
    Insert new videos from playlist metadata into the database if not already present.

    Args:
        conn: Active database connection.
        cursor: Database cursor.
        playlist_info (List[ProcessVideo]): List of video metadata objects.
    """
    all_video_ids = [row.video_id for row in playlist_info]

    cursor.execute(
        "SELECT * FROM standup_raw.process_video WHERE video_id = ANY(%s)",
        (all_video_ids,),
    )
    existing_video_ids = {row[4] for row in cursor.fetchall()}

    new_video_ids = set(all_video_ids) - existing_video_ids

    if new_video_ids:
        new_videos_to_insert = [v for v in playlist_info if v.video_id in new_video_ids]

        logging.info("New videos to insert: %d", len(new_videos_to_insert))

        for video_obj in new_videos_to_insert:
            cursor.execute(
                """
                INSERT INTO standup_raw.process_video
                (channel_id, channel_name, playlist_id, playlist_title,
                 video_id, video_title, video_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    video_obj.channel_id,
                    video_obj.channel_name,
                    video_obj.playlist_id,
                    video_obj.playlist_title,
                    video_obj.video_id,
                    video_obj.video_title,
                    video_obj.video_url,
                ),
            )
            conn.commit()
