import json
import logging

import psycopg

from config import settings
from pydantic_models import ProcessVideo
from utils import try_except_with_log


@try_except_with_log("Подключение к Postgres")
def get_db_connection():
    """
    Подключение к Postgres
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
def get_row_from_db(video_obj, cur):
    """
    Проверка наличия данных в Postgres
    """
    cur.execute(
        "SELECT * FROM standup_raw.process_video WHERE video_id = %s and process_status is NULL",
        (video_obj.video_id,),
    )
    row = cur.fetchone()
    if row:
        columns = [desc[0] for desc in cur.description]
        row_dict = dict(zip(columns, row))
        row_from_db = ProcessVideo.model_validate(row_dict)
        return row_from_db
    else:
        return


@try_except_with_log()
def obj_to_db(conn, cur, column, value, where, json_type=None):
    """
    Загрузка JSON объекта в Postgres по определенному video_id
    """
    if json_type is True:
        value = json.dumps(value)
    cur.execute(
        f"""
        UPDATE standup_raw.process_video
        SET {column} = %s
        WHERE video_id = %s
        """,
        (value, where),
    )
    conn.commit()


@try_except_with_log()
def new_video_from_playlist_info_to_db(conn, cur, playlist_info):
    """
    Добавление всех новых видео из загруженных метаданных плейлиста.
    """

    # Получение всех video_id из плейлиста
    all_video_ids = [row.video_id for row in playlist_info]

    # Получение существующих видео из БД
    cur.execute(
        "SELECT * FROM standup_raw.process_video WHERE video_id = ANY(%s)",
        (all_video_ids,),
    )
    existing_video_ids = set(row[4] for row in cur.fetchall())

    # Список новых видео, которых нет в плейлисте в БД
    new_video = set(all_video_ids) - existing_video_ids

    if new_video:
        new_video_to_db = [v for v in playlist_info if v.video_id in new_video]

        logging.info(f"Новых видео - {len(new_video_to_db)}")
        # Добавление новых видео в БД
        for video_obj in new_video_to_db:
            cur.execute(
                """
                    INSERT INTO standup_raw.process_video (channel_id, channel_name, playlist_id, playlist_title, video_id, video_title, video_url)
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
