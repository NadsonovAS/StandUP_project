import json

import psycopg

from config import settings
from pydantic_models import ProcessVideo


def get_db_connection():
    conn = psycopg.connect(
        host=settings.POSTGRES_HOST,
        dbname=settings.POSTGRES_DB,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
        port=settings.POSTGRES_PORT,
    )
    return conn


def check_row_in_db(video_obj, cur):
    """
    Проверка наличия данных в Postgres
    """
    cur.execute(
        "SELECT * FROM standup_raw.process_video WHERE video_id = %s",
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


def json_to_db(column, value, where, conn, cur):
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


def obj_to_db(column, value, where, conn, cur):
    cur.execute(
        f"""
        UPDATE standup_raw.process_video
        SET {column} = %s
        WHERE video_id = %s
        """,
        (value, where),
    )
    conn.commit()
