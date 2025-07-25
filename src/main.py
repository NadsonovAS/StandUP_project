import argparse
import json
import logging
import subprocess
from datetime import datetime

import psycopg

import config
import transcribe
import youtube_downloader

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def process_video(video_url):
    """
    Полный пайплайн обработки видео: получение метаифнормации, скачивание аудио, транскрибация, форматирование, детекция смеха.
    """
    # Подключение к БД
    conn = psycopg.connect(**config.CONN_PARAMS)
    conn.autocommit = True
    cur = conn.cursor()
    logging.info("Выполнено подключение к БД")

    # 1. Загрузка метаданных плейлиста в Postgres
    logging.info("Получение метаинформации по плейлисту")
    playlist_info = youtube_downloader.yt_playlist_extract_info(video_url)
    logging.info("Метаинформация плейлиста получена")

    # Проверка на дубликаты перед вставкой
    all_video_ids = [item["video_id"] for item in playlist_info]
    cur.execute(
        "SELECT video_id FROM standup_raw.process_video WHERE video_id = ANY(%s)",
        (all_video_ids,),
    )
    existing_ids = {row[0] for row in cur.fetchall()}
    logging.info(f"Обнаружено {len(existing_ids)} уже существующих видео в БД.")

    # Фильтрация для вставки только новых видео
    new_videos_info = [
        item for item in playlist_info if item["video_id"] not in existing_ids
    ]

    if new_videos_info:
        insert_query = """
        INSERT INTO standup_raw.process_video (
        channel_id, channel_name, playlist_id, playlist_title, video_id, video_title, 
        viedeo_url, duration, view_count, comment_count, like_count, upload_date, 
        transcribe_json, llm_chapter_json, SoundFileClassifier_app)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Подготовка данных: список кортежей из значений словарей
        data = [
            (
                item["channel_id"],
                item["channel_name"],
                item["playlist_id"],
                item["playlist_title"],
                item["video_id"],
                item["video_title"],
                item["viedeo_url"],
                item["duration"],
                item["view_count"],
                item["comment_count"],
                item["like_count"],
                item["upload_date"],
                item["transcribe_json"],
                item["llm_chapter_json"],
                item["SoundFileClassifier_app"],
            )
            for item in new_videos_info
        ]

        # Выполнение множественной вставки
        cur.executemany(insert_query, data)
        logging.info(f"В БД добавлено {len(new_videos_info)} новых записей.")
    else:
        logging.info("Новых видео для добавления в БД не найдено.")

    try:
        for index, raw in enumerate(playlist_info):
            video_url = raw["viedeo_url"]
            video_title = raw["video_title"]

            # 1. Загрузка метаинфорации видео в Postgres, обновление playlist_info
            try:
                logging.info(f"Загрузка в Postgres метаинфорации видео - {video_title}")
                video_url = raw["viedeo_url"]
                video_info = youtube_downloader.yt_video_extract_info(video_url)

                # Обновление данных playlist_info
                date = datetime.strptime(video_info["upload_date"], "%Y%m%d").date()
                playlist_info[index]["duration"] = video_info["duration"]
                playlist_info[index]["like_count"] = video_info["like_count"]
                playlist_info[index]["view_count"] = video_info["view_count"]
                playlist_info[index]["comment_count"] = video_info["comment_count"]
                playlist_info[index]["upload_date"] = date

                # SQL-запрос для вставки
                update_yt_meta = f"""
                UPDATE standup_raw.process_video SET duration={video_info["duration"]},
                like_count={video_info["like_count"]},
                view_count={video_info["view_count"]},
                comment_count={video_info["comment_count"]},
                upload_date='{date}'
                where video_id = '{playlist_info[index]["video_id"]}';
                """

                # Выполнение вставки
                cur.execute(update_yt_meta)

                logging.info("Метаинфорация загружена")

            except Exception as error:
                print(f"Ошибка: {error}")

            # 2. Загрузка аудио
            try:
                audio_path = youtube_downloader.download_audio(
                    video_url, playlist_info[index]["video_title"]
                )
                # SQL-запрос для вставки
                update_audio_path = f"""
                UPDATE standup_raw.process_video SET audio_path='{audio_path}'
                where video_id = '{playlist_info[index]["video_id"]}';
                """
                # Выполнение множественной вставки
                cur.execute(update_audio_path)

            except Exception as error:
                print(f"Ошибка: {error}")

            # 3. Транскрибация аудио
            try:
                transcribe_json = transcribe.transcribe_audio(audio_path)
                transcribe_json_string = json.dumps(transcribe_json, ensure_ascii=False)

                update_transcribe_json = """
                UPDATE standup_raw.process_video SET transcribe_json=%s
                where video_id = %s;
                """

                # Выполнение множественной вставки
                cur.execute(
                    update_transcribe_json,
                    (transcribe_json_string, playlist_info[index]["video_id"]),
                )
            except Exception as error:
                print(f"Ошибка: {error}")

            # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! --- РАЗБИЕНИЕ НА БЛОКИ ОТ ЛЛМ --- #

            # 5. Детекция смеха
            try:
                logging.info(f"Запуск детекции смеха видео - {video_title}")
                cmd = [
                    "./src/SoundFileClassifier",
                    str(audio_path),
                    str(config.WINDOW_DURATION_SECONDS),
                    str(config.PREFERRED_TIMESCALE),
                    str(config.CONFIDENCE_THRESHOLD),
                    str(config.OVERLAP_FACTOR),
                ]

                result = subprocess.run(
                    cmd,
                    capture_output=True,  # захват stdout и stderr
                    text=True,  # чтобы результат был строкой, а не байтами
                    check=True,  # выбросит исключение при ненулевом коде возврата
                )

                sound_classifier_json = json.loads(result.stdout)
                sound_classifier_json_string = json.dumps(
                    sound_classifier_json, ensure_ascii=False
                )

                update_sound_classifier_json = """
                UPDATE standup_raw.process_video SET SoundFileClassifier_app=%s
                where video_id = %s;
                """

                # Выполнение множественной вставки
                cur.execute(
                    update_sound_classifier_json,
                    (
                        sound_classifier_json_string,
                        playlist_info[index]["video_id"],
                    ),
                )
                logging.info("Информация по детекции смеха загружена в Postgres")

            except Exception as error:
                print(f"Ошибка: {error}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="URL ссылка на видео или плейлист youtube.com"
    )
    parser.add_argument(
        "argument",
        help="URL видео или плейлиста",
    )
    args = parser.parse_args()
    process_video(args.argument)
