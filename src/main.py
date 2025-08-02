import argparse
import logging
import time

from pydantic import ValidationError

import config
import database
import llm_chapter
import sound_classifier
import transcribe
import youtube_downloader

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


### ВНЕДРИТЬ ЛОГГИНГ try/except полностью !!!


def process_video(youtube_url):
    """
    Пайплайн обработки видео.

    Загрузка:
    - метаданных плейлиста
    - метаданных видео
    - аудио
    - транскрибация
    - извлечение тем из транскрипта
    - !!! - ТРЕБУЕТСЯ: добавление классификации темы с помощью Gemini !!!
    - выделения тега "смех" из аудио с временными метками
    """

    # 1. Загрузка метаданных плейлиста
    playlist_info = youtube_downloader.yt_playlist_extract_info(youtube_url)
    time.sleep(3)

    # Внесение метаданных из плейлиста о новых видео в Postgres
    database.new_video_from_playlist_info_to_db(playlist_info, conn, cur)

    # Обрабатываем существующие и новые видео в одном цикле
    for video_obj in playlist_info:
        logging.info("====================================================")
        logging.info(f"Запуск процесса обработки - {video_obj.video_title}")

        try:
            row_from_db = database.get_row_from_db(video_obj, cur)

            # Если строка имеется в БД, то проверяем на пустые значения
            if row_from_db:
                # 2.1 Заполнение video_meta_json
                if row_from_db.video_meta_json is None:
                    row_from_db.video_meta_json = (
                        youtube_downloader.yt_video_extract_info(row_from_db.video_url)
                    )
                    database.obj_to_db(
                        conn,
                        cur,
                        column="video_meta_json",
                        value=row_from_db.video_meta_json,
                        where=row_from_db.video_id,
                        json_type=True,
                    )

                time.sleep(3)

                #  2.2 Загрузка аудиофайла
                # ДОРАБОТАТЬ ЛОГИКУ СОХРАНЕНИЯ и ПОЛУЧЕНИЯ

                row_from_db.audio_path = youtube_downloader.download_audio_toS3(
                    row_from_db.video_url, row_from_db.video_title
                )
                database.obj_to_db(
                    conn,
                    cur,
                    column="audio_path",
                    value=str(row_from_db.audio_path),
                    where=row_from_db.video_id,
                )

                #  2.3 Заполнение transcribe_json
                if row_from_db.transcribe_json is None:
                    row_from_db.transcribe_json = transcribe.transcribe_audio(
                        row_from_db.audio_path
                    )
                    database.obj_to_db(
                        conn,
                        cur,
                        column="transcribe_json",
                        value=row_from_db.transcribe_json,
                        where=row_from_db.video_id,
                        json_type=True,
                    )

                #  2.4 Заполнение llm_chapter_json
                if row_from_db.llm_chapter_json is None:
                    tsv_text = llm_chapter.format_json_to_tsv(
                        row_from_db.transcribe_json
                    )
                    row_from_db.llm_chapter_json = llm_chapter.format_text_with_llm(
                        tsv_text
                    )
                    database.obj_to_db(
                        conn,
                        cur,
                        column="llm_chapter_json",
                        value=row_from_db.llm_chapter_json,
                        where=row_from_db.video_id,
                        json_type=True,
                    )

                #  2.5 Заполнение sound_classifier
                if row_from_db.sound_classifier is None:
                    row_from_db.sound_classifier = (
                        sound_classifier.get_sound_classifier(row_from_db.audio_path)
                    )

                    database.obj_to_db(
                        conn,
                        cur,
                        column="sound_classifier",
                        value=row_from_db.sound_classifier,
                        where=row_from_db.video_id,
                        json_type=True,
                    )

        except Exception as e:
            logging.error(f"Ошибка при сохранении видео {video_obj.video_id}: {e}")
            continue


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(
            description="URL ссылка на видео или плейлист youtube.com"
        )
        parser.add_argument("argument", help="URL видео или плейлиста")
        args = parser.parse_args()

        youtube_url = config.VideoURLModel(url=args.argument)

        # Подключение к БД
        conn = database.get_db_connection()
        cur = conn.cursor()

        process_video(str(youtube_url.url))

    except ValidationError as e:
        logging.error(f"Ошибка валидации URL: {e}")
    except KeyboardInterrupt:
        logging.warning("Операция прервана пользователем (KeyboardInterrupt)")
    finally:
        # Закрытие сессии БД
        if cur:
            cur.close()
        if conn:
            conn.close()
