import argparse
import logging
import time

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


# TODO Доработка процессинга
# Требуется: уйти от монолитной функции
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
    database.new_video_from_playlist_info_to_db(conn, cur, playlist_info)

    # Обрабатываем существующие и новые видео в одном цикле
    for video in playlist_info:
        try:
            row_from_db = database.get_row_from_db(video, cur)

            # Если строка имеется в БД, то проверяем на пустые значения
            if row_from_db:
                logging.info("====================================================")
                logging.info(f"Запуск процесса обработки - {video.video_title}")

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
                    time.sleep(2)

                #  2.2 Загрузка аудиофайла
                if (
                    row_from_db.transcribe_json is None
                    or row_from_db.sound_classifier_json is None
                ):
                    row_from_db.audio_path = youtube_downloader.download_audio(
                        minio_client, row_from_db.video_url, row_from_db.video_title
                    )

                    #  2.3 Выполнение transcribe_audio
                    if row_from_db.transcribe_json is None:
                        row_from_db.transcribe_json = transcribe.transcribe_audio(
                            row_from_db.audio_path
                        )
                        hallucination_list = transcribe.check_hallucination(
                            row_from_db.transcribe_json
                        )
                        if hallucination_list:
                            row_from_db.transcribe_json = (
                                transcribe.re_transcribe_audio(
                                    row_from_db.audio_path,
                                    row_from_db.transcribe_json,
                                    hallucination_list,
                                )
                            )

                        database.obj_to_db(
                            conn,
                            cur,
                            column="transcribe_json",
                            value=row_from_db.transcribe_json,
                            where=row_from_db.video_id,
                            json_type=True,
                        )

                    #  2.4 Выполнение get_sound_classifier
                    if row_from_db.sound_classifier_json is None:
                        row_from_db.sound_classifier_json = (
                            sound_classifier.get_sound_classifier(
                                row_from_db.audio_path
                            )
                        )

                        database.obj_to_db(
                            conn,
                            cur,
                            column="sound_classifier_json",
                            value=row_from_db.sound_classifier_json,
                            where=row_from_db.video_id,
                            json_type=True,
                        )

                    # Удаление локальной версии аудио файла
                    utils.remove_audio_cache()

                #  2.5 Выполнение get_chapter_with_llm
                if row_from_db.llm_chapter_json is None:
                    tsv_text = llm.format_json_to_tsv(row_from_db.transcribe_json)
                    row_from_db.llm_chapter_json = llm.get_chapter_with_llm(tsv_text)
                    database.obj_to_db(
                        conn,
                        cur,
                        column="llm_chapter_json",
                        value=row_from_db.llm_chapter_json,
                        where=row_from_db.video_id,
                        json_type=True,
                    )

                if row_from_db.llm_chapter_json:
                    #  2.6 Выполнение get_summarize_with_llm
                    if row_from_db.llm_summarize_json is None:
                        row_from_db.llm_summarize_json = llm.get_summarize_with_llm(
                            row_from_db.transcribe_json, row_from_db.llm_chapter_json
                        )
                        database.obj_to_db(
                            conn,
                            cur,
                            column="llm_summarize_json",
                            value=row_from_db.llm_summarize_json,
                            where=row_from_db.video_id,
                            json_type=True,
                        )

                    #  2.7 Выполнение get_classifier_with_llm
                    if row_from_db.llm_classifier_json is None:
                        row_from_db.llm_classifier_json = llm.get_classifier_with_llm(
                            row_from_db.llm_summarize_json
                        )
                        database.obj_to_db(
                            conn,
                            cur,
                            column="llm_classifier_json",
                            value=row_from_db.llm_classifier_json,
                            where=row_from_db.video_id,
                            json_type=True,
                        )

                    #  2.8 Выставление статуса - finished, если получены все значения
                    if (
                        row_from_db.video_meta_json
                        and row_from_db.transcribe_json
                        and row_from_db.llm_chapter_json
                        and row_from_db.sound_classifier_json
                    ) is not None:
                        row_from_db.process_status = "finished"
                        database.obj_to_db(
                            conn,
                            cur,
                            column="process_status",
                            value=row_from_db.process_status,
                            where=row_from_db.video_id,
                        )

        except Exception as e:
            logging.error(f"Ошибка при сохранении видео {video.video_id}: {e}")
            continue


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(
            description="URL ссылка на видео или плейлист youtube.com"
        )
        parser.add_argument("argument", help="URL видео или плейлиста")
        args = parser.parse_args()

        # Валидация URL ссылки
        youtube_url = config.VideoURLModel(url=args.argument)

        # Подключение к БД
        conn = database.get_db_connection()
        cur = conn.cursor()

        # Подключение к MinIO
        minio_client = Minio(
            settings.MINIO_DOMAIN,
            access_key=settings.MINIO_ROOT_USER,
            secret_key=settings.MINIO_ROOT_PASSWORD,
            secure=False,
        )

        # Запуск пайплайна
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
