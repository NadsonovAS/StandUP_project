import argparse
import logging

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


def process_video(youtube_url):
    """
    Полный пайплайн обработки видео с использованием Pydantic моделей и upsert функции
    """

    logging.info("Запуск обработки плейлиста")

    # 1. Загрузка метаданных плейлиста
    try:
        logging.info("Загрузка метаданных плейлиста с youtube")
        playlist_info = youtube_downloader.yt_playlist_extract_info(youtube_url)
        logging.info(
            f"Метаданные загружены, размер плейлиста - {len(playlist_info)} видео"
        )

        if not playlist_info:
            logging.warning("Плейлист пуст или не удалось получить данные")
            return

    except Exception as e:
        logging.error(f"Ошибка при получении метаданных от youtube: {e}")
        return

    # 2. Пайплайн обработки данных
    logging.info("Запуск процесса обработки каждого видео в плейлисте")

    try:
        # Подключение к БД
        conn = database.get_db_connection()
        cur = conn.cursor()

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
                try:
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
                    logging.info(f"Добавлено новое видео в БД: {video_obj.video_title}")
                except Exception as e:
                    logging.error(
                        f"Ошибка при добавлении нового видео {video_obj.video_id}: {e}"
                    )
        # Обрабатываем существующие и новые видео в одном цикле

        for video_obj in playlist_info:
            logging.info(f"Запуск процесса обработки - {video_obj.video_title}")

            try:
                row_from_db = database.check_row_in_db(video_obj, cur)

                # Если строка имеется в БД, то проверяем на пустые значения
                if row_from_db:
                    # 2.1 Заполнение video_meta_json
                    if row_from_db.video_meta_json is None:
                        try:
                            row_from_db.video_meta_json = (
                                youtube_downloader.yt_video_extract_info(
                                    row_from_db.video_url
                                )
                            )
                            column = "video_meta_json"
                            value = row_from_db.video_meta_json
                            where = row_from_db.video_id
                            database.json_to_db(column, value, where, conn, cur)
                        except Exception as e:
                            logging.error(
                                f"Ошибка при загрузке метаданных видео {row_from_db.video_id}: {e}"
                            )

                    #  2.2 Загрузка аудиофайла
                    # ДОРАБОТАТЬ ЛОГИКУ СОХРАНЕНИЯ и ПОЛУЧЕНИЯ
                    try:
                        row_from_db.audio_path = youtube_downloader.download_audio(
                            row_from_db.video_url, row_from_db.video_title
                        )
                        column = "audio_path"
                        value = str(row_from_db.audio_path)
                        where = row_from_db.video_id
                        database.obj_to_db(column, value, where, conn, cur)
                    except Exception as e:
                        logging.error(
                            f"Ошибка при загрузке аудио {row_from_db.video_id}: {e}"
                        )
                        continue

                    #  2.3 Заполнение transcribe_json
                    if row_from_db.transcribe_json is None:
                        try:
                            row_from_db.transcribe_json = transcribe.transcribe_audio(
                                row_from_db.audio_path
                            )
                            column = "transcribe_json"
                            value = row_from_db.transcribe_json
                            where = row_from_db.video_id
                            database.json_to_db(column, value, where, conn, cur)
                        except Exception as e:
                            logging.error(
                                f"Ошибка при транскрипции аудио {row_from_db.video_id}: {e}"
                            )

                    #  2.4 Заполнение llm_chapter_json
                    if row_from_db.llm_chapter_json is None:
                        try:
                            tsv_text = llm_chapter.format_json_to_tsv(
                                row_from_db.transcribe_json
                            )
                            row_from_db.llm_chapter_json = (
                                llm_chapter.format_text_with_llm(tsv_text)
                            )
                            column = "llm_chapter_json"
                            value = row_from_db.llm_chapter_json
                            where = row_from_db.video_id
                            database.json_to_db(column, value, where, conn, cur)
                        except Exception as e:
                            logging.error(
                                f"Ошибка при обработке LLM {row_from_db.video_id}: {e}"
                            )

                    #  2.5 Заполнение sound_classifier
                    if row_from_db.sound_classifier is None:
                        try:
                            row_from_db.sound_classifier = (
                                sound_classifier.get_sound_classifier(
                                    row_from_db.audio_path
                                )
                            )

                            column = "sound_classifier"
                            value = row_from_db.sound_classifier
                            where = row_from_db.video_id
                            database.json_to_db(column, value, where, conn, cur)

                        except Exception as e:
                            logging.error(
                                f"Ошибка при работе модуля детекции смеха {row_from_db.video_id}: {e}"
                            )

                logging.info("==========================================")

            except Exception as e:
                logging.error(f"Ошибка при сохранении видео {video_obj.video_id}: {e}")
                continue

    except Exception as e:
        logging.error(f"Ошибка при подключении к БД: {e}")

    finally:
        # Закрытие сессии БД
        if cur:
            cur.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(
            description="URL ссылка на видео или плейлист youtube.com"
        )
        parser.add_argument("argument", help="URL видео или плейлиста")
        args = parser.parse_args()

        youtube_url = config.VideoURLModel(url=args.argument)
        process_video(str(youtube_url.url))

    except ValidationError as e:
        logging.error(f"Ошибка валидации URL: {e}")
    except KeyboardInterrupt:
        logging.warning("Операция прервана пользователем (KeyboardInterrupt)")
