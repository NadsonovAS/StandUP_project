import logging
import re
from typing import List

import yt_dlp
from minio import Minio
from minio.error import S3Error
from pydantic import ValidationError

from config import settings
from pydantic_models import ProcessVideo


def normalize_title(title: str) -> str:
    # Заменяем всё, что не буква или цифра, на подчёркивание
    temp = re.sub(r"[^\w\dа-яА-ЯёЁ]+", "_", title)
    # Удаляем повторяющиеся подчёркивания
    temp = re.sub(r"__+", "_", temp)
    # Удаляем подчёркивания в начале и в конце
    return temp.strip("_")


def yt_video_extract_info(video_url):
    """
    Загрузка метаданных видео
    """
    with yt_dlp.YoutubeDL(settings.YDL_OPTS) as ydl:
        video_info = ydl.extract_info(video_url, download=False)

        list_of_key = ["duration", "like_count", "view_count", "comment_count"]
        filtered_info = {
            key: video_info[key] for key in list_of_key if key in video_info
        }

        return filtered_info


def yt_playlist_extract_info(youtube_url: str) -> List[ProcessVideo]:
    """
    Загрузка метаданных плейлиста с улучшенной обработкой ошибок
    """
    try:
        with yt_dlp.YoutubeDL(settings.YDL_OPTS_PLAYLIST) as ydl:
            all_playlist_info = ydl.extract_info(youtube_url, download=False)

            # Проверяем, что данные плейлиста получены
            if not all_playlist_info:
                raise ValueError("Не удалось получить информацию о плейлисте")

            # Данные плейлиста с проверкой на None
            playlist_id = all_playlist_info.get("id")
            playlist_title = all_playlist_info.get("title")

            if not playlist_id:
                raise ValueError("Не удалось получить ID плейлиста")

            # Получаем список видео
            entries = all_playlist_info.get("entries", [])
            if not entries:
                logging.warning("Плейлист не содержит видео")
                return []

            playlist_info = []
            failed_videos = []

            for idx, entry in enumerate(entries):
                try:
                    # Валидация обязательных полей
                    video_id = entry.get("id")
                    channel_id = entry.get("channel_id")

                    if not video_id:
                        logging.warning(f"Пропущено видео {idx}: отсутствует video_id")
                        failed_videos.append(idx)
                        continue

                    if not channel_id:
                        logging.warning(
                            f"Пропущено видео {idx}: отсутствует channel_id"
                        )
                        failed_videos.append(idx)
                        continue

                    # Подготовка данных с валидацией
                    video_title = entry.get("title")
                    normalized_title = (
                        normalize_title(video_title)
                        if video_title
                        else f"video_{video_id}"
                    )

                    video_data = {
                        "channel_id": channel_id,
                        "channel_name": entry.get("channel"),
                        "playlist_id": playlist_id,
                        "playlist_title": playlist_title,
                        "video_id": video_id,
                        "video_title": normalized_title,
                        "video_url": entry.get("url"),
                    }

                    # Создаем и валидируем Pydantic объект
                    video_obj = ProcessVideo(**video_data)
                    playlist_info.append(video_obj)

                except ValidationError as e:
                    logging.error(f"Ошибка валидации для видео {idx}: {e}")
                    failed_videos.append(idx)
                    continue
                except Exception as e:
                    logging.error(f"Неожиданная ошибка для видео {idx}: {e}")
                    failed_videos.append(idx)
                    continue

            if failed_videos:
                logging.warning(
                    f"Не удалось обработать {len(failed_videos)} видео из {len(entries)}"
                )

            return playlist_info

    except yt_dlp.DownloadError as e:
        logging.error(f"Ошибка yt-dlp при обработке {youtube_url}: {e}")
        raise
    except Exception as e:
        logging.error(f"Неожиданная ошибка при извлечении информации о плейлисте: {e}")
        raise


def download_audio(video_url, video_title):
    """
    Скачивает аудио из видео по URL, сохраняет его в MinIO и возвращает пути.
    """
    logging.info(f"Запуск скачивания аудио для: {video_title}")

    local_audio_path = settings.AUDIO_DIR / f"{video_title}.m4a"
    s3_key = f"{settings.MINIO_AUDIO_PREFIX}/{video_title}.m4a"

    try:
        minio_client = Minio(
            settings.MINIO_DOMAIN,
            access_key=settings.MINIO_ROOT_USER,
            secret_key=settings.MINIO_ROOT_PASSWORD,
            secure=False,
        )
        bucket_name = settings.MINIO_AUDIO_BUCKET

        # 1. Проверяем, существует ли бакет
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            logging.info(f"Бакет {bucket_name} создан.")

        # 2. Проверяем, существует ли файл в MinIO
        try:
            minio_client.stat_object(bucket_name, s3_key)
            logging.info(f"Файл уже существует в MinIO: {s3_key}")

            # Если файла нет локально, скачиваем из MinIO
            if not local_audio_path.exists():
                minio_client.fget_object(bucket_name, s3_key, str(local_audio_path))
                logging.info(f"Файл загружен из MinIO в {local_audio_path}")
            else:
                logging.info(f"Файл уже существует локально: {local_audio_path}")

            return local_audio_path

        except S3Error as e:
            if e.code == "NoSuchKey":
                logging.info(
                    f"Файл не найден в MinIO. Скачиваем с YouTube: {video_url}"
                )
            else:
                raise

        # 3. Если файла нет в MinIO, скачиваем с YouTube
        ydl_opts = settings.YDL_OPTS.copy()
        ydl_opts["outtmpl"] = str(local_audio_path)

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])

        logging.info(f"Аудио скачано: {local_audio_path}")

        # 4. Загружаем файл в MinIO
        minio_client.fput_object(bucket_name, s3_key, str(local_audio_path))
        logging.info(f"Файл {local_audio_path} загружен в MinIO: {s3_key}")

        return local_audio_path

    except Exception as e:
        logging.error(f"Ошибка при обработке {video_url}: {e}")
        return
