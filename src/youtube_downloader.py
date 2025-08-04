import re
from typing import List

import yt_dlp
from minio.error import S3Error

from config import settings
from pydantic_models import ProcessVideo
from utils import try_except_with_log


@try_except_with_log()
def normalize_title(title: str) -> str:
    """
    Форматирование имени аудио для файловой системы
    """
    # Заменяем всё, что не буква или цифра, на подчёркивание
    temp = re.sub(r"[^\w\dа-яА-ЯёЁ]+", "_", title)
    # Удаляем повторяющиеся подчёркивания
    temp = re.sub(r"__+", "_", temp)
    # Удаляем подчёркивания в начале и в конце
    temp = temp.strip("_")

    return temp


@try_except_with_log("Загрузка метаданных видео")
def yt_video_extract_info(video_url):
    """
    Загрузка метаданных видео
    """
    with yt_dlp.YoutubeDL(settings.YDL_OPTS) as ydl:
        # Загрузка метаданных
        video_info = ydl.extract_info(video_url, download=False)

        # Фильтрация по ключам
        list_of_key = ["duration", "like_count", "view_count", "comment_count"]
        filtered_info = {
            key: video_info[key] for key in list_of_key if key in video_info
        }

        return filtered_info


@try_except_with_log("Загрузка метаданных плейлиста")
def yt_playlist_extract_info(youtube_url: str) -> List[ProcessVideo]:
    """
    Загрузка метаданных плейлиста
    """
    with yt_dlp.YoutubeDL(settings.YDL_OPTS_PLAYLIST) as ydl:
        # Объявление результирующего списка
        playlist_info = []

        # Загрузка метаданных
        all_playlist_info = ydl.extract_info(youtube_url, download=False)

        # Данные плейлиста
        playlist_id = all_playlist_info.get("id")
        playlist_title = all_playlist_info.get("title")

        # Получаем список видео
        entries = all_playlist_info.get("entries", [])

        # Получение данных по каждому видео внутри вложенного списка - entries
        for entry in entries:
            video_id = entry.get("id")
            channel_id = entry.get("channel_id")
            video_title = entry.get("title")
            normalized_title = (
                normalize_title(video_title) if video_title else f"video_{video_id}"
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

        return playlist_info


@try_except_with_log("Загрузка аудио")
def download_audio(minio_client, video_url, video_title):
    """
    Скачивает аудио, сохраняет его в MinIO и возвращает путь (путь в MinIO соотвествует локальному пути).
    """

    local_audio_path = settings.AUDIO_DIR / f"{video_title}.m4a"
    s3_key = f"{settings.MINIO_AUDIO_PATH}/{video_title}.m4a"

    # Проверяем, существует ли файл в MinIO
    try:
        minio_client.stat_object(settings.MINIO_AUDIO_BUCKET, s3_key)

        # Если файла нет локально, скачиваем из MinIO
        if not local_audio_path.exists():
            minio_client.fget_object(
                settings.MINIO_AUDIO_BUCKET, s3_key, str(local_audio_path)
            )

        return local_audio_path

    except S3Error as e:
        if e.code == "NoSuchKey":
            pass
        else:
            raise

    # Если файла нет в MinIO, скачиваем с YouTube
    ydl_opts = settings.YDL_OPTS.copy()
    ydl_opts["outtmpl"] = str(local_audio_path)

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([video_url])

    # Загружаем файл в MinIO
    minio_client.fput_object(settings.MINIO_AUDIO_BUCKET, s3_key, str(local_audio_path))

    return local_audio_path
