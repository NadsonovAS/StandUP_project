import logging
import re

import yt_dlp

import config

# Для обработки одного URL --- ДОРАБОТАТЬ ЛОГИКУ по одиночным видео,сейас реализовано по плейлисту---
# --- Проверить есть ли URL в БД:
# ---   Если есть, вывести ЛОГ что в базе уже есть и вернуть строку со всеми данными
# ---   Если нет, то запустить функцию обработки URL и создать и заполнить новую строку в БД (информацию по плейлисту оставить пустым,либо найти в мете что-то про плейлист)


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
    with yt_dlp.YoutubeDL(config.YDL_OPTS) as ydl:
        video_info = ydl.extract_info(video_url, download=False)
        return video_info


# Для обработки целого плейлиста
def yt_playlist_extract_info(playlist_url):
    """
    Загрузка метаданных плейлиста
    """
    with yt_dlp.YoutubeDL(config.YDL_OPTS_PLAYLIST) as ydl:
        all_playlist_info = ydl.extract_info(playlist_url, download=False)

        # Данные плейлиста
        playlist_id = all_playlist_info["id"]
        playlist_title = all_playlist_info["title"]

        # В рамках парамтра extract_flat отсутсвуют метаинформация - view_count(есть,но округлен), comment_count, like_count, upload_date, chapter
        # Чтобы не делать цикл загрузки всех метаданных и создавать большой пул запросов, то каждый URL будет обрабатываться отдельно в рамках отдельного ETL цикла

        # Задаем нужные поля для каждого URL
        # ["entries"] — метаинформация о каждом видео в плейлисте
        entries = all_playlist_info["entries"]
        playlist_info = []
        for entry in entries:
            playlist_info.append(
                {
                    # Заполнение функцией **yt_playlist_extract_info**
                    "channel_id": entry.get("channel_id"),
                    "channel_name": entry.get("channel"),
                    "playlist_id": playlist_id,
                    "playlist_title": playlist_title,
                    "video_id": entry.get("id"),
                    "video_title": normalize_title(entry.get("title")),
                    "viedeo_url": entry.get("url"),
                    # Создание пустых полей, заполнение в рамках ETL цикла
                    # Заполнение функцией **yt_video_extract_info**
                    "duration": None,
                    "view_count": None,
                    "comment_count": None,
                    "like_count": None,
                    "upload_date": None,
                    # Заполнение в рамках transcribe.py
                    "transcribe_json": None,
                    # Заполнение в рамках llm.py
                    "llm_chapter_json": None,
                    # Заполнение в рамках llm.py
                    "SoundFileClassifier_app": None,
                }
            )

        return playlist_info


def download_audio(video_url, video_title):
    """
    Скачивает аудио из видео по URL. Если файл уже существует, возвращает путь к нему.
    """

    logging.info(f"Скачивание аудио по URL: {video_url}")

    try:
        audio_path = config.AUDIO_DIR / f"{video_title}.m4a"

        if audio_path.exists():
            logging.info(f"Файл скачан ранее: {audio_path}")
            return audio_path

        # Копируем конфиг и задаём имя файла
        ydl_opts = config.YDL_OPTS.copy()
        ydl_opts["outtmpl"] = str(config.AUDIO_DIR / f"{video_title}.%(ext)s")

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.extract_info(video_url, download=True)

        logging.info("Скачивание аудио завершено")

        return audio_path

    except Exception as e:
        logging.error(f"Ошибка при скачивании аудио для {video_url}: {e}")
        return
