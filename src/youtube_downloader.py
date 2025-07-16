import logging
from pathlib import Path

import yt_dlp

import config


def get_playlist_urls(playlist_url):
    """
    Получает список URL-адресов видео из плейлиста YouTube.
    """
    logging.info(f"Получение URL-адресов из плейлиста: {playlist_url}")
    urls = []
    try:
        with yt_dlp.YoutubeDL({"extract_flat": True, "quiet": True}) as ydl:
            info_dict = ydl.extract_info(playlist_url, download=False)
            if "entries" in info_dict:
                urls = [entry["url"] for entry in info_dict["entries"]]
                logging.info(f"Найдено {len(urls)} видео в плейлисте.")
    except Exception as e:
        logging.error(
            f"Ошибка при получении URL-адресов из плейлиста {playlist_url}: {e}"
        )
    return urls


def download_audio(video_url):
    """
    Скачивает аудио из видео по URL. Если файл уже существует, возвращает путь к нему.
    """
    logging.info(f"Скачивание аудио по URL: {video_url}")

    try:
        with yt_dlp.YoutubeDL(config.YDL_OPTS) as ydl:
            # 1. Получаем информацию о видео, не скачивая его
            info_dict = ydl.extract_info(video_url, download=False)

            # 2. Определяем ожидаемый путь к файлу
            filename = ydl.prepare_filename(info_dict)
            audio_path = Path(filename).with_suffix(".m4a")

            # 3. Проверяем, существует ли файл
            if audio_path.exists():
                logging.info(f"Файл скачан ранее: {audio_path}")
                return audio_path

            # 4. Если файла нет, запускаем полное скачивание и обработку
            ydl.extract_info(video_url, download=True)

            logging.info("Скачивание аудио завершено")

            return audio_path

    except Exception as e:
        logging.error(f"Ошибка при скачивании аудио для {video_url}: {e}")
        return
