import logging
from pathlib import Path

import yt_dlp

import config


def download_audio(video_url):
    """
    Скачивает аудио из видео по URL. Если файл уже существует, возвращает путь к нему.

    Args:
        video_url: URL видео на YouTube.

    Returns:
        Path к скачанному аудиофайлу.
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
