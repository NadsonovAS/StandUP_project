import argparse
import json
import logging
import subprocess

import config
import llm
import transcribe
import youtube_downloader

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def check_and_save_to_json(file_path, arg_for_func, func):
    """
    Сохранение json объекта
    """
    # Проверка наличия файла
    if file_path.exists():
        logging.info(f"Файл ранее обработан - {func.__name__}")
        with open(file_path, "r", encoding="utf-8") as f:
            json_file = json.load(f)
    # Иначе сохранить в файл
    else:
        json_file = func(arg_for_func)

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(json_file, f, ensure_ascii=False, indent=4)
    return json_file


def process_video(video_url):
    """
    Полный пайплайн обработки видео: скачивание, транскрибация, форматирование.
    """

    # 1. Скачивание аудио
    audio_path = youtube_downloader.download_audio(video_url)

    # 2. Транскрибация
    transcribe_text_path = config.TRANSCRIPTS_DIR / f"{audio_path.stem}.json"

    transcribe_json = check_and_save_to_json(
        transcribe_text_path, audio_path, transcribe.transcribe_audio
    )

    # 3. Форматирование с помощью LLM
    llm_response_path = config.LLM_BLOCK_DIR / f"{audio_path.stem}.json"

    check_and_save_to_json(llm_response_path, transcribe_json, llm.format_text_with_llm)

    # 4. Детекция смеха
    logging.info("Запуск детекции смеха...")
    laughter_json_path = config.LAUGHTER_DIR / f"{audio_path.stem}.json"
    subprocess.run(
        [
            "./src/SoundFileClassifier_app",
            audio_path,
            laughter_json_path,
            config.WINDOW_DURATION_SECONDS,
            config.PREFERRED_TIMESCALE,
            config.CONFIDENCE_THRESHOLD,
            config.OVERLAP_FACTOR,
        ]
    )

    logging.info("Детекция смеха завершена")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Обработка видео с YouTube.")
    parser.add_argument(
        "argument",
        help="URL видео.",
    )
    args = parser.parse_args()
    process_video(args.argument)
