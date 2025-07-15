import json
import logging

from google import genai

import config

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def format_text_with_llm(raw_text) -> str | None:
    """
    Форматирует текст с помощью Gemini, разбивая его на темы на русском языке.

    Args:
        raw_text: Исходный транскрибированный текст в json формате.

    Returns:
        Отформатированный текст или None в случае ошибки.
    """
    logging.info("Запуск форматирования текста с помощью LLM...")

    # Преобразование входного json объекта к строке, т.к. промт LLM не принимает json
    str_transcribe_json = str(raw_text)

    try:
        # Используем модель и настройки из конфига
        client = genai.Client(api_key=config.GEMINI_API_KEY)

        prompt = f"""
        Вы — опытный эксперт по тематическому анализу аудио/видео транскриптов.

        На входе предоставляется список сегментов в формате JSON:
        [
            {{ "id": 0, "start": 0.0, "end": 2.0, "text": "Я ходил к психологу, и..." }},
            {{ "id": 1, "start": 2.0, "end": 3.8, "text": "Ну, это вот, в эту тюремную тему." }},
            {{ "id": 2, "start": 4.46, "end": 5.6, "text": "Я ходил к психологу." }},
            ...
        ]

        Ваша задача:
        1. Проанализировать все фрагменты и выделить глобальные ключевые темы. Каждая тема — строго одно слово.
        2. Упорядочить темы по хронологии их первого упоминания.
        3. Для каждой темы определить диапазон времени в секундах:
        - "start_sec" = время окончания предыдущей темы (или 0 для первой);
        - "end_sec" = максимальная отметка `end` у фрагментов текущей темы.
        4. Обеспечить непрерывность: начало каждой темы совпадает с концом предыдущей.

        Вернуть **только** чистый JSON-объект без комментариев, обёрток или форматирования:
        ```
        {{{{
        "политика":   {{"start_sec": 0,   "end_sec": 15}},
        "музыка":     {{"start_sec": 15,  "end_sec": 67}},
        "психология": {{"start_sec": 67,  "end_sec": 210}}
        }}}}
        ```

        Сегменты транскрипции:
        ---
        {str_transcribe_json}
        ---
        """

        response = client.models.generate_content(
            model=config.GEMINI_MODEL,
            contents=prompt,
            config={
                "response_mime_type": "application/json",
            },
        )

        llm_response_json_path = json.loads(response.text)

        if response:
            logging.info("Получен ответ от LLM.")
            return llm_response_json_path
        else:
            logging.warning(
                f"Gemini не вернул текст. Причина: {response.prompt_feedback}"
            )
            return (
                f"Не удалось обработать текст LLM. Причина: {response.prompt_feedback}"
            )

    except Exception as e:
        logging.error(f"Ошибка при взаимодействии с Gemini API: {e}")
        return
