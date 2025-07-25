import json
import logging

from google import genai

import config


def format_text_with_llm(raw_text) -> str | None:
    """
    Форматирует текст с помощью Gemini, разбивая его на темы.
    """
    logging.info("Запуск форматирования текста с помощью LLM...")

    # Преобразование входного json объекта к строке, т.к. промт LLM не принимает json
    str_transcribe_json = str(raw_text)

    try:
        client = genai.Client(api_key=config.GEMINI_API_KEY)

        prompt = f"""
        Вы — опытный эксперт по тематическому анализу текстов.

        Ваша задача:
        1. Проанализировать все фрагменты и выделить глобальные ключевые темы.
        2. Упорядочить темы по хронологии их первого упоминания.
        3. Для каждой темы определить диапазон времени в секундах:
        - "start_sec" = время окончания предыдущей темы (или 0 для первой);
        - "end_sec" = максимальная отметка `end` у фрагментов текущей темы.
        4. Обеспечить непрерывность: начало каждой темы совпадает с концом предыдущей.
        5. Перепроверь себя, что тема не должна быть слишком короткой, например длиться меньше 30 секунд.

        Вернуть **только** чистый JSON-объект без комментариев, обёрток или форматирования:
        ```
        {{{{
        "тема 1":   {{"start_sec": 0,   "end_sec": 67}},
        "тема 2":     {{"start_sec": 67,  "end_sec": 150}},
        "тема N": {{"start_sec": 150,  "end_sec": 315}}
        }}}}
        ```

        Сегменты транскрипции:
        ---
        {str_transcribe_json}
        ---
        """

        response = client.models.generate_content(
            model=config.GEMINI_MODEL_FLASH,
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
