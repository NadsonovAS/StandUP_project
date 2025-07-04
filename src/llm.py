import json
import logging

from google import genai

import config

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def format_text_with_llm(raw_text: json) -> str | None:
    """
    Форматирует текст с помощью Gemini, разбивая его на темы на русском языке.

    Args:
        raw_text: Исходный транскрибированный текст в json формате.

    Returns:
        Отформатированный текст или None в случае ошибки.
    """

    # Преобразование входного json объекта к строке, т.к. промт LLM не принимает json
    str_transcribe_json = str(raw_text)

    try:
        # Используем модель и настройки из конфига
        client = genai.Client(api_key=config.GEMINI_API_KEY)

        prompt = f"""
        Вы — эксперт по тематическому анализу аудио/видео транскриптов.

        На входе вы получаете список объектов вида:
        [
        {{'timestamp': [0.2, 1.64],  'text': 'Какие у вас политические взгляды?'}},
        {{'timestamp': [3.32, 5.44], 'text': 'Вот всегда, всегда вот такой нервный смешок.'}},
        ...
        ]

        Ваша задача:
        1. Проанализировать все фрагменты текста и выделить глобальные ключевые темы (например, "политические взгляды", "медиа", "самоидентификация").
        2. Для каждой темы определить временной диапазон: от минимальной до максимальной отметки `timestamp`, относящейся к сообщениям этой темы.
        3. Вернуть результат **строго в формате JSON**, без каких-либо комментариев, префиксов, пояснений или форматирования (например, не оборачивать в ```json).

        Формат ожидаемого результата (пример):
        {{
        "политические взгляды": {{"start_sec": 1.564, "end_sec": 3.530}},
        "медиа": {{"start_sec": 8.951, "end_sec": 9.312}},
        "самоидентификация": {{"start_sec": 16.077, "end_sec": 17.647}}
        }}

        Вот расшифровка:
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
        return None
