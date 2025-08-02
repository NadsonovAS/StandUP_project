import json
import logging
import re
from io import StringIO

import pandas as pd
from openai import OpenAI

import pydantic_models
from config import settings
from utils import try_except_with_log


@try_except_with_log()
def format_json_to_tsv(transcribe_json):
    """
    Конвертация json в TSV (start, text)
    """
    value = json.dumps(transcribe_json)

    df = pd.read_json(StringIO(value))
    df["start"] = df["start"].round().astype(int)
    df_export = df[["start", "text"]]

    tsv_text = df_export.to_csv(sep="\t", index=False)
    return tsv_text


@try_except_with_log()
def extract_json_block(text: str) -> str:
    """
    Обработка полученного от Gemini json
    """
    match = re.search(r"\{.*\}", text, re.DOTALL)
    return match.group(0) if match else text


@try_except_with_log()
def format_text_with_llm(raw_text) -> dict | None:
    """
    Обработка TSV данных, выделение ключевых тем и их временных меток с помощью Gemini API
    """

    lines = raw_text.strip().split("\n")[1:]
    last_line = lines[-1]
    duration = last_line.split("\t")[0]

    prompt = f"""
    **Роль:** Ты — продвинутый ИИ-аналитик, эксперт в тематическом анализе юмористических шоу и стендапов.
    **Задача:** Проанализировать предоставленный транскрипт выступления и выделить основные смысловые темы, указав их таймкоды.

    ВАЖНАЯ ИНФОРМАЦИЯ:
    Общая длительность выступления: {duration} секунд.
    Ответ должен быть только на русском языке.

    # Правила Анализа
    1.  **Крупные блоки:** Выделяй только крупные, ключевые темы. Тема — это смысловой блок, обсуждаемый несколько минут (например, "отношения с родителями", "опыт жизни за границей", "неловкие ситуации"). Не дроби выступление на отдельные шутки.
    2.  **Строгая хронология:** Темы должны идти в том же порядке, в котором они появляются. Массив `timestamp` должен быть строго отсортирован по возрастанию.
    3.  **Рекламные интеграции:** Обязательно распознавай рекламные блоки. Для них используй строго только название темы (theme) "Реклама".
    4.  **Контроль длительности:** Последний таймкод НЕ ДОЛЖЕН превышать общую длительность выступления ({duration} секунд).
    5.  **Контроль пропорций:** Ни одна тема не должна занимать более трети от общего времени выступления.

    # Верни результат строго в JSON формате с полями "theme" (массив строк) и "timestamp" (массив чисел).
    """

    client = OpenAI(
        base_url=settings.PROXY_URL,
        api_key=settings.GEMINI_API_KEY,
    )

    response = client.chat.completions.create(
        model="gemini-2.5-flash",
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": raw_text},
        ],
        temperature=0,
        response_format={"type": "json_object"},
    )

    content = response.choices[0].message.content

    # Парсинг ответа. Очистка, валидация, получение json
    raw_json = json.loads(extract_json_block(content))
    parsed = pydantic_models.LLMresponse(**raw_json)
    llm_response_json = parsed.model_dump()

    logging.info("Получен ответ от Gemini")
    return llm_response_json
