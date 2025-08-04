import json
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

    df_export = df[["id", "text"]]
    tsv_text = df_export.to_csv(sep="\t", index=False)

    return tsv_text


@try_except_with_log()
def extract_json_block(text: str) -> str:
    """
    Обработка полученного от Gemini json
    """
    match = re.search(r"\{.*\}", text, re.DOTALL)
    return match.group(0) if match else text


@try_except_with_log("Отправка запроса Gemini")
def get_chapter_with_llm(tsv_text) -> dict | None:
    """
    Обработка TSV данных, выделение ключевых тем и их id с помощью Gemini API
    """

    prompt = """
    **Роль:** Ты — продвинутый ИИ-аналитик, эксперт в тематическом анализе юмористических шоу и стендапов.
    **Задача:** Проанализировать предоставленный транскрипт выступления и выделить основные смысловые темы.

    # Правила Анализа
    1.  **Крупные блоки:** Выделяй только крупные, ключевые темы. Тема — это смысловой блок. Не дроби выступление на отдельные шутки.
    2.  **Строгая хронология:** Темы должны идти в том же порядке, в котором они появляются.
    3.  **Рекламные интеграции:** Обязательно распознавай рекламные блоки. Для них используй строго только название темы (theme) "Реклама".

    # Верни результат строго в JSON формате с полями "theme" (массив строк) и "id" (массив чисел).
    # Пример вывода:
    "theme": [
    "Случай в метро: узнавание со спины и страх",
    "Реклама",
    "Родник в Москве и аналогия слежки за курьером с охотой",
    "Реклама",
    "Реклама",
    "Анализ сказки 'Тысяча и одна ночь' и психология султана",
    "Различия между детьми и взрослыми, а не между полами",
    "Необратимость ошибок и несправедливость врожденной красоты",
    "Уязвимость человека в момент выноса мусора и способы увидеть его настоящим"
    ],
    "id": [
        88,
        228,
        309,
        557,
        617,
        776,
        1218,
        1452,
        1955
    ]
    """

    client = OpenAI(
        base_url=settings.PROXY_URL,
        api_key=settings.GEMINI_API_KEY,
    )

    response = client.chat.completions.create(
        model="gemini-2.5-flash",
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": tsv_text},
        ],
        temperature=0,
        response_format={"type": "json_object"},
    )

    content = response.choices[0].message.content

    # Парсинг ответа. Очистка, валидация, получение json
    raw_json = json.loads(extract_json_block(content))
    parsed = pydantic_models.LLMresponse(**raw_json)
    llm_response_json = parsed.model_dump()

    return llm_response_json
