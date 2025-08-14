import json
import time
from io import StringIO
from pathlib import Path

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

    # Очистка от пустых строк
    df = df[df["text"] != ""]

    # Группировка подряд идущих одинаковых текстов
    group_id = (df["text"] != df["text"].shift()).cumsum()
    # Посчитаем длину каждой группы
    group_sizes = df.groupby(group_id)["text"].transform("count")
    # Удалим строки, если группа состоит из 3 и более одинаковых подряд идущих текстов
    df_cleaned = df[group_sizes < 3].reset_index(drop=True)

    df_export = df_cleaned[["id", "text"]]
    tsv_text = df_export.to_csv(sep="\t", index=False)

    return tsv_text


@try_except_with_log("Отправка запроса Gemini для выделения тем")
def get_chapter_with_llm(tsv_text) -> dict | None:
    """
    Обработка TSV данных, выделение ключевых тем и их id с помощью Gemini API
    """

    prompt = """
Role: You are an advanced AI analyst, an expert in thematic analysis of comedy shows and stand-up performances.

Task: Analyze the provided transcript and extract the main semantic themes in Russian.

Analysis Rules:
1. Major blocks only: Extract only large, key themes. A theme is a semantic block. Do not break the performance into individual jokes.
2. Strict chronology: Themes must follow the exact order in which they appear in the transcript.
3. Advertising segments: Always detect advertising blocks. For them, use strictly the Russian theme name "Реклама".

Output Format:
Return the result strictly in JSON format with the following fields:
- "theme" — array of strings (themes in Russian).
- "id" — array of numbers (IDs of the starting positions for each theme).

Example Output:
{
  "theme": [
    "Реклама",
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
  "id": [0, 88, 228, 309, 557, 617, 776, 1218, 1452, 1955]
}
"""

    client = OpenAI(
        base_url=settings.PROXY_URL,
        api_key=settings.GEMINI_API_KEY,
    )

    response = client.chat.completions.parse(
        model="gemini-2.5-flash",
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": tsv_text},
        ],
        temperature=0,
        response_format=pydantic_models.LlmResponseTheme,
    )

    content = response.choices[0].message.parsed
    llm_response = content.model_dump()

    return llm_response


@try_except_with_log("Отправка запроса Gemini для получения summarize")
def get_summarize_with_llm(transcribe_json, llm_chapter_json) -> dict | None:
    """
    Summarizes Russian (or other language) segments into English paragraphs using LLM.
    The LLM is instructed to always output in English only.
    """

    strict_prompt = """
You are an assistant that always responds in English regardless of the input language.  
Your task: Read the provided text (which may be in Russian or another language) and write a concise summary in one paragraph in English.  
Focus only on the main ideas and key facts. Exclude opinions, subjective statements, and unnecessary details.  
Do not use any non-English words in your response. Output must be exclusively in English.

Example:
[Input in Russian]: "Вчера в Москве состоялась конференция по искусственному интеллекту..."
[Output in English]: "A conference on artificial intelligence took place in Moscow yesterday, bringing together experts to discuss recent advancements."

Now, summarize the following text in English:
"""

    data = transcribe_json
    split_points = llm_chapter_json.get("id")

    if not split_points or not data:
        return None

    client = OpenAI(
        base_url="http://localhost:1234/v1",
        api_key="lm-studio",
    )

    result_list = []

    for index, _ in enumerate(split_points):
        start_idx = split_points[index]
        end_idx = (
            split_points[index + 1] if index + 1 < len(split_points) else len(data)
        )

        joined_text = " ".join(item["text"] for item in data[start_idx:end_idx]).strip()

        if not joined_text:
            result_list.append("No relevant content to summarize.")
            continue

        response = client.chat.completions.create(
            model="lm-studio",
            messages=[
                {"role": "system", "content": strict_prompt},
                {"role": "user", "content": joined_text},
            ],
            temperature=0,
        )

        content = response.choices[0].message.content.strip()
        result_list.append(content)
        time.sleep(1)

    return {"summarize": result_list}


@try_except_with_log("Отправка запроса Gemini для получения классификаций ")
def get_classifier_with_llm(llm_summarize_json) -> str | None:
    """
    Классификация суммаризованного текста с помощью LLM на основе categories.yaml.
    Возвращает словарь с результатами классификации.
    """

    # Load YAML classifier from a file
    categories_path = Path(__file__).parent / "categories.yaml"
    with open(categories_path, "r", encoding="utf-8") as f:
        categories_yaml = f.read()

    # English instruction for the LLM, embedding the YAML
    system_prompt = f"""
You are a classification system for stand-up comedy content.

Here is your YAML classifier of categories and subcategories:

{categories_yaml}

Your task:
1. Analyze the provided text.
2. Use ONLY the categories and subcategories present in the YAML above.
3. Identify exactly ONE main category (`main_category`) that best fits the text.
4. Inside that main category, choose exactly ONE subcategory (`subcategory`).
5. If multiple categories could be relevant, pick the one where the humor or topic is most prominent.
6. Return the result as JSON in the following format:

{{
  "main_category": "<exact main category name from YAML>",
  "subcategory": "<exact subcategory name from YAML>",
  "reason": "<short explanation of your choice>"
}}

Rules:
- Do NOT invent new categories or subcategories.
- Keep the spelling of category and subcategory EXACTLY as in the YAML.
- Keep the explanation ("reason") concise.
"""

    result_list = []
    result_dict = dict()

    summarize_list = llm_summarize_json.get("summarize")

    for summarize in summarize_list:
        client = OpenAI(
            base_url="http://localhost:1234/v1",
            api_key="lm-studio",
        )

        response = client.chat.completions.parse(
            model="lm-studio",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": summarize},
            ],
            temperature=0,
            response_format=pydantic_models.LlmResponseClassifier,
        )

        content = response.choices[0].message.parsed
        llm_response = content.model_dump()
        result_list.append(llm_response)

        result_dict.update({"classifier": result_list})
        time.sleep(1)

    return result_dict
