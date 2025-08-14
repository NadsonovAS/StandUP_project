import copy
import re
from collections import Counter

import mlx.core as mx
import mlx_whisper

from config import settings
from utils import try_except_with_log


@try_except_with_log("Запуск транскрибации аудио")
def transcribe_audio(audio_path):
    """
    Транскрибирует аудиофайл с использованием mlx-whisper.
    """

    result = mlx_whisper.transcribe(
        str(audio_path),
        path_or_hf_repo=settings.WHISPER_MODEL,
        **settings.TRANSCRIBE_PARAMS,
    )

    # Фильтрация нужных ключей
    filter = {"id", "start", "end", "text", "tokens"}
    filtered_result = [{k: d[k] for k in filter if k in d} for d in result["segments"]]

    # Очистка кэша вручную
    mx.clear_cache()

    return filtered_result


@try_except_with_log()
def re_transcribe_audio(audio_path, transcribe_json, segment_idx_list):
    """
    Повторная транскрибация аудиофайла с использованием mlx-whisper с отличающейся конфигурацией
    """

    transcribe_json_copy = copy.deepcopy(transcribe_json)

    for index in segment_idx_list:
        segment = transcribe_json_copy[index]
        try:
            result = mlx_whisper.transcribe(
                str(audio_path),
                path_or_hf_repo=settings.WHISPER_MODEL,
                clip_timestamps=[segment["start"], segment["end"]],
                **settings.RE_TRANSCRIBE_PARAMS,
            )

            transcribe_json_copy[index]["text"] = result.get("text")

            all_tokens_in_segments = []
            for segment in result["segments"]:
                all_tokens_in_segments.extend(segment["tokens"])
            transcribe_json_copy[index]["tokens"] = all_tokens_in_segments

        except Exception as e:
            print(f"Ошибка при обработке сегмента: {e}")
            continue

    # Очистка кэша вручную
    mx.clear_cache()

    return transcribe_json_copy


@try_except_with_log()
def check_hallucination(transcribe_json):
    """
    Проверка аудио файла на наличие галюцинаций транскрибирования, возвращает список индексов.
    """
    check_list = [
        "А.Семкин",
        "Таня Курасова",
        "А.Егорова",
        "DimaTorzok",
        "Продолжение следует",
        "Я очень извиняюсь",
        "Девушки отдыхают",
    ]

    segment_idx_list = []

    # Итерируемся по индексам для возможности замены
    for segment_idx, segment in enumerate(transcribe_json):
        if not segment or not segment.get("tokens"):
            continue

        # Подсчет токенов
        token_counts = Counter(segment["tokens"])
        if token_counts:
            most_common_token, count = token_counts.most_common(1)[0]
        else:
            count = 0

        text_to_check = segment.get("text", "")
        time = segment.get("end", 0) - segment.get("start", 0)

        # Проверка по условиям (галюцинации)
        # (Более 30 токенов в сегменте) или (нахождение в чек листе и время сегмента более 29сек)
        if (count > 30) or (
            text_to_check
            and any(
                re.search(re.escape(phrase), text_to_check, re.IGNORECASE)
                for phrase in check_list
            )
            and time > 29
        ):
            segment_idx_list.append(segment_idx)

    return segment_idx_list
