import json
from collections import Counter

import mlx_whisper
import pandas as pd

from config import settings
from utils import try_except_with_log


@try_except_with_log("Запуск транскрибации аудио")
def transcribe_audio(audio_path) -> str:
    """
    Транскрибирует аудиофайл с использованием mlx-whisper.
    """

    result = mlx_whisper.transcribe(
        str(audio_path),
        path_or_hf_repo=settings.WHISPER_MODEL,
        temperature=0,
    )

    # Создание DF из словаря транскрибации
    df = pd.DataFrame.from_dict(result["segments"])

    # Очистка пустных и повторяющихся срок
    df = df[df["text"].astype(bool)].reset_index(drop=True)
    group_id = (df["text"] != df["text"].shift()).cumsum()
    group_sizes = df.groupby(group_id)["text"].transform("count")
    df = df[group_sizes < 3].reset_index(drop=True)

    # Подсчёт времени до следующей или текущей фразы (если длинна участка более 25 секунд -> считаем галюцинацией, т.к. участок почти никогда не превышает 15 секунд)
    df["next_start"] = df["start"].shift(-1) - df["start"]
    df["end_minus_start"] = df["end"] - df["start"]

    # Повторный запуск транскрибации при галюцинациях
    for idx, row in df.iterrows():
        _, count = Counter(row["tokens"]).most_common(1)[0]

        # Перезапуск при галюцинациях (более 30 одинаковых токенов не участке)
        if count > 30:
            res2 = mlx_whisper.transcribe(
                str(audio_path),
                path_or_hf_repo=settings.WHISPER_MODEL,
                clip_timestamps=[row["start"], row["end"]],
                temperature=(0.0, 0.2),
                initial_prompt="Это юмористическое шоу",
            )
            df.loc[idx, "text"] = res2["text"]
            df.at[idx, "tokens"] = res2["segments"][0]["tokens"]

        # Обработка временных галюцинаций
        if (row["next_start"] or row["end_minus_start"]) > 25:
            res3 = mlx_whisper.transcribe(
                str(audio_path),
                path_or_hf_repo=settings.WHISPER_MODEL,
                clip_timestamps=[row["start"], row["next_start"]],
                temperature=(0.0, 0.2),
                initial_prompt="Это юмористическое шоу",
            )
            df.loc[idx, "text"] = res3["text"]
            df.at[idx, "end"] = row["next_start"]
            df.at[idx, "tokens"] = result["segments"][0]["tokens"]

    result = df.to_json(force_ascii=False, indent=2)
    transcribe_json = json.loads(result)

    return transcribe_json
