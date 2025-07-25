import logging
from collections import Counter

import mlx_whisper
import pandas as pd

import config


def transcribe_audio(audio_path) -> str:
    """
    Транскрибирует аудиофайл с использованием mlx-whisper.
    """

    logging.info("Запуск транскрибации...")

    try:
        result = mlx_whisper.transcribe(
            str(audio_path),
            path_or_hf_repo=config.WHISPER_MODEL,
            temperature=0,
        )

        # Создание DF из словаря транскрибации
        df = pd.DataFrame.from_dict(result["segments"])

        # Очистка пустных и повторяющихся срок
        df = df[df["text"].astype(bool)].reset_index(drop=True)
        group_id = (df["text"] != df["text"].shift()).cumsum()
        group_sizes = df.groupby(group_id)["text"].transform("count")
        df = df[group_sizes < 3].reset_index(drop=True)

        # Подсчёт времени до следующей фразы (если длинна участка более 25 секунд -> считаем галюцинацией, т.к. участок почти никогда не превышает 15 секунд)
        df["next_start"] = df["start"].shift(-1) - df["start"]

        # Повторный запуск транскрибации при галюцинациях
        for idx, row in df.iterrows():
            _, count = Counter(row["tokens"]).most_common(1)[0]

            # Перезапуск при галюцинациях (более 30 одинаковых токенов не участке)
            if count > 30:
                res2 = mlx_whisper.transcribe(
                    str(audio_path),
                    path_or_hf_repo=config.WHISPER_MODEL,
                    clip_timestamps=[row["start"], row["end"]],
                    temperature=(0.0, 0.2),
                    initial_prompt="Это юмористическое шоу",
                )
                df.loc[idx, "text"] = res2["text"]
                df.at[idx, "tokens"] = res2["segments"][0]["tokens"]

            # Обработка временных галюцинаций
            if row["next_start"] > 25:
                res3 = mlx_whisper.transcribe(
                    str(audio_path),
                    path_or_hf_repo=config.WHISPER_MODEL,
                    clip_timestamps=[row["start"], row["next_start"]],
                    temperature=(0.0, 0.2),
                    initial_prompt="Это юмористическое шоу",
                )
                df.loc[idx, "text"] = res3["text"]
                df.at[idx, "end"] = row["next_start"]
                df.at[idx, "tokens"] = result["segments"][0]["tokens"]

        result = df.to_json(force_ascii=False, indent=2)

        if result:
            logging.info("Транскрибация успешно завершена.")
            return result
        else:
            logging.warning("Транскрибация не успешна")
            return None
    except Exception as e:
        logging.error(f"Ошибка во время транскрибации {audio_path}: {e}")
        return None
