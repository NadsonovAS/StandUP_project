import logging
import traceback
from functools import wraps

from config import settings


def try_except_with_log(message=None):
    """
    Декоратор для логирования исключений
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                if message:
                    logging.info(f"{message}")
                return func(*args, **kwargs)
            except Exception as e:
                logging.error(f"Ошибка - {func.__name__}: {e}")
                traceback.print_exc()

        return wrapper

    return decorator


def remove_audio_cache():
    """
    Очистка всех файлов в папке data/audio
    """

    folder = settings.AUDIO_DIR

    for file in folder.iterdir():
        if file.is_file():
            file.unlink()
