import logging
from functools import wraps


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

        return wrapper

    return decorator
