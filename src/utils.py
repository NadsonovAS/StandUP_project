import logging

# import traceback
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Optional

from config import settings

logger = logging.getLogger(__name__)


def try_except_with_log(log_message: Optional[str] = None) -> Callable:
    """
    Decorator for logging exceptions in wrapped functions.

    Args:
        log_message (Optional[str]): An optional message to log before executing the function.

    Returns:
        Callable: The wrapped function with exception logging.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                if log_message:
                    logger.info(log_message)
                return func(*args, **kwargs)
            except Exception as e:
                logger.error("Error in %s: %s", func.__name__, e)
                # traceback.print_exc()

        return wrapper

    return decorator


def remove_audio_cache() -> None:
    """
    Remove all files in the data directory specified by settings.DATA_DIR.

    Logs each file removal. If a file cannot be removed, logs a warning.
    """
    folder: Path = settings.DATA_DIR

    for file in folder.iterdir():
        if file.is_file():
            try:
                file.unlink()
            except Exception as e:
                logger.warning("Failed to remove file %s: %s", file, e)
