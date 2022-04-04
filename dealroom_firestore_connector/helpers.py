import logging
import traceback
from uuid import UUID
from typing import Union


def is_valid_uuid(value: Union[str, int, None]) -> bool:
    try:
        UUID(hex=value, version=4)
    except (TypeError, ValueError, AttributeError):
        return False
    return True


def is_valid_id(value: Union[str, int, None]) -> bool:
    if isinstance(value, str):
        return value and value.isnumeric() and int(value) > 0
    elif isinstance(value, int):
        return value and int(value) > 0
    else:
        return False


def error_logger(message, error_code=0):
    """Logs formatted error messages on the stderr file."""
    formatted_exc = traceback.format_exc()
    logging.error(f"Error trace: {formatted_exc}\n[Error code {error_code}] {message}")
