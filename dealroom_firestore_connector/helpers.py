import logging
import traceback


def error_logger(message, error_code=0):
    """Logs formatted error messages on the stderr file."""
    formatted_exc = traceback.format_exc()
    logging.error(f"Error trace: {formatted_exc}\n[Error code {error_code}] {message}")
