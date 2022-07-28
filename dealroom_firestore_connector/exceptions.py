import json
from functools import wraps
from typing import Callable, Optional, Any
from google.api_core.exceptions import InvalidArgument
from .status_codes import StatusCode


class FirestoreConnectorError(Exception):
    def __init__(
        self,
        operation: str,
        exc: Optional[Exception] = None,
        error_code: Optional[int] = None,
    ) -> None:
        if exc:
            message = f"Operation '{operation}' raised: {exc.__class__.__name__}: {exc}"
        elif error_code:
            message = f"Operation '{operation}' returned error code {error_code}"
        super().__init__(message)


class InvalidIdentifier(ValueError):

    """Raised if identifier is neither a valid ID nor a valid UUID."""

    def __init__(self, identifier: Any) -> None:
        message = f"{identifier} is neither a valid UUID or a valid ID"
        super().__init__(message)


# TODO: remove and adjust breaking changes (DN-932: https://dealroom.atlassian.net/browse/DN-932)
def exc_handler(func: Callable) -> Callable:
    """Decorator that handles exception FirestoreConnectorError by printing to
    std out and returning ERROR code.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FirestoreConnectorError as exc:
            print(f"{func.__class__.__name__}: {exc.__class__.__name__}, {exc}")
            return StatusCode.ERROR
        except InvalidArgument as exc:
            # this error is difficult to track, at least in this way we have a clue why or which process is raising it
            print(
                f"{func.__class__.__name__}: {exc.__class__.__name__}, {exc} - args = {json.dumps(args, default=str)} - kwargs = {json.dumps(kwargs, default=str)}"
            )
            raise exc

    return wrapper
