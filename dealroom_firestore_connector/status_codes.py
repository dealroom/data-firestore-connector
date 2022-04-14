from enum import Enum


class Code(int, Enum):

    """Container for codes that encode the result of firestore-connector operations."""

    ERROR = -1
    SUCCESS = 0
    CREATED = 1
    UPDATED = 2
