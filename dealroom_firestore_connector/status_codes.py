from enum import Enum


class Code(int, Enum):
    ERROR = -1
    SUCCESS = 0
    CREATED = 1
    UPDATED = 2
