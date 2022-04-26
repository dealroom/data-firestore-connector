from __future__ import annotations
from typing import Union, Optional
from enum import Enum
from .helpers import is_valid_uuid, is_valid_id
from .exceptions import InvalidIdentifier


class DealroomEntity(int, Enum):

    """Container for special values for Dealroom ID/UUID in Firestore documents."""

    # We mark the deleted entities from DR database with -2 entity so we can easily identify them.
    DELETED = -2
    # We mark the entities that don't exist in DR with -1.
    NOT_IN_DB = -1


_FIELD_NAME_UUID = "dealroom_uuid"
_FIELD_NAME_ID = "dealroom_id"


class DealroomIdentifier:

    """Can model a Dealroom ID or a UUID in a Firestore document."""

    def __init__(self, value: Union[str, int]) -> None:
        self._value = value
        if isinstance(value, int):
            self._field_name = _FIELD_NAME_ID
        elif isinstance(value, str):
            self._field_name = _FIELD_NAME_UUID
        else:
            raise TypeError(
                f"value '{value}' must be int or str, but {type(value)} provided."
            )

    @property
    def value(self) -> Union[str, int]:
        """Value to be set/used."""
        return self._value

    @property
    def field_name(self) -> str:
        """Name of field in document that holds the value."""
        return self._field_name

    @property
    def field_name_old(self) -> str:
        """Name of field in document where the value is stored when entity is
        deleted.
        """
        return self._field_name + "_old"

    def __repr__(self) -> str:
        return f"DealroomIdentifier(value={self._value})"

    def __eq__(self, other: DealroomIdentifier) -> bool:
        if not isinstance(other, DealroomIdentifier):
            return NotImplemented
        return (self._value, self._field_name) == (other.value, other.field_name)


def determine_identifier(identifier: Union[str, int]) -> Optional[DealroomIdentifier]:
    """Check if input is a valid identifier and return an instance with that value.

    Args:
        identifier: the identifier to check.

    Raises:
        InvalidIdentifier: if it is not a valid ID or UUID.

    Returns:
        the dealroom identifier as an object that holds the value and the names
        of the fields. None input is falsy.
    """
    if not identifier:
        return None

    elif is_valid_id(identifier):
        return DealroomIdentifier(int(identifier))

    elif is_valid_uuid(identifier):
        return DealroomIdentifier(str(identifier))

    else:
        raise InvalidIdentifier(identifier)
