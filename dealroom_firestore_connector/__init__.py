"""Firestore connector for exception handling with Dealroom data"""
import logging
import os
import traceback
from time import sleep
from typing import Any, List, Optional, Tuple, Union, Iterator, Dict
from datetime import datetime, timezone

from google.cloud import firestore
from google.cloud.firestore_v1.collection import CollectionReference
from google.cloud.firestore_v1.document import DocumentReference, DocumentSnapshot
from dealroom_urlextract import extract, InvalidURLFormat

from .batch import Batcher
from .helpers import error_logger, is_valid_id, is_valid_uuid
from .exceptions import FirestoreConnectorError, InvalidIdentifier, exc_handler
from .status_codes import StatusCode
from .identifier import DealroomIdentifier, determine_identifier, DealroomEntity


# Time to sleep in seconds when a exception occurrs until retrying
EXCEPTION_SLEEP_TIME = 5


@exc_handler
def new_connection(
    project: str, credentials_path: Optional[str] = None
) -> firestore.Client:
    """Start a new connection with Firestore.

    Args:
        project: project id of Firestore database.
        credentials_path: path to credentials json file.

    Raises:
        FirestoreConnectorError: if an arbitrary exception occurred. Caught by
            decorator to return error code.

    Returns:
        Firestore db instance or -1 exception (error after retrying a second
        time - from decorator).
    """
    try:
        if credentials_path:
            return firestore.Client.from_service_account_json(credentials_path)
        else:
            return firestore.Client(project=project)

    except Exception as exc:
        __log_exception(5, credentials_path, True)
        raise FirestoreConnectorError("new_connection", exc)


@exc_handler
def get(doc_ref: DocumentReference, *args, **kwargs) -> DocumentSnapshot:
    """Retrieve a document from Firestore

    Args:
        doc_ref: Firestore reference to the document.

    Raises:
        FirestoreConnectorError: if an arbitrary exception occurred after retrying.
            Caught by decorator to return error code.

    Returns:
        Firestore document object or -1 exception (error after retrying a second
        time - from decorator).
    """
    try:
        return doc_ref.get(*args, **kwargs)

    except Exception:
        __log_exception(3, doc_ref)
        sleep(EXCEPTION_SLEEP_TIME)

        try:
            # Retry
            return doc_ref.get(*args, **kwargs)

        except Exception as exc:
            __log_exception(3, doc_ref, True)
            raise FirestoreConnectorError("get", exc)


def _update_last_edit(doc_ref: DocumentReference) -> None:
    """If the document reference points to the history collection
    then update its "last_edit" field to the currrent datetime. This
    datetame is parsed as a Timestamp in the document.

    Args:
        doc_ref: Firestore object reference to the document.
    """
    _path = doc_ref.path.split("/")
    if len(_path) == 2 and _path[0] == "history":
        # why is this using bare firestore update instead of update function?
        doc_ref.update({"last_edit": datetime.now(timezone.utc)})


@exc_handler
def set(doc_ref: DocumentReference, *args, **kwargs) -> StatusCode:
    """Create a new document in Firestore.

    If the document is inside the "history" collection also create
    the "last_edit" timestamp field.

    Args:
        doc_ref: Firestore reference to the document that will be created.

    Raises:
        FirestoreConnectorError: if an arbitrary exception occurred after retrying.
            Caught by decorator to return error code.

    Returns:
        0 success (1st or 2nd time tried) or -1 exception (error after retrying
        a second time - from decorator).
    """
    try:
        doc_ref.set(*args, **kwargs, merge=True)
        _update_last_edit(doc_ref)
        return StatusCode.SUCCESS

    except Exception:
        __log_exception(4, doc_ref)
        sleep(EXCEPTION_SLEEP_TIME)

        try:
            # Retry
            doc_ref.set(*args, **kwargs, merge=True)
            _update_last_edit(doc_ref)
            return StatusCode.SUCCESS

        except Exception as exc:
            __log_exception(4, doc_ref, True)
            raise FirestoreConnectorError("set", exc)


@exc_handler
def update(doc_ref: DocumentReference, *args, **kwargs) -> StatusCode:
    """Update a Firestore document.

    Args:
        doc_ref: Firestore reference to the document that will be updated.

    Raises:
        FirestoreConnectorError: if an arbitrary exception occurred after retrying.
            Caught by decorator to return error code.

    Returns:
        0 success (1st or 2nd time tried) or -1 exception (error after retrying
        a second time - from decorator).
    """
    try:
        doc_ref.update(*args, **kwargs)
        _update_last_edit(doc_ref)
        return StatusCode.SUCCESS

    except Exception:
        __log_exception(2, doc_ref)
        sleep(EXCEPTION_SLEEP_TIME)

        try:
            # Retry
            doc_ref.update(*args, **kwargs)
            _update_last_edit(doc_ref)
            return StatusCode.SUCCESS

        except Exception as exc:
            __log_exception(2, doc_ref, True)
            raise FirestoreConnectorError("update", exc)


@exc_handler
def stream(
    collection_ref: CollectionReference, *args, **kwargs
) -> Iterator[DocumentSnapshot]:
    """Returns a Firestore stream for a specified collection or query.

    Args:
        collection_ref: Firestore reference to a collection.

    Raises:
        FirestoreConnectorError: if an arbitrary exception occurred after retrying.
            Caught by decorator to return error code.

    Returns:
        yields document snapshots or -1 exception (error after retrying a second
        time - from decorator).
    """
    try:
        return collection_ref.stream(*args, **kwargs)

    except Exception:
        __log_exception(1, collection_ref)
        sleep(EXCEPTION_SLEEP_TIME)

        try:
            # Retry
            return collection_ref.stream(*args, **kwargs)

        except Exception as exc:
            __log_exception(1, collection_ref, True)
            raise FirestoreConnectorError("stream", exc)


def get_all(base_query, page_size=20000):
    """Useful to get queries on firestore with too many results (more than 100k),
    that cannot be fetched with the normal .get due to the 60s deadline window.

    Note: for limited queries this will still get all the docs, no matter the limit.

    Args:
        base_query ([type]): The firestore query to get()
        page_size (int, optional): Change this only if you have a valid reason. Usually 2000 can be fetched in the 60s window. Defaults to 2000.

    Returns:
        list: The results of the query

    Examples:
        >>> db = firestore.Client(project="sustained-hold-288413")
        >>> query = db.collections("urls").where("dealroom_id", ">", -1)
        >>> get_all(query)
    """

    def _get_all(res=[], start_at=None):
        query = base_query

        print(f"{len(res)} documents fetched so far.")

        if start_at:
            query = query.start_after(start_at)

        query = query.limit(page_size)
        docs = stream(query)
        if docs == StatusCode.ERROR:
            # TODO: raise Custom Exception (DN-932: https://dealroom.atlassian.net/browse/DN-932)
            return StatusCode.ERROR

        results = [doc_snapshot for doc_snapshot in docs]
        sum_results = [*res, *results]

        has_more_results = len(results) >= page_size
        # If there are more results then we continue to the next batch of .get
        # using start at the last element from the last results.
        if has_more_results:
            start_at_next = results[-1]
            return _get_all(sum_results, start_at_next)
        else:
            return sum_results

    return _get_all()


def collection_exists(collection_ref: CollectionReference) -> bool:
    """A helper method to check whether a collection exists

    Args:
        collection_ref: The reference object to check if exists.

    Returns:
        bool: True if it exists, False otherwise.

    Examples:
        >>> db = new_connection(project=FIRESTORE_PROJECT_ID)
        >>> col_ref = db.collection("MY_COLLECTION")
        >>> print(fc.collection_exists(col_ref))
        False
    """
    docs = get(collection_ref.limit(1))

    if docs == -1:
        logging.error(
            "Couldn't get collection_ref. Please check the logs above for possible errors."
        )
        return False
    return len(docs) > 0


HISTORY_COLLECTION_PATH = "history"


def _filtered_stream(
    collection_ref: CollectionReference, field_path: str, op_string: str, value: Any
) -> List[DocumentSnapshot]:
    """Like stream, but with filters."""
    query = collection_ref.where(field_path, op_string, value)
    docs = stream(query)
    if docs == StatusCode.ERROR:
        raise FirestoreConnectorError("filtered_stream", error_code=StatusCode.ERROR)
    return [doc for doc in docs]


def _filtered_stream_refs(
    collection_ref: CollectionReference, field_path: str, op_string: str, value: Any
) -> List[DocumentReference]:
    """Like stream, but with filters and returns a list of document references."""
    return [
        doc.reference
        for doc in _filtered_stream(collection_ref, field_path, op_string, value)
    ]


@exc_handler
def get_history_doc_refs(
    db: firestore.Client,
    final_url: Optional[str] = None,
    dealroom_id: Union[int, str, None] = None,
) -> Dict[str, List[DocumentReference]]:
    """Match documents on certain fields and return, for each field matched, the
    matching document refs.

    Args:
        db: the client that will perform the operations.
        final_url: A domain. Query documents that match this parameter on fields
            "final_url" and "current_related_urls".
        dealroom_id: A dealroom ID or UUID. Query documents that match this
            parameter on fields "dealroom_id" and "dealroom_id_old", or on
            "dealroom_uuid" and "dealroom_uuid_old" respectively.

    Raises:
        FirestoreConnectorError: if querying matching documents returned error code.
            Caught by decorator to return error code.

    Returns:
        a dictionary made of lists of document references matching the input
        parameter (the values). The keys indicate which fields were matched:
        any of final_url, current_related_urls, dealroom_id, dealroom_id_old
        or dealroom_uuid, dealroom_uuid_old.

    Examples:
        >>> db = new_connection(project=FIRESTORE_PROJECT_ID)
        >>> doc_refs = get_history_refs(db, "dealroom.co")
    """

    if not final_url and not dealroom_id:
        # TODO: raise Custom Exception (DN-932: https://dealroom.atlassian.net/browse/DN-932)
        logging.error(
            "Any of `final_url` or `dealroom_id` need to be used as a unique identifier"
        )
        return StatusCode.ERROR

    try:
        dr_id = determine_identifier(dealroom_id)
    except InvalidIdentifier as exc:
        dr_id = None

    history_ref = db.collection(HISTORY_COLLECTION_PATH)
    result = {}

    # Add results for matched documents over `dealroom_id`
    if dr_id:
        result[dr_id.field_name] = _filtered_stream_refs(
            history_ref, dr_id.field_name, "==", dr_id.value
        )
        result[dr_id.field_name_old] = _filtered_stream_refs(
            history_ref, dr_id.field_name_old, "==", dr_id.value
        )

    # Add results for matched documents over `final_url`
    if final_url:
        # Extract the final_url in the required format, so we can query the collection with the exact match.
        try:
            website_url = extract(final_url)
        except InvalidURLFormat as exc:
            # TODO: raise Custom Exception (DN-932: https://dealroom.atlassian.net/browse/DN-932)
            logging.error(f"'final_url': {final_url} is not a valid url: {exc}")
            return StatusCode.ERROR

        result["final_url"] = _filtered_stream_refs(
            history_ref, "final_url", "==", website_url
        )

        result["current_related_urls"] = _filtered_stream_refs(
            history_ref, "current_related_urls", "array_contains", website_url
        )

    return result


def _validate_dealroom_id(dealroom_id: Union[str, int]) -> None:
    # this validation function was changed to ensure that the following new test
    # passes: test_set_history_doc_refs_as_deleted_on_id_0
    allowed = [-1, -2, "-1", "-2"]
    if not is_valid_id(dealroom_id) and dealroom_id not in allowed:
        raise ValueError(
            f"'dealroom_id'={dealroom_id} must be an integer and bigger than -2. Use -2 for deleted entities & -1 for not dealroom entities"
        )


def _validate_dealroom_uuid(dealroom_uuid: Union[str, int]) -> None:
    allowed = [-1, -2, "-1", "-2"]
    if not is_valid_uuid(dealroom_uuid) and dealroom_uuid not in allowed:
        raise ValueError(
            f"'dealroom_uuid'={dealroom_uuid} must be a valid UUID or use -2 for deleted entities & -1 for not dealroom entities"
        )


def _validate_final_url(final_url: str) -> None:
    # Empty string for final_url is valid in case that this entity, doesn't use that url anymore.
    if final_url == "":
        return

    # Extract method has internally validation rules. Check here:
    # https://github.com/dealroom/data-urlextract/blob/main/dealroom_urlextract/__init__.py#L33-L35
    try:
        extract(final_url)
    except InvalidURLFormat as exc:
        raise ValueError(f"'final_url'={final_url} must have a url-like format: {exc}")


def _validate_new_history_doc_payload(payload: dict) -> None:
    """Validate the required fields in the payload when creating a new document"""

    if "final_url" not in payload:
        raise KeyError("'final_url' must be present in payload")
    _validate_final_url(payload["final_url"])

    if "dealroom_id" not in payload and "dealroom_uuid" not in payload:
        raise KeyError(
            "at least one of 'dealroom_id', 'dealroom_uuid' must be present in payload"
        )

    if "dealroom_id" in payload:
        _validate_dealroom_id(payload["dealroom_id"])
    if "dealroom_uuid" in payload:
        _validate_dealroom_uuid(payload["dealroom_uuid"])

    # Validate that there is one of final_url, dealroom_id, dealroom_uuid as a unique identifier
    empty_final_url = not payload.get("final_url")
    empty_dealroom_id = (
        payload.get("dealroom_id", DealroomEntity.NOT_IN_DB) == DealroomEntity.NOT_IN_DB
    )
    empty_dealroom_uuid = (
        payload.get("dealroom_uuid", DealroomEntity.NOT_IN_DB)
        == DealroomEntity.NOT_IN_DB
    )
    if empty_final_url and empty_dealroom_id and empty_dealroom_uuid:
        raise ValueError(
            "There is no unique identifier for this document. `final_url`, `dealroom_id` and `dealroom_uuid` are all empty."
        )


def _validate_update_history_doc_payload(payload: dict) -> None:
    """Validate the required fields in the payload when updating"""

    if "final_url" in payload:
        _validate_final_url(payload["final_url"])

    if "dealroom_id" in payload:
        _validate_dealroom_id(payload["dealroom_id"])

    if "dealroom_uuid" in payload:
        _validate_dealroom_uuid(payload["dealroom_uuid"])


def _get_final_url_and_dealroom_id(
    payload: dict, finalurl_or_dealroomid: Optional[str] = None
) -> Tuple[str, Union[int, DealroomIdentifier]]:
    """Retrieve the final_url from `payload` and/or `finalurl_or_dealroomid`
    but not dealroom_id because:
    - either we want to update an existing doc matching by final_url and dealroom_id=-1
    - or we don't want to override an existing dealroom_id>0 matching by final_url https://dealroom.atlassian.net/browse/DS2-104
    """

    final_url, dealroom_id = "", DealroomEntity.NOT_IN_DB.value

    # If finalurl_or_dealroomid is not set then try to find them in payload
    if not finalurl_or_dealroomid:
        final_url = payload.get("final_url") or final_url
        return final_url, dealroom_id

    try:
        dr_id = determine_identifier(finalurl_or_dealroomid)
    except InvalidIdentifier:
        return finalurl_or_dealroomid or "", dealroom_id
    else:
        dealroom_id = dr_id or dealroom_id
        final_url = payload.get("final_url") or final_url
        return final_url, dealroom_id


def check_for_deleted_profiles(
    doc_refs: List[DocumentReference],
    identifier: DealroomIdentifier,
    count_history_refs: int,
) -> int:
    """Decrease the input counter for any doc in input list that represents a
    deleted entity.
    """
    value = identifier.value
    field_name = identifier.field_name
    field_name_old = identifier.field_name_old

    # https://dealroom.atlassian.net/browse/DS2-154
    for doc_ref in doc_refs:
        doc = doc_ref.get().to_dict()
        if not doc:
            continue

        is_a_deleted_entity = doc.get(field_name) == DealroomEntity.DELETED
        # this check is useless, but we'll keep it
        dealroom_id_was_already_used = doc.get(field_name_old) == value
        if is_a_deleted_entity and not dealroom_id_was_already_used:
            # Substract 1 meaning that for the current doc matching this final_url, it was deleted
            # but this is a new company. In other words, the dealroom id for this company was never
            # present in the history collection.
            count_history_refs -= 1
    return count_history_refs


def check_for_in_progress_profiles(
    doc_refs: List[DocumentReference],
    identifier: DealroomIdentifier,
    count_history_refs: int,
) -> int:
    """Decrease the input counter for any doc in input list that represents an
    in-progress entity (id = -1).
    """
    field_name = identifier.field_name

    for doc_ref in doc_refs:
        doc = doc_ref.get().to_dict()
        if not doc:
            continue

        is_an_in_progress_entity = doc.get(field_name) == DealroomEntity.NOT_IN_DB
        if not is_an_in_progress_entity:
            # Substract 1 meaning that for the current doc matching this final_url, it has already a dealroom_id>0
            # but this is a new company. In other words, the dealroom id for this company was never
            # present in the history collection, but a new one with the same final_url has been created.
            count_history_refs -= 1
    return count_history_refs


def set_history_doc_refs(
    db: firestore.Client, payload: dict, finalurl_or_dealroomid: str = None
) -> StatusCode:
    """Updates or creates a document in history collection

    Args:
        db: the client that will perform the operations.
        payload: The actual data that the newly created document will have or
            the fields to update. Any of 'final_url', 'dealroom_id' or
            'dealroom_uuid' is required to find the correct document to set.
        finalurl_or_dealroomid: either a domain, a dealroom ID or a dealroom UUID.
            Query documents that match this parameter.

    Returns:
        integer code to signify what operation was carried out.

    Examples:
        >>> db = new_connection(project=FIRESTORE_PROJECT_ID)
        >>> set_history_refs(db, {"final_url": "dealroom.co", "dealroom_id": "1111111")
    """

    history_col = db.collection(HISTORY_COLLECTION_PATH)

    _payload = {**payload}

    history_refs = {}

    # lookup for the document using both identifiers, final_url & dealroom_id
    final_url, dealroom_id = _get_final_url_and_dealroom_id(
        payload, finalurl_or_dealroomid
    )

    if isinstance(dealroom_id, DealroomIdentifier):
        value = dealroom_id.value
    else:
        value = dealroom_id

    history_refs = get_history_doc_refs(db, final_url, value)
    if history_refs == StatusCode.ERROR:
        # TODO: raise Custom Exception (DN-932: https://dealroom.atlassian.net/browse/DN-932)
        return StatusCode.ERROR

    operation_status_code = StatusCode.ERROR
    key_found = None

    if "dealroom_id" in history_refs and len(history_refs["dealroom_id"]) > 0:
        count_history_refs = len(history_refs["dealroom_id"])
        key_found = "dealroom_id"
    elif "dealroom_id_old" in history_refs and len(history_refs["dealroom_id_old"]) > 0:
        count_history_refs = len(history_refs["dealroom_id_old"])
        key_found = "dealroom_id_old"

    elif "dealroom_uuid" in history_refs and len(history_refs["dealroom_uuid"]) > 0:
        count_history_refs = len(history_refs["dealroom_uuid"])
        key_found = "dealroom_uuid"
    elif (
        "dealroom_uuid_old" in history_refs
        and len(history_refs["dealroom_uuid_old"]) > 0
    ):
        count_history_refs = len(history_refs["dealroom_uuid_old"])
        key_found = "dealroom_uuid_old"

    elif "final_url" in history_refs and len(history_refs["final_url"]) > 0:
        count_history_refs = len(history_refs["final_url"])
        key_found = "final_url"
    elif (
        "current_related_urls" in history_refs
        and len(history_refs["current_related_urls"]) > 0
    ):
        count_history_refs = len(history_refs["current_related_urls"])
        key_found = "current_related_urls"

    else:
        count_history_refs = 0

    document_matches_by_final_url = key_found == "final_url"
    if document_matches_by_final_url and isinstance(dealroom_id, DealroomIdentifier):
        count_history_refs = check_for_deleted_profiles(
            history_refs[key_found], dealroom_id, count_history_refs
        )
        count_history_refs = check_for_in_progress_profiles(
            history_refs[key_found], dealroom_id, count_history_refs
        )

    # CREATE: If there are not available documents in history
    if count_history_refs <= 0:
        if isinstance(dealroom_id, DealroomIdentifier):
            # TODO: Add UUID/ID depending on what identifier was passed (DS2-285: https://dealroom.atlassian.net/browse/DS2-285)
            _payload = {
                dealroom_id.field_name: dealroom_id.value,
                "final_url": "",
                **payload,
            }
        else:
            _payload = {
                "dealroom_id": DealroomEntity.NOT_IN_DB.value,
                "dealroom_uuid": DealroomEntity.NOT_IN_DB.value,
                "final_url": finalurl_or_dealroomid,
                **payload,
            }

        # Validate that the new document will have the minimum required fields
        try:
            _validate_new_history_doc_payload(_payload)
        except (ValueError, KeyError) as ex:
            # TODO: raise Custom Exception (DN-932: https://dealroom.atlassian.net/browse/DN-932)
            logging.error(ex)
            return StatusCode.ERROR
        history_ref = history_col.document()
        operation_status_code = StatusCode.CREATED

    # UPDATE:
    elif count_history_refs == 1:
        try:
            _validate_update_history_doc_payload(_payload)
        except ValueError as ex:
            # TODO: raise Custom Exception (DN-932: https://dealroom.atlassian.net/browse/DN-932)
            logging.error(ex)
            return StatusCode.ERROR

        history_ref = history_refs[key_found][0]
        operation_status_code = StatusCode.UPDATED
    # If more than one document were found then it's an error.
    else:
        # TODO: Raise a Custom Exception (DuplicateDocumentsException) with the same message when we replace ERROR constant with actual exceptions
        #   (DN-932: https://dealroom.atlassian.net/browse/DN-932)
        logging.error("Found more than one documents to update for this payload")
        return StatusCode.ERROR

    # Ensure that dealroom_id is type of number
    if "dealroom_id" in _payload:
        _payload["dealroom_id"] = int(_payload["dealroom_id"])

    res = set(history_ref, _payload)
    if res == StatusCode.ERROR:
        # TODO: Raise a Custom Exception (FirestoreException) with the same message when we replace ERROR constant with actual exceptions
        #   (DN-932: https://dealroom.atlassian.net/browse/DN-932)
        logging.error(
            f"Couldn't `set` document {finalurl_or_dealroomid}. Please check logs above."
        )
        return StatusCode.ERROR

    return operation_status_code


def __log_exception(
    error_code: int, ref: DocumentReference, was_retried: bool = False
) -> None:
    message = "Unknown error"
    if error_code == 1:
        message = f"An error occurred retrieving stream for collection {ref.path}."
    elif error_code == 2:
        message = f"An error occurred updating document {ref.path}."
    elif error_code == 3:
        message = f"An error occurred getting document {ref.path}."
    elif error_code == 4:
        message = f"An error occurred creating document {ref.path}."
    elif error_code == 5:
        message = f"Error connecting with db with credentials file {ref.path}."

    if was_retried:
        # TODO save to csv or json
        error_logger(message, error_code)
    else:
        logging.error(f"[Error code {error_code}] {message} Retrying...")


# The name of this function is completely misleading: it returns snapshots, not
# references. Not changing it to avoid breaking-changes.
@exc_handler
def get_people_doc_refs(
    db: firestore.Client, field_name: str, operator: str, field_value: Any
) -> Optional[List[DocumentSnapshot]]:
    """Query 'people' collection for a document whose 'field_name' has 'operator'
    relation with 'field_value'.

    Args:
        field_name: the field to query for.
        operator: determines the condition for matching ('==', ...).
        field_value: the value that satisfies the condition.

    Raises:
        FirestoreConnectorError: if querying matching documents returned error code.
            Caught by decorator to return error code.

    Returns:
        a list of matching document snapshots. None if no documents are found.

    Examples:
        >>> fc.get_people_doc_refs(db, "dealroom_id", "==", 1169416)[0].to_dict()
        >>> fc.get_people_doc_refs(db, "linkedin", "array_contains", "https://www.linkedin.com/in/vess/")[0].to_dict()
    """

    people_ref = db.collection("people")
    matching_docs = [
        doc for doc in _filtered_stream(people_ref, field_name, operator, field_value)
    ]

    # This is super weird, why return None when you can return a PERFECTLY EMPTY list?
    # Not changing it to avoid breaking-changes.
    if len(matching_docs) == 0:
        return None

    return matching_docs


def _validate_new_people_doc_payload(payload: dict):
    """Validate the required fields in the payload when creating a new document in the people collection"""

    if "dealroom_id" not in payload:
        if "dealroom_uuid" not in payload:
            raise KeyError(
                "at least one of 'dealroom_id', 'dealroom_uuid' must be present in payload"
            )
        else:
            _validate_dealroom_uuid(payload["dealroom_uuid"])
    else:
        _validate_dealroom_id(payload["dealroom_id"])
        if "dealroom_uuid" in payload:
            _validate_dealroom_uuid(payload["dealroom_uuid"])


# This method is also very very weird, I hope it is not used in many places:
# * dealroom_jd and dealroom_uuid are always set to -1, even if we pass perfectly
# good params in payload.
# * maybe intentional, but we do not take care of inserting dealroom_id in the
# payload if that is the field we are matching by
def set_people_doc_ref(
    db: firestore.Client,
    field_name: str,
    operator: str,
    field_value,
    payload: dict,
) -> StatusCode:
    """Updates or Creates a single document from 'people' collection with 'payload' where 'field_name' has 'operator'
    relation with 'field_value'.

    Args:
        field_name: the field to query for.
        operator: determines the condition for matching ('==', ...).
        field_value: the value that satisfies the condition.
        payload: The actual data that the newly created document will have OR the fields to update.

    Returns:
        integer signifying what the status of the operation is.

    Examples:
        >>> fc.set_people_doc_ref(db, "dealroom_id", "==", 1003809000, {"foo":"bar"})
        >>> fc.set_people_doc_ref(db, "linkedin", "array_contains", "https://www.linkedin.com/in/vess/", {"foo":["bar",2]})
    """
    people_collection_ref = db.collection("people")

    _payload = {**payload}

    people_refs = (
        get_people_doc_refs(db, field_name, operator, field_value)
        if field_name and operator and field_value
        else []
    )
    matching_docs = len(people_refs) if people_refs else 0

    # CREATE: If there are not matching documents in people
    if matching_docs == 0:
        # Validate that the new document will have the minimum required fields
        try:
            _validate_new_people_doc_payload(_payload)
        except (ValueError, KeyError) as ex:
            # TODO: raise Custom Exception (DN-932: https://dealroom.atlassian.net/browse/DN-932)
            logging.error(ex)
            return StatusCode.ERROR
        people_doc_ref = people_collection_ref.document()
        operation_status_code = StatusCode.CREATED

    # UPDATE:
    elif matching_docs == 1:
        # Ensure that dealroom_id is type of number if it appears on the payload
        if "dealroom_id" in _payload:
            _validate_dealroom_id(_payload["dealroom_id"])
        people_doc_ref = people_refs[0].reference
        operation_status_code = StatusCode.UPDATED
    # If more than one document were found then it's an error.
    else:
        # TODO: Raise a Custom Exception (DuplicateDocumentsException) with the same message when we replace ERROR constant with actual exceptions
        #   (DN-932: https://dealroom.atlassian.net/browse/DN-932)
        logging.error("Found more than one documents to update for this payload")
        return StatusCode.ERROR

    res = set(people_doc_ref, _payload)
    if res == StatusCode.ERROR:
        # TODO: Raise a Custom Exception (FirestoreException) with the same message when we replace ERROR constant with actual exceptions
        #   (DN-932: https://dealroom.atlassian.net/browse/DN-932)
        logging.error(
            f"Couldn't `set` document {people_doc_ref.id}. Please check logs above."
        )
        return StatusCode.ERROR

    return operation_status_code
