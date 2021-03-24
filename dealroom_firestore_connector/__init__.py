""" Firestore connector for exception handling with Dealroom data"""
import logging
import os
import traceback
from time import sleep
from typing import Tuple, Union

from dealroom_urlextract import extract
from google.cloud import firestore
from google.cloud.firestore_v1.collection import CollectionReference
from google.cloud.firestore_v1.document import DocumentReference

from .batch import Batcher
from .helpers import error_logger
from .status_codes import ERROR, SUCCESS, CREATED, UPDATED
from datetime import datetime, timezone

# Time to sleep in seconds when a exception occurrs until retrying
EXCEPTION_SLEEP_TIME = 5


def new_connection(project: str, credentials_path: str = None):
    """Start a new connection with Firestore.
    Args:
        project (str): project id of Firestore database
        credentials_path (str): path to credentials json file
    Returns:
        [object]: Firestore db instance
    """
    try:
        if credentials_path:
            return firestore.Client.from_service_account_json(credentials_path)
        else:
            return firestore.Client(project=project)
    except Exception as identifier:
        __log_exception(5, credentials_path, identifier, True)
        return ERROR


def get(doc_ref, *args, **kwargs):
    """Retrieve a document from Firestore
    Args:
        doc_ref (object): Firestore reference to the document.
    Returns:
        [object]: Firestore document object
        -1 [int]: exception (error after retrying a second time)
    """
    try:
        return doc_ref.get(*args, **kwargs)
    except Exception as identifier:
        # log error
        __log_exception(3, doc_ref, identifier)
        # Wait before continue
        sleep(EXCEPTION_SLEEP_TIME)
        try:
            # Retry
            return doc_ref.get(*args, **kwargs)
        except Exception as identifier:
            # log error
            __log_exception(3, doc_ref, identifier, True)
            return ERROR

def _update_last_edit(doc_ref):
    """If the document reference points to the history collection
    then update its "last_edit" field to the currrent datetime. This
    datetame is parsed as a Timestamp in the document.

    Args:
        doc_ref: Firestore object reference to the document
    """
    _path = doc_ref.path.split("/")
    if len(_path) == 2 and _path[0] == "history":
        doc_ref.update({"last_edit": datetime.now(timezone.utc)})

def set(doc_ref, *args, **kwargs):
    """Create a new document in Firestore

    If the document is inside the "history" collection also create
    the "last_edit" timestamp field.

    Args:
        doc_ref (object): Firestore reference to the document that will be created.
    Returns:
        0 [int]: success (1st or 2nd time tried)
        -1 [int]: exception (error after retrying a second time)
    """
    try:
        doc_ref.set(*args, **kwargs, merge=True)
        _update_last_edit(doc_ref)
        return SUCCESS
    except Exception as identifier:
        # log error
        __log_exception(4, doc_ref, identifier)
        # Wait before continue
        sleep(EXCEPTION_SLEEP_TIME)
        try:
            # Retry
            doc_ref.set(*args, **kwargs, merge=True)
            _update_last_edit(doc_ref)
            return SUCCESS
        except Exception as identifier:
            # log error
            __log_exception(4, doc_ref, identifier, True)
            return ERROR


def update(doc_ref, *args, **kwargs):
    """Update a Firestore document.
    Args:
        doc_ref (object): Firestore reference to the document that will be updated.
    Returns:
        0 [int]: success (1st or 2nd time tried)
        -1 [int]: exception (error after retrying a second time)
    """
    try:
        doc_ref.update(*args, **kwargs)
        _update_last_edit(doc_ref)
        return SUCCESS
    except Exception as identifier:
        # log error
        __log_exception(2, doc_ref, identifier)
        # Wait before continue
        sleep(EXCEPTION_SLEEP_TIME)
        try:
            # Retry
            doc_ref.update(*args, **kwargs)
            _update_last_edit(doc_ref)
            return SUCCESS
        except Exception as identifier:
            # log error
            __log_exception(2, doc_ref, identifier, True)
            return ERROR


def stream(collection_ref, *args, **kwargs):
    """Returns a Firestore stream for a specified collection or query.
    Args:
        collection_ref (object): Firestore reference to a collection
    Returns:
        stream [object]: Firestore stream object.
        -1 [int]: exception (error after retrying a second time)
    """
    try:
        return collection_ref.stream(*args, **kwargs)
    except Exception as identifier:
        # log error
        __log_exception(1, collection_ref, identifier)
        # Wait before continue
        sleep(EXCEPTION_SLEEP_TIME)
        try:
            # Retry
            return collection_ref.stream(*args, **kwargs)
        except Exception as identifier:
            # log error
            __log_exception(1, collection_ref, identifier, True)
            return ERROR


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
        if docs == ERROR:
            return ERROR

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


def collection_exists(collection_ref: CollectionReference):
    """A helper method to check whether a collection exists

    Args:
        collection_ref (CollectionReference): The reference object to check if exists

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


def get_history_doc_refs(db: firestore.Client, websiteurl_or_dealroomid: str):
    """Returns a DocumentReference based on the final_url field, current_related_urls or dealroom_id
    field.

    Args:
        db (firestore.Client): the client that will perform the operations.
        websiteurl_or_dealroomid (str): either a domain or a dealroom ID. Query documents that match this parameter.

    Returns:
        dict[DocumentReference]: a dictionary made of lists of document references matching the input parameter
            indicating if the match occured with the final_url field, current_related_urls or dealroom_id.

    Examples:
        >>> db = new_connection(project=FIRESTORE_PROJECT_ID)
        >>> doc_refs = get_history_refs(db, "dealroom.co")
    """

    collection_ref = db.collection(HISTORY_COLLECTION_PATH)
    result = {}

    if str(websiteurl_or_dealroomid).isnumeric():
        query_params = ["dealroom_id", "==", int(websiteurl_or_dealroomid)]
        query = collection_ref.where(*query_params)
        docs = stream(query)
        if docs == ERROR:
            logging.error("Couldn't stream query.")
            return ERROR
        result["dealroom_id"] = [doc.reference for doc in docs]
    else:
        # Extract the final_url in the required format, so we can query the collection with the exact match.
        try:
            website_url = extract(websiteurl_or_dealroomid)
        except:
            logging.error(f"'final_url': {websiteurl_or_dealroomid} is not a valid url")
            return ERROR

        query_params = ["final_url", "==", website_url]
        query = collection_ref.where(*query_params)
        docs = stream(query)
        if docs == ERROR:
            logging.error("Couldn't stream query.")
            return ERROR
        result["final_url"] = [doc.reference for doc in docs]

        query_params = ["current_related_urls", "array_contains", website_url]
        query = collection_ref.where(*query_params)
        docs = stream(query)
        if docs == ERROR:
            logging.error("Couldn't stream query.")
            return ERROR
        result["current_related_urls"] = [doc.reference for doc in docs]

    return result


# We mark the deleted entities from DR database with -2 entity so we can easily identify them.
_DELETED_DEALROOM_ENTITY_ID = -2
# We mark the entities that don't exist in DR with -1.
_NOT_IN_DEALROOM_ENTITY_ID = -1


def _validate_dealroomid(dealroom_id: Union[str, int]):
    try:
        # This will raise a ValueError in case that dealroom_id is not an integer
        int_dealroom_id = int(dealroom_id)
        if int_dealroom_id < -2:
            raise ValueError
    except ValueError:
        raise ValueError(
            "'dealroom_id' must be an integer and bigger than -2. Use -2 for deleted entities & -1 for not dealroom entities"
        )


def _validate_final_url(final_url: str):
    # Empty string for final_url is valid in case that this entity, doesn't use that url anymore.
    if final_url == "":
        return

    # Extract method has internally validation rules. Check here:
    # https://github.com/dealroom/data-urlextract/blob/main/dealroom_urlextract/__init__.py#L33-L35
    try:
        extract(final_url)
    except:
        raise ValueError("'final_url' must have a url-like format")


def _validate_new_history_doc_payload(payload: dict):
    """Validate the required fields in the payload when creating a new document"""

    if "final_url" not in payload:
        raise KeyError("'final_url' must be present payload")

    _validate_final_url(payload["final_url"])

    if "dealroom_id" not in payload:
        raise KeyError("'dealroom_id' must be present payload")

    _validate_dealroomid(payload["dealroom_id"])


def _validate_update_history_doc_payload(payload: dict):
    """Validate the required fields in the payload when updating"""

    if "final_url" in payload:
        _validate_final_url(payload["final_url"])

    if "dealroom_id" in payload:
        _validate_dealroomid(payload["dealroom_id"])


def set_history_doc_refs(
    db: firestore.Client, payload: dict, finalurl_or_dealroomid: str = None
) -> None:
    """Updates or creates a document in history collection

    Args:
        db (firestore.Client): the client that will perform the operations.
        payload (dict): The actual data that the newly created will have or the fields to update.
                        'final_url' or 'dealroom_id' is required to find the correct document to set.
        finalurl_or_dealroomid (str): either a domain or a dealroom ID. Query documents that match this parameter.

    Examples:
        >>> db = new_connection(project=FIRESTORE_PROJECT_ID)
        >>> set_history_refs(db, {"final_url": "dealroom.co", "dealroom_id": "1111111")
    """

    history_col = db.collection(HISTORY_COLLECTION_PATH)

    _payload = {**payload}

    # If finalurl_or_dealroomid is not provided then a new document will be created.
    history_refs = get_history_doc_refs(db, finalurl_or_dealroomid) if finalurl_or_dealroomid else {}

    operation_status_code = ERROR

    if history_refs == ERROR:
        return ERROR
    if "dealroom_id" in history_refs:
        count_history_refs = len(history_refs["dealroom_id"])
        key_found = "dealroom_id"
    elif "final_url" in history_refs and len(history_refs["final_url"]) > 0:
        count_history_refs = len(history_refs["final_url"])
        key_found = "final_url"
    elif "current_related_urls" in history_refs and len(history_refs["current_related_urls"]) > 0:
        count_history_refs = len(history_refs["current_related_urls"])
        key_found = "current_related_urls"
    else:
        count_history_refs = 0
    # CREATE: If there are not available documents in history
    if count_history_refs == 0:
        # Add any default values to the payload
        _payload = {
            "dealroom_id": _NOT_IN_DEALROOM_ENTITY_ID,
            "final_url": "",
            **payload,
        }
        # Validate that the new document will have the minimum required fields
        try:
            _validate_new_history_doc_payload(_payload)
        except (ValueError, KeyError) as ex:
            logging.error(ex)
            return ERROR
        history_ref = history_col.document()
        operation_status_code = CREATED

    # UPDATE:
    elif count_history_refs == 1:
        try:
            _validate_update_history_doc_payload(_payload)
        except ValueError as ex:
            logging.error(ex)
            return ERROR

        history_ref = history_refs[key_found][0]
        operation_status_code = UPDATED
    # If more than one document were found then it's an error.
    else:
        # TODO: Raise a Custom Exception (DuplicateDocumentsException) with the same message when we replace ERROR constant with actual exceptions
        logging.error("Found more than one documents to update for this payload")
        return ERROR

    # Ensure that dealroom_id is type of number
    if "dealroom_id" in _payload:
        _payload["dealroom_id"] = int(_payload["dealroom_id"])
    res = set(history_ref, _payload)

    if res == ERROR:
        # TODO: Raise a Custom Exception (FirestoreException) with the same message when we replace ERROR constant with actual exceptions
        logging.error(
            f"Couldn't `set` document {finalurl_or_dealroomid}. Please check logs above."
        )
        return ERROR

    return operation_status_code


def __log_exception(error_code, ref, identifier, was_retried=False):
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
