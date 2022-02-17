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
        # TODO: raise Custom Exception (DN-932: https://dealroom.atlassian.net/browse/DN-932)
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
            # TODO: raise Custom Exception (DN-932: https://dealroom.atlassian.net/browse/DN-932)
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
            # TODO: raise Custom Exception (DN-932: https://dealroom.atlassian.net/browse/DN-932)
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
            # TODO: raise Custom Exception (DN-932: https://dealroom.atlassian.net/browse/DN-932)
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
            # TODO: raise Custom Exception (DN-932: https://dealroom.atlassian.net/browse/DN-932)
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
            # TODO: raise Custom Exception (DN-932: https://dealroom.atlassian.net/browse/DN-932)
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


def get_history_doc_refs(
    db: firestore.Client, final_url: str = None, dealroom_id: int = None
):
    """Returns a DocumentReference based on the final_url field, current_related_urls or dealroom_id
    field.

    Args:
        db (firestore.Client): the client that will perform the operations.
        final_url (str): A domain. Query documents that match this parameter.
        dealroom_id (int): A dealroom ID. Query documents that match this parameter.
    Returns:
        dict[DocumentReference]: a dictionary made of lists of document references matching the input parameter
            indicating if the match occured with the final_url field, current_related_urls or dealroom_id.

    Examples:
        >>> db = new_connection(project=FIRESTORE_PROJECT_ID)
        >>> doc_refs = get_history_refs(db, "dealroom.co")
    """

    is_valid_dealroom_id = dealroom_id and int(dealroom_id) > 0
    if not final_url and not is_valid_dealroom_id:
        raise ValueError(
            "Any of `final_url` or `dealroom_id` need to be used as a unique identifier"
        )

    collection_ref = db.collection(HISTORY_COLLECTION_PATH)
    result = {}

    # Add results for matched documents over `dealroom_id`
    if is_valid_dealroom_id:
        query_params = ["dealroom_id", "==", int(dealroom_id)]
        query = collection_ref.where(*query_params)
        docs = stream(query)
        if docs == ERROR:
            # TODO: raise Custom Exception (DN-932: https://dealroom.atlassian.net/browse/DN-932)
            logging.error("Couldn't stream query.")
            return ERROR
        result["dealroom_id"] = [doc.reference for doc in docs]

        query_params = ["dealroom_id_old", "==", int(dealroom_id)]
        query = collection_ref.where(*query_params)
        docs = stream(query)
        if docs == ERROR:
            # TODO: raise Custom Exception (DN-932: https://dealroom.atlassian.net/browse/DN-932)
            logging.error("Couldn't stream query.")
            return ERROR
        result["dealroom_id_old"] = [doc.reference for doc in docs]

    # Add results for matched documents over `final_url`
    if final_url:
        # Extract the final_url in the required format, so we can query the collection with the exact match.
        try:
            website_url = extract(final_url)
        except:
            # TODO: raise Custom Exception (DN-932: https://dealroom.atlassian.net/browse/DN-932)
            logging.error(f"'final_url': {final_url} is not a valid url")
            return ERROR

        query_params = ["final_url", "==", website_url]
        query = collection_ref.where(*query_params)
        docs = stream(query)
        if docs == ERROR:
            # TODO: raise Custom Exception (DN-932: https://dealroom.atlassian.net/browse/DN-932)
            logging.error("Couldn't stream query.")
            return ERROR
        result["final_url"] = [doc.reference for doc in docs]

        query_params = ["current_related_urls", "array_contains", website_url]
        query = collection_ref.where(*query_params)
        docs = stream(query)
        if docs == ERROR:
            # TODO: raise Custom Exception (DN-932: https://dealroom.atlassian.net/browse/DN-932)
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

    # Validate that there is either a final_url and/or dealroom_id as a unique identifier
    if not payload.get("final_url", "") and (
        payload.get("dealroom_id", _NOT_IN_DEALROOM_ENTITY_ID)
        == _NOT_IN_DEALROOM_ENTITY_ID
    ):
        raise ValueError(
            "There is no unique identifier for this document. `final_url` & `dealroom_id` are empty."
        )


def _validate_update_history_doc_payload(payload: dict):
    """Validate the required fields in the payload when updating"""

    if "final_url" in payload:
        _validate_final_url(payload["final_url"])

    if "dealroom_id" in payload:
        _validate_dealroomid(payload["dealroom_id"])


def _get_final_url_and_dealroom_id(
    payload: dict, finalurl_or_dealroomid: str = None
) -> tuple:
    """Retrieve the final_url
    from `payload` and/or `finalurl_or_dealroomid`
    but not dealroom_id because
    - we want to update an existing doc matching by final_url and dealroom_id=-1
    or
    - we don't want to override an existing dealroom_id>0 matching by final_url https://dealroom.atlassian.net/browse/DS2-104
    """

    final_url, dealroom_id = "", _NOT_IN_DEALROOM_ENTITY_ID
    # If finalurl_or_dealroomid is not set then try to find them in payload
    if not finalurl_or_dealroomid:
        final_url = payload.get("final_url", None) or final_url
    # otherwise combine them
    elif is_dealroom_id := str(finalurl_or_dealroomid).isnumeric():
        dealroom_id = int(finalurl_or_dealroomid)
        final_url = payload.get("final_url", None) or final_url
    else:
        final_url = finalurl_or_dealroomid

    return final_url, dealroom_id


def check_for_deleted_profiles(
    doc_refs, dealroom_id, count_history_refs
):
    # https://dealroom.atlassian.net/browse/DS2-154
    # check all matching docs
    for doc_ref in doc_refs:
        doc = doc_ref.get().to_dict()
        is_a_deleted_entity = doc["dealroom_id"] == _DELETED_DEALROOM_ENTITY_ID
        dealroom_id_was_already_used = doc.get("dealroom_id_old") == dealroom_id
        if (
            is_a_deleted_entity
            and dealroom_id > 0
            and not dealroom_id_was_already_used
        ):
            # Substract 1 meaning that for the current doc matching this final_url, it was deleted
            # but this is a new company. In other words, the dealroom id for this company was never
            # present in the history collection.
            count_history_refs -= 1
    return count_history_refs

def check_for_in_progress_profiles(
    doc_refs, dealroom_id, count_history_refs
):
    # check all matching docs
    for doc_ref in doc_refs:
        doc = doc_ref.get().to_dict()
        is_an_in_progress_entity = doc["dealroom_id"] == _NOT_IN_DEALROOM_ENTITY_ID
        if (
            not is_an_in_progress_entity
            and dealroom_id > 0
        ):
            # Substract 1 meaning that for the current doc matching this final_url, it has already a dealroom_id>0
            # but this is a new company. In other words, the dealroom id for this company was never
            # present in the history collection, but a new one with the same final_url has been created.
            count_history_refs -= 1
    return count_history_refs


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

    history_refs = {}

    # lookup for the document using both identifiers, final_url & dealroom_id
    final_url, dealroom_id = _get_final_url_and_dealroom_id(
        payload, finalurl_or_dealroomid
    )
    history_refs = get_history_doc_refs(db, final_url, dealroom_id)

    operation_status_code = ERROR
    key_found = None

    if history_refs == ERROR:
        # TODO: raise Custom Exception (DN-932: https://dealroom.atlassian.net/browse/DN-932)
        return ERROR
    if "dealroom_id" in history_refs and len(history_refs["dealroom_id"]) > 0:
        count_history_refs = len(history_refs["dealroom_id"])
        key_found = "dealroom_id"
    elif "dealroom_id_old" in history_refs and len(history_refs["dealroom_id_old"]) > 0:
        count_history_refs = len(history_refs["dealroom_id_old"])
        key_found = "dealroom_id_old"
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
    if document_matches_by_final_url:
        count_history_refs = check_for_deleted_profiles(
            history_refs[key_found], dealroom_id, count_history_refs
        )
        count_history_refs = check_for_in_progress_profiles(
            history_refs[key_found], dealroom_id, count_history_refs
        )

    # CREATE: If there are not available documents in history
    if count_history_refs <= 0:
        # Add any default values to the payload
        is_dealroom_id = str(finalurl_or_dealroomid).isnumeric()
        # Set `dealroom_id` or `final_url` with a default value from the given identifier
        _payload = {
            "dealroom_id": finalurl_or_dealroomid
            if is_dealroom_id
            else _NOT_IN_DEALROOM_ENTITY_ID,
            "final_url": finalurl_or_dealroomid if not is_dealroom_id else "",
            **payload,
        }

        # Validate that the new document will have the minimum required fields
        try:
            _validate_new_history_doc_payload(_payload)
        except (ValueError, KeyError) as ex:
            # TODO: raise Custom Exception (DN-932: https://dealroom.atlassian.net/browse/DN-932)
            logging.error(ex)
            return ERROR
        history_ref = history_col.document()
        operation_status_code = CREATED

    # UPDATE:
    elif count_history_refs == 1:
        try:
            _validate_update_history_doc_payload(_payload)
        except ValueError as ex:
            # TODO: raise Custom Exception (DN-932: https://dealroom.atlassian.net/browse/DN-932)
            logging.error(ex)
            return ERROR

        history_ref = history_refs[key_found][0]
        operation_status_code = UPDATED
    # If more than one document were found then it's an error.
    else:
        # TODO: Raise a Custom Exception (DuplicateDocumentsException) with the same message when we replace ERROR constant with actual exceptions
        #   (DN-932: https://dealroom.atlassian.net/browse/DN-932)
        logging.error("Found more than one documents to update for this payload")
        return ERROR

    # Ensure that dealroom_id is type of number
    if "dealroom_id" in _payload:
        _payload["dealroom_id"] = int(_payload["dealroom_id"])
    res = set(history_ref, _payload)
    if res == ERROR:
        # TODO: Raise a Custom Exception (FirestoreException) with the same message when we replace ERROR constant with actual exceptions
        #   (DN-932: https://dealroom.atlassian.net/browse/DN-932)
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


def get_people_doc_refs(
    db: firestore.Client, field_name: str, operator: str, field_value,
):
    """Query 'people' collection for a document whose 'field_name' has 'operator'
    relation with 'field_value'.

    Args:
        field_name (str): the field to query for.
        operator (str): determines the condition for matching ('==', ...).
        field_value (Any): the value that satisfies the condition.

    Returns:
        [DocumentReference]: if no documents are found, return None, else a list of matching documents.

    Examples:
        >>> fc.get_people_doc_refs(db, "dealroom_id", "==", 1169416)[0].to_dict()
        >>> fc.get_people_doc_refs(db, "linkedin", "array_contains", "https://www.linkedin.com/in/vess/")[0].to_dict()
    """

    people_collection_ref = db.collection("people")

    query = people_collection_ref.where(field_name, operator, field_value)
    streamed_query = stream(query)
    if streamed_query == ERROR:
        # TODO: raise Custom Exception (DN-932: https://dealroom.atlassian.net/browse/DN-932)
        logging.error("Couldn't stream query.")
        return ERROR

    matching_docs = [doc_ref for doc_ref in streamed_query]

    num_matching_docs = len(matching_docs)
    if num_matching_docs == 0:
        return None

    return matching_docs


def _validate_new_people_doc_payload(payload: dict):
    """Validate the required fields in the payload when creating a new document in the people collection"""

    if "dealroom_id" not in payload:
        raise KeyError("'dealroom_id' must be present payload")

    _validate_dealroomid(payload["dealroom_id"])


def set_people_doc_ref(
    db: firestore.Client, field_name: str, operator: str, field_value, payload: dict,
):
    """Updates or Creates a single document from 'people' collection with 'payload' where 'field_name' has 'operator'
    relation with 'field_value'.

    Args:
        field_name (str): the field to query for.
        operator (str): determines the condition for matching ('==', ...).
        field_value (Any): the value that satisfies the condition.
        payload (dict): The actual data that the newly created document will have OR the fields to update.

    Returns:
        [DocumentReference]: if no documents are found, return None, else a list of matching documents.

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

    # CREATE: If there are not mayching documents in people
    if matching_docs == 0:
        # Add any default values to the payload
        _payload["dealroom_id"] = _NOT_IN_DEALROOM_ENTITY_ID

        # Validate that the new document will have the minimum required fields
        try:
            _validate_new_people_doc_payload(_payload)
        except (ValueError, KeyError) as ex:
            # TODO: raise Custom Exception (DN-932: https://dealroom.atlassian.net/browse/DN-932)
            logging.error(ex)
            return ERROR
        people_doc_ref = people_collection_ref.document()
        operation_status_code = CREATED
    # UPDATE:
    elif matching_docs == 1:
        # Ensure that dealroom_id is type of number if it appears on the payload
        if "dealroom_id" in _payload:
            _validate_dealroomid(_payload["dealroom_id"])
        people_doc_ref = people_refs[0].reference
        operation_status_code = UPDATED
    # If more than one document were found then it's an error.
    else:
        # TODO: Raise a Custom Exception (DuplicateDocumentsException) with the same message when we replace ERROR constant with actual exceptions
        #   (DN-932: https://dealroom.atlassian.net/browse/DN-932)
        logging.error("Found more than one documents to update for this payload")
        return ERROR

    res = set(people_doc_ref, _payload)

    if res == ERROR:
        # TODO: Raise a Custom Exception (FirestoreException) with the same message when we replace ERROR constant with actual exceptions
        #   (DN-932: https://dealroom.atlassian.net/browse/DN-932)
        logging.error(
            f"Couldn't `set` document {people_doc_ref.id}. Please check logs above."
        )
        return ERROR

    return operation_status_code
