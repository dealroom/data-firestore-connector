""" Firestore connector for exception handling with Dealroom data"""
import logging
import os
import traceback
from time import sleep

import google
from google.cloud import firestore
from google.cloud.firestore import WriteBatch

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
        elif os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            return firestore.Client(project=project)
        else:
            raise Exception("Please set 'GOOGLE_APPLICATION_CREDENTIALS' env var")
    except Exception as identifier:
        __log_exception(5, credentials_path, identifier, True)
        return -1


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
            return -1


def set(doc_ref, *args, **kwargs):
    """Create a new document in Firestore
    Args:
        doc_ref (object): Firestore reference to the document that will be created.
    Returns:
        0 [int]: success (1st or 2nd time tried)
        -1 [int]: exception (error after retrying a second time)
    """
    try:
        doc_ref.set(*args, **kwargs, merge=True)
        # Return success code 0
        return 0
    except Exception as identifier:
        # log error
        __log_exception(4, doc_ref, identifier)
        # Wait before continue
        sleep(EXCEPTION_SLEEP_TIME)
        try:
            # Retry
            doc_ref.set(*args, **kwargs, merge=True)
            # Return success code 0
            return 0
        except Exception as identifier:
            # log error
            __log_exception(4, doc_ref, identifier, True)
            return -1


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
        # Return success code 0
        return 0
    except Exception as identifier:
        # log error
        __log_exception(2, doc_ref, identifier)
        # Wait before continue
        sleep(EXCEPTION_SLEEP_TIME)
        try:
            # Retry
            doc_ref.update(*args, **kwargs)
            # Return success code 0
            return 0
        except Exception as identifier:
            # log error
            __log_exception(2, doc_ref, identifier, True)
            return -1


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
            return -1


def __log_exception(exception_code, ref, identifier, was_retried=False):
    message = "Unknown error"
    if exception_code == 1:
        message = "An error occurred retrieving stream for collection %s." % (ref)
    elif exception_code == 2:
        message = "An error occurred updating document %s." % (ref)
    elif exception_code == 3:
        message = "An error occurred getting document %s." % (ref)
    elif exception_code == 4:
        message = "An error occurred creating document %s." % (ref)
    elif exception_code == 5:
        message = "Error connecting with db with credentials file %s." % (ref)

    if was_retried:
        # TODO save to csv or json
        logging.error(
            "[Error code %d] %s Error trace: %s."
            % (exception_code, message, identifier)
        )
    else:
        logging.error("[Error code %d] %s Retrying..." % (exception_code, message))


class Batcher(WriteBatch):
    """Accumulate write operations to be sent in a batch.
    This has the same set of methods for write operations that
    :class:`~google.cloud.firestore.DocumentReference` does,
    e.g. :meth:`~google.cloud.firestore.DocumentReference.create`.
    Args:
        client (:class:`~google.cloud.firestore.Client`):
            The client that created this batch.
    """

    def set(self, doc_ref, *args, **kwargs):
        """Creates a document in firestore or updates it if it already exists.
        When the document exists it always updates the document and never overrides it.

        See  :meth:`google.cloud.firestore.DocumentReference.set` for more details.
        """
        final_kwargs = {**kwargs, "merge": True}
        return super().set(doc_ref, *args, **final_kwargs)
