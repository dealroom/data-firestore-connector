import functools
import logging
import traceback

from google.api_core.exceptions import InvalidArgument
from google.cloud import firestore

from .helpers import error_logger
from .status_codes import StatusCode
from datetime import datetime, timezone


class Batcher(firestore.WriteBatch):
    """Accumulate write operations to be sent in a batch.
    This has the same set of methods for write operations that
    :class:`~google.cloud.firestore.DocumentReference` does,
    e.g. :meth:`~google.cloud.firestore.DocumentReference.create`.
    Args:
        client (:class:`~google.cloud.firestore.Client`):
            The client that created this batch.
    """

    # This is the limit set by firestore. See https://firebase.google.com/docs/firestore/manage-data/transactions#batched-writes.
    MAX_WRITES_PER_BATCH = 500

    def __init__(self, client):
        super().__init__(client)
        self.__total_writes = 0

    @property
    def total_writes(self):
        """The total writes for the current batch"""
        return self.__total_writes

    def _count_write(func):
        """Decorator to be attached in write operations. Observes
        how many batches we are going to flush at once.
        """

        # Keep function meta-information
        @functools.wraps(func)
        def count_write_wrapper(self, *args, **kwargs):
            self.__total_writes += 1

            if self.__total_writes > self.MAX_WRITES_PER_BATCH:
                raise InvalidArgument(
                    f"Maximum {self.MAX_WRITES_PER_BATCH} writes allowed per request"
                )

            func(self, *args, **kwargs)

        return count_write_wrapper

    @_count_write
    def _update_last_edit(self, doc_ref):
        super().update(doc_ref, {"last_edit": datetime.now(timezone.utc)})

    def _check_if_update_last_edit(self, doc_ref):
        """If the document reference points to the history collection
        then update its "last_edit" field to the currrent datetime. This
        datetame is parsed as a Timestamp in the document.

        Args:
            doc_ref ([type]): [description]
        """
        _path = doc_ref.path.split("/")
        if len(_path) == 2 and _path[0] == "history":
            self._update_last_edit(doc_ref)

    @_count_write
    def set(self, doc_ref, *args, **kwargs):
        """Creates a document in firestore or updates it if it already exists.
        When the document exists it always updates the document and never overrides it.

        See :meth:`google.cloud.firestore.base_batch.BaseWriteBatch.set` for more details.
        """
        final_kwargs = {**kwargs, "merge": True}
        super().set(doc_ref, *args, **final_kwargs)
        self._check_if_update_last_edit(doc_ref)

    @_count_write
    def create(self, doc_ref, document_data):
        """See :meth:`google.cloud.firestore.base_batch.BaseWriteBatch.create` for details."""
        return super().create(doc_ref, document_data)

    @_count_write
    def delete(self, doc_ref, **kwargs):
        """See :meth:`google.cloud.firestore.base_batch.BaseWriteBatch.delete` for details."""
        return super().delete(doc_ref, **kwargs)

    @_count_write
    def update(self, doc_ref, *args, **kwargs):
        """See :meth:`google.cloud.firestore.base_batch.BaseWriteBatch.update` for details."""
        super().update(doc_ref, *args, **kwargs)
        self._check_if_update_last_edit(doc_ref)

    def commit(self, retry=True):
        """Commit the changes accumulated in the current batch but can retry on failure.
        See :meth:`google.cloud.firestore.base_batch.BaseWriteBatch.commit` for details.
        """
        try:
            super().commit()

            # Reset counter after a succesfull commit, to keep monitoring.
            self.__total_writes = 0

            return StatusCode.SUCCESS
        except Exception as exc:
            if retry:
                return self.commit(retry=False)
            else:
                error_logger("Failed to batch commit.")
                return StatusCode.ERROR
