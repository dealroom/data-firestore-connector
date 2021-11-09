import random
import string
from random import randint
from assertpy import assert_that
import dealroom_firestore_connector as fc
import pytest
from dealroom_firestore_connector.status_codes import ERROR, SUCCESS


def _get_random_string(length):
    return "".join(random.choice(string.ascii_letters) for _ in range(length)).lower()


TEST_PROJECT = "sustained-hold-288413"  # Replace with a project ID for testing


def test_collection_exists():
    db = fc.new_connection(project=TEST_PROJECT)
    col_ref = db.collection("NOT_EXISTING_COLLECTION")
    assert fc.collection_exists(col_ref) == False


def test_set_history_doc_refs_empty_final_url():
    """Creating a new document, without a final_url should raise an error"""
    db = fc.new_connection(project=TEST_PROJECT)
    # TODO: Use it as soon as firestore-connector will raise a proper Error
    # with pytest.raises(KeyError, match=r"'final_url'"):
    res = fc.set_history_doc_refs(db, {"dealroom_id": "123123"})
    assert res == ERROR


def test_set_history_doc_refs_wrong_final_url():
    """Creating a new document, with invalid final_url should raise an error"""
    db = fc.new_connection(project=TEST_PROJECT)
    # TODO: Use it as soon as firestore-connector will raise a proper Error
    # with pytest.raises(Exception):
    res = fc.set_history_doc_refs(
        db, {"final_url": "asddsadsdsd", "dealroom_id": "123123"}
    )
    assert res == ERROR


def test_set_history_doc_refs_new():
    """Creating a new document, with empty final_url & dealroom_id, should be ok"""
    db = fc.new_connection(project=TEST_PROJECT)
    res = fc.set_history_doc_refs(
        db,
        {
            # An empty doc
        },
    )

    assert res == SUCCESS


def test_set_history_doc_refs_new():
    """Creating a new document, with valid final_url & w/o dealroom_id, should be ok"""
    db = fc.new_connection(project=TEST_PROJECT)
    res = fc.set_history_doc_refs(db, {"final_url": f"{_get_random_string(10)}.com"})

    assert res == SUCCESS


def test_set_history_doc_refs_new():
    """Creating a new document, with valid final_url & valid dealroom id should be ok"""
    db = fc.new_connection(project=TEST_PROJECT)
    res = fc.set_history_doc_refs(
        db,
        {
            "final_url": f"{_get_random_string(10)}.com",
            "dealroom_id": randint(1e5, 1e8),
        },
    )

    assert res == SUCCESS


def test_set_history_doc_refs_empty_dealroom_id():
    """Updating a new document, using a valid final_url, should be ok"""
    db = fc.new_connection(project=TEST_PROJECT)
    random_field = _get_random_string(10)
    res = fc.set_history_doc_refs(db, {"test_field": random_field}, "foo2.bar")
    assert res == SUCCESS


def test_set_history_doc_refs_empty_final_url():
    """Updating a new document, using a valid dealroom_id, should be ok"""
    db = fc.new_connection(project=TEST_PROJECT)
    random_field = _get_random_string(10)
    EXISTING_DOC_DR_ID = "10000000000023"

    res = fc.set_history_doc_refs(db, {"test_field": random_field}, EXISTING_DOC_DR_ID)

    assert res == SUCCESS


def test_set_history_doc_refs_wrong_dealroom_id():
    """Creating a new document, with invalid dealroomid should raise an error"""
    db = fc.new_connection(project=TEST_PROJECT)
    # TODO: Use it as soon as firestore-connector will raise a proper Error
    # with pytest.raises(ValueError, match=r"'dealroom_id'"):
    res = fc.set_history_doc_refs(
        db, {"final_url": f"{_get_random_string(10)}.com", "dealroom_id": "foobar"}
    )
    assert res == ERROR


def test_set_history_doc_refs_as_deleted():
    """Marking an entity as deleted (dealroom_id = -2), should be ok"""
    db = fc.new_connection(project=TEST_PROJECT)
    EXISTING_DOC_FINAL_URL = "foo2.bar"
    res = fc.set_history_doc_refs(db, {"dealroom_id": "-2"}, EXISTING_DOC_FINAL_URL)

    assert res == SUCCESS


@pytest.mark.parametrize(
    "payload,identifier,expected",
    [
        ({}, 123, ("", 123)),
        ({}, "dealroom.co", ("dealroom.co", -1)),
        (
            {"dealroom_id": 123},
            "dealroom.co",
            ("dealroom.co", 123),
        ),
        (
            {"final_url": "dealroom.co"},
            123,
            ("dealroom.co", 123),
        ),
        (
            {"final_url": "dealroom.co", "dealroom_id": 123},
            None,
            ("dealroom.co", 123),
        ),
    ],
)
def test___get_final_url_and_dealroom_id(payload, identifier, expected):
    """It should give valid output for input"""
    assert_that(
        fc._get_final_url_and_dealroom_id(
            payload,
            identifier,
        )
    ).is_equal_to(expected)
