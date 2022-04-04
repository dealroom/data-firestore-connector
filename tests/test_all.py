import random
import string
from random import randint
from assertpy import assert_that
import dealroom_firestore_connector as fc
from google.cloud import firestore
import pytest
from dealroom_firestore_connector.status_codes import ERROR, SUCCESS, UPDATED, CREATED


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


def test_set_history_doc_refs_new_valid_url():
    """Creating a new document, with valid final_url & w/o dealroom_id, should be ok"""
    db = fc.new_connection(project=TEST_PROJECT)
    res = fc.set_history_doc_refs(db, {"final_url": f"{_get_random_string(10)}.com"})

    assert res == CREATED


def test_set_history_doc_refs_new_valid_url_id():
    """Creating a new document, with valid final_url & valid dealroom id should be ok"""
    db = fc.new_connection(project=TEST_PROJECT)
    res = fc.set_history_doc_refs(
        db,
        {
            "final_url": f"{_get_random_string(10)}.com",
            "dealroom_id": randint(1e5, 1e8),
        },
    )

    assert res == CREATED


def test_set_history_doc_refs_empty_dealroom_id_valid_url():
    """Updating a new document, using a valid final_url, should be ok"""
    db = fc.new_connection(project=TEST_PROJECT)
    random_field = _get_random_string(10)
    res = fc.set_history_doc_refs(db, {"test_field": random_field}, "foo2.bar")
    assert res == UPDATED


def test_set_history_doc_refs_empty_final_url_valid_id():
    """Updating a new document, using a valid dealroom_id, should be ok"""
    db = fc.new_connection(project=TEST_PROJECT)
    random_field = _get_random_string(10)
    EXISTING_DOC_DR_ID = "10000000000023"

    res = fc.set_history_doc_refs(db, {"test_field": random_field}, EXISTING_DOC_DR_ID)

    assert res == UPDATED


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
    FINAL_URL = "foo7.bar"
    fc.set_history_doc_refs(
        db, {"dealroom_id": randint(1e5, 1e8), "final_url": FINAL_URL}
    )
    res = fc.set_history_doc_refs(db, {"dealroom_id": "-2"}, FINAL_URL)

    assert res == UPDATED
    doc_ref = fc.get_history_doc_refs(db, final_url="foo7.bar")["final_url"][0]
    doc_ref.delete()


def test_set_history_doc_refs_existing_by_url_with_wrong_dealroom_id():
    """Create a new document, using a valid but already used final_url (with another dealroom_id), should be ok"""
    db = fc.new_connection(project=TEST_PROJECT)
    wrong_dr_id = randint(1e5, 1e8)
    res = fc.set_history_doc_refs(db, {"final_url": "foo3.bar"}, wrong_dr_id)
    assert res == CREATED


def test_set_history_doc_refs_existing_by_url_with_new_dealroom_id():
    """Update a new document, using a valid but already used final_url (with another dealroom_id=-1), should be ok"""
    db = fc.new_connection(project=TEST_PROJECT)
    new_dr_id = randint(1e5, 1e8)
    fc.set_history_doc_refs(db, {"final_url": "foo9.bar", "dealroom_id": -1})
    res = fc.set_history_doc_refs(
        db, {"final_url": "foo9.bar", "dealroom_id": new_dr_id}, new_dr_id
    )
    assert res == UPDATED
    doc_ref = fc.get_history_doc_refs(db, dealroom_id=new_dr_id)["dealroom_id"][0]
    doc_ref.delete()


def test_set_history_doc_refs_existing_by_url():
    """Update an existing document with dealroom_id=-1, using a the final_url"""
    db = fc.new_connection(project=TEST_PROJECT)
    random_field = _get_random_string(10)
    res = fc.set_history_doc_refs(
        db, {"test_field": random_field}, finalurl_or_dealroomid="foo4.bar"
    )
    assert res == UPDATED


def test_set_history_doc_refs_existing_by_url_using_payload():
    """Update an existing document with dealroom_id=-1, using a the final_url from the payload"""
    db = fc.new_connection(project=TEST_PROJECT)
    fc.set_history_doc_refs(db, {"final_url": "foo5.bar", "dealroom_id": -1})
    dealroom_id = randint(1e5, 1e8)
    res = fc.set_history_doc_refs(
        db, {"final_url": "foo5.bar", "dealroom_id": dealroom_id}
    )

    assert res == UPDATED
    doc_ref = fc.get_history_doc_refs(db, dealroom_id=dealroom_id)["dealroom_id"][0]
    doc_ref.delete()


def test_set_history_doc_refs_for_deleted_company():
    """Create a document for a new company that appears previously as deleted should be ok."""
    # Tests this bug https://dealroom.atlassian.net/browse/DS2-154
    db = fc.new_connection(project=TEST_PROJECT)
    dealroom_id = randint(1e5, 1e8)
    res = fc.set_history_doc_refs(
        db, {"final_url": "foo6.bar", "dealroom_id": dealroom_id}, dealroom_id
    )
    # NOTE: if the call doesn't have the dealroom_id as a parameter this fails. Since we removed
    # the logic to extract the dealroom_id from the payload here: https://dealroom.atlassian.net/browse/DS2-104
    assert res == CREATED
    doc_ref = fc.get_history_doc_refs(db, dealroom_id=dealroom_id)["dealroom_id"][0]
    doc_ref.delete()


def test_set_history_doc_refs_for_deleted_company_2():
    """Update a document for a new company that appears previously as deleted should be ok."""
    db = fc.new_connection(project=TEST_PROJECT)
    dealroom_id = 666666666666  # fixed id in firestore
    random_value = _get_random_string(10)
    res = fc.set_history_doc_refs(db, {"test_field": random_value}, dealroom_id)
    assert res == UPDATED
    final_url = "foo8.bar"  # fixed url in firestore
    random_value = _get_random_string(10)
    res = fc.set_history_doc_refs(db, {"test_field": random_value}, final_url)
    assert res == UPDATED


@pytest.mark.parametrize(
    "payload,identifier,expected",
    [
        ({}, 123, ("", 123)),
        ({}, "dealroom.co", ("dealroom.co", -1)),
        (
            {"dealroom_id": 123},
            "dealroom.co",
            ("dealroom.co", -1),
        ),
        (
            {"final_url": "dealroom.co"},
            123,
            ("dealroom.co", 123),
        ),
        (
            {"final_url": "dealroom.co", "dealroom_id": 123},
            None,
            ("dealroom.co", -1),
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