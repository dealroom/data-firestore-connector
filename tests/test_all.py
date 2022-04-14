"""
These tests provide very good coverage for the methods that are mostly used in
this package.

Improvements:
* tests for batcher
* tests for people collection methods
"""
import string
from uuid import uuid4
from random import choice, randint
import pytest
from assertpy import assert_that
import dealroom_firestore_connector as fc
from dealroom_firestore_connector.status_codes import StatusCode


def _get_random_string(length: int) -> str:
    return "".join(choice(string.ascii_letters) for _ in range(length)).lower()


def uuid() -> str:
    return str(uuid4())


# Replace with a project ID for testing
TEST_PROJECT = "sustained-hold-288413"


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
    assert res == StatusCode.ERROR


def test_set_history_doc_refs_empty_final_url_w_uuid():
    """Creating a new document, without a final_url should raise an error"""
    db = fc.new_connection(project=TEST_PROJECT)
    # TODO: Use it as soon as firestore-connector will raise a proper Error
    # with pytest.raises(KeyError, match=r"'final_url'"):
    res = fc.set_history_doc_refs(db, {"dealroom_uuid": uuid()})
    assert res == StatusCode.ERROR


def test_set_history_doc_refs_wrong_final_url():
    """Creating a new document, with invalid final_url should raise an error"""
    db = fc.new_connection(project=TEST_PROJECT)
    # TODO: Use it as soon as firestore-connector will raise a proper Error
    # with pytest.raises(Exception):
    res = fc.set_history_doc_refs(
        db, {"final_url": "asddsadsdsd", "dealroom_id": "123123"}
    )
    assert res == StatusCode.ERROR


def test_set_history_doc_refs_wrong_final_url_w_uuid():
    """Creating a new document, with invalid final_url should raise an error"""
    db = fc.new_connection(project=TEST_PROJECT)
    # TODO: Use it as soon as firestore-connector will raise a proper Error
    # with pytest.raises(Exception):
    res = fc.set_history_doc_refs(
        db, {"final_url": "asddsadsdsd", "dealroom_uuid": uuid()}
    )
    assert res == StatusCode.ERROR


def test_set_history_doc_refs_new_empty():
    """Creating a new document, with empty final_url & dealroom_id, should raise an error"""
    db = fc.new_connection(project=TEST_PROJECT)
    empty_doc_payload = {}
    res = fc.set_history_doc_refs(db, empty_doc_payload)
    assert res == StatusCode.ERROR


def test_set_history_doc_refs_new_valid_url():
    """Creating a new document, with valid final_url & w/o dealroom_id, should be ok"""
    db = fc.new_connection(project=TEST_PROJECT)
    res = fc.set_history_doc_refs(db, {"final_url": f"{_get_random_string(10)}.com"})
    assert res == StatusCode.CREATED


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

    assert res == StatusCode.CREATED


def test_set_history_doc_refs_new_valid_url_uuid():
    """Creating a new document, with valid final_url & valid dealroom uuid should be ok"""
    db = fc.new_connection(project=TEST_PROJECT)
    res = fc.set_history_doc_refs(
        db,
        {
            "final_url": f"{_get_random_string(10)}.com",
            "dealroom_uuid": uuid(),
        },
    )

    assert res == StatusCode.CREATED


def test_set_history_doc_refs_new_valid_url_uuid_as_id():
    """Creating a new document, with valid final_url & uuid given for id should raise an error"""
    db = fc.new_connection(project=TEST_PROJECT)
    res = fc.set_history_doc_refs(
        db,
        {
            "final_url": f"{_get_random_string(10)}.com",
            "dealroom_id": uuid(),
        },
    )

    assert res == StatusCode.ERROR


def test_set_history_doc_refs_new_valid_url_id_as_uuid():
    """Creating a new document, with valid final_url & id given for uuid should raise an error"""
    db = fc.new_connection(project=TEST_PROJECT)
    res = fc.set_history_doc_refs(
        db,
        {
            "final_url": f"{_get_random_string(10)}.com",
            "dealroom_uuid": randint(1e5, 1e8),
        },
    )

    assert res == StatusCode.ERROR


def test_set_history_doc_refs_empty_dealroom_id_valid_url():
    """Updating a new document, using a valid final_url, should be ok"""
    db = fc.new_connection(project=TEST_PROJECT)
    random_field = _get_random_string(10)
    res = fc.set_history_doc_refs(db, {"test_field": random_field}, "foo2.bar")
    assert res == StatusCode.UPDATED


def test_set_history_doc_refs_empty_final_url_valid_id():
    """Updating an existing document, using a valid dealroom_id, should be ok"""
    db = fc.new_connection(project=TEST_PROJECT)
    random_field = _get_random_string(10)
    EXISTING_DOC_DR_ID = "10000000000023"

    res = fc.set_history_doc_refs(db, {"test_field": random_field}, EXISTING_DOC_DR_ID)

    assert res == StatusCode.UPDATED


def test_set_history_doc_refs_empty_final_url_valid_uuid():
    """Updating an existing document, using a valid dealroom_uuid, should be ok"""
    db = fc.new_connection(project=TEST_PROJECT)
    random_field = _get_random_string(10)
    EXISTING_DOC_DR_UUID = "ef314e25-4543-4636-a5b7-c428886e3dd3"

    res = fc.set_history_doc_refs(
        db, {"test_field": random_field}, EXISTING_DOC_DR_UUID
    )

    assert res == StatusCode.UPDATED


def test_set_history_doc_refs_wrong_dealroom_id():
    """Creating a new document, with invalid dealroomid should raise an error"""
    db = fc.new_connection(project=TEST_PROJECT)
    # TODO: Use it as soon as firestore-connector will raise a proper Error
    # with pytest.raises(ValueError, match=r"'dealroom_id'"):
    res = fc.set_history_doc_refs(
        db, {"final_url": f"{_get_random_string(10)}.com", "dealroom_id": "foobar"}
    )
    assert res == StatusCode.ERROR


def test_set_history_doc_refs_wrong_dealroom_uuid():
    """Creating a new document, with invalid dealroom uuid should raise an error"""
    db = fc.new_connection(project=TEST_PROJECT)
    # TODO: Use it as soon as firestore-connector will raise a proper Error
    # with pytest.raises(ValueError, match=r"'dealroom_id'"):
    res = fc.set_history_doc_refs(
        db, {"final_url": f"{_get_random_string(10)}.com", "dealroom_uuid": "foobar"}
    )
    assert res == StatusCode.ERROR


def test_set_history_doc_refs_as_deleted_on_id():
    """Marking an entity as deleted (dealroom_id = -2), should be ok"""
    db = fc.new_connection(project=TEST_PROJECT)
    FINAL_URL = "foo7.bar"
    fc.set_history_doc_refs(
        db, {"dealroom_id": randint(1e5, 1e8), "final_url": FINAL_URL}
    )
    res = fc.set_history_doc_refs(db, {"dealroom_id": "-2"}, FINAL_URL)

    assert res == StatusCode.UPDATED
    doc_ref = fc.get_history_doc_refs(db, final_url=FINAL_URL)["final_url"][0]
    doc_ref.delete()


def test_set_history_doc_refs_as_deleted_on_id_0():
    """Marking an entity with dealroom_id = 0, should raise an error"""
    db = fc.new_connection(project=TEST_PROJECT)
    FINAL_URL = "foo7.bar"
    fc.set_history_doc_refs(
        db, {"dealroom_id": randint(1e5, 1e8), "final_url": FINAL_URL}
    )
    res = fc.set_history_doc_refs(db, {"dealroom_id": "0"}, FINAL_URL)

    assert res == StatusCode.ERROR
    doc_ref = fc.get_history_doc_refs(db, final_url=FINAL_URL)["final_url"][0]
    doc_ref.delete()


def test_set_history_doc_refs_as_deleted_on_uuid():
    """Marking an entity as deleted (dealroom_uuid = -2), should be ok"""
    db = fc.new_connection(project=TEST_PROJECT)
    FINAL_URL = "foo77.bar"
    fc.set_history_doc_refs(db, {"dealroom_uuid": uuid(), "final_url": FINAL_URL})
    res = fc.set_history_doc_refs(db, {"dealroom_uuid": -2}, FINAL_URL)

    assert res == StatusCode.UPDATED
    doc_ref = fc.get_history_doc_refs(db, final_url=FINAL_URL)["final_url"][0]
    doc_ref.delete()


def test_set_history_doc_refs_as_deleted_on_uuid_0():
    """Marking an entity with dealroom_uuid = 0, should raise an error"""
    db = fc.new_connection(project=TEST_PROJECT)
    FINAL_URL = "foo77.bar"
    fc.set_history_doc_refs(db, {"dealroom_uuid": uuid(), "final_url": FINAL_URL})
    res = fc.set_history_doc_refs(db, {"dealroom_uuid": "0"}, FINAL_URL)

    assert res == StatusCode.ERROR
    doc_ref = fc.get_history_doc_refs(db, final_url=FINAL_URL)["final_url"][0]
    doc_ref.delete()


def test_set_history_doc_refs_existing_by_url_with_wrong_dealroom_id():
    """Create a new document, using a valid but already used final_url (with another dealroom_id), should be ok"""
    db = fc.new_connection(project=TEST_PROJECT)
    wrong_dr_id = randint(1e5, 1e8)
    res = fc.set_history_doc_refs(db, {"final_url": "foo3.bar"}, wrong_dr_id)
    assert res == StatusCode.CREATED


def test_set_history_doc_refs_existing_by_url_with_wrong_dealroom_uuid():
    """Create a new document, using a valid but already used final_url (with another dealroom_id), should be ok"""
    db = fc.new_connection(project=TEST_PROJECT)
    wrong_dr_uuid = uuid()
    res = fc.set_history_doc_refs(db, {"final_url": "foo33.bar"}, wrong_dr_uuid)
    assert res == StatusCode.CREATED


def test_set_history_doc_refs_existing_by_url_with_new_dealroom_id():
    """Update a new document, using a valid but already used final_url (with another dealroom_id=-1), should be ok"""
    db = fc.new_connection(project=TEST_PROJECT)
    new_dr_id = randint(1e5, 1e8)
    fc.set_history_doc_refs(db, {"final_url": "foo9.bar", "dealroom_id": -1})
    res = fc.set_history_doc_refs(
        db, {"final_url": "foo9.bar", "dealroom_id": new_dr_id}, new_dr_id
    )
    assert res == StatusCode.UPDATED
    doc_ref = fc.get_history_doc_refs(db, dealroom_id=new_dr_id)["dealroom_id"][0]
    doc_ref.delete()


def test_set_history_doc_refs_existing_by_url_with_new_dealroom_uuid():
    """Update a new document, using a valid but already used final_url (with another dealroom_uuid=-1), should be ok"""
    db = fc.new_connection(project=TEST_PROJECT)
    new_dr_uuid = uuid()
    fc.set_history_doc_refs(db, {"final_url": "foo99.bar", "dealroom_uuid": -1})
    res = fc.set_history_doc_refs(
        db, {"final_url": "foo99.bar", "dealroom_uuid": new_dr_uuid}, new_dr_uuid
    )
    assert res == StatusCode.UPDATED
    doc_ref = fc.get_history_doc_refs(db, dealroom_id=new_dr_uuid)["dealroom_uuid"][0]
    doc_ref.delete()


def test_set_history_doc_refs_existing_by_url():
    """Update an existing document with dealroom_id=-1 and dealroom_uuid=-1, using the final_url"""
    db = fc.new_connection(project=TEST_PROJECT)
    random_field = _get_random_string(10)
    res = fc.set_history_doc_refs(
        db, {"test_field": random_field}, finalurl_or_dealroomid="foo4.bar"
    )
    assert res == StatusCode.UPDATED


def test_set_history_doc_refs_existing_by_url_using_payload_w_id():
    """Update an existing document with dealroom_id=-1, using the final_url from the payload"""
    db = fc.new_connection(project=TEST_PROJECT)
    fc.set_history_doc_refs(db, {"final_url": "foo5.bar", "dealroom_id": -1})
    dealroom_id = randint(1e5, 1e8)
    res = fc.set_history_doc_refs(
        db, {"final_url": "foo5.bar", "dealroom_id": dealroom_id}
    )

    assert res == StatusCode.UPDATED
    doc_ref = fc.get_history_doc_refs(db, dealroom_id=dealroom_id)["dealroom_id"][0]
    doc_ref.delete()


# maybe redundant?
def test_set_history_doc_refs_existing_by_url_using_payload_w_uuid():
    """Update an existing document with dealroom_uuid=-1, using the final_url from the payload"""
    db = fc.new_connection(project=TEST_PROJECT)
    fc.set_history_doc_refs(db, {"final_url": "foo55.bar", "dealroom_uuid": -1})
    dealroom_uuid = uuid()
    res = fc.set_history_doc_refs(
        db, {"final_url": "foo55.bar", "dealroom_uuid": dealroom_uuid}
    )

    assert res == StatusCode.UPDATED
    doc_ref = fc.get_history_doc_refs(db, dealroom_id=dealroom_uuid)["dealroom_uuid"][0]
    doc_ref.delete()


def test_set_history_doc_refs_for_deleted_company_w_id():
    """Create a document for a new company that appears previously as deleted should be ok."""
    # Tests this bug https://dealroom.atlassian.net/browse/DS2-154
    db = fc.new_connection(project=TEST_PROJECT)
    dealroom_id = randint(1e5, 1e8)
    print(dealroom_id)
    res = fc.set_history_doc_refs(
        db, {"final_url": "foo6.bar", "dealroom_id": dealroom_id}, dealroom_id
    )
    doc_ref = fc.get_history_doc_refs(db, dealroom_id=dealroom_id)["dealroom_id"][0]
    doc_ref.delete()
    # NOTE: if the call doesn't have the dealroom_id as a parameter this fails. Since we removed
    # the logic to extract the dealroom_id from the payload here: https://dealroom.atlassian.net/browse/DS2-104
    assert res == StatusCode.CREATED


def test_set_history_doc_refs_for_deleted_company_w_uuid():
    """Create a document for a new company that appears previously as deleted should be ok."""
    # Tests this bug https://dealroom.atlassian.net/browse/DS2-154
    db = fc.new_connection(project=TEST_PROJECT)
    dealroom_uuid = uuid()
    print(dealroom_uuid)
    res = fc.set_history_doc_refs(
        db, {"final_url": "foo66.bar", "dealroom_uuid": dealroom_uuid}, dealroom_uuid
    )
    doc_ref = fc.get_history_doc_refs(db, dealroom_id=dealroom_uuid)["dealroom_uuid"][0]
    doc_ref.delete()
    # NOTE: if the call doesn't have the dealroom_id as a parameter this fails. Since we removed
    # the logic to extract the dealroom_id from the payload here: https://dealroom.atlassian.net/browse/DS2-104
    assert res == StatusCode.CREATED


def test_set_history_doc_refs_for_deleted_company_w_id_2():
    """Update a document for a new company that appears previously as deleted (id) should be ok."""
    db = fc.new_connection(project=TEST_PROJECT)

    # fixed dealroom_id_old in firestore
    dealroom_id = 666666666666
    random_value = _get_random_string(10)
    res = fc.set_history_doc_refs(db, {"test_field": random_value}, dealroom_id)
    assert res == StatusCode.UPDATED

    # fixed url in firestore
    final_url = "foo8.bar"
    random_value = _get_random_string(10)
    res = fc.set_history_doc_refs(db, {"test_field": random_value}, final_url)
    assert res == StatusCode.UPDATED


def test_set_history_doc_refs_for_deleted_company_w_uuid_2():
    """Update a document for a new company that appears previously as deleted (uuid) should be ok."""
    db = fc.new_connection(project=TEST_PROJECT)

    # fixed dealroom_uuid_old in firestore
    dealroom_uuid = "49ada2cf-e234-4fa5-937d-1d65a9bbe2b0"
    random_value = _get_random_string(10)
    res = fc.set_history_doc_refs(db, {"test_field": random_value}, dealroom_uuid)
    assert res == StatusCode.UPDATED

    # fixed url in firestore
    final_url = "foo88.bar"
    random_value = _get_random_string(10)
    res = fc.set_history_doc_refs(db, {"test_field": random_value}, final_url)
    assert res == StatusCode.UPDATED


@pytest.mark.parametrize(
    "payload,identifier,expected",
    [
        ({}, "dealroom.co", ("dealroom.co", -1)),
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


@pytest.mark.parametrize(
    "payload,identifier,expected",
    [
        ({}, 123, ("", fc.DealroomIdentifier(123))),
        (
            {"dealroom_id": fc.DealroomIdentifier(123)},
            "dealroom.co",
            ("dealroom.co", -1),
        ),
        (
            {"final_url": "dealroom.co"},
            123,
            ("dealroom.co", fc.DealroomIdentifier(123)),
        ),
        (
            {"final_url": "dealroom.co", "dealroom_id": 123},
            None,
            ("dealroom.co", -1),
        ),
    ],
)
def test___get_final_url_and_dealroom_id_w_id(payload, identifier, expected):
    """It should give valid output for input"""
    assert_that(
        fc._get_final_url_and_dealroom_id(
            payload,
            identifier,
        )
    ).is_equal_to(expected)


@pytest.mark.parametrize(
    "payload,identifier,expected",
    [
        (
            {},
            "f996c3fc-effe-48eb-a1d5-c01f3f379c73",
            ("", fc.DealroomIdentifier("f996c3fc-effe-48eb-a1d5-c01f3f379c73")),
        ),
        (
            {"dealroom_uuid": "f996c3fc-effe-48eb-a1d5-c01f3f379c73"},
            "dealroom.co",
            ("dealroom.co", -1),
        ),
        (
            {"final_url": "dealroom.co"},
            "f996c3fc-effe-48eb-a1d5-c01f3f379c73",
            (
                "dealroom.co",
                fc.DealroomIdentifier("f996c3fc-effe-48eb-a1d5-c01f3f379c73"),
            ),
        ),
        (
            {
                "final_url": "dealroom.co",
                "dealroom_uuid": "f996c3fc-effe-48eb-a1d5-c01f3f379c73",
            },
            None,
            ("dealroom.co", -1),
        ),
    ],
)
def test___get_final_url_and_dealroom_id_w_uuid(payload, identifier, expected):
    """It should give valid output for input"""
    assert_that(
        fc._get_final_url_and_dealroom_id(
            payload,
            identifier,
        )
    ).is_equal_to(expected)


@pytest.mark.parametrize(
    "identifier,expected",
    [
        ("", None),
        (None, None),
        (0, None),
        (10, fc.DealroomIdentifier(10)),
        ("10", fc.DealroomIdentifier(10)),
        (
            "2cd8f956-b929-468e-9097-2d0093a8a070",
            fc.DealroomIdentifier("2cd8f956-b929-468e-9097-2d0093a8a070"),
        ),
    ],
)
def test___determine_identifier(identifier, expected):
    """It should give valid output for input"""
    assert_that(fc.determine_identifier(identifier)).is_equal_to(expected)


def test__raise_determine_identifier():
    """It should raise InvalidIdentifier for invalid input"""
    wrong_ids = [
        "ciao",
        "ciao.com",
        "1000.0",
        1000.0,
        # not hex: there's a letter 't'
        "2cd8f956-b929-468e-9097-2d0093a8t070",
        # not UUID: extra stuff
        "2cd8f956-b929-468e-90976-2d0093a8f070",
    ]
    for identifier in wrong_ids:
        with pytest.raises(fc.InvalidIdentifier):
            fc.determine_identifier(identifier)
