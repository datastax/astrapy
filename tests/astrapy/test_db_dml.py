# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Tests for the `db.py` parts on data manipulation "standard" methods
(i.e. non `vector_*` methods)
"""

import uuid
import logging

import pytest
from faker import Faker

from astrapy.db import AstraDB, AstraDBCollection

TEST_TRUNCATED_NONVECTOR_COLLECTION_NAME = "ephemeral_tr_non_v_col"
TEST_TRUNCATED_VECTOR_COLLECTION_NAME = "ephemeral_tr_v_col"

logger = logging.getLogger(__name__)
fake = Faker()


@pytest.mark.describe("should fail truncating a non-existent collection")
def test_truncate_collection_fail(db: AstraDB) -> None:
    with pytest.raises(ValueError):
        db.truncate_collection("this$does%not exist!!!")


@pytest.mark.describe("should truncate a nonvector collection")
def test_truncate_nonvector_collection(db: AstraDB) -> None:
    col = db.create_collection(TEST_TRUNCATED_NONVECTOR_COLLECTION_NAME)
    try:
        col.insert_one({"a": 1})
        assert len(col.find()["data"]["documents"]) == 1
        db.truncate_collection(TEST_TRUNCATED_NONVECTOR_COLLECTION_NAME)
        assert len(col.find()["data"]["documents"]) == 0
    finally:
        db.delete_collection(TEST_TRUNCATED_NONVECTOR_COLLECTION_NAME)


@pytest.mark.describe("should truncate a collection")
def test_truncate_vector_collection(db: AstraDB) -> None:
    col = db.create_collection(TEST_TRUNCATED_VECTOR_COLLECTION_NAME, dimension=2)
    try:
        col.insert_one({"a": 1, "$vector": [0.1, 0.2]})
        assert len(col.find()["data"]["documents"]) == 1
        db.truncate_collection(TEST_TRUNCATED_VECTOR_COLLECTION_NAME)
        assert len(col.find()["data"]["documents"]) == 0
    finally:
        db.delete_collection(TEST_TRUNCATED_VECTOR_COLLECTION_NAME)


###
###
###



@pytest.mark.describe("find_one, not through vector")
def test_find_one_filter_novector(readonly_vector_collection: AstraDBCollection, cliff_uuid: str) -> None:
    response = readonly_vector_collection.find_one(
        filter={"_id": "1"},
    )
    document = response["data"]["document"]
    assert document["text"] == "Sample entry number <1>"
    assert document.keys() ^ {"_id", "text", "otherfield", "anotherfield", "$vector"} == set()

    response_no = readonly_vector_collection.find_one(
        filter={"_id": "Z"},
    )
    document_no = response_no["data"]["document"]
    assert document_no is None


@pytest.mark.describe("find, not through vector")
def test_find_filter_novector(readonly_vector_collection: AstraDBCollection) -> None:
    response_n2 = readonly_vector_collection.find(
        filter={"anotherfield": "alpha"},
    )
    documents_n2 = response_n2["data"]["documents"]
    assert isinstance(documents_n2, list)
    assert {document["_id"] for document in documents_n2} == {"1", "2"}

    response_n1 = readonly_vector_collection.find(
        filter={"anotherfield": "alpha"},
        options={"limit": 1},
    )
    documents_n1 = response_n1["data"]["documents"]
    assert isinstance(documents_n1, list)
    assert len(documents_n1) == 1
    assert documents_n1[0]["_id"] in {"1", "2"}


@pytest.mark.describe("obey projection in find and find_one")
def test_find_find_one_projection(readonly_vector_collection: AstraDBCollection) -> None:
    query = [0.2, 0.6]
    sort = {"$vector": query}
    options = {"limit": 1}

    projs = [
        None,
        {},
        {"text": 1},
        {"$vector": 1},
        {"text": 1, "$vector": 1},
    ]
    exp_fieldsets = [
        {"$vector", "_id", "otherfield", "anotherfield", "text"},
        {"$vector", "_id", "otherfield", "anotherfield", "text"},
        {"_id", "text"},
        {"$vector", "_id"},
        {"$vector", "_id", "text"},
    ]
    for proj, exp_fields in zip(projs, exp_fieldsets):
        response_n = readonly_vector_collection.find(sort=sort, options=options, projection=proj)
        fields = set(response_n["data"]["documents"][0].keys())
        assert fields == exp_fields
        #
        response_1 = readonly_vector_collection.find_one(sort=sort, projection=proj)
        fields = set(response_1["data"]["document"].keys())
        assert fields == exp_fields


@pytest.mark.describe("find through vector")
def test_find(readonly_vector_collection: AstraDBCollection) -> None:
    sort = {"$vector": [0.2, 0.6]}
    options = {"limit": 100}

    response = readonly_vector_collection.find(sort=sort, options=options)
    assert isinstance(response["data"]["documents"], list)


@pytest.mark.describe("proper error raising in find")
def test_find_error(readonly_vector_collection: AstraDBCollection) -> None:
    """Wrong type of arguments should raise an API error (ValueError)."""
    sort = {"$vector": "clearly not a list of floats!"}
    options = {"limit": 100}

    with pytest.raises(ValueError):
        readonly_vector_collection.find(sort=sort, options=options)


@pytest.mark.describe("find through vector, without explicit limit")
def test_find_limitless(readonly_vector_collection: AstraDBCollection) -> None:
    sort = {"$vector": [0.2, 0.6]}
    projection = {"$vector": 1}

    response = readonly_vector_collection.find(sort=sort, projection=projection)
    assert response is not None
    assert isinstance(response["data"]["documents"], list)

###
###
###

@pytest.mark.describe("should create a vector document")
def test_create_document(collection: AstraDBCollection) -> None:
    test_uuid = str(uuid.uuid4())

    json_query = {
        "_id": test_uuid,
        "name": "Coded Cleats Copy",
        "description": "ChatGPT integrated sneakers that talk to you",
        "$vector": [0.25, 0.25, 0.25, 0.25, 0.25],
    }

    res = collection.insert_one(document=json_query)

    assert res is not None


@pytest.mark.describe("should create multiple documents: nonvector")
def test_insert_many(collection: AstraDBCollection) -> None:
    id_1 = fake.bothify(text="????????")
    id_2 = fake.bothify(text="????????")
    id_3 = fake.bothify(text="????????")
    documents = [
        {
            "_id": id_1,
            "first_name": "Dang",
            "last_name": "Son",
        },
        {
            "_id": id_2,
            "first_name": "Yep",
            "last_name": "Boss",
        },
    ]
    res = collection.insert_many(documents=documents)
    assert res is not None

    documents2 = [
        {
            "_id": id_2,
            "first_name": "Yep",
            "last_name": "Boss",
        },
        {
            "_id": id_3,
            "first_name": "Miv",
            "last_name": "Fuff",
        },
    ]
    res = collection.insert_many(
        documents=documents2,
        partial_failures_allowed=True,
    )
    print(res)
    assert set(res["status"]["insertedIds"]) == set()

    res = collection.insert_many(
        documents=documents2,
        options={"ordered": False},
        partial_failures_allowed=True,
    )
    print(res)
    assert set(res["status"]["insertedIds"]) == {id_3}

    document = collection.find(filter={"first_name": "Yep"})
    assert document is not None


@pytest.mark.describe("create many vector documents")
def test_create_documents(collection: AstraDBCollection, vv_uuid: str) -> None:
    json_query = [
        {
            "_id": str(uuid.uuid4()),
            "name": "Coded Cleats",
            "description": "ChatGPT integrated sneakers that talk to you",
            "$vector": [0.1, 0.15, 0.3, 0.12, 0.05],
        },
        {
            "_id": str(uuid.uuid4()),
            "name": "Logic Layers",
            "description": "An AI quilt to help you sleep forever",
            "$vector": [0.45, 0.09, 0.01, 0.2, 0.11],
        },
        {
            "_id": vv_uuid,
            "name": "Vision Vector Frame",
            "description": "Vision Vector Frame - A deep learning display that controls your mood",
            "$vector": [0.1, 0.05, 0.08, 0.3, 0.6],
        },
    ]

    res = collection.insert_many(documents=json_query)
    assert res is not None


@pytest.mark.describe("should create a subdocument")
def test_create_subdocument(collection: AstraDBCollection, cliff_uuid: str) -> None:
    document = collection.update_one(
        filter={"_id": cliff_uuid},
        update={"$set": {"name": "Eric"}},
    )
    print("SUBSUB", document)

    document = collection.find_one(filter={"_id": cliff_uuid})
    print("SUBDOC", document)
    assert document["data"]["document"]["name"] == "Eric"


@pytest.mark.describe("should create a document without an ID")
def test_create_document_without_id(collection: AstraDBCollection) -> None:
    response = collection.insert_one(
        document={
            "first_name": "New",
            "last_name": "Guy",
        }
    )
    assert response is not None
    document = collection.find_one(filter={"first_name": "New"})
    assert document["data"]["document"]["last_name"] == "Guy"


@pytest.mark.describe("should update a document")
def test_update_document(collection: AstraDBCollection, cliff_uuid: str) -> None:
    collection.update_one(
        filter={"_id": cliff_uuid},
        update={"$set": {"name": "Bob"}},
    )

    document = collection.find_one(filter={"_id": cliff_uuid})

    assert document["data"]["document"]["_id"] == cliff_uuid
    assert document["data"]["document"]["name"] == "Bob"


@pytest.mark.describe("replace a non-vector document")
def test_replace_document(collection: AstraDBCollection, cliff_uuid: str) -> None:
    collection.find_one_and_replace(
        filter={"_id": cliff_uuid},
        replacement={
            "_id": cliff_uuid,
            "addresses": {
                "work": {
                    "city": "New York",
                    "state": "NY",
                }
            },
        },
    )
    document = collection.find_one(filter={"_id": cliff_uuid})
    print(document)

    assert document is not None
    document_2 = collection.find_one(
        filter={"_id": cliff_uuid}, projection={"addresses.work.city": 1}
    )

@pytest.mark.describe("should delete a subdocument")
def test_delete_subdocument(collection: AstraDBCollection, cliff_uuid: str) -> None:
    response = collection.delete_subdocument(id=cliff_uuid, subdoc="addresses")
    document = collection.find(filter={"_id": cliff_uuid})
    assert response is not None
    assert document is not None


@pytest.mark.describe("should delete a single document")
def test_delete_one_document(collection: AstraDBCollection, cliff_uuid: str) -> None:
    response = collection.delete_one(id=cliff_uuid)
    assert response is not None

    document = collection.find_one(filter={"_id": cliff_uuid})
    assert document["data"]["document"] is None


@pytest.mark.describe("should delete multiple documents")
def test_delete_many_documents(collection: AstraDBCollection) -> None:
    response = collection.delete_many(filter={"anotherfield": "alpha"})
    assert response is not None

    documents = collection.find(filter={"anotherfield": "alpha"})
    assert not documents["data"]["documents"]




@pytest.mark.describe("Find one and update with vector search")
def test_find_one_and_update_vector(collection: AstraDBCollection) -> None:
    sort = {"$vector": [0.15, 0.1, 0.1, 0.35, 0.55]}
    update = {"$set": {"status": "active"}}
    options = {"returnDocument": "after"}

    result = collection.find_one_and_update(sort=sort, update=update, options=options)
    print(result)
    document = collection.find_one(filter={"status": "active"})
    print(document)
    assert document["data"]["document"] is not None




@pytest.mark.describe("should find a single document, non-vector")
def test_find_one_document(collection: AstraDBCollection) -> None:
    user_id = str(uuid.uuid4())
    collection.insert_one(
        document={
            "_id": user_id,
            "first_name": f"Cliff-{user_id}",
            "last_name": "Wicklow",
        },
    )
    user_id_2 = str(uuid.uuid4())
    collection.insert_one(
        document={
            "_id": user_id_2,
            "first_name": f"Cliff-{user_id}",
            "last_name": "Danger",
        },
    )
    document = collection.find_one(filter={"first_name": f"Cliff-{user_id}"})
    print("DOCUMENT", document)

    assert document["data"]["document"] is not None

    document = collection.find_one(filter={"first_name": "Cliff-Not-There"})
    assert document["data"]["document"] is None




@pytest.mark.describe("upsert a document")
def test_upsert_document(collection: AstraDBCollection) -> None:
    new_uuid = str(uuid.uuid4())

    collection.upsert(
        {
            "_id": new_uuid,
            "addresses": {
                "work": {
                    "city": "Seattle",
                    "state": "WA",
                }
            },
        }
    )

    document = collection.find_one(filter={"_id": new_uuid})

    # Check the document exists and that the city field is Seattle
    assert document is not None
    assert document["data"]["document"]["addresses"]["work"]["city"] == "Seattle"
    assert "country" not in document["data"]["document"]["addresses"]["work"]

    collection.upsert(
        {
            "_id": new_uuid,
            "addresses": {"work": {"city": "Everett", "state": "WA", "country": "USA"}},
        }
    )

    document = collection.find_one(filter={"_id": new_uuid})

    assert document is not None
    assert document["data"]["document"]["addresses"]["work"]["city"] == "Everett"
    assert "country" in document["data"]["document"]["addresses"]["work"]


@pytest.mark.describe("should use document functions")
def test_functions(collection: AstraDBCollection) -> None:
    user_id = str(uuid.uuid4())
    collection.insert_one(
        document={
            "_id": user_id,
            "first_name": f"Cliff-{user_id}",
            "last_name": "Wicklow",
            "roles": ["admin", "user"],
        },
    )
    pop = {"roles": 1}
    options = {"returnDocument": "after"}

    _ = collection.pop(filter={"_id": user_id}, pop=pop, options=options)

    doc_1 = collection.find_one(filter={"_id": user_id})
    assert doc_1["data"]["document"]["_id"] == user_id

    push = {"roles": "users"}
    options = {"returnDocument": "after"}

    collection.push(filter={"_id": user_id}, push=push, options=options)
    doc_2 = collection.find_one(filter={"_id": user_id})
    assert doc_2["data"]["document"]["_id"] == user_id
