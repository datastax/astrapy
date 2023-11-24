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

import uuid
import logging

import pytest
from faker import Faker

from astrapy.db import AstraDB, AstraDBCollection
from astrapy.defaults import DEFAULT_KEYSPACE_NAME

TEST_CREATE_DELETE_VECTOR_COLLECTION_NAME = "ephemeral_v_col"
TEST_CREATE_DELETE_NONVECTOR_COLLECTION_NAME = "ephemeral_non_v_col"

logger = logging.getLogger(__name__)
fake = Faker()


@pytest.mark.describe("should fail truncating a non-existent collection")
def test_truncate_collection_fail(db: AstraDB) -> None:
    with pytest.raises(ValueError):
        db.truncate_collection("this$does%not exist!!!")





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


@pytest.mark.describe("Find one document")
def test_find_document(collection: AstraDBCollection, cliff_uuid: str) -> None:
    document = collection.find_one(filter={"_id": cliff_uuid})
    print("DOC", document)
    assert document is not None


@pytest.mark.describe("Vector find one document")
def test_vector_find_document(collection: AstraDBCollection) -> None:
    documents = collection.vector_find_one(
        [0.15, 0.1, 0.1, 0.35, 0.55],
    )

    assert documents is not None
    assert len(documents) > 0


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
    response = collection.delete_many(filter={"anotherfield": "delete_me"})
    assert response is not None

    documents = collection.find(filter={"anotherfield": "delete_me"})
    assert not documents["data"]["documents"]


@pytest.mark.describe("Find documents using vector search")
def test_find_documents_vector(collection: AstraDBCollection) -> None:
    sort = {"$vector": [0.15, 0.1, 0.1, 0.35, 0.55]}
    options = {"limit": 100}

    document = collection.find(sort=sort, options=options)
    assert document is not None


@pytest.mark.describe("Vector find documents using vector search")
def test_vector_find_documents_vector(collection: AstraDBCollection) -> None:
    documents_sim_1 = collection.vector_find(
        vector=[0.15, 0.1, 0.1, 0.35, 0.55],
        limit=3,
    )

    assert documents_sim_1 is not None
    assert isinstance(documents_sim_1, list)
    assert len(documents_sim_1) > 0
    assert "_id" in documents_sim_1[0]
    assert "$vector" in documents_sim_1[0]
    assert "name" in documents_sim_1[0]
    assert "$similarity" in documents_sim_1[0]

    documents_sim_2 = collection.vector_find(
        vector=[0.15, 0.1, 0.1, 0.35, 0.55],
        limit=3,
        include_similarity=True,
    )

    assert documents_sim_2 is not None
    assert isinstance(documents_sim_2, list)
    assert len(documents_sim_2) > 0
    assert "_id" in documents_sim_2[0]
    assert "$vector" in documents_sim_2[0]
    assert "name" in documents_sim_2[0]
    assert "$similarity" in documents_sim_2[0]

    documents_no_sim = collection.vector_find(
        [0.15, 0.1, 0.1, 0.35, 0.55],
        limit=3,
        fields=["_id", "$vector"],
        include_similarity=False,
    )

    assert documents_no_sim is not None
    assert isinstance(documents_no_sim, list)
    assert len(documents_no_sim) > 0
    assert "_id" in documents_no_sim[0]
    assert "$vector" in documents_no_sim[0]
    assert "name" not in documents_no_sim[0]
    assert "$similarity" not in documents_no_sim[0]


@pytest.mark.describe("Find documents using vector search with error")
def test_find_documents_vector_error(collection: AstraDBCollection) -> None:
    # passing 'sort' as a tuple must raise an error by the API:
    sort = ({"$vector": [0.15, 0.1, 0.1, 0.35, 0.55]},)
    options = {"limit": 100}

    with pytest.raises(ValueError):
        collection.find(sort=sort, options=options)  # type: ignore


@pytest.mark.describe("Find documents using vector search and projection")
def test_find_documents_vector_proj_limit_sim(collection: AstraDBCollection) -> None:
    sort = {"$vector": [0.15, 0.1, 0.1, 0.35, 0.55]}
    options = {"limit": 100}
    projection = {"$vector": 1}

    document = collection.find(sort=sort, options=options, projection=projection)
    assert document is not None


@pytest.mark.describe("Find a document using vector search and projection")
def test_find_documents_vector_proj_nolimit(collection: AstraDBCollection) -> None:
    sort = {"$vector": [0.15, 0.1, 0.1, 0.35, 0.55]}
    projection = {"$vector": 1}

    document = collection.find(sort=sort, options={}, projection=projection)
    assert document is not None


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


@pytest.mark.describe("Vector find documents using vector search")
def test_vector_find_one_and_update_vector(collection: AstraDBCollection) -> None:
    update = {"$set": {"status": "active"}}

    collection.vector_find_one_and_update(
        vector=[0.15, 0.1, 0.1, 0.35, 0.55],
        update=update,
    )

    document = collection.vector_find_one(
        vector=[0.15, 0.1, 0.1, 0.35, 0.55],
        filter={"status": "active"},
    )

    assert document is not None


@pytest.mark.describe("Find one and replace with vector search")
def test_find_one_and_replace_vector(
    collection: AstraDBCollection, vv_uuid: str
) -> None:
    sort = {"$vector": [0.15, 0.1, 0.1, 0.35, 0.55]}
    replacement = {
        "_id": vv_uuid,
        "name": "Vision Vector Frame",
        "description": "Vision Vector Frame - A deep learning display that controls your mood",
        "$vector": [0.1, 0.05, 0.08, 0.3, 0.6],
        "status": "inactive",
    }
    options = {"returnDocument": "after"}

    collection.find_one_and_replace(sort=sort, replacement=replacement, options=options)
    document = collection.find_one(filter={"name": "Vision Vector Frame"})
    assert document["data"]["document"] is not None


@pytest.mark.describe("Vector find documents using vector search")
def test_vector_find_one_and_replace_vector(
    collection: AstraDBCollection, vv_uuid: str
) -> None:
    replacement = {
        "_id": vv_uuid,
        "name": "Vision Vector Frame 2",
        "description": "Vision Vector Frame - A deep learning display that controls your mood",
        "$vector": [0.1, 0.05, 0.08, 0.3, 0.6],
        "status": "inactive",
    }

    collection.vector_find_one_and_replace(
        vector=[0.15, 0.1, 0.1, 0.35, 0.55],
        replacement=replacement,
    )

    document = collection.vector_find_one(
        vector=[0.15, 0.1, 0.1, 0.35, 0.55],
        filter={"name": "Vision Vector Frame 2"},
    )

    assert document is not None


@pytest.mark.describe("should find documents, non-vector")
def test_find_documents(collection: AstraDBCollection) -> None:
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
    document = collection.find(filter={"first_name": f"Cliff-{user_id}"})
    assert document is not None


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


@pytest.mark.describe("obey projection in find")
def test_find_projection(projection_collection: AstraDBCollection) -> None:
    query = [0.15, 0.1, 0.1, 0.35, 0.55]
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
        docs = projection_collection.find(sort=sort, options=options, projection=proj)
        fields = set(docs["data"]["documents"][0].keys())
        assert fields == exp_fields


@pytest.mark.describe("obey projection in vector_find")
def test_vector_find_projection(projection_collection: AstraDBCollection) -> None:
    query = [0.15, 0.1, 0.1, 0.35, 0.55]

    req_fieldsets = [
        None,
        set(),
        {"text"},
        {"$vector"},
        {"text", "$vector"},
    ]
    exp_fieldsets = [
        {"$vector", "_id", "otherfield", "anotherfield", "text"},
        {"$vector", "_id", "otherfield", "anotherfield", "text"},
        {"_id", "text"},
        {"$vector", "_id"},
        {"$vector", "_id", "text"},
    ]
    for include_similarity in [True, False]:
        for req_fields, exp_fields0 in zip(req_fieldsets, exp_fieldsets):
            vdocs = projection_collection.vector_find(
                query,
                limit=1,
                fields=list(req_fields) if req_fields is not None else req_fields,
                include_similarity=include_similarity,
            )
            if include_similarity:
                exp_fields = exp_fields0 | {"$similarity"}
            else:
                exp_fields = exp_fields0
            assert set(vdocs[0].keys()) == exp_fields


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


@pytest.mark.describe("should truncate a collection")
def test_truncate_collection(db: AstraDB, collection: AstraDBCollection) -> None:
    res = collection.vector_find([0.1, 0.1, 0.2, 0.5, 1], limit=3)

    assert len(res) > 0

    db.truncate_collection(collection_name=collection.collection_name)

    res = collection.vector_find([0.1, 0.1, 0.2, 0.5, 1], limit=3)

    assert len(res) == 0


@pytest.mark.describe("should truncate a nonvector collection")
def test_truncate_nonvector_collection(db: AstraDB) -> None:
    col = db.create_collection("test_nonvector")
    col.insert_one({"a": 1})
    assert len(col.find()["data"]["documents"]) == 1
    db.truncate_collection("test_nonvector")
    assert len(col.find()["data"]["documents"]) == 0
    db.delete_collection("test_nonvector")




