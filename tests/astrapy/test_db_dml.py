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
from typing import Dict, List, Optional, Union

import pytest

from astrapy.types import API_DOC
from astrapy.db import AstraDB, AstraDBCollection

TEST_TRUNCATED_NONVECTOR_COLLECTION_NAME = "ephemeral_tr_non_v_col"
TEST_TRUNCATED_VECTOR_COLLECTION_NAME = "ephemeral_tr_v_col"

logger = logging.getLogger(__name__)


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


@pytest.mark.describe("find_one, not through vector")
def test_find_one_filter_novector(
    readonly_vector_collection: AstraDBCollection, cliff_uuid: str
) -> None:
    response = readonly_vector_collection.find_one(
        filter={"_id": "1"},
    )
    document = response["data"]["document"]
    assert document["text"] == "Sample entry number <1>"
    assert (
        document.keys() ^ {"_id", "text", "otherfield", "anotherfield", "$vector"}
        == set()
    )

    response_not_by_id = readonly_vector_collection.find_one(
        filter={"text": "Sample entry number <1>"},
    )
    document_not_by_id = response_not_by_id["data"]["document"]
    assert document_not_by_id["_id"] == "1"
    assert (
        document_not_by_id.keys()
        ^ {"_id", "text", "otherfield", "anotherfield", "$vector"}
        == set()
    )

    response_no = readonly_vector_collection.find_one(
        filter={"_id": "Z"},
    )
    document_no = response_no["data"]["document"]
    assert document_no is None

    response_no_not_by_id = readonly_vector_collection.find_one(
        filter={"text": "No such text."},
    )
    document_no_not_by_id = response_no_not_by_id["data"]["document"]
    assert document_no_not_by_id is None


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
def test_find_find_one_projection(
    readonly_vector_collection: AstraDBCollection,
) -> None:
    query = [0.2, 0.6]
    sort = {"$vector": query}
    options = {"limit": 1}
    projs: List[Optional[Dict[str, Union[int, None]]]] = [
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
        response_n = readonly_vector_collection.find(
            sort=sort, options=options, projection=proj
        )
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


@pytest.mark.describe("correctly count documents according to predicate")
def test_count_documents(
    readonly_vector_collection: AstraDBCollection,
) -> None:
    c_all_response0 = readonly_vector_collection.count_documents()
    assert c_all_response0["status"]["count"] == 3

    c_all_response1 = readonly_vector_collection.count_documents(filter={})
    assert c_all_response1["status"]["count"] == 3

    c_pred_response = readonly_vector_collection.count_documents(
        filter={"anotherfield": "alpha"}
    )
    assert c_pred_response["status"]["count"] == 2

    c_no_response = readonly_vector_collection.count_documents(
        filter={"false_field": 137}
    )
    assert c_no_response["status"]["count"] == 0


@pytest.mark.describe("insert_one, w/out _id, w/out vector")
def test_create_document(writable_vector_collection: AstraDBCollection) -> None:
    i_vector = [0.3, 0.5]
    id_v_i = str(uuid.uuid4())
    result_v_i = writable_vector_collection.insert_one(
        {
            "_id": id_v_i,
            "a": 1,
            "$vector": i_vector,
        }
    )
    assert result_v_i["status"]["insertedIds"] == [id_v_i]
    assert (
        writable_vector_collection.find_one(
            {"_id": result_v_i["status"]["insertedIds"][0]}
        )["data"]["document"]["a"]
        == 1
    )

    id_n_i = str(uuid.uuid4())
    result_n_i = writable_vector_collection.insert_one(
        {
            "_id": id_n_i,
            "a": 2,
        }
    )
    assert result_n_i["status"]["insertedIds"] == [id_n_i]
    assert (
        writable_vector_collection.find_one(
            {"_id": result_n_i["status"]["insertedIds"][0]}
        )["data"]["document"]["a"]
        == 2
    )

    with pytest.raises(ValueError):
        result_n_i = writable_vector_collection.insert_one(
            {
                "_id": id_n_i,
                "a": 3,
            }
        )

    result_v_n = writable_vector_collection.insert_one(
        {
            "a": 4,
            "$vector": i_vector,
        }
    )
    assert isinstance(result_v_n["status"]["insertedIds"], list)
    assert isinstance(result_v_n["status"]["insertedIds"][0], str)
    assert len(result_v_n["status"]["insertedIds"]) == 1
    assert (
        writable_vector_collection.find_one(
            {"_id": result_v_n["status"]["insertedIds"][0]}
        )["data"]["document"]["a"]
        == 4
    )

    result_n_n = writable_vector_collection.insert_one(
        {
            "a": 5,
        }
    )
    assert isinstance(result_n_n["status"]["insertedIds"], list)
    assert isinstance(result_n_n["status"]["insertedIds"][0], str)
    assert len(result_n_n["status"]["insertedIds"]) == 1
    assert (
        writable_vector_collection.find_one(
            {"_id": result_n_n["status"]["insertedIds"][0]}
        )["data"]["document"]["a"]
        == 5
    )


@pytest.mark.describe("should truncate a nonvector collection")
def test_insert_float32(
    writable_vector_collection: AstraDBCollection, N: int = 2
) -> None:
    _id0 = str(uuid.uuid4())
    document = {
        "_id": _id0,
        "name": "Coerce",
        "$vector": [f"{(i+1)/N+2:.4f}" for i in range(N)],
    }
    response = writable_vector_collection.insert_one(document)
    assert response is not None
    inserted_ids = response["status"]["insertedIds"]
    assert len(inserted_ids) == 1
    assert inserted_ids[0] == _id0


@pytest.mark.describe("insert_many")
def test_insert_many(writable_vector_collection: AstraDBCollection) -> None:
    _id0 = str(uuid.uuid4())
    _id2 = str(uuid.uuid4())
    documents: List[API_DOC] = [
        {
            "_id": _id0,
            "name": "Abba",
            "traits": [10, 9, 3],
            "$vector": [0.6, 0.2],
        },
        {
            "name": "Bacchus",
            "happy": True,
        },
        {
            "_id": _id2,
            "name": "Ciccio",
            "description": "The thid in this list",
            "$vector": [0.4, 0.3],
        },
    ]

    response = writable_vector_collection.insert_many(documents)
    assert response is not None
    inserted_ids = set(response["status"]["insertedIds"])
    assert len(inserted_ids - {_id0, _id2}) == 1
    assert isinstance(list(inserted_ids - {_id0, _id2})[0], str)


@pytest.mark.describe("chunked_insert_many")
def test_chunked_insert_many(
    writable_vector_collection: AstraDBCollection,
) -> None:
    _id0 = str(uuid.uuid4())
    _id2 = str(uuid.uuid4())
    documents: List[API_DOC] = [
        {
            "_id": "c6910e2a-22d3-4995-b7e2-8993d36aac5f",
            "name": "Abba",
            "traits": [10, 9, 3],
            "$vector": [0.6, 0.2],
        },
        {
            "name": "Bacchus",
            "happy": True,
            "_id": "5a056a1d-493a-4823-900f-e996ea320510",
        },
        {
            "_id": "4d567bc8-0c30-4ce7-bbc2-756d2e92afe4",
            "name": "Ciccio",
            "description": "The third in this list",
            "$vector": [0.4, 0.3],
        },
        {
            "_id": "3c163130-24e3-44f0-86c3-f9ef3863206c",
            "name": "Abba",
            "traits": [10, 9, 3],
            "$vector": [0.6, 0.2],
        },
        {
            "name": "Bacchus",
            "happy": True,
            "_id": "3994bff5-c1e2-4bb3-9f4e-913d59c103d9",
        },
        {
            "_id": "529f5064-6630-4ce1-b065-6a5b3846a70b",
            "name": "Ciccio",
            "description": "The third in this list",
            "$vector": [0.4, 0.3],
        },
        {
            "_id": "a3bb7271-966a-4b85-9171-46e97b1bcbbe",
            "name": "Abba",
            "traits": [10, 9, 3],
            "$vector": [0.6, 0.2],
        },
        {
            "name": "Bacchus",
            "happy": True,
            "_id": "acde6241-3ced-45dd-9cee-f31887bb2409",
        },
        {
            "_id": "686206fc-1416-48b3-9f62-1bb104e35681",
            "name": "Ciccio",
            "description": "The third in this list",
            "$vector": [0.4, 0.3],
        },
        {
            "_id": "09a5f695-b3b2-4bd7-ab4f-2dc4bcb24dbb",
            "name": "Abba",
            "traits": [10, 9, 3],
            "$vector": [0.6, 0.2],
        },
        {
            "name": "Bacchus",
            "happy": True,
            "_id": "4c88e9fb-a262-493a-aa17-5668f78bf1db",
        },
        {
            "_id": "40935112-9a5d-4b42-bbe8-c3446a7f847f",
            "name": "Ciccio",
            "description": "The third in this list",
            "$vector": [0.4, 0.3],
        },
        {
            "_id": "43b28d05-889c-448f-b179-4394b94981e9",
            "name": "Abba",
            "traits": [10, 9, 3],
            "$vector": [0.6, 0.2],
        },
        {
            "name": "Bacchus",
            "happy": True,
            "_id": "8d7fcb5f-1c2d-4aa9-a258-889981889f33",
        },
        {
            "_id": "c70abde2-722a-4102-94bb-5f1c69368b84",
            "name": "Ciccio",
            "description": "The third in this list",
            "$vector": [0.4, 0.3],
        },
        {
            "_id": "76c76dd8-1166-4398-9c39-4d4d52764055",
            "name": "Abba",
            "traits": [10, 9, 3],
            "$vector": [0.6, 0.2],
        },
        {
            "name": "Bacchus",
            "happy": True,
            "_id": "2291711b-3ce3-4f44-8dd3-5d025c82b58b",
        },
        {
            "_id": "ec0f78ed-80be-401d-8be5-8b2d40d7eda7",
            "name": "Ciccio",
            "description": "The third in this list",
            "$vector": [0.4, 0.3],
        },
        {
            "_id": "dc4fab70-e235-47ce-9d72-b71fde42308d",
            "name": "Abba",
            "traits": [10, 9, 3],
            "$vector": [0.6, 0.2],
        },
        {
            "name": "Bacchus",
            "happy": True,
            "_id": "15be4aaa-2c22-4a25-bbfb-e475f94e6ebd",
        },
        {
            "_id": "c8e55fc3-851c-44a9-ac58-2ae9f2fe8b35",
            "name": "Ciccio",
            "description": "The third in this list",
            "$vector": [0.4, 0.3],
        },
        {
            "_id": "c113e718-646d-4358-821a-a3adfa2f5cd8",
            "name": "Abba",
            "traits": [10, 9, 3],
            "$vector": [0.6, 0.2],
        },
        {
            "name": "Bacchus",
            "happy": True,
            "_id": "c587f215-4f95-415b-b12b-98e067fd2954",
        },
        {
            "_id": "405d12f7-e754-4405-8b1a-7c6c808d4ee2",
            "name": "Ciccio",
            "description": "The third in this list",
            "$vector": [0.4, 0.3],
        },
        {
            "_id": "5c2d39bd-5766-4c46-bc20-105e4a3ccad5",
            "name": "Abba",
            "traits": [10, 9, 3],
            "$vector": [0.6, 0.2],
        },
        {
            "name": "Bacchus",
            "happy": True,
            "_id": "e267e980-aff1-4344-8432-651764e3c118",
        },
        {
            "_id": "f38ee411-f53b-4e2c-a3c3-7f274d4bd471",
            "name": "Ciccio",
            "description": "The third in this list",
            "$vector": [0.4, 0.3],
        },
        {
            "_id": "88f67701-0e0b-4ea0-95d7-5e2b1b9b3809",
            "name": "Abba",
            "traits": [10, 9, 3],
            "$vector": [0.6, 0.2],
        },
        {
            "name": "Bacchus",
            "happy": True,
            "_id": "aa7571ad-a429-4b4f-9498-519ea9303de1",
        },
        {
            "_id": "90249293-07a7-43a0-a7f7-310a1ad91bad",
            "name": "Ciccio",
            "description": "The third in this list",
            "$vector": [0.4, 0.3],
        },
    ]

    response = writable_vector_collection.chunked_insert_many(documents)
    assert response is not None
    inserted_ids_list = [set(r["status"]["insertedIds"]) for r in response]
    inserted_ids = set.union(*inserted_ids_list)
    assert len(inserted_ids) == 30
    assert isinstance(list(inserted_ids)[0], str)


@pytest.mark.describe("insert_many with 'ordered' set to False")
def test_insert_many_ordered_false(
    writable_vector_collection: AstraDBCollection,
) -> None:
    _id0 = str(uuid.uuid4())
    _id1 = str(uuid.uuid4())
    _id2 = str(uuid.uuid4())
    documents_a = [
        {
            "_id": _id0,
            "first_name": "Dang",
            "last_name": "Son",
        },
        {
            "_id": _id1,
            "first_name": "Yep",
            "last_name": "Boss",
        },
    ]
    response_a = writable_vector_collection.insert_many(documents_a)
    assert response_a is not None
    assert response_a["status"]["insertedIds"] == [_id0, _id1]

    documents_b = [
        {
            "_id": _id1,
            "first_name": "Maureen",
            "last_name": "Caloggero",
        },
        {
            "_id": _id2,
            "first_name": "Miv",
            "last_name": "Fuff",
        },
    ]
    response_b = writable_vector_collection.insert_many(
        documents_b,
        partial_failures_allowed=True,
    )
    assert response_b is not None
    assert response_b["status"]["insertedIds"] == []

    response_b2 = writable_vector_collection.insert_many(
        documents=documents_b,
        options={"ordered": False},
        partial_failures_allowed=True,
    )
    assert response_b2 is not None
    assert response_b2["status"]["insertedIds"] == [_id2]

    check_response = writable_vector_collection.find_one(filter={"first_name": "Yep"})
    assert check_response is not None
    assert check_response["data"]["document"]["_id"] == _id1


@pytest.mark.describe("upsert_many")
def test_upsert_many(
    writable_vector_collection: AstraDBCollection,
) -> None:
    _id0 = str(uuid.uuid4())
    _id1 = str(uuid.uuid4())

    documents = [
        {
            "_id": _id0,
            "addresses": {
                "work": {
                    "city": "Seattle",
                    "state": "WA",
                },
            },
        },
        {
            "_id": _id1,
            "addresses": {
                "work": {
                    "city": "Seattle",
                    "state": "WA",
                },
            },
        },
    ]
    upsert_result0 = writable_vector_collection.upsert_many(documents)
    assert upsert_result0[0] == _id0
    assert upsert_result0[1] == _id1

    response0 = writable_vector_collection.find_one(filter={"_id": _id0})
    assert response0 is not None
    assert response0["data"]["document"] == documents[0]

    response0 = writable_vector_collection.find_one(filter={"_id": _id1})
    assert response0 is not None
    assert response0["data"]["document"] == documents[1]

    documents2 = [
        {
            "_id": _id0,
            "addresses": {
                "work": {
                    "state": "MN",
                    "floor": 12,
                },
            },
            "hobbies": [
                "ice skating",
                "accounting",
            ],
        }
    ]
    upsert_result2 = writable_vector_collection.upsert_many(documents2)
    assert upsert_result2[0] == _id0

    response2 = writable_vector_collection.find_one(filter={"_id": _id0})
    assert response2 is not None
    assert response2["data"]["document"] == documents2[0]


@pytest.mark.describe("upsert")
def test_upsert_document(writable_vector_collection: AstraDBCollection) -> None:
    _id = str(uuid.uuid4())

    document0 = {
        "_id": _id,
        "addresses": {
            "work": {
                "city": "Seattle",
                "state": "WA",
            },
        },
    }
    upsert_result0 = writable_vector_collection.upsert(document0)
    assert upsert_result0 == _id

    response0 = writable_vector_collection.find_one(filter={"_id": _id})
    assert response0 is not None
    assert response0["data"]["document"] == document0

    document1 = {
        "_id": _id,
        "addresses": {
            "work": {
                "state": "MN",
                "floor": 12,
            },
        },
        "hobbies": [
            "ice skating",
            "accounting",
        ],
    }
    upsert_result1 = writable_vector_collection.upsert(document1)
    assert upsert_result1 == _id

    response1 = writable_vector_collection.find_one(filter={"_id": _id})
    assert response1 is not None
    assert response1["data"]["document"] == document1


@pytest.mark.describe("update_one to create a subdocument, not through vector")
def test_update_one_create_subdocument_novector(
    disposable_vector_collection: AstraDBCollection,
) -> None:
    update_one_response = disposable_vector_collection.update_one(
        filter={"_id": "1"},
        update={"$set": {"name": "Eric"}},
    )

    assert update_one_response["status"]["matchedCount"] >= 1
    assert update_one_response["status"]["modifiedCount"] == 1

    response = disposable_vector_collection.find_one(filter={"_id": "1"})
    assert response["data"]["document"]["name"] == "Eric"


@pytest.mark.describe("delete_subdocument, not through vector")
def test_delete_subdocument_novector(
    disposable_vector_collection: AstraDBCollection,
) -> None:
    delete_subdocument_response = disposable_vector_collection.delete_subdocument(
        id="1",
        subdoc="otherfield.subfield",
    )

    assert delete_subdocument_response["status"]["matchedCount"] >= 1
    assert delete_subdocument_response["status"]["modifiedCount"] == 1

    response = disposable_vector_collection.find_one(filter={"_id": "1"})
    assert response["data"]["document"]["otherfield"] == {}


@pytest.mark.describe("find_one_and_update, through vector")
def test_find_one_and_update_vector(
    disposable_vector_collection: AstraDBCollection,
) -> None:
    find_filter = {"status": {"$exists": True}}
    response0 = disposable_vector_collection.find_one(filter=find_filter)
    assert response0["data"]["document"] is None

    sort = {"$vector": [0.2, 0.6]}

    update0 = {"$set": {"status": "active"}}
    options0 = {"returnDocument": "after"}

    update_response0 = disposable_vector_collection.find_one_and_update(
        sort=sort, update=update0, options=options0
    )
    assert isinstance(update_response0["data"]["document"], dict)
    assert update_response0["data"]["document"]["status"] == "active"
    assert update_response0["status"]["matchedCount"] >= 1
    assert update_response0["status"]["modifiedCount"] >= 1

    response1 = disposable_vector_collection.find_one(filter=find_filter)
    assert isinstance(response1["data"]["document"], dict)
    assert response1["data"]["document"]["status"] == "active"

    update1 = {"$set": {"status": "inactive"}}
    options1 = {"returnDocument": "before"}

    update_response1 = disposable_vector_collection.find_one_and_update(
        sort=sort, update=update1, options=options1
    )
    assert isinstance(update_response1["data"]["document"], dict)
    assert update_response1["data"]["document"]["status"] == "active"
    assert update_response1["status"]["matchedCount"] >= 1
    assert update_response1["status"]["modifiedCount"] >= 1

    response2 = disposable_vector_collection.find_one(filter=find_filter)
    assert isinstance(response2["data"]["document"], dict)
    assert response2["data"]["document"]["status"] == "inactive"

    filter2 = {"nonexistent_subfield": 10}
    update2 = update1
    options2 = options1

    update_response2 = disposable_vector_collection.find_one_and_update(
        sort=sort, update=update2, options=options2, filter=filter2
    )
    assert update_response2["data"]["document"] is None
    assert update_response2["status"]["matchedCount"] == 0
    assert update_response2["status"]["modifiedCount"] == 0


@pytest.mark.describe("find_one_and_update, not through vector")
def test_find_one_and_update_novector(
    disposable_vector_collection: AstraDBCollection,
) -> None:
    find_filter = {"status": {"$exists": True}}
    response0 = disposable_vector_collection.find_one(filter=find_filter)
    assert response0["data"]["document"] is None

    update_filter = {"anotherfield": "omega"}

    update0 = {"$set": {"status": "active"}}
    options0 = {"returnDocument": "after"}

    update_response0 = disposable_vector_collection.find_one_and_update(
        filter=update_filter, update=update0, options=options0
    )
    assert isinstance(update_response0["data"]["document"], dict)
    assert update_response0["data"]["document"]["status"] == "active"
    assert update_response0["status"]["matchedCount"] >= 1
    assert update_response0["status"]["modifiedCount"] >= 1

    response1 = disposable_vector_collection.find_one(filter=find_filter)
    assert isinstance(response1["data"]["document"], dict)
    assert response1["data"]["document"]["status"] == "active"

    update1 = {"$set": {"status": "inactive"}}
    options1 = {"returnDocument": "before"}

    update_response1 = disposable_vector_collection.find_one_and_update(
        filter=update_filter, update=update1, options=options1
    )
    assert isinstance(update_response1["data"]["document"], dict)
    assert update_response1["data"]["document"]["status"] == "active"
    assert update_response1["status"]["matchedCount"] >= 1
    assert update_response1["status"]["modifiedCount"] >= 1

    response2 = disposable_vector_collection.find_one(filter=find_filter)
    assert isinstance(response2["data"]["document"], dict)
    assert response2["data"]["document"]["status"] == "inactive"

    filter2 = {**update_filter, **{"nonexistent_subfield": 10}}
    update2 = update1
    options2 = options1

    update_response2 = disposable_vector_collection.find_one_and_update(
        filter=filter2, update=update2, options=options2
    )
    assert update_response2["data"]["document"] is None
    assert update_response2["status"]["matchedCount"] == 0
    assert update_response2["status"]["modifiedCount"] == 0


@pytest.mark.describe("find_one_and_replace, through vector")
def test_find_one_and_replace_vector(
    disposable_vector_collection: AstraDBCollection,
) -> None:
    sort = {"$vector": [0.2, 0.6]}

    response0 = disposable_vector_collection.find_one(sort=sort)
    assert response0 is not None
    assert "anotherfield" in response0["data"]["document"]

    doc0vector = response0["data"]["document"]["$vector"]

    replace_response0 = disposable_vector_collection.find_one_and_replace(
        sort=sort,
        replacement={
            "phyla": ["Echinodermata", "Platelminta", "Chordata"],
            "$vector": doc0vector,  # to find this doc again below!
        },
    )
    assert replace_response0 is not None
    assert "anotherfield" in replace_response0["data"]["document"]

    response1 = disposable_vector_collection.find_one(sort=sort)
    assert response1 is not None
    assert response1["data"]["document"]["phyla"] == [
        "Echinodermata",
        "Platelminta",
        "Chordata",
    ]
    assert "anotherfield" not in response1["data"]["document"]

    replace_response1 = disposable_vector_collection.find_one_and_replace(
        sort=sort,
        replacement={
            "phone": "0123-4567",
            "$vector": doc0vector,
        },
    )
    assert replace_response1 is not None
    assert replace_response1["data"]["document"]["phyla"] == [
        "Echinodermata",
        "Platelminta",
        "Chordata",
    ]
    assert "anotherfield" not in replace_response1["data"]["document"]

    response2 = disposable_vector_collection.find_one(sort=sort)
    assert response2 is not None
    assert response2["data"]["document"]["phone"] == "0123-4567"
    assert "phyla" not in response2["data"]["document"]

    # non-existing-doc case
    filter_no = {"nonexisting_field": -123}
    replace_response_no = disposable_vector_collection.find_one_and_replace(
        sort=sort,
        filter=filter_no,
        replacement={
            "whatever": -123,
            "$vector": doc0vector,
        },
    )
    assert replace_response_no is not None
    assert replace_response_no["data"]["document"] is None


@pytest.mark.describe("find_one_and_replace, not through vector")
def test_find_one_and_replace_novector(
    disposable_vector_collection: AstraDBCollection,
) -> None:
    response0 = disposable_vector_collection.find_one(filter={"_id": "1"})
    assert response0 is not None
    assert response0["data"]["document"]["anotherfield"] == "alpha"

    replace_response0 = disposable_vector_collection.find_one_and_replace(
        filter={"_id": "1"},
        replacement={
            "_id": "1",
            "phyla": ["Echinodermata", "Platelminta", "Chordata"],
        },
    )
    assert replace_response0 is not None
    assert replace_response0["data"]["document"]["anotherfield"] == "alpha"

    response1 = disposable_vector_collection.find_one(filter={"_id": "1"})
    assert response1 is not None
    assert response1["data"]["document"]["phyla"] == [
        "Echinodermata",
        "Platelminta",
        "Chordata",
    ]
    assert "anotherfield" not in response1["data"]["document"]

    replace_response1 = disposable_vector_collection.find_one_and_replace(
        filter={"_id": "1"},
        replacement={
            "phone": "0123-4567",
        },
    )
    assert replace_response1 is not None
    assert replace_response1["data"]["document"]["phyla"] == [
        "Echinodermata",
        "Platelminta",
        "Chordata",
    ]
    assert "anotherfield" not in replace_response1["data"]["document"]

    response2 = disposable_vector_collection.find_one(filter={"_id": "1"})
    assert response2 is not None
    assert response2["data"]["document"]["phone"] == "0123-4567"
    assert "phyla" not in response2["data"]["document"]

    # non-existing-doc case
    replace_response_no = disposable_vector_collection.find_one_and_replace(
        filter={"_id": "z"},
        replacement={
            "whatever": -123,
        },
    )
    assert replace_response_no is not None
    assert replace_response_no["data"]["document"] is None


@pytest.mark.describe("delete_one, not through vector")
def test_delete_one_novector(disposable_vector_collection: AstraDBCollection) -> None:
    delete_response = disposable_vector_collection.delete_one(id="3")
    assert delete_response["status"]["deletedCount"] == 1

    response = disposable_vector_collection.find_one(filter={"_id": "3"})
    assert response["data"]["document"] is None

    delete_response_no = disposable_vector_collection.delete_one(id="3")
    assert delete_response_no["status"]["deletedCount"] == 0


@pytest.mark.describe("delete_many, not through vector")
def test_delete_many_novector(disposable_vector_collection: AstraDBCollection) -> None:
    delete_response = disposable_vector_collection.delete_many(
        filter={"anotherfield": "alpha"}
    )
    assert delete_response["status"]["deletedCount"] == 2

    documents_no = disposable_vector_collection.find(filter={"anotherfield": "alpha"})
    assert documents_no["data"]["documents"] == []

    delete_response_no = disposable_vector_collection.delete_many(
        filter={"anotherfield": "alpha"}
    )
    assert delete_response_no["status"]["deletedCount"] == 0


@pytest.mark.describe("pop, push functions, not through vector")
def test_pop_push_novector(disposable_vector_collection: AstraDBCollection) -> None:
    user_id = str(uuid.uuid4())
    disposable_vector_collection.insert_one(
        document={
            "_id": user_id,
            "first_name": "Cliff",
            "last_name": "Wicklow",
            "roles": ["user", "admin"],
        },
    )

    pop = {"roles": 1}
    options = {"returnDocument": "after"}

    pop_response = disposable_vector_collection.pop(
        filter={"_id": user_id}, pop=pop, options=options
    )
    assert pop_response is not None
    assert pop_response["data"]["document"]["roles"] == ["user"]
    assert pop_response["status"]["matchedCount"] >= 1
    assert pop_response["status"]["modifiedCount"] == 1

    response1 = disposable_vector_collection.find_one(filter={"_id": user_id})
    assert response1 is not None
    assert response1["data"]["document"]["roles"] == ["user"]

    push = {"roles": "auditor"}

    push_response = disposable_vector_collection.push(
        filter={"_id": user_id}, push=push, options=options
    )
    assert push_response is not None
    assert push_response["data"]["document"]["roles"] == ["user", "auditor"]
    assert push_response["status"]["matchedCount"] >= 1
    assert push_response["status"]["modifiedCount"] == 1

    response2 = disposable_vector_collection.find_one(filter={"_id": user_id})
    assert response2 is not None
    assert response2["data"]["document"]["roles"] == ["user", "auditor"]
