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
import datetime
import logging
from typing import Any, cast, Dict, Iterable, List, Literal, Optional, Union

import pytest

from astrapy.api import APIRequestError
from astrapy.types import API_DOC
from astrapy.db import AsyncAstraDB, AsyncAstraDBCollection

TEST_TRUNCATED_NONVECTOR_COLLECTION_NAME = "ephemeral_tr_non_v_col"
TEST_TRUNCATED_VECTOR_COLLECTION_NAME = "ephemeral_tr_v_col"

logger = logging.getLogger(__name__)


@pytest.mark.describe("should fail truncating a non-existent collection")
async def test_truncate_collection_fail(async_db: AsyncAstraDB) -> None:
    with pytest.raises(ValueError):
        await async_db.truncate_collection("this$does%not exist!!!")


@pytest.mark.describe("should truncate a nonvector collection")
async def test_truncate_nonvector_collection(async_db: AsyncAstraDB) -> None:
    col = await async_db.create_collection(TEST_TRUNCATED_NONVECTOR_COLLECTION_NAME)
    try:
        await col.insert_one({"a": 1})
        assert len((await col.find())["data"]["documents"]) == 1
        await async_db.truncate_collection(TEST_TRUNCATED_NONVECTOR_COLLECTION_NAME)
        assert len((await col.find())["data"]["documents"]) == 0
    finally:
        await async_db.delete_collection(TEST_TRUNCATED_NONVECTOR_COLLECTION_NAME)


@pytest.mark.describe("should truncate a collection")
async def test_truncate_vector_collection(async_db: AsyncAstraDB) -> None:
    col = await async_db.create_collection(
        TEST_TRUNCATED_VECTOR_COLLECTION_NAME, dimension=2
    )
    try:
        await col.insert_one({"a": 1, "$vector": [0.1, 0.2]})
        assert len((await col.find())["data"]["documents"]) == 1
        await async_db.truncate_collection(TEST_TRUNCATED_VECTOR_COLLECTION_NAME)
        assert len((await col.find())["data"]["documents"]) == 0
    finally:
        await async_db.delete_collection(TEST_TRUNCATED_VECTOR_COLLECTION_NAME)


@pytest.mark.describe("find_one, not through vector")
async def test_find_one_filter_novector(
    async_readonly_vector_collection: AsyncAstraDBCollection, cliff_uuid: str
) -> None:
    response = await async_readonly_vector_collection.find_one(
        filter={"_id": "1"},
    )
    document = response["data"]["document"]
    assert document["text"] == "Sample entry number <1>"
    assert (
        document.keys() ^ {"_id", "text", "otherfield", "anotherfield", "$vector"}
        == set()
    )

    response_not_by_id = await async_readonly_vector_collection.find_one(
        filter={"text": "Sample entry number <1>"},
    )
    document_not_by_id = response_not_by_id["data"]["document"]
    assert document_not_by_id["_id"] == "1"
    assert (
        document_not_by_id.keys()
        ^ {"_id", "text", "otherfield", "anotherfield", "$vector"}
        == set()
    )

    response_no = await async_readonly_vector_collection.find_one(
        filter={"_id": "Z"},
    )
    document_no = response_no["data"]["document"]
    assert document_no is None

    response_no_not_by_id = await async_readonly_vector_collection.find_one(
        filter={"text": "No such text."},
    )
    document_no_not_by_id = response_no_not_by_id["data"]["document"]
    assert document_no_not_by_id is None


@pytest.mark.describe("find, not through vector")
async def test_find_filter_novector(
    async_readonly_vector_collection: AsyncAstraDBCollection,
) -> None:
    response_n2 = await async_readonly_vector_collection.find(
        filter={"anotherfield": "alpha"},
    )
    documents_n2 = response_n2["data"]["documents"]
    assert isinstance(documents_n2, list)
    assert {document["_id"] for document in documents_n2} == {"1", "2"}

    response_n1 = await async_readonly_vector_collection.find(
        filter={"anotherfield": "alpha"},
        options={"limit": 1},
    )
    documents_n1 = response_n1["data"]["documents"]
    assert isinstance(documents_n1, list)
    assert len(documents_n1) == 1
    assert documents_n1[0]["_id"] in {"1", "2"}


@pytest.mark.describe("obey projection in find and find_one")
async def test_find_find_one_projection(
    async_readonly_vector_collection: AsyncAstraDBCollection,
) -> None:
    query = [0.2, 0.6]
    sort = {"$vector": query}
    options = {"limit": 1}

    projs: List[Optional[Dict[str, Literal[1]]]] = [
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
        response_n = await async_readonly_vector_collection.find(
            sort=sort, options=options, projection=proj
        )
        fields = set(response_n["data"]["documents"][0].keys())
        assert fields == exp_fields
        #
        response_1 = await async_readonly_vector_collection.find_one(
            sort=sort, projection=proj
        )
        fields = set(response_1["data"]["document"].keys())
        assert fields == exp_fields


@pytest.mark.describe("should coerce vectors in the find sort argument")
async def test_find_float32(
    async_readonly_vector_collection: AsyncAstraDBCollection,
) -> None:
    def ite() -> Iterable[str]:
        for v in [0.1, 0.2]:
            yield f"{v}"

    # we surreptitously trick typing here
    sort = {"$vector": cast(List[float], ite())}
    options = {"limit": 5}

    response = await async_readonly_vector_collection.find(sort=sort, options=options)
    assert isinstance(response["data"]["documents"], list)


@pytest.mark.describe("find through vector")
async def test_find(async_readonly_vector_collection: AsyncAstraDBCollection) -> None:
    sort = {"$vector": [0.2, 0.6]}
    options = {"limit": 100}

    response = await async_readonly_vector_collection.find(sort=sort, options=options)
    assert isinstance(response["data"]["documents"], list)


@pytest.mark.describe("proper error raising in find")
async def test_find_error(
    async_readonly_vector_collection: AsyncAstraDBCollection,
) -> None:
    """Wrong type of arguments should raise an API error (ValueError)."""
    sort = {"$vector": [0, "clearly not a list of floats!"]}
    options = {"limit": 100}

    with pytest.raises(APIRequestError):
        await async_readonly_vector_collection.find(sort=sort, options=options)


@pytest.mark.describe("find through vector, without explicit limit")
async def test_find_limitless(
    async_readonly_vector_collection: AsyncAstraDBCollection,
) -> None:
    sort = {"$vector": [0.2, 0.6]}
    projection = {"$vector": 1}

    response = await async_readonly_vector_collection.find(
        sort=sort, projection=projection
    )
    assert response is not None
    assert isinstance(response["data"]["documents"], list)


@pytest.mark.describe("correctly count documents according to predicate")
async def test_count_documents(
    async_readonly_vector_collection: AsyncAstraDBCollection,
) -> None:
    c_all_response0 = await async_readonly_vector_collection.count_documents()
    assert c_all_response0["status"]["count"] == 3

    c_all_response1 = await async_readonly_vector_collection.count_documents(filter={})
    assert c_all_response1["status"]["count"] == 3

    c_pred_response = await async_readonly_vector_collection.count_documents(
        filter={"anotherfield": "alpha"}
    )
    assert c_pred_response["status"]["count"] == 2

    c_no_response = await async_readonly_vector_collection.count_documents(
        filter={"false_field": 137}
    )
    assert c_no_response["status"]["count"] == 0


@pytest.mark.describe("insert_one, w/out _id, w/out vector")
async def test_create_document(
    async_writable_vector_collection: AsyncAstraDBCollection,
) -> None:
    i_vector = [0.3, 0.5]
    id_v_i = str(uuid.uuid4())
    result_v_i = await async_writable_vector_collection.insert_one(
        {
            "_id": id_v_i,
            "a": 1,
            "$vector": i_vector,
        }
    )
    assert result_v_i["status"]["insertedIds"] == [id_v_i]
    assert (
        await async_writable_vector_collection.find_one(
            {"_id": result_v_i["status"]["insertedIds"][0]}
        )
    )["data"]["document"]["a"] == 1

    id_n_i = str(uuid.uuid4())
    result_n_i = await async_writable_vector_collection.insert_one(
        {
            "_id": id_n_i,
            "a": 2,
        }
    )
    assert result_n_i["status"]["insertedIds"] == [id_n_i]
    assert (
        await async_writable_vector_collection.find_one(
            {"_id": result_n_i["status"]["insertedIds"][0]}
        )
    )["data"]["document"]["a"] == 2

    with pytest.raises(ValueError):
        await async_writable_vector_collection.insert_one(
            {
                "_id": id_n_i,
                "a": 3,
            }
        )

    result_v_n = await async_writable_vector_collection.insert_one(
        {
            "a": 4,
            "$vector": i_vector,
        }
    )
    assert isinstance(result_v_n["status"]["insertedIds"], list)
    assert isinstance(result_v_n["status"]["insertedIds"][0], str)
    assert len(result_v_n["status"]["insertedIds"]) == 1
    assert (
        await async_writable_vector_collection.find_one(
            {"_id": result_v_n["status"]["insertedIds"][0]}
        )
    )["data"]["document"]["a"] == 4

    result_n_n = await async_writable_vector_collection.insert_one(
        {
            "a": 5,
        }
    )
    assert isinstance(result_n_n["status"]["insertedIds"], list)
    assert isinstance(result_n_n["status"]["insertedIds"][0], str)
    assert len(result_n_n["status"]["insertedIds"]) == 1
    assert (
        await async_writable_vector_collection.find_one(
            {"_id": result_n_n["status"]["insertedIds"][0]}
        )
    )["data"]["document"]["a"] == 5


@pytest.mark.describe("should coerce vectors to plain lists of floats")
async def test_insert_float32(
    async_writable_vector_collection: AsyncAstraDBCollection, N: int = 2
) -> None:
    _id0 = str(uuid.uuid4())
    document = {
        "_id": _id0,
        "name": "Coerce",
        "$vector": [f"{(i+1)/N+2:.4f}" for i in range(N)],
    }
    response = await async_writable_vector_collection.insert_one(document)
    assert response is not None
    inserted_ids = response["status"]["insertedIds"]
    assert len(inserted_ids) == 1
    assert inserted_ids[0] == _id0


@pytest.mark.describe("insert_many")
async def test_insert_many(
    async_writable_vector_collection: AsyncAstraDBCollection,
) -> None:
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
            "description": "The third in this list",
            "$vector": [0.4, 0.3],
        },
    ]

    response = await async_writable_vector_collection.insert_many(documents)
    assert response is not None
    inserted_ids = set(response["status"]["insertedIds"])
    assert len(inserted_ids - {_id0, _id2}) == 1
    assert isinstance(list(inserted_ids - {_id0, _id2})[0], str)


@pytest.mark.describe("chunked_insert_many")
async def test_chunked_insert_many(
    async_writable_vector_collection: AsyncAstraDBCollection,
) -> None:
    _ids0 = [str(uuid.uuid4()) for _ in range(20)]
    documents0: List[API_DOC] = [
        {
            "_id": _id,
            "specs": {
                "gen": "0",
                "doc_idx": doc_idx,
            },
            "$vector": [1, doc_idx],
        }
        for doc_idx, _id in enumerate(_ids0)
    ]

    responses0: List[
        Union[Dict[str, Any], Exception]
    ] = await async_writable_vector_collection.chunked_insert_many(
        documents0, chunk_size=3
    )
    assert responses0 is not None
    inserted_ids0 = [
        ins_id
        for response in responses0
        if isinstance(response, dict)  # Add type check here
        for ins_id in response["status"]["insertedIds"]
    ]
    assert inserted_ids0 == _ids0

    response0a = await async_writable_vector_collection.find_one(
        filter={"_id": _ids0[0]}
    )
    assert response0a is not None
    assert response0a["data"]["document"] == documents0[0]

    # partial overlap of IDs for failure modes
    _ids1 = [
        _id0 if idx % 3 == 0 else str(uuid.uuid4()) for idx, _id0 in enumerate(_ids0)
    ]
    documents1: List[API_DOC] = [
        {
            "_id": _id,
            "specs": {
                "gen": "1",
                "doc_idx": doc_idx,
            },
            "$vector": [1, doc_idx],
        }
        for doc_idx, _id in enumerate(_ids1)
    ]

    with pytest.raises(ValueError):
        _ = await async_writable_vector_collection.chunked_insert_many(
            documents1, chunk_size=3
        )

    responses1_ok = await async_writable_vector_collection.chunked_insert_many(
        documents1,
        chunk_size=3,
        options={"ordered": False},
        partial_failures_allowed=True,
    )
    inserted_ids1 = [
        ins_id
        for response in responses1_ok
        if isinstance(response, dict)
        and "status" in response
        and "insertedIds" in response["status"]
        for ins_id in response["status"]["insertedIds"]
    ]
    # insertions that succeeded are those with a new ID
    assert set(inserted_ids1) == set(_ids1) - set(_ids0)
    # we can check that the failures are as many as the preexisting docs
    errors1 = [
        err
        for response in responses1_ok
        if isinstance(response, dict) and "errors" in response
        for err in response["errors"]
    ]
    assert len(set(_ids0) & set(_ids1)) == len(errors1)


@pytest.mark.describe("chunked_insert_many concurrently")
async def test_concurrent_chunked_insert_many(
    async_writable_vector_collection: AsyncAstraDBCollection,
) -> None:
    _ids0 = [str(uuid.uuid4()) for _ in range(20)]
    documents0: List[API_DOC] = [
        {
            "_id": _id,
            "specs": {
                "gen": "0",
                "doc_idx": doc_idx,
            },
            "$vector": [2, doc_idx],
        }
        for doc_idx, _id in enumerate(_ids0)
    ]

    responses0 = await async_writable_vector_collection.chunked_insert_many(
        documents0, chunk_size=3, concurrency=4
    )
    assert responses0 is not None
    inserted_ids0 = [
        ins_id
        for response in responses0
        if isinstance(response, dict)
        and "status" in response
        and "insertedIds" in response["status"]
        for ins_id in response["status"]["insertedIds"]
    ]
    assert inserted_ids0 == _ids0

    response0a = await async_writable_vector_collection.find_one(
        filter={"_id": _ids0[0]}
    )
    assert response0a is not None
    assert response0a["data"]["document"] == documents0[0]

    # partial overlap of IDs for failure modes
    _ids1 = [
        _id0 if idx % 3 == 0 else str(uuid.uuid4()) for idx, _id0 in enumerate(_ids0)
    ]
    documents1: List[API_DOC] = [
        {
            "_id": _id,
            "specs": {
                "gen": "1",
                "doc_idx": doc_idx,
            },
            "$vector": [1, doc_idx],
        }
        for doc_idx, _id in enumerate(_ids1)
    ]

    with pytest.raises(ValueError):
        _ = await async_writable_vector_collection.chunked_insert_many(
            documents1, chunk_size=3, concurrency=4
        )

    responses1_ok = await async_writable_vector_collection.chunked_insert_many(
        documents1,
        chunk_size=3,
        options={"ordered": False},
        partial_failures_allowed=True,
        concurrency=4,
    )
    inserted_ids1 = [
        ins_id
        for response in responses1_ok
        if isinstance(response, dict)
        and "status" in response
        and "insertedIds" in response["status"]
        for ins_id in response["status"]["insertedIds"]
    ]
    # insertions that succeeded are those with a new ID
    assert set(inserted_ids1) == set(_ids1) - set(_ids0)
    # we can check that the failures are as many as the preexisting docs
    errors1 = [
        err
        for response in responses1_ok
        if isinstance(response, dict) and "errors" in response
        for err in response["errors"]
    ]
    assert len(set(_ids0) & set(_ids1)) == len(errors1)


@pytest.mark.describe("insert_many with 'ordered' set to False")
async def test_insert_many_ordered_false(
    async_writable_vector_collection: AsyncAstraDBCollection,
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
    response_a = await async_writable_vector_collection.insert_many(documents_a)
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
    response_b = await async_writable_vector_collection.insert_many(
        documents_b,
        partial_failures_allowed=True,
    )
    assert response_b is not None
    assert response_b["status"]["insertedIds"] == []

    response_b2 = await async_writable_vector_collection.insert_many(
        documents=documents_b,
        options={"ordered": False},
        partial_failures_allowed=True,
    )
    assert response_b2 is not None
    assert response_b2["status"]["insertedIds"] == [_id2]

    check_response = await async_writable_vector_collection.find_one(
        filter={"first_name": "Yep"}
    )
    assert check_response is not None
    assert check_response["data"]["document"]["_id"] == _id1


@pytest.mark.describe("upsert_many")
async def test_upsert_many(
    async_writable_vector_collection: AsyncAstraDBCollection,
) -> None:
    _ids0 = [str(uuid.uuid4()) for _ in range(12)]
    documents0 = [
        {
            "_id": _id,
            "specs": {
                "gen": "0",
                "doc_i": doc_i,
            },
        }
        for doc_i, _id in enumerate(_ids0)
    ]

    upsert_result0 = await async_writable_vector_collection.upsert_many(documents0)
    assert upsert_result0 == [doc["_id"] for doc in documents0]

    response0a = await async_writable_vector_collection.find_one(
        filter={"_id": _ids0[0]}
    )
    assert response0a is not None
    assert response0a["data"]["document"] == documents0[0]

    response0b = await async_writable_vector_collection.find_one(
        filter={"_id": _ids0[-1]}
    )
    assert response0b is not None
    assert response0b["data"]["document"] == documents0[-1]

    _ids1 = _ids0[::2] + [str(uuid.uuid4()) for _ in range(3)]
    documents1 = [
        {
            "_id": _id,
            "specs": {
                "gen": "1",
                "doc_i": doc_i,
            },
        }
        for doc_i, _id in enumerate(_ids1)
    ]
    upsert_result1 = await async_writable_vector_collection.upsert_many(
        documents1,
        concurrency=5,
    )
    assert upsert_result1 == [doc["_id"] for doc in documents1]

    response1a = await async_writable_vector_collection.find_one(
        filter={"_id": _ids1[0]}
    )
    assert response1a is not None
    assert response1a["data"]["document"] == documents1[0]

    response1b = await async_writable_vector_collection.find_one(
        filter={"_id": _ids1[-1]}
    )
    assert response1b is not None
    assert response1b["data"]["document"] == documents1[-1]


@pytest.mark.describe("upsert")
async def test_upsert_document(
    async_writable_vector_collection: AsyncAstraDBCollection,
) -> None:
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
    upsert_result0 = await async_writable_vector_collection.upsert(document0)
    assert upsert_result0 == _id

    response0 = await async_writable_vector_collection.find_one(filter={"_id": _id})
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
    upsert_result1 = await async_writable_vector_collection.upsert(document1)
    assert upsert_result1 == _id

    response1 = await async_writable_vector_collection.find_one(filter={"_id": _id})
    assert response1 is not None
    assert response1["data"]["document"] == document1


@pytest.mark.describe("update_one to create a subdocument, not through vector")
async def test_update_one_create_subdocument_novector(
    async_disposable_vector_collection: AsyncAstraDBCollection,
) -> None:
    update_one_response = await async_disposable_vector_collection.update_one(
        filter={"_id": "1"},
        update={"$set": {"name": "Eric"}},
    )

    assert update_one_response["status"]["matchedCount"] >= 1
    assert update_one_response["status"]["modifiedCount"] == 1

    response = await async_disposable_vector_collection.find_one(filter={"_id": "1"})
    assert response["data"]["document"]["name"] == "Eric"


@pytest.mark.describe("delete_subdocument, not through vector")
async def test_delete_subdocument_novector(
    async_disposable_vector_collection: AsyncAstraDBCollection,
) -> None:
    delete_subdocument_response = (
        await async_disposable_vector_collection.delete_subdocument(
            id="1",
            subdoc="otherfield.subfield",
        )
    )

    assert delete_subdocument_response["status"]["matchedCount"] >= 1
    assert delete_subdocument_response["status"]["modifiedCount"] == 1

    response = await async_disposable_vector_collection.find_one(filter={"_id": "1"})
    assert response["data"]["document"]["otherfield"] == {}


@pytest.mark.describe("find_one_and_update, through vector")
async def test_find_one_and_update_vector(
    async_disposable_vector_collection: AsyncAstraDBCollection,
) -> None:
    find_filter = {"status": {"$exists": True}}
    response0 = await async_disposable_vector_collection.find_one(filter=find_filter)
    assert response0["data"]["document"] is None

    sort = {"$vector": [0.2, 0.6]}

    update0 = {"$set": {"status": "active"}}
    options0 = {"returnDocument": "after"}

    update_response0 = await async_disposable_vector_collection.find_one_and_update(
        sort=sort, update=update0, options=options0
    )
    assert isinstance(update_response0["data"]["document"], dict)
    assert update_response0["data"]["document"]["status"] == "active"
    assert update_response0["status"]["matchedCount"] >= 1
    assert update_response0["status"]["modifiedCount"] >= 1

    response1 = await async_disposable_vector_collection.find_one(filter=find_filter)
    assert isinstance(response1["data"]["document"], dict)
    assert response1["data"]["document"]["status"] == "active"

    update1 = {"$set": {"status": "inactive"}}
    options1 = {"returnDocument": "before"}

    update_response1 = await async_disposable_vector_collection.find_one_and_update(
        sort=sort, update=update1, options=options1
    )
    assert isinstance(update_response1["data"]["document"], dict)
    assert update_response1["data"]["document"]["status"] == "active"
    assert update_response1["status"]["matchedCount"] >= 1
    assert update_response1["status"]["modifiedCount"] >= 1

    response2 = await async_disposable_vector_collection.find_one(filter=find_filter)
    assert isinstance(response2["data"]["document"], dict)
    assert response2["data"]["document"]["status"] == "inactive"

    filter2 = {"nonexistent_subfield": 10}
    update2 = update1
    options2 = options1

    update_response2 = await async_disposable_vector_collection.find_one_and_update(
        sort=sort, update=update2, options=options2, filter=filter2
    )
    assert update_response2["data"]["document"] is None
    assert update_response2["status"]["matchedCount"] == 0
    assert update_response2["status"]["modifiedCount"] == 0


@pytest.mark.describe("find_one_and_update, not through vector")
async def test_find_one_and_update_novector(
    async_disposable_vector_collection: AsyncAstraDBCollection,
) -> None:
    find_filter = {"status": {"$exists": True}}
    response0 = await async_disposable_vector_collection.find_one(filter=find_filter)
    assert response0["data"]["document"] is None

    update_filter = {"anotherfield": "omega"}

    update0 = {"$set": {"status": "active"}}
    options0 = {"returnDocument": "after"}

    update_response0 = await async_disposable_vector_collection.find_one_and_update(
        filter=update_filter, update=update0, options=options0
    )
    assert isinstance(update_response0["data"]["document"], dict)
    assert update_response0["data"]["document"]["status"] == "active"
    assert update_response0["status"]["matchedCount"] >= 1
    assert update_response0["status"]["modifiedCount"] >= 1

    response1 = await async_disposable_vector_collection.find_one(filter=find_filter)
    assert isinstance(response1["data"]["document"], dict)
    assert response1["data"]["document"]["status"] == "active"

    update1 = {"$set": {"status": "inactive"}}
    options1 = {"returnDocument": "before"}

    update_response1 = await async_disposable_vector_collection.find_one_and_update(
        filter=update_filter, update=update1, options=options1
    )
    assert isinstance(update_response1["data"]["document"], dict)
    assert update_response1["data"]["document"]["status"] == "active"
    assert update_response1["status"]["matchedCount"] >= 1
    assert update_response1["status"]["modifiedCount"] >= 1

    response2 = await async_disposable_vector_collection.find_one(filter=find_filter)
    assert isinstance(response2["data"]["document"], dict)
    assert response2["data"]["document"]["status"] == "inactive"

    filter2 = {**update_filter, **{"nonexistent_subfield": 10}}
    update2 = update1
    options2 = options1

    update_response2 = await async_disposable_vector_collection.find_one_and_update(
        filter=filter2, update=update2, options=options2
    )
    assert update_response2["data"]["document"] is None
    assert update_response2["status"]["matchedCount"] == 0
    assert update_response2["status"]["modifiedCount"] == 0


@pytest.mark.describe("find_one_and_replace, through vector")
async def test_find_one_and_replace_vector(
    async_disposable_vector_collection: AsyncAstraDBCollection,
) -> None:
    sort = {"$vector": [0.2, 0.6]}

    response0 = await async_disposable_vector_collection.find_one(sort=sort)
    assert response0 is not None
    assert "anotherfield" in response0["data"]["document"]

    doc0vector = response0["data"]["document"]["$vector"]

    replace_response0 = await async_disposable_vector_collection.find_one_and_replace(
        sort=sort,
        replacement={
            "phyla": ["Echinodermata", "Platelminta", "Chordata"],
            "$vector": doc0vector,  # to find this doc again below!
        },
    )
    assert replace_response0 is not None
    assert "anotherfield" in replace_response0["data"]["document"]

    response1 = await async_disposable_vector_collection.find_one(sort=sort)
    assert response1 is not None
    assert response1["data"]["document"]["phyla"] == [
        "Echinodermata",
        "Platelminta",
        "Chordata",
    ]
    assert "anotherfield" not in response1["data"]["document"]

    replace_response1 = await async_disposable_vector_collection.find_one_and_replace(
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

    response2 = await async_disposable_vector_collection.find_one(sort=sort)
    assert response2 is not None
    assert response2["data"]["document"]["phone"] == "0123-4567"
    assert "phyla" not in response2["data"]["document"]

    # non-existing-doc case
    filter_no = {"nonexisting_field": -123}
    replace_response_no = await async_disposable_vector_collection.find_one_and_replace(
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
async def test_find_one_and_replace_novector(
    async_disposable_vector_collection: AsyncAstraDBCollection,
) -> None:
    response0 = await async_disposable_vector_collection.find_one(filter={"_id": "1"})
    assert response0 is not None
    assert response0["data"]["document"]["anotherfield"] == "alpha"

    replace_response0 = await async_disposable_vector_collection.find_one_and_replace(
        filter={"_id": "1"},
        replacement={
            "_id": "1",
            "phyla": ["Echinodermata", "Platelminta", "Chordata"],
        },
    )
    assert replace_response0 is not None
    assert replace_response0["data"]["document"]["anotherfield"] == "alpha"

    response1 = await async_disposable_vector_collection.find_one(filter={"_id": "1"})
    assert response1 is not None
    assert response1["data"]["document"]["phyla"] == [
        "Echinodermata",
        "Platelminta",
        "Chordata",
    ]
    assert "anotherfield" not in response1["data"]["document"]

    replace_response1 = await async_disposable_vector_collection.find_one_and_replace(
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

    response2 = await async_disposable_vector_collection.find_one(filter={"_id": "1"})
    assert response2 is not None
    assert response2["data"]["document"]["phone"] == "0123-4567"
    assert "phyla" not in response2["data"]["document"]

    # non-existing-doc case
    replace_response_no = await async_disposable_vector_collection.find_one_and_replace(
        filter={"_id": "z"},
        replacement={
            "whatever": -123,
        },
    )
    assert replace_response_no is not None
    assert replace_response_no["data"]["document"] is None


@pytest.mark.describe("delete_one, not through vector")
async def test_delete_one_novector(
    async_disposable_vector_collection: AsyncAstraDBCollection,
) -> None:
    delete_response = await async_disposable_vector_collection.delete_one(id="3")
    assert delete_response["status"]["deletedCount"] == 1

    response = await async_disposable_vector_collection.find_one(filter={"_id": "3"})
    assert response["data"]["document"] is None

    delete_response_no = await async_disposable_vector_collection.delete_one(id="3")
    assert delete_response_no["status"]["deletedCount"] == 0


@pytest.mark.describe("delete_many, not through vector")
async def test_delete_many_novector(
    async_disposable_vector_collection: AsyncAstraDBCollection,
) -> None:
    delete_response = await async_disposable_vector_collection.delete_many(
        filter={"anotherfield": "alpha"}
    )
    assert delete_response["status"]["deletedCount"] == 2

    documents_no = await async_disposable_vector_collection.find(
        filter={"anotherfield": "alpha"}
    )
    assert documents_no["data"]["documents"] == []

    delete_response_no = await async_disposable_vector_collection.delete_many(
        filter={"anotherfield": "alpha"}
    )
    assert delete_response_no["status"]["deletedCount"] == 0


@pytest.mark.describe("pop, push functions, not through vector")
async def test_pop_push_novector(
    async_disposable_vector_collection: AsyncAstraDBCollection,
) -> None:
    user_id = str(uuid.uuid4())
    await async_disposable_vector_collection.insert_one(
        document={
            "_id": user_id,
            "first_name": "Cliff",
            "last_name": "Wicklow",
            "roles": ["user", "admin"],
        },
    )

    pop = {"roles": 1}
    options = {"returnDocument": "after"}

    pop_response = await async_disposable_vector_collection.pop(
        filter={"_id": user_id}, pop=pop, options=options
    )
    assert pop_response is not None
    assert pop_response["data"]["document"]["roles"] == ["user"]
    assert pop_response["status"]["matchedCount"] >= 1
    assert pop_response["status"]["modifiedCount"] == 1

    response1 = await async_disposable_vector_collection.find_one(
        filter={"_id": user_id}
    )
    assert response1 is not None
    assert response1["data"]["document"]["roles"] == ["user"]

    push = {"roles": "auditor"}

    push_response = await async_disposable_vector_collection.push(
        filter={"_id": user_id}, push=push, options=options
    )
    assert push_response is not None
    assert push_response["data"]["document"]["roles"] == ["user", "auditor"]
    assert push_response["status"]["matchedCount"] >= 1
    assert push_response["status"]["modifiedCount"] == 1

    response2 = await async_disposable_vector_collection.find_one(
        filter={"_id": user_id}
    )
    assert response2 is not None
    assert response2["data"]["document"]["roles"] == ["user", "auditor"]


@pytest.mark.describe("store and retrieve dates and datetimes correctly")
async def test_insert_find_with_dates(
    async_writable_vector_collection: AsyncAstraDBCollection,
) -> None:
    date0 = datetime.date(2024, 1, 12)
    datetime0 = datetime.datetime(2024, 1, 12, 0, 0)
    date1 = datetime.date(2024, 1, 13)
    datetime1 = datetime.datetime(2024, 1, 13, 0, 0)

    d_doc_id = str(uuid.uuid4())
    d_document = {
        "_id": d_doc_id,
        "my_date": date0,
        "my_datetime": datetime0,
        "nested": {
            "n_date": date1,
            "n_datetime": datetime1,
        },
        "nested_list": {
            "the_list": [
                date0,
                datetime0,
                date1,
                datetime1,
            ]
        },
    }
    expected_d_document = {
        "_id": d_doc_id,
        "my_date": datetime0,
        "my_datetime": datetime0,
        "nested": {
            "n_date": datetime1,
            "n_datetime": datetime1,
        },
        "nested_list": {
            "the_list": [
                datetime0,
                datetime0,
                datetime1,
                datetime1,
            ]
        },
    }

    await async_writable_vector_collection.insert_one(d_document)

    # retrieve it, simple
    response0 = await async_writable_vector_collection.find_one(
        filter={"_id": d_doc_id}
    )
    assert response0 is not None
    document0 = response0["data"]["document"]
    assert document0 == expected_d_document

    # retrieve it, lt condition on a date
    response1 = await async_writable_vector_collection.find_one(
        filter={"nested_list.the_list.0": {"$lt": date1}}
    )
    assert response1 is not None
    document1 = response1["data"]["document"]
    assert document1 == expected_d_document

    # retrieve it, gte condition on a datetime
    response2 = await async_writable_vector_collection.find_one(
        filter={"nested.n_date": {"$gte": datetime0}}
    )
    assert response2 is not None
    document2 = response2["data"]["document"]
    assert document2 == expected_d_document

    # retrieve it, filter == condition on a datetime
    response3 = await async_writable_vector_collection.find_one(
        filter={"my_date": datetime0}
    )
    assert response3 is not None
    document3 = response3["data"]["document"]
    assert document3 == expected_d_document
