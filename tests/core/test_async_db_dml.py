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
from typing import cast, Any, Dict, Iterable, List, Literal, Optional, Union

import pytest

from astrapy.core.api import APIRequestError
from astrapy.core.core_types import API_DOC
from astrapy.core.db import AsyncAstraDB, AsyncAstraDBCollection


logger = logging.getLogger(__name__)


def _cleanvec(doc: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in doc.items() if k != "$vector"}


@pytest.mark.describe("should fail clearing a non-existent collection (async)")
async def test_clear_collection_fail(async_db: AsyncAstraDB) -> None:
    with pytest.raises(APIRequestError):
        await (await async_db.collection("this$does%not exist!!!")).clear()


@pytest.mark.describe("should clear a nonvector collection (async)")
async def test_clear_nonvector_collection(
    async_empty_nonv_collection: AsyncAstraDBCollection,
) -> None:
    await async_empty_nonv_collection.insert_one({"a": 1})
    assert len((await async_empty_nonv_collection.find())["data"]["documents"]) == 1
    tr_response = await async_empty_nonv_collection.clear()
    assert len((await async_empty_nonv_collection.find())["data"]["documents"]) == 0
    assert tr_response["status"]["deletedCount"] == -1


@pytest.mark.describe("should clear a collection (async)")
async def test_clear_vector_collection(
    async_empty_v_collection: AsyncAstraDBCollection,
) -> None:
    await async_empty_v_collection.insert_one({"a": 1, "$vector": [0.1, 0.2]})
    assert len((await async_empty_v_collection.find())["data"]["documents"]) == 1
    tr_response = await async_empty_v_collection.clear()
    assert len((await async_empty_v_collection.find())["data"]["documents"]) == 0
    assert tr_response["status"]["deletedCount"] == -1


@pytest.mark.describe("find_one, not through vector (async)")
async def test_find_one_filter_novector(
    async_readonly_v_collection: AsyncAstraDBCollection,
) -> None:
    response = await async_readonly_v_collection.find_one(
        filter={"_id": "1"},
    )
    document = response["data"]["document"]
    assert document["text"] == "Sample entry number <1>"
    assert (set(document.keys()) - {"$vector"}) ^ {
        "_id",
        "text",
        "otherfield",
        "anotherfield",
    } == set()

    response_not_by_id = await async_readonly_v_collection.find_one(
        filter={"text": "Sample entry number <1>"},
    )
    document_not_by_id = response_not_by_id["data"]["document"]
    assert document_not_by_id["_id"] == "1"
    assert (set(document_not_by_id.keys()) - {"$vector"}) ^ {
        "_id",
        "text",
        "otherfield",
        "anotherfield",
    } == set()

    response_no = await async_readonly_v_collection.find_one(
        filter={"_id": "Z"},
    )
    document_no = response_no["data"]["document"]
    assert document_no is None

    response_no_not_by_id = await async_readonly_v_collection.find_one(
        filter={"text": "No such text."},
    )
    document_no_not_by_id = response_no_not_by_id["data"]["document"]
    assert document_no_not_by_id is None


@pytest.mark.describe("find, not through vector (async)")
async def test_find_filter_novector(
    async_readonly_v_collection: AsyncAstraDBCollection,
) -> None:
    response_n2 = await async_readonly_v_collection.find(
        filter={"anotherfield": "alpha"},
    )
    documents_n2 = response_n2["data"]["documents"]
    assert isinstance(documents_n2, list)
    assert {document["_id"] for document in documents_n2} == {"1", "2"}

    response_n1 = await async_readonly_v_collection.find(
        filter={"anotherfield": "alpha"},
        options={"limit": 1},
    )
    documents_n1 = response_n1["data"]["documents"]
    assert isinstance(documents_n1, list)
    assert len(documents_n1) == 1
    assert documents_n1[0]["_id"] in {"1", "2"}


@pytest.mark.describe("obey projection in find and find_one (async)")
async def test_find_find_one_projection(
    async_readonly_v_collection: AsyncAstraDBCollection,
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
        {
            "$vector",
            "_id",
            "otherfield",
            "anotherfield",
            "text",
        },
        {"$vector", "_id", "text"},
    ]
    for proj, exp_fields in zip(projs, exp_fieldsets):
        response_n = await async_readonly_v_collection.find(
            sort=sort, options=options, projection=proj
        )
        vkeys_novec = set(response_n["data"]["documents"][0].keys()) - {"$vector"}
        expkeys_novec = exp_fields - {"$vector"}
        assert vkeys_novec == expkeys_novec
        # but in some cases $vector must be there:
        if "$vector" in (proj or set()):
            assert "$vector" in response_n["data"]["documents"][0]
        #
        response_1 = await async_readonly_v_collection.find_one(
            sort=sort, projection=proj
        )
        vkeys_novec = set(response_1["data"]["document"].keys()) - {"$vector"}
        expkeys_novec = exp_fields - {"$vector"}
        assert vkeys_novec == expkeys_novec
        # but in some cases $vector must be there:
        if "$vector" in (proj or set()):
            assert "$vector" in response_1["data"]["document"]


@pytest.mark.describe("should coerce vectors in the find sort argument (async)")
async def test_find_float32(
    async_readonly_v_collection: AsyncAstraDBCollection,
) -> None:
    def ite() -> Iterable[str]:
        for v in [0.1, 0.2]:
            yield f"{v}"

    # we surreptitously trick typing here
    sort = {"$vector": cast(List[float], ite())}
    options = {"limit": 5}

    response = await async_readonly_v_collection.find(sort=sort, options=options)
    assert isinstance(response["data"]["documents"], list)


@pytest.mark.describe("find through vector (async)")
async def test_find(
    async_readonly_v_collection: AsyncAstraDBCollection,
) -> None:
    sort = {"$vector": [0.2, 0.6]}
    options = {"limit": 100}

    response = await async_readonly_v_collection.find(sort=sort, options=options)
    assert isinstance(response["data"]["documents"], list)


@pytest.mark.describe("proper error raising in find (async)")
async def test_find_error(
    async_readonly_v_collection: AsyncAstraDBCollection,
) -> None:
    """Wrong type of arguments should raise an API error (ValueError)."""
    sort = {"$vector": [0, "clearly not a list of floats!"]}
    options = {"limit": 100}

    with pytest.raises(APIRequestError):
        await async_readonly_v_collection.find(sort=sort, options=options)


@pytest.mark.describe("find through vector, without explicit limit (async)")
async def test_find_limitless(
    async_readonly_v_collection: AsyncAstraDBCollection,
) -> None:
    sort = {"$vector": [0.2, 0.6]}
    projection = {"$vector": 1}

    response = await async_readonly_v_collection.find(sort=sort, projection=projection)
    assert response is not None
    assert isinstance(response["data"]["documents"], list)


@pytest.mark.describe("correctly count documents according to predicate (async)")
async def test_count_documents(
    async_readonly_v_collection: AsyncAstraDBCollection,
) -> None:
    c_all_response0 = await async_readonly_v_collection.count_documents()
    assert c_all_response0["status"]["count"] == 3

    c_all_response1 = await async_readonly_v_collection.count_documents(filter={})
    assert c_all_response1["status"]["count"] == 3

    c_pred_response = await async_readonly_v_collection.count_documents(
        filter={"anotherfield": "alpha"}
    )
    assert c_pred_response["status"]["count"] == 2

    c_no_response = await async_readonly_v_collection.count_documents(
        filter={"false_field": 137}
    )
    assert c_no_response["status"]["count"] == 0


@pytest.mark.describe("insert_one, w/out _id, w/out vector (async)")
async def test_create_document(
    async_writable_v_collection: AsyncAstraDBCollection,
) -> None:
    i_vector = [0.3, 0.5]
    id_v_i = str(uuid.uuid4())
    result_v_i = await async_writable_v_collection.insert_one(
        {
            "_id": id_v_i,
            "a": 1,
            "$vector": i_vector,
        }
    )
    assert result_v_i["status"]["insertedIds"] == [id_v_i]
    assert (
        await async_writable_v_collection.find_one(
            {"_id": result_v_i["status"]["insertedIds"][0]}
        )
    )["data"]["document"]["a"] == 1

    id_n_i = str(uuid.uuid4())
    result_n_i = await async_writable_v_collection.insert_one(
        {
            "_id": id_n_i,
            "a": 2,
        }
    )
    assert result_n_i["status"]["insertedIds"] == [id_n_i]
    assert (
        await async_writable_v_collection.find_one(
            {"_id": result_n_i["status"]["insertedIds"][0]}
        )
    )["data"]["document"]["a"] == 2

    with pytest.raises(ValueError):
        await async_writable_v_collection.insert_one(
            {
                "_id": id_n_i,
                "a": 3,
            }
        )

    result_v_n = await async_writable_v_collection.insert_one(
        {
            "a": 4,
            "$vector": i_vector,
        }
    )
    assert isinstance(result_v_n["status"]["insertedIds"], list)
    assert isinstance(result_v_n["status"]["insertedIds"][0], str)
    assert len(result_v_n["status"]["insertedIds"]) == 1
    assert (
        await async_writable_v_collection.find_one(
            {"_id": result_v_n["status"]["insertedIds"][0]}
        )
    )["data"]["document"]["a"] == 4

    result_n_n = await async_writable_v_collection.insert_one(
        {
            "a": 5,
        }
    )
    assert isinstance(result_n_n["status"]["insertedIds"], list)
    assert isinstance(result_n_n["status"]["insertedIds"][0], str)
    assert len(result_n_n["status"]["insertedIds"]) == 1
    assert (
        await async_writable_v_collection.find_one(
            {"_id": result_n_n["status"]["insertedIds"][0]}
        )
    )["data"]["document"]["a"] == 5


@pytest.mark.describe("should coerce vectors to plain lists of floats (async)")
async def test_insert_float32(
    async_writable_v_collection: AsyncAstraDBCollection, N: int = 2
) -> None:
    _id0 = str(uuid.uuid4())
    document = {
        "_id": _id0,
        "name": "Coerce",
        "$vector": [f"{(i+1)/N+2:.4f}" for i in range(N)],
    }
    response = await async_writable_v_collection.insert_one(document)
    assert response is not None
    inserted_ids = response["status"]["insertedIds"]
    assert len(inserted_ids) == 1
    assert inserted_ids[0] == _id0


@pytest.mark.describe("insert_many (async)")
async def test_insert_many(
    async_writable_v_collection: AsyncAstraDBCollection,
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

    response = await async_writable_v_collection.insert_many(documents)
    assert response is not None
    inserted_ids = set(response["status"]["insertedIds"])
    assert len(inserted_ids - {_id0, _id2}) == 1
    assert isinstance(list(inserted_ids - {_id0, _id2})[0], str)


@pytest.mark.describe("chunked_insert_many (async)")
async def test_chunked_insert_many(
    async_writable_v_collection: AsyncAstraDBCollection,
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

    responses0: List[Union[Dict[str, Any], Exception]] = (
        await async_writable_v_collection.chunked_insert_many(documents0, chunk_size=3)
    )
    assert responses0 is not None
    inserted_ids0 = [
        ins_id
        for response in responses0
        if isinstance(response, dict)  # Add type check here
        for ins_id in response["status"]["insertedIds"]
    ]
    # unordered inserts: this only has to be a set equality
    assert set(inserted_ids0) == set(_ids0)

    response0a = await async_writable_v_collection.find_one(filter={"_id": _ids0[0]})
    assert response0a is not None
    assert _cleanvec(response0a["data"]["document"]) == _cleanvec(documents0[0])

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

    with pytest.raises(APIRequestError):
        _ = await async_writable_v_collection.chunked_insert_many(
            documents1,
            chunk_size=3,
            options={"ordered": True},
        )

    responses1_ok = await async_writable_v_collection.chunked_insert_many(
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


@pytest.mark.describe("chunked_insert_many concurrently (async)")
async def test_concurrent_chunked_insert_many(
    async_writable_v_collection: AsyncAstraDBCollection,
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

    responses0 = await async_writable_v_collection.chunked_insert_many(
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
    # unordered inserts: this only has to be a set equality
    assert set(inserted_ids0) == set(_ids0)

    response0a = await async_writable_v_collection.find_one(filter={"_id": _ids0[0]})
    assert response0a is not None
    assert _cleanvec(response0a["data"]["document"]) == _cleanvec(documents0[0])

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

    with pytest.raises(APIRequestError):
        # the first doc must be pre-existing
        # and the doc array size must be <= chunk size
        # for this not to spoil the rest of the test
        docs_for_error = documents0[0:1] + [{"_id": str(uuid.uuid4())}]
        _ = await async_writable_v_collection.chunked_insert_many(
            docs_for_error,
            chunk_size=3,
            concurrency=4,
            options={"ordered": True},
        )

    responses1_ok = await async_writable_v_collection.chunked_insert_many(
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


@pytest.mark.describe("chunked_insert_many, failure modes (async)")
async def test_chunked_insert_many_failures(
    async_empty_v_collection: AsyncAstraDBCollection,
) -> None:
    bad_docs = [{"_id": tid} for tid in ["a", "b", "c", ValueError, "e", "f"]]
    dup_docs = [{"_id": tid} for tid in ["a", "b", "b", "d", "e", "f"]]

    await async_empty_v_collection.delete_many({})
    with pytest.raises(TypeError):
        await async_empty_v_collection.chunked_insert_many(
            bad_docs,
            options={"ordered": True},
            partial_failures_allowed=False,
            chunk_size=2,
            concurrency=1,
        )
    assert len((await async_empty_v_collection.find({}))["data"]["documents"]) == 2

    await async_empty_v_collection.delete_many({})
    with pytest.raises(TypeError):
        await async_empty_v_collection.chunked_insert_many(
            bad_docs,
            options={"ordered": True},
            partial_failures_allowed=False,
            chunk_size=2,
            concurrency=2,
        )

    await async_empty_v_collection.delete_many({})
    with pytest.raises(TypeError):
        await async_empty_v_collection.chunked_insert_many(
            bad_docs,
            options={"ordered": False},
            partial_failures_allowed=True,
            chunk_size=2,
            concurrency=1,
        )
    assert len((await async_empty_v_collection.find({}))["data"]["documents"]) >= 2

    await async_empty_v_collection.delete_many({})
    with pytest.raises(TypeError):
        await async_empty_v_collection.chunked_insert_many(
            bad_docs,
            options={"ordered": False},
            partial_failures_allowed=True,
            chunk_size=2,
            concurrency=2,
        )

    await async_empty_v_collection.delete_many({})
    with pytest.raises(APIRequestError):
        await async_empty_v_collection.chunked_insert_many(
            dup_docs,
            options={"ordered": True},
            partial_failures_allowed=False,
            chunk_size=2,
            concurrency=1,
        )
    assert len((await async_empty_v_collection.find({}))["data"]["documents"]) == 2

    await async_empty_v_collection.delete_many({})
    with pytest.raises(APIRequestError):
        await async_empty_v_collection.chunked_insert_many(
            dup_docs,
            options={"ordered": True},
            partial_failures_allowed=False,
            chunk_size=2,
            concurrency=2,
        )
    assert len((await async_empty_v_collection.find({}))["data"]["documents"]) >= 2

    await async_empty_v_collection.delete_many({})
    ins_result = await async_empty_v_collection.chunked_insert_many(
        dup_docs,
        options={"ordered": False},
        partial_failures_allowed=True,
        chunk_size=2,
        concurrency=1,
    )
    assert isinstance(ins_result[0], dict)
    assert set(ins_result[0].keys()) == {"status"}
    assert isinstance(ins_result[1], dict)
    assert set(ins_result[1].keys()) == {"errors", "status"}
    assert isinstance(ins_result[2], dict)
    assert set(ins_result[2].keys()) == {"status"}
    assert len((await async_empty_v_collection.find({}))["data"]["documents"]) == 5


@pytest.mark.describe("insert_many with 'ordered' set to True (async)")
async def test_insert_many_ordered_true(
    async_writable_v_collection: AsyncAstraDBCollection,
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
    response_a = await async_writable_v_collection.insert_many(
        documents_a,
        options={"ordered": True},
    )
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
    response_b = await async_writable_v_collection.insert_many(
        documents_b,
        partial_failures_allowed=True,
        options={"ordered": True},
    )
    assert response_b is not None
    assert response_b["status"]["insertedIds"] == []

    response_b2 = await async_writable_v_collection.insert_many(
        documents=documents_b,
        options={"ordered": False},
        partial_failures_allowed=True,
    )
    assert response_b2 is not None
    assert response_b2["status"]["insertedIds"] == [_id2]

    check_response = await async_writable_v_collection.find_one(
        filter={"first_name": "Yep"}
    )
    assert check_response is not None
    assert check_response["data"]["document"]["_id"] == _id1


@pytest.mark.describe("upsert_many (async)")
async def test_upsert_many(
    async_writable_v_collection: AsyncAstraDBCollection,
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

    upsert_result0 = await async_writable_v_collection.upsert_many(documents0)
    assert upsert_result0 == [doc["_id"] for doc in documents0]

    response0a = await async_writable_v_collection.find_one(filter={"_id": _ids0[0]})
    assert response0a is not None
    assert response0a["data"]["document"] == documents0[0]

    response0b = await async_writable_v_collection.find_one(filter={"_id": _ids0[-1]})
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
    upsert_result1 = await async_writable_v_collection.upsert_many(
        documents1,
        concurrency=5,
    )
    assert upsert_result1 == [doc["_id"] for doc in documents1]

    response1a = await async_writable_v_collection.find_one(filter={"_id": _ids1[0]})
    assert response1a is not None
    assert response1a["data"]["document"] == documents1[0]

    response1b = await async_writable_v_collection.find_one(filter={"_id": _ids1[-1]})
    assert response1b is not None
    assert response1b["data"]["document"] == documents1[-1]


@pytest.mark.describe("upsert one (async)")
async def test_upsert_one_document(
    async_writable_v_collection: AsyncAstraDBCollection,
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
    upsert_result0 = await async_writable_v_collection.upsert_one(document0)
    assert upsert_result0 == _id

    response0 = await async_writable_v_collection.find_one(filter={"_id": _id})
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
    upsert_result1 = await async_writable_v_collection.upsert_one(document1)
    assert upsert_result1 == _id

    response1 = await async_writable_v_collection.find_one(filter={"_id": _id})
    assert response1 is not None
    assert response1["data"]["document"] == document1


@pytest.mark.describe("upsert should catch general errors from API")
async def test_upsert_api_errors(
    async_writable_v_collection: AsyncAstraDBCollection,
) -> None:
    _id0 = str(uuid.uuid4())
    _id1 = str(uuid.uuid4())

    document0a = {
        "_id": _id0,
        "nature": "good vector",
        "$vector": [10, 11],
    }
    upsert_result0 = await async_writable_v_collection.upsert_one(document0a)
    assert upsert_result0 == _id0

    # triggering an API error for the already-exists path of the upsert
    document0b = {
        "_id": _id0,
        "nature": "faulty vector",
        "$vector": [10, 11, 999, -153],
    }
    with pytest.raises(ValueError):
        _ = await async_writable_v_collection.upsert_one(document0b)

    # triggering an API error for the already-exists path of the upsert
    document1 = {
        "_id": _id1,
        "nature": "faulty vector from the start",
        "$vector": [10, 11, 999, -153],
    }
    with pytest.raises(ValueError):
        _ = await async_writable_v_collection.upsert_one(document1)


@pytest.mark.describe("update_one to create a subdocument, not through vector (async)")
async def test_update_one_create_subdocument_novector(
    async_writable_v_collection: AsyncAstraDBCollection,
) -> None:
    _id = str(uuid.uuid4())
    await async_writable_v_collection.insert_one({"_id": _id, "name": "Not Eric!"})
    update_one_response = await async_writable_v_collection.update_one(
        filter={"_id": _id},
        update={"$set": {"name": "Eric"}},
    )

    assert update_one_response["status"]["matchedCount"] >= 1
    assert update_one_response["status"]["modifiedCount"] == 1

    response = await async_writable_v_collection.find_one(filter={"_id": _id})
    assert response["data"]["document"]["name"] == "Eric"


@pytest.mark.describe("update_many to create a subdocument, not through vector (async)")
async def test_update_many_create_subdocument_novector(
    async_empty_v_collection: AsyncAstraDBCollection,
) -> None:
    _id1 = str(uuid.uuid4())
    _id2 = str(uuid.uuid4())
    await async_empty_v_collection.insert_one({"_id": _id1, "name": "Not Eric!"})
    await async_empty_v_collection.insert_one({"_id": _id2, "name": "Not Eric!"})
    update_many_response = await async_empty_v_collection.update_many(
        filter={"name": "Not Eric!"},
        update={"$set": {"name": "Eric"}},
    )

    assert update_many_response["status"]["matchedCount"] > 1
    assert update_many_response["status"]["modifiedCount"] > 1

    response = await async_empty_v_collection.find(filter={"name": "Eric"})
    assert len(response["data"]["documents"]) > 1


@pytest.mark.describe("delete_subdocument, not through vector (async)")
async def test_delete_subdocument_novector(
    async_writable_v_collection: AsyncAstraDBCollection,
) -> None:
    _id = str(uuid.uuid4())
    await async_writable_v_collection.insert_one(
        {
            "_id": _id,
            "text": "Sample entry",
            "otherfield": {"subfield": "abc"},
            "anotherfield": "alpha",
            "$vector": [0.1, 0.9],
        },
    )
    delete_subdocument_response = await async_writable_v_collection.delete_subdocument(
        id=_id,
        subdoc="otherfield.subfield",
    )

    assert delete_subdocument_response["status"]["matchedCount"] >= 1
    assert delete_subdocument_response["status"]["modifiedCount"] == 1

    response = await async_writable_v_collection.find_one(filter={"_id": _id})
    assert response["data"]["document"]["otherfield"] == {}


@pytest.mark.describe("find_one_and_update, through vector (async)")
async def test_find_one_and_update_vector(
    async_disposable_v_collection: AsyncAstraDBCollection,
) -> None:
    find_filter = {"status": {"$exists": True}}
    response0 = await async_disposable_v_collection.find_one(filter=find_filter)
    assert response0["data"]["document"] is None

    sort = {"$vector": [0.2, 0.6]}

    update0 = {"$set": {"status": "active"}}
    options0 = {"returnDocument": "after"}

    update_response0 = await async_disposable_v_collection.find_one_and_update(
        sort=sort, update=update0, options=options0
    )
    assert isinstance(update_response0["data"]["document"], dict)
    assert update_response0["data"]["document"]["status"] == "active"
    assert update_response0["status"]["matchedCount"] >= 1
    assert update_response0["status"]["modifiedCount"] >= 1

    response1 = await async_disposable_v_collection.find_one(filter=find_filter)
    assert isinstance(response1["data"]["document"], dict)
    assert response1["data"]["document"]["status"] == "active"

    update1 = {"$set": {"status": "inactive"}}
    options1 = {"returnDocument": "before"}

    update_response1 = await async_disposable_v_collection.find_one_and_update(
        sort=sort, update=update1, options=options1
    )
    assert isinstance(update_response1["data"]["document"], dict)
    assert update_response1["data"]["document"]["status"] == "active"
    assert update_response1["status"]["matchedCount"] >= 1
    assert update_response1["status"]["modifiedCount"] >= 1

    response2 = await async_disposable_v_collection.find_one(filter=find_filter)
    assert isinstance(response2["data"]["document"], dict)
    assert response2["data"]["document"]["status"] == "inactive"

    filter2 = {"nonexistent_subfield": 10}
    update2 = update1
    options2 = options1

    update_response2 = await async_disposable_v_collection.find_one_and_update(
        sort=sort, update=update2, options=options2, filter=filter2
    )
    assert update_response2["data"]["document"] is None
    assert update_response2["status"]["matchedCount"] == 0
    assert update_response2["status"]["modifiedCount"] == 0


@pytest.mark.describe("find_one_and_delete, through vector (async)")
async def test_find_one_and_delete_vector(
    async_empty_v_collection: AsyncAstraDBCollection,
) -> None:
    await async_empty_v_collection.insert_many(
        [
            {"a": "A", "seq": 1},
            {"a": "A", "seq": 3},
            {"a": "A", "seq": 2},
            {"a": "Z", "seq": 4},
        ]
    )
    del_ok = await async_empty_v_collection.find_one_and_delete(
        sort={"seq": -1},
        filter={"a": "A"},
        projection={"_id": False, "a": False},
    )
    assert del_ok["data"]["document"] == {"seq": 3}
    assert del_ok["status"]["deletedCount"] == 1

    del_no = await async_empty_v_collection.find_one_and_delete(
        sort={"seq": -1},
        filter={"a": "X"},
        projection={"_id": False},
    )
    assert "data" not in del_no
    assert del_no["status"]["deletedCount"] == 0


@pytest.mark.describe("find_one_and_update, not through vector (async)")
async def test_find_one_and_update_novector(
    async_disposable_v_collection: AsyncAstraDBCollection,
) -> None:
    find_filter = {"status": {"$exists": True}}
    response0 = await async_disposable_v_collection.find_one(filter=find_filter)
    assert response0["data"]["document"] is None

    update_filter = {"anotherfield": "omega"}

    update0 = {"$set": {"status": "active"}}
    options0 = {"returnDocument": "after"}

    update_response0 = await async_disposable_v_collection.find_one_and_update(
        filter=update_filter, update=update0, options=options0
    )
    assert isinstance(update_response0["data"]["document"], dict)
    assert update_response0["data"]["document"]["status"] == "active"
    assert update_response0["status"]["matchedCount"] >= 1
    assert update_response0["status"]["modifiedCount"] >= 1

    response1 = await async_disposable_v_collection.find_one(filter=find_filter)
    assert isinstance(response1["data"]["document"], dict)
    assert response1["data"]["document"]["status"] == "active"

    update1 = {"$set": {"status": "inactive"}}
    options1 = {"returnDocument": "before"}

    update_response1 = await async_disposable_v_collection.find_one_and_update(
        filter=update_filter, update=update1, options=options1
    )
    assert isinstance(update_response1["data"]["document"], dict)
    assert update_response1["data"]["document"]["status"] == "active"
    assert update_response1["status"]["matchedCount"] >= 1
    assert update_response1["status"]["modifiedCount"] >= 1

    response2 = await async_disposable_v_collection.find_one(filter=find_filter)
    assert isinstance(response2["data"]["document"], dict)
    assert response2["data"]["document"]["status"] == "inactive"

    filter2 = {**update_filter, **{"nonexistent_subfield": 10}}
    update2 = update1
    options2 = options1

    update_response2 = await async_disposable_v_collection.find_one_and_update(
        filter=filter2, update=update2, options=options2
    )
    assert update_response2["data"]["document"] is None
    assert update_response2["status"]["matchedCount"] == 0
    assert update_response2["status"]["modifiedCount"] == 0


@pytest.mark.describe("find_one_and_replace, through vector (async)")
async def test_find_one_and_replace_vector(
    async_disposable_v_collection: AsyncAstraDBCollection,
) -> None:
    sort = {"$vector": [0.2, 0.6]}

    response0 = await async_disposable_v_collection.find_one(
        sort=sort, projection={"*": 1}
    )
    assert response0 is not None
    assert "anotherfield" in response0["data"]["document"]

    doc0vector = response0["data"]["document"]["$vector"]

    replace_response0 = await async_disposable_v_collection.find_one_and_replace(
        sort=sort,
        replacement={
            "phyla": ["Echinodermata", "Platelminta", "Chordata"],
            "$vector": doc0vector,  # to find this doc again below!
        },
    )
    assert replace_response0 is not None
    assert "anotherfield" in replace_response0["data"]["document"]

    response1 = await async_disposable_v_collection.find_one(sort=sort)
    assert response1 is not None
    assert response1["data"]["document"]["phyla"] == [
        "Echinodermata",
        "Platelminta",
        "Chordata",
    ]
    assert "anotherfield" not in response1["data"]["document"]

    replace_response1 = await async_disposable_v_collection.find_one_and_replace(
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

    response2 = await async_disposable_v_collection.find_one(sort=sort)
    assert response2 is not None
    assert response2["data"]["document"]["phone"] == "0123-4567"
    assert "phyla" not in response2["data"]["document"]

    # non-existing-doc case
    filter_no = {"nonexisting_field": -123}
    replace_response_no = await async_disposable_v_collection.find_one_and_replace(
        sort=sort,
        filter=filter_no,
        replacement={
            "whatever": -123,
            "$vector": doc0vector,
        },
    )
    assert replace_response_no is not None
    assert replace_response_no["data"]["document"] is None


@pytest.mark.describe("find_one_and_replace, not through vector (async)")
async def test_find_one_and_replace_novector(
    async_disposable_v_collection: AsyncAstraDBCollection,
) -> None:
    response0 = await async_disposable_v_collection.find_one(filter={"_id": "1"})
    assert response0 is not None
    assert response0["data"]["document"]["anotherfield"] == "alpha"

    replace_response0 = await async_disposable_v_collection.find_one_and_replace(
        filter={"_id": "1"},
        replacement={
            "_id": "1",
            "phyla": ["Echinodermata", "Platelminta", "Chordata"],
        },
    )
    assert replace_response0 is not None
    assert replace_response0["data"]["document"]["anotherfield"] == "alpha"

    response1 = await async_disposable_v_collection.find_one(filter={"_id": "1"})
    assert response1 is not None
    assert response1["data"]["document"]["phyla"] == [
        "Echinodermata",
        "Platelminta",
        "Chordata",
    ]
    assert "anotherfield" not in response1["data"]["document"]

    replace_response1 = await async_disposable_v_collection.find_one_and_replace(
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

    response2 = await async_disposable_v_collection.find_one(filter={"_id": "1"})
    assert response2 is not None
    assert response2["data"]["document"]["phone"] == "0123-4567"
    assert "phyla" not in response2["data"]["document"]

    # non-existing-doc case
    replace_response_no = await async_disposable_v_collection.find_one_and_replace(
        filter={"_id": "z"},
        replacement={
            "whatever": -123,
        },
    )
    assert replace_response_no is not None
    assert replace_response_no["data"]["document"] is None


@pytest.mark.describe("delete_one, not through vector (async)")
async def test_delete_one_novector(
    async_disposable_v_collection: AsyncAstraDBCollection,
) -> None:
    delete_response = await async_disposable_v_collection.delete_one(id="3")
    assert delete_response["status"]["deletedCount"] == 1

    response = await async_disposable_v_collection.find_one(filter={"_id": "3"})
    assert response["data"]["document"] is None

    delete_response_no = await async_disposable_v_collection.delete_one(id="3")
    assert delete_response_no["status"]["deletedCount"] == 0


@pytest.mark.describe("delete_one_by_predicate, not through vector (async)")
async def test_delete_one_by_predicate_novector(
    async_empty_v_collection: AsyncAstraDBCollection,
) -> None:
    await async_empty_v_collection.insert_one({"k": "v1"})
    await async_empty_v_collection.insert_one({"k": "v1"})
    await async_empty_v_collection.insert_one({"k": "v1"})
    await async_empty_v_collection.insert_one({"k": "v2"})
    assert (await async_empty_v_collection.count_documents())["status"]["count"] == 4
    assert (await async_empty_v_collection.count_documents({"k": "v1"}))["status"][
        "count"
    ] == 3

    await async_empty_v_collection.delete_one_by_predicate({"k": "v1"})
    await async_empty_v_collection.delete_one_by_predicate({"k": "zz"})

    assert (await async_empty_v_collection.count_documents())["status"]["count"] == 3
    assert (await async_empty_v_collection.count_documents({"k": "v1"}))["status"][
        "count"
    ] == 2


@pytest.mark.describe("delete_many, not through vector (async)")
async def test_delete_many_novector(
    async_disposable_v_collection: AsyncAstraDBCollection,
) -> None:
    delete_response = await async_disposable_v_collection.delete_many(
        filter={"anotherfield": "alpha"}
    )
    assert delete_response["status"]["deletedCount"] == 2

    documents_no = await async_disposable_v_collection.find(
        filter={"anotherfield": "alpha"}
    )
    assert documents_no["data"]["documents"] == []

    delete_response_no = await async_disposable_v_collection.delete_many(
        filter={"anotherfield": "alpha"}
    )
    assert delete_response_no["status"]["deletedCount"] == 0


@pytest.mark.describe("chunked delete_many, not through vector (async)")
async def test_chunked_delete_many_novector(
    async_empty_v_collection: AsyncAstraDBCollection,
) -> None:
    await async_empty_v_collection.chunked_insert_many(
        [{"seq": i, "kind": "a"} for i in range(50)]
    )
    await async_empty_v_collection.chunked_insert_many(
        [{"seq": i, "kind": "b"} for i in range(10)]
    )
    response_a = await async_empty_v_collection.chunked_delete_many({"kind": "a"})
    assert sum(rsp["status"]["deletedCount"] for rsp in response_a) == 50
    response_b = await async_empty_v_collection.chunked_delete_many({"kind": "b"})
    assert len(response_b) == 1
    assert response_b[0]["status"]["deletedCount"] == 10
    await async_empty_v_collection.chunked_insert_many(
        [{"seq": i, "kind": "x"} for i in range(25)]
    )
    response_x = await async_empty_v_collection.chunked_delete_many({})
    assert len(response_x) == 1
    assert response_x[0]["status"]["deletedCount"] == -1
    assert (await async_empty_v_collection.count_documents())["status"]["count"] == 0


@pytest.mark.describe("pop, push functions, not through vector (async)")
async def test_pop_push_novector(
    async_empty_v_collection: AsyncAstraDBCollection,
) -> None:
    user_id = str(uuid.uuid4())
    await async_empty_v_collection.insert_one(
        document={
            "_id": user_id,
            "first_name": "Cliff",
            "last_name": "Wicklow",
            "roles": ["user", "admin"],
        },
    )

    pop = {"roles": 1}
    options = {"returnDocument": "after"}

    pop_response = await async_empty_v_collection.pop(
        filter={"_id": user_id}, pop=pop, options=options
    )
    assert pop_response is not None
    assert pop_response["data"]["document"]["roles"] == ["user"]
    assert pop_response["status"]["matchedCount"] >= 1
    assert pop_response["status"]["modifiedCount"] == 1

    response1 = await async_empty_v_collection.find_one(filter={"_id": user_id})
    assert response1 is not None
    assert response1["data"]["document"]["roles"] == ["user"]

    push = {"roles": "auditor"}

    push_response = await async_empty_v_collection.push(
        filter={"_id": user_id}, push=push, options=options
    )
    assert push_response is not None
    assert push_response["data"]["document"]["roles"] == ["user", "auditor"]
    assert push_response["status"]["matchedCount"] >= 1
    assert push_response["status"]["modifiedCount"] == 1

    response2 = await async_empty_v_collection.find_one(filter={"_id": user_id})
    assert response2 is not None
    assert response2["data"]["document"]["roles"] == ["user", "auditor"]


@pytest.mark.describe("store and retrieve dates and datetimes correctly (async)")
async def test_insert_find_with_dates(
    async_writable_v_collection: AsyncAstraDBCollection,
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

    await async_writable_v_collection.insert_one(d_document)

    # retrieve it, simple
    response0 = await async_writable_v_collection.find_one(filter={"_id": d_doc_id})
    assert response0 is not None
    document0 = response0["data"]["document"]
    assert document0 == expected_d_document

    # retrieve it, lt condition on a date
    response1 = await async_writable_v_collection.find_one(
        filter={"nested_list.the_list.0": {"$lt": date1}}
    )
    assert response1 is not None
    document1 = response1["data"]["document"]
    assert document1 == expected_d_document

    # retrieve it, gte condition on a datetime
    response2 = await async_writable_v_collection.find_one(
        filter={"nested.n_date": {"$gte": datetime0}}
    )
    assert response2 is not None
    document2 = response2["data"]["document"]
    assert document2 == expected_d_document

    # retrieve it, filter == condition on a datetime
    response3 = await async_writable_v_collection.find_one(
        filter={"my_date": datetime0}
    )
    assert response3 is not None
    document3 = response3["data"]["document"]
    assert document3 == expected_d_document
