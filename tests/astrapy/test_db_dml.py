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
import json
import httpx
from typing import cast, Dict, Iterable, List, Literal, Optional, Set

import pytest

from astrapy.api import APIRequestError
from astrapy.types import API_DOC
from astrapy.db import AstraDB, AstraDBCollection


logger = logging.getLogger(__name__)


@pytest.mark.describe("should fail clearing a non-existent collection")
def test_clear_collection_fail(db: AstraDB) -> None:
    with pytest.raises(APIRequestError):
        db.collection("this$does%not exist!!!").clear()


@pytest.mark.describe("should truncate a nonvector collection through AstraDB")
def test_truncate_nonvector_collection_through_astradb(
    db: AstraDB, empty_nonv_collection: AstraDBCollection
) -> None:
    empty_nonv_collection.insert_one({"a": 1})
    assert len(empty_nonv_collection.find()["data"]["documents"]) == 1
    with pytest.warns(DeprecationWarning):
        tr_response_col = db.truncate_collection(empty_nonv_collection.collection_name)
    assert len(empty_nonv_collection.find()["data"]["documents"]) == 0
    assert isinstance(tr_response_col, AstraDBCollection)
    assert tr_response_col.collection_name == empty_nonv_collection.collection_name


@pytest.mark.describe("should truncate a collection through AstraDB")
def test_truncate_vector_collection_through_astradb(
    db: AstraDB, empty_v_collection: AstraDBCollection
) -> None:
    empty_v_collection.insert_one({"a": 1, "$vector": [0.1, 0.2]})
    assert len(empty_v_collection.find()["data"]["documents"]) == 1
    with pytest.warns(DeprecationWarning):
        tr_response_col = db.truncate_collection(empty_v_collection.collection_name)
    assert len(empty_v_collection.find()["data"]["documents"]) == 0
    assert isinstance(tr_response_col, AstraDBCollection)
    assert tr_response_col.collection_name == empty_v_collection.collection_name


@pytest.mark.describe("should clear a nonvector collection")
def test_clear_nonvector_collection(
    empty_nonv_collection: AstraDBCollection,
) -> None:
    empty_nonv_collection.insert_one({"a": 1})
    assert len(empty_nonv_collection.find()["data"]["documents"]) == 1
    tr_response = empty_nonv_collection.clear()
    assert len(empty_nonv_collection.find()["data"]["documents"]) == 0
    assert tr_response["status"]["deletedCount"] == -1


@pytest.mark.describe("should clear a collection")
def test_clear_vector_collection(empty_v_collection: AstraDBCollection) -> None:
    empty_v_collection.insert_one({"a": 1, "$vector": [0.1, 0.2]})
    assert len(empty_v_collection.find()["data"]["documents"]) == 1
    tr_response = empty_v_collection.clear()
    assert len(empty_v_collection.find()["data"]["documents"]) == 0
    assert tr_response["status"]["deletedCount"] == -1


@pytest.mark.describe("find_one, not through vector")
def test_find_one_filter_novector(readonly_v_collection: AstraDBCollection) -> None:
    response = readonly_v_collection.find_one(
        filter={"_id": "1"},
    )
    document = response["data"]["document"]
    assert document["text"] == "Sample entry number <1>"
    assert (
        document.keys() ^ {"_id", "text", "otherfield", "anotherfield", "$vector"}
        == set()
    )

    response_not_by_id = readonly_v_collection.find_one(
        filter={"text": "Sample entry number <1>"},
    )
    document_not_by_id = response_not_by_id["data"]["document"]
    assert document_not_by_id["_id"] == "1"
    assert (
        document_not_by_id.keys()
        ^ {"_id", "text", "otherfield", "anotherfield", "$vector"}
        == set()
    )

    response_no = readonly_v_collection.find_one(
        filter={"_id": "Z"},
    )
    document_no = response_no["data"]["document"]
    assert document_no is None

    response_no_not_by_id = readonly_v_collection.find_one(
        filter={"text": "No such text."},
    )
    document_no_not_by_id = response_no_not_by_id["data"]["document"]
    assert document_no_not_by_id is None


@pytest.mark.describe("find, not through vector")
def test_find_filter_novector(readonly_v_collection: AstraDBCollection) -> None:
    response_n2 = readonly_v_collection.find(
        filter={"anotherfield": "alpha"},
    )
    documents_n2 = response_n2["data"]["documents"]
    assert isinstance(documents_n2, list)
    assert {document["_id"] for document in documents_n2} == {"1", "2"}

    response_n1 = readonly_v_collection.find(
        filter={"anotherfield": "alpha"},
        options={"limit": 1},
    )
    documents_n1 = response_n1["data"]["documents"]
    assert isinstance(documents_n1, list)
    assert len(documents_n1) == 1
    assert documents_n1[0]["_id"] in {"1", "2"}


@pytest.mark.describe("obey projection in find and find_one")
def test_find_find_one_projection(
    readonly_v_collection: AstraDBCollection,
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
    exp_fieldsets: List[Set[str]] = [
        {"$vector", "_id", "otherfield", "anotherfield", "text"},
        {"$vector", "_id", "otherfield", "anotherfield", "text"},
        {"_id", "text"},
        {"$vector", "_id"},
        {"$vector", "_id", "text"},
    ]
    for proj, exp_fields in zip(projs, exp_fieldsets):
        response_n = readonly_v_collection.find(
            sort=sort, options=options, projection=proj
        )
        fields = set(response_n["data"]["documents"][0].keys())
        assert fields == exp_fields
        #
        response_1 = readonly_v_collection.find_one(sort=sort, projection=proj)
        fields = set(response_1["data"]["document"].keys())
        assert fields == exp_fields


@pytest.mark.describe("should coerce vectors in the find sort argument")
def test_find_float32(
    readonly_v_collection: AstraDBCollection,
) -> None:
    def ite() -> Iterable[str]:
        for v in [0.1, 0.2]:
            yield f"{v}"

    # we surreptitously trick typing here
    sort = {"$vector": cast(List[float], ite())}
    options = {"limit": 5}

    response = readonly_v_collection.find(sort=sort, options=options)
    assert isinstance(response["data"]["documents"], list)


@pytest.mark.describe("find through vector")
def test_find(readonly_v_collection: AstraDBCollection) -> None:
    sort = {"$vector": [0.2, 0.6]}
    options = {"limit": 100}

    response = readonly_v_collection.find(sort=sort, options=options)
    assert isinstance(response["data"]["documents"], list)


@pytest.mark.describe("proper error raising in find")
def test_find_error(readonly_v_collection: AstraDBCollection) -> None:
    """Wrong type of arguments should raise an API error (ValueError)."""
    sort = {"$vector": [0, "clearly not a list of floats!"]}
    options = {"limit": 100}

    with pytest.raises(APIRequestError):
        readonly_v_collection.find(sort=sort, options=options)


@pytest.mark.describe("find through vector, without explicit limit")
def test_find_limitless(readonly_v_collection: AstraDBCollection) -> None:
    sort = {"$vector": [0.2, 0.6]}
    projection = {"$vector": 1}

    response = readonly_v_collection.find(sort=sort, projection=projection)
    assert response is not None
    assert isinstance(response["data"]["documents"], list)


@pytest.mark.describe("correctly count documents according to predicate")
def test_count_documents(
    readonly_v_collection: AstraDBCollection,
) -> None:
    c_all_response0 = readonly_v_collection.count_documents()
    assert c_all_response0["status"]["count"] == 3

    c_all_response1 = readonly_v_collection.count_documents(filter={})
    assert c_all_response1["status"]["count"] == 3

    c_pred_response = readonly_v_collection.count_documents(
        filter={"anotherfield": "alpha"}
    )
    assert c_pred_response["status"]["count"] == 2

    c_no_response = readonly_v_collection.count_documents(filter={"false_field": 137})
    assert c_no_response["status"]["count"] == 0


@pytest.mark.describe("insert_one, w/out _id, w/out vector")
def test_create_document(writable_v_collection: AstraDBCollection) -> None:
    i_vector = [0.3, 0.5]
    id_v_i = str(uuid.uuid4())
    result_v_i = writable_v_collection.insert_one(
        {
            "_id": id_v_i,
            "a": 1,
            "$vector": i_vector,
        }
    )
    assert result_v_i["status"]["insertedIds"] == [id_v_i]
    assert (
        writable_v_collection.find_one({"_id": result_v_i["status"]["insertedIds"][0]})[
            "data"
        ]["document"]["a"]
        == 1
    )

    id_n_i = str(uuid.uuid4())
    result_n_i = writable_v_collection.insert_one(
        {
            "_id": id_n_i,
            "a": 2,
        }
    )
    assert result_n_i["status"]["insertedIds"] == [id_n_i]
    assert (
        writable_v_collection.find_one({"_id": result_n_i["status"]["insertedIds"][0]})[
            "data"
        ]["document"]["a"]
        == 2
    )

    with pytest.raises(ValueError):
        result_n_i = writable_v_collection.insert_one(
            {
                "_id": id_n_i,
                "a": 3,
            }
        )

    result_v_n = writable_v_collection.insert_one(
        {
            "a": 4,
            "$vector": i_vector,
        }
    )
    assert isinstance(result_v_n["status"]["insertedIds"], list)
    assert isinstance(result_v_n["status"]["insertedIds"][0], str)
    assert len(result_v_n["status"]["insertedIds"]) == 1
    assert (
        writable_v_collection.find_one({"_id": result_v_n["status"]["insertedIds"][0]})[
            "data"
        ]["document"]["a"]
        == 4
    )

    result_n_n = writable_v_collection.insert_one(
        {
            "a": 5,
        }
    )
    assert isinstance(result_n_n["status"]["insertedIds"], list)
    assert isinstance(result_n_n["status"]["insertedIds"][0], str)
    assert len(result_n_n["status"]["insertedIds"]) == 1
    assert (
        writable_v_collection.find_one({"_id": result_n_n["status"]["insertedIds"][0]})[
            "data"
        ]["document"]["a"]
        == 5
    )


@pytest.mark.describe("should coerce 'vectors' to lists of floats")
def test_insert_float32(writable_v_collection: AstraDBCollection, N: int = 2) -> None:
    _id0 = str(uuid.uuid4())
    document = {
        "_id": _id0,
        "name": "Coerce",
        "$vector": [f"{(i+1)/N+2:.4f}" for i in range(N)],
    }
    response = writable_v_collection.insert_one(document)
    assert response is not None
    inserted_ids = response["status"]["insertedIds"]
    assert len(inserted_ids) == 1
    assert inserted_ids[0] == _id0


@pytest.mark.describe("insert_many")
def test_insert_many(writable_v_collection: AstraDBCollection) -> None:
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

    response = writable_v_collection.insert_many(documents)
    assert response is not None
    inserted_ids = set(response["status"]["insertedIds"])
    assert len(inserted_ids - {_id0, _id2}) == 1
    assert isinstance(list(inserted_ids - {_id0, _id2})[0], str)


@pytest.mark.describe("chunked_insert_many")
def test_chunked_insert_many(
    writable_v_collection: AstraDBCollection,
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

    responses0 = writable_v_collection.chunked_insert_many(documents0, chunk_size=3)
    assert responses0 is not None
    inserted_ids0 = [
        ins_id
        for response in responses0
        if isinstance(response, dict)
        for ins_id in response["status"]["insertedIds"]
    ]
    # unordered inserts: this only has to be a set equality
    assert set(inserted_ids0) == set(_ids0)

    response0a = writable_v_collection.find_one(filter={"_id": _ids0[0]})
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
        _ = writable_v_collection.chunked_insert_many(
            documents1,
            chunk_size=3,
            options={"ordered": True},
        )

    responses1_ok = writable_v_collection.chunked_insert_many(
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
def test_concurrent_chunked_insert_many(
    writable_v_collection: AstraDBCollection,
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

    responses0 = writable_v_collection.chunked_insert_many(
        documents0, chunk_size=3, concurrency=4
    )
    assert responses0 is not None
    inserted_ids0 = [
        ins_id
        for response in responses0
        if isinstance(response, dict)
        for ins_id in response["status"]["insertedIds"]
    ]
    # unordered inserts: this only has to be a set equality
    assert set(inserted_ids0) == set(_ids0)

    response0a = writable_v_collection.find_one(filter={"_id": _ids0[0]})
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
        # the first doc must be pre-existing
        # and the doc array size must be <= chunk size
        # for this not to spoil the rest of the test
        docs_for_error = documents0[0:1] + [{"_id": str(uuid.uuid4())}]
        _ = writable_v_collection.chunked_insert_many(
            docs_for_error,
            chunk_size=3,
            concurrency=4,
            options={"ordered": True},
        )

    responses1_ok = writable_v_collection.chunked_insert_many(
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


@pytest.mark.describe("insert_many with 'ordered' set to True")
def test_insert_many_ordered_true(
    writable_v_collection: AstraDBCollection,
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
    response_a = writable_v_collection.insert_many(
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
    response_b = writable_v_collection.insert_many(
        documents_b, partial_failures_allowed=True, options={"ordered": True}
    )
    assert response_b is not None
    assert response_b["status"]["insertedIds"] == []

    response_b2 = writable_v_collection.insert_many(
        documents=documents_b,
        options={"ordered": False},
        partial_failures_allowed=True,
    )
    assert response_b2 is not None
    assert response_b2["status"]["insertedIds"] == [_id2]

    check_response = writable_v_collection.find_one(filter={"first_name": "Yep"})
    assert check_response is not None
    assert check_response["data"]["document"]["_id"] == _id1


@pytest.mark.describe("test error handling - duplicate document")
def test_error_handling_duplicate(
    writable_v_collection: AstraDBCollection,
) -> None:
    _id1 = str(uuid.uuid4())

    result1 = writable_v_collection.insert_one(
        {
            "_id": _id1,
            "a": 1,
            "$vector": [0.3, 0.5],
        }
    )

    assert result1["status"]["insertedIds"] == [_id1]
    assert (
        writable_v_collection.find_one({"_id": result1["status"]["insertedIds"][0]})[
            "data"
        ]["document"]["a"]
        == 1
    )

    with pytest.raises(ValueError):
        writable_v_collection.insert_one(
            {
                "_id": _id1,
                "a": 1,
                "$vector": [0.3, 0.5],
            }
        )

    try:
        writable_v_collection.insert_one(
            {
                "_id": _id1,
                "a": 1,
                "$vector": [0.3, 0.5],
            }
        )
    except ValueError as e:
        message = str(e)
        parsed_json = json.loads(message)

        assert parsed_json["errors"][0]["errorCode"] == "DOCUMENT_ALREADY_EXISTS"


@pytest.mark.describe("test error handling - network error")
def test_error_handling_network(
    invalid_writable_v_collection: AstraDBCollection,
) -> None:
    _id1 = str(uuid.uuid4())

    with pytest.raises(httpx.ConnectError):
        invalid_writable_v_collection.insert_one(
            {
                "_id": _id1,
                "a": 1,
                "$vector": [0.3, 0.5],
            }
        )


@pytest.mark.describe("upsert_many")
def test_upsert_many(
    writable_v_collection: AstraDBCollection,
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

    upsert_result0 = writable_v_collection.upsert_many(documents0)
    assert upsert_result0 == [doc["_id"] for doc in documents0]

    response0a = writable_v_collection.find_one(filter={"_id": _ids0[0]})
    assert response0a is not None
    assert response0a["data"]["document"] == documents0[0]

    response0b = writable_v_collection.find_one(filter={"_id": _ids0[-1]})
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
    upsert_result1 = writable_v_collection.upsert_many(
        documents1,
        concurrency=5,
    )
    assert upsert_result1 == [doc["_id"] for doc in documents1]

    response1a = writable_v_collection.find_one(filter={"_id": _ids1[0]})
    assert response1a is not None
    assert response1a["data"]["document"] == documents1[0]

    response1b = writable_v_collection.find_one(filter={"_id": _ids1[-1]})
    assert response1b is not None
    assert response1b["data"]["document"] == documents1[-1]


@pytest.mark.describe("upsert")
def test_upsert_document(writable_v_collection: AstraDBCollection) -> None:
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
    upsert_result0 = writable_v_collection.upsert(document0)
    assert upsert_result0 == _id

    response0 = writable_v_collection.find_one(filter={"_id": _id})
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
    upsert_result1 = writable_v_collection.upsert(document1)
    assert upsert_result1 == _id

    response1 = writable_v_collection.find_one(filter={"_id": _id})
    assert response1 is not None
    assert response1["data"]["document"] == document1


@pytest.mark.describe("update_one to create a subdocument, not through vector")
def test_update_one_create_subdocument_novector(
    writable_v_collection: AstraDBCollection,
) -> None:
    _id = str(uuid.uuid4())
    writable_v_collection.insert_one({"_id": _id, "name": "Not Eric!"})
    update_one_response = writable_v_collection.update_one(
        filter={"_id": _id},
        update={"$set": {"name": "Eric"}},
    )

    assert update_one_response["status"]["matchedCount"] >= 1
    assert update_one_response["status"]["modifiedCount"] == 1

    response = writable_v_collection.find_one(filter={"_id": _id})
    assert response["data"]["document"]["name"] == "Eric"


@pytest.mark.describe("delete_subdocument, not through vector")
def test_delete_subdocument_novector(
    writable_v_collection: AstraDBCollection,
) -> None:
    _id = str(uuid.uuid4())
    writable_v_collection.insert_one(
        {
            "_id": _id,
            "text": "Sample entry",
            "otherfield": {"subfield": "abc"},
            "anotherfield": "alpha",
            "$vector": [0.1, 0.9],
        },
    )
    delete_subdocument_response = writable_v_collection.delete_subdocument(
        id=_id,
        subdoc="otherfield.subfield",
    )

    assert delete_subdocument_response["status"]["matchedCount"] >= 1
    assert delete_subdocument_response["status"]["modifiedCount"] == 1

    response = writable_v_collection.find_one(filter={"_id": _id})
    assert response["data"]["document"]["otherfield"] == {}


@pytest.mark.describe("find_one_and_update, through vector")
def test_find_one_and_update_vector(
    disposable_v_collection: AstraDBCollection,
) -> None:
    find_filter = {"status": {"$exists": True}}
    response0 = disposable_v_collection.find_one(filter=find_filter)
    assert response0["data"]["document"] is None

    sort = {"$vector": [0.2, 0.6]}

    update0 = {"$set": {"status": "active"}}
    options0 = {"returnDocument": "after"}

    update_response0 = disposable_v_collection.find_one_and_update(
        sort=sort, update=update0, options=options0
    )
    assert isinstance(update_response0["data"]["document"], dict)
    assert update_response0["data"]["document"]["status"] == "active"
    assert update_response0["status"]["matchedCount"] >= 1
    assert update_response0["status"]["modifiedCount"] >= 1

    response1 = disposable_v_collection.find_one(filter=find_filter)
    assert isinstance(response1["data"]["document"], dict)
    assert response1["data"]["document"]["status"] == "active"

    update1 = {"$set": {"status": "inactive"}}
    options1 = {"returnDocument": "before"}

    update_response1 = disposable_v_collection.find_one_and_update(
        sort=sort, update=update1, options=options1
    )
    assert isinstance(update_response1["data"]["document"], dict)
    assert update_response1["data"]["document"]["status"] == "active"
    assert update_response1["status"]["matchedCount"] >= 1
    assert update_response1["status"]["modifiedCount"] >= 1

    response2 = disposable_v_collection.find_one(filter=find_filter)
    assert isinstance(response2["data"]["document"], dict)
    assert response2["data"]["document"]["status"] == "inactive"

    filter2 = {"nonexistent_subfield": 10}
    update2 = update1
    options2 = options1

    update_response2 = disposable_v_collection.find_one_and_update(
        sort=sort, update=update2, options=options2, filter=filter2
    )
    assert update_response2["data"]["document"] is None
    assert update_response2["status"]["matchedCount"] == 0
    assert update_response2["status"]["modifiedCount"] == 0


@pytest.mark.describe("find_one_and_update, not through vector")
def test_find_one_and_update_novector(
    disposable_v_collection: AstraDBCollection,
) -> None:
    find_filter = {"status": {"$exists": True}}
    response0 = disposable_v_collection.find_one(filter=find_filter)
    assert response0["data"]["document"] is None

    update_filter = {"anotherfield": "omega"}

    update0 = {"$set": {"status": "active"}}
    options0 = {"returnDocument": "after"}

    update_response0 = disposable_v_collection.find_one_and_update(
        filter=update_filter, update=update0, options=options0
    )
    assert isinstance(update_response0["data"]["document"], dict)
    assert update_response0["data"]["document"]["status"] == "active"
    assert update_response0["status"]["matchedCount"] >= 1
    assert update_response0["status"]["modifiedCount"] >= 1

    response1 = disposable_v_collection.find_one(filter=find_filter)
    assert isinstance(response1["data"]["document"], dict)
    assert response1["data"]["document"]["status"] == "active"

    update1 = {"$set": {"status": "inactive"}}
    options1 = {"returnDocument": "before"}

    update_response1 = disposable_v_collection.find_one_and_update(
        filter=update_filter, update=update1, options=options1
    )
    assert isinstance(update_response1["data"]["document"], dict)
    assert update_response1["data"]["document"]["status"] == "active"
    assert update_response1["status"]["matchedCount"] >= 1
    assert update_response1["status"]["modifiedCount"] >= 1

    response2 = disposable_v_collection.find_one(filter=find_filter)
    assert isinstance(response2["data"]["document"], dict)
    assert response2["data"]["document"]["status"] == "inactive"

    filter2 = {**update_filter, **{"nonexistent_subfield": 10}}
    update2 = update1
    options2 = options1

    update_response2 = disposable_v_collection.find_one_and_update(
        filter=filter2, update=update2, options=options2
    )
    assert update_response2["data"]["document"] is None
    assert update_response2["status"]["matchedCount"] == 0
    assert update_response2["status"]["modifiedCount"] == 0


@pytest.mark.describe("find_one_and_replace, through vector")
def test_find_one_and_replace_vector(
    disposable_v_collection: AstraDBCollection,
) -> None:
    sort = {"$vector": [0.2, 0.6]}

    response0 = disposable_v_collection.find_one(sort=sort)
    assert response0 is not None
    assert "anotherfield" in response0["data"]["document"]

    doc0vector = response0["data"]["document"]["$vector"]

    replace_response0 = disposable_v_collection.find_one_and_replace(
        sort=sort,
        replacement={
            "phyla": ["Echinodermata", "Platelminta", "Chordata"],
            "$vector": doc0vector,  # to find this doc again below!
        },
    )
    assert replace_response0 is not None
    assert "anotherfield" in replace_response0["data"]["document"]

    response1 = disposable_v_collection.find_one(sort=sort)
    assert response1 is not None
    assert response1["data"]["document"]["phyla"] == [
        "Echinodermata",
        "Platelminta",
        "Chordata",
    ]
    assert "anotherfield" not in response1["data"]["document"]

    replace_response1 = disposable_v_collection.find_one_and_replace(
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

    response2 = disposable_v_collection.find_one(sort=sort)
    assert response2 is not None
    assert response2["data"]["document"]["phone"] == "0123-4567"
    assert "phyla" not in response2["data"]["document"]

    # non-existing-doc case
    filter_no = {"nonexisting_field": -123}
    replace_response_no = disposable_v_collection.find_one_and_replace(
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
    disposable_v_collection: AstraDBCollection,
) -> None:
    response0 = disposable_v_collection.find_one(filter={"_id": "1"})
    assert response0 is not None
    assert response0["data"]["document"]["anotherfield"] == "alpha"

    replace_response0 = disposable_v_collection.find_one_and_replace(
        filter={"_id": "1"},
        replacement={
            "_id": "1",
            "phyla": ["Echinodermata", "Platelminta", "Chordata"],
        },
    )
    assert replace_response0 is not None
    assert replace_response0["data"]["document"]["anotherfield"] == "alpha"

    response1 = disposable_v_collection.find_one(filter={"_id": "1"})
    assert response1 is not None
    assert response1["data"]["document"]["phyla"] == [
        "Echinodermata",
        "Platelminta",
        "Chordata",
    ]
    assert "anotherfield" not in response1["data"]["document"]

    replace_response1 = disposable_v_collection.find_one_and_replace(
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

    response2 = disposable_v_collection.find_one(filter={"_id": "1"})
    assert response2 is not None
    assert response2["data"]["document"]["phone"] == "0123-4567"
    assert "phyla" not in response2["data"]["document"]

    # non-existing-doc case
    replace_response_no = disposable_v_collection.find_one_and_replace(
        filter={"_id": "z"},
        replacement={
            "whatever": -123,
        },
    )
    assert replace_response_no is not None
    assert replace_response_no["data"]["document"] is None


@pytest.mark.describe("delete_one, not through vector")
def test_delete_one_novector(disposable_v_collection: AstraDBCollection) -> None:
    delete_response = disposable_v_collection.delete_one(id="3")
    assert delete_response["status"]["deletedCount"] == 1

    response = disposable_v_collection.find_one(filter={"_id": "3"})
    assert response["data"]["document"] is None

    delete_response_no = disposable_v_collection.delete_one(id="3")
    assert delete_response_no["status"]["deletedCount"] == 0


@pytest.mark.describe("delete_many, not through vector")
def test_delete_many_novector(disposable_v_collection: AstraDBCollection) -> None:
    delete_response = disposable_v_collection.delete_many(
        filter={"anotherfield": "alpha"}
    )
    assert delete_response["status"]["deletedCount"] == 2

    documents_no = disposable_v_collection.find(filter={"anotherfield": "alpha"})
    assert documents_no["data"]["documents"] == []

    delete_response_no = disposable_v_collection.delete_many(
        filter={"anotherfield": "alpha"}
    )
    assert delete_response_no["status"]["deletedCount"] == 0


@pytest.mark.describe("pop, push functions, not through vector")
def test_pop_push_novector(empty_v_collection: AstraDBCollection) -> None:
    user_id = str(uuid.uuid4())
    empty_v_collection.insert_one(
        document={
            "_id": user_id,
            "first_name": "Cliff",
            "last_name": "Wicklow",
            "roles": ["user", "admin"],
        },
    )

    pop = {"roles": 1}
    options = {"returnDocument": "after"}

    pop_response = empty_v_collection.pop(
        filter={"_id": user_id}, pop=pop, options=options
    )
    assert pop_response is not None
    assert pop_response["data"]["document"]["roles"] == ["user"]
    assert pop_response["status"]["matchedCount"] >= 1
    assert pop_response["status"]["modifiedCount"] == 1

    response1 = empty_v_collection.find_one(filter={"_id": user_id})
    assert response1 is not None
    assert response1["data"]["document"]["roles"] == ["user"]

    push = {"roles": "auditor"}

    push_response = empty_v_collection.push(
        filter={"_id": user_id}, push=push, options=options
    )
    assert push_response is not None
    assert push_response["data"]["document"]["roles"] == ["user", "auditor"]
    assert push_response["status"]["matchedCount"] >= 1
    assert push_response["status"]["modifiedCount"] == 1

    response2 = empty_v_collection.find_one(filter={"_id": user_id})
    assert response2 is not None
    assert response2["data"]["document"]["roles"] == ["user", "auditor"]


@pytest.mark.describe("find/find_one with non-equality operators in filter")
def test_find_find_one_non_equality_operators(
    empty_nonv_collection: AstraDBCollection,
) -> None:
    full_document = {
        "_id": "1",
        "marker": "abc",
        "metadata_boolean": True,
        "metadata_boolean_array": [
            True,
            False,
            True,
        ],
        "metadata_byte": 1,
        "metadata_calendar": {
            "$date": 1704727049823,
        },
        "metadata_character": "c",
        "metadata_date": {
            "$date": 1704727049823,
        },
        "metadata_double": 1213.343243,
        "metadata_double_array": [
            1.0,
            2.0,
            3.0,
        ],
        "metadata_enum": "GCP",
        "metadata_enum_array": ["GCP", "AWS"],
        "metadata_float": 1.1232435,
        "metadata_float_array": [
            1.0,
            2.0,
            3.0,
        ],
        "metadata_instant": {
            "$date": 1704727049822,
        },
        "metadata_int": 1,
        "metadata_int_array": [
            1,
            2,
            3,
        ],
        "metadata_list": [
            "value1",
            "value2",
        ],
        "metadata_long": 12321323,
        "metadata_long_array": [
            1,
            2,
            3,
        ],
        "metadata_map": {
            "key1": "value1",
            "key2": "value2",
        },
        "metadata_object": {
            "product_name": "name",
            "product_price": 1.0,
        },
        "metadata_short": 1,
        "metadata_short_array": [
            1,
            2,
            3,
        ],
        "metadata_string": "hello",
        "metadata_string_array": [
            "a",
            "b",
            "c",
        ],
        "metadata_uuid": "2123d205-2d8e-45f0-b22f-0e6980cd56c8",
        "metadata_uuid_array": [
            "b98b4bbc-5a48-4b07-86a6-1c98fd7d5821",
            "b525cd48-abf7-4b40-b9ef-0c3248fbb8e8",
        ],
    }
    empty_nonv_collection.insert_one(full_document)
    projection = {"marker": 1}

    # find by id
    resp0 = empty_nonv_collection.find_one(filter={"_id": "1"}, projection=projection)
    assert resp0["data"]["document"]["marker"] == "abc"

    # find with $in
    resp1 = empty_nonv_collection.find(
        filter={"metadata_string": {"$in": ["hello", "world"]}}, projection=projection
    )
    assert resp1["data"]["documents"][0]["marker"] == "abc"

    # find with $nin
    resp2 = empty_nonv_collection.find(
        filter={"metadata_string": {"$nin": ["Hallo", "Welt"]}}, projection=projection
    )
    assert resp2["data"]["documents"][0]["marker"] == "abc"

    # find with $size
    resp3 = empty_nonv_collection.find(
        filter={"metadata_boolean_array": {"$size": 3}}, projection=projection
    )
    assert resp3["data"]["documents"][0]["marker"] == "abc"

    # find with $lt
    resp4 = empty_nonv_collection.find(
        filter={"metadata_int": {"$lt": 2}}, projection=projection
    )
    assert resp4["data"]["documents"][0]["marker"] == "abc"

    # find with $lte
    resp5 = empty_nonv_collection.find(
        filter={"metadata_int": {"$lte": 1}}, projection=projection
    )
    assert resp5["data"]["documents"][0]["marker"] == "abc"

    # find with $gt
    resp6 = empty_nonv_collection.find(
        filter={"metadata_int": {"$gt": 0}}, projection=projection
    )
    assert resp6["data"]["documents"][0]["marker"] == "abc"

    # find with $gte
    resp7 = empty_nonv_collection.find(
        filter={"metadata_int": {"$gte": 1}}, projection=projection
    )
    assert resp7["data"]["documents"][0]["marker"] == "abc"

    # find with $gte on a Date
    resp8 = empty_nonv_collection.find(
        filter={"metadata_instant": {"$lt": {"$date": 1704727050218}}},
        projection=projection,
    )
    assert resp8["data"]["documents"][0]["marker"] == "abc"


@pytest.mark.describe("store and retrieve dates and datetimes correctly")
def test_insert_find_with_dates(
    writable_v_collection: AstraDBCollection,
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

    writable_v_collection.insert_one(d_document)

    # retrieve it, simple
    response0 = writable_v_collection.find_one(filter={"_id": d_doc_id})
    assert response0 is not None
    document0 = response0["data"]["document"]
    assert document0 == expected_d_document

    # retrieve it, lt condition on a date
    response1 = writable_v_collection.find_one(
        filter={"nested_list.the_list.0": {"$lt": date1}}
    )
    assert response1 is not None
    document1 = response1["data"]["document"]
    assert document1 == expected_d_document

    # retrieve it, gte condition on a datetime
    response2 = writable_v_collection.find_one(
        filter={"nested.n_date": {"$gte": datetime0}}
    )
    assert response2 is not None
    document2 = response2["data"]["document"]
    assert document2 == expected_d_document

    # retrieve it, filter == condition on a datetime
    response3 = writable_v_collection.find_one(filter={"my_date": datetime0})
    assert response3 is not None
    document3 = response3["data"]["document"]
    assert document3 == expected_d_document
