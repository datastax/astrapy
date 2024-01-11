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
Tests for the `db.py` parts on data manipulation `vector_*` methods
"""

import logging
from typing import cast

import pytest

from astrapy.db import AstraDBCollection
from astrapy.types import API_DOC

logger = logging.getLogger(__name__)


@pytest.mark.describe("vector_find and include_similarity parameter")
def test_vector_find(readonly_vector_collection: AstraDBCollection) -> None:
    documents_sim_1 = readonly_vector_collection.vector_find(
        vector=[0.2, 0.6],
        limit=3,
    )

    assert documents_sim_1 is not None
    assert isinstance(documents_sim_1, list)
    assert len(documents_sim_1) > 0
    assert "_id" in documents_sim_1[0]
    assert "$vector" in documents_sim_1[0]
    assert "text" in documents_sim_1[0]
    assert "$similarity" in documents_sim_1[0]

    documents_sim_2 = readonly_vector_collection.vector_find(
        vector=[0.2, 0.6],
        limit=3,
        include_similarity=True,
    )

    assert documents_sim_2 is not None
    assert isinstance(documents_sim_2, list)
    assert len(documents_sim_2) > 0
    assert "_id" in documents_sim_2[0]
    assert "$vector" in documents_sim_2[0]
    assert "text" in documents_sim_2[0]
    assert "$similarity" in documents_sim_2[0]

    documents_no_sim = readonly_vector_collection.vector_find(
        vector=[0.2, 0.6],
        limit=3,
        fields=["_id", "$vector"],
        include_similarity=False,
    )

    assert documents_no_sim is not None
    assert isinstance(documents_no_sim, list)
    assert len(documents_no_sim) > 0
    assert "_id" in documents_no_sim[0]
    assert "$vector" in documents_no_sim[0]
    assert "text" not in documents_no_sim[0]
    assert "$similarity" not in documents_no_sim[0]


@pytest.mark.describe("should coerce vectors in vector_find")
def test_vector_find_float32(
    readonly_vector_collection: AstraDBCollection,
) -> None:
    def ite():
        for v in [0.1, 0.2]:
            yield f"{v}"

    documents_sim_1 = readonly_vector_collection.vector_find(
        vector=ite(),
        limit=3,
    )

    assert documents_sim_1 is not None
    assert isinstance(documents_sim_1, list)
    assert len(documents_sim_1) > 0
    assert "_id" in documents_sim_1[0]
    assert "$vector" in documents_sim_1[0]
    assert "text" in documents_sim_1[0]
    assert "$similarity" in documents_sim_1[0]


@pytest.mark.describe("vector_find, obey projection")
def test_vector_find_projection(readonly_vector_collection: AstraDBCollection) -> None:
    query = [0.2, 0.6]

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
            vdocs = readonly_vector_collection.vector_find(
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


@pytest.mark.describe("vector_find with filters")
def test_vector_find_filters(readonly_vector_collection: AstraDBCollection) -> None:
    documents = readonly_vector_collection.vector_find(
        vector=[0.2, 0.6],
        filter={"anotherfield": "alpha"},
        limit=3,
    )
    assert isinstance(documents, list)
    assert len(documents) == 2
    assert {doc["otherfield"]["subfield"] for doc in documents} == {"x1y", "x2y"}

    documents_no = readonly_vector_collection.vector_find(
        vector=[0.2, 0.6],
        filter={"anotherfield": "epsilon"},
        limit=3,
    )
    assert isinstance(documents_no, list)
    assert len(documents_no) == 0


@pytest.mark.describe("vector_find_one and include_similarity parameter")
def test_vector_find_one(readonly_vector_collection: AstraDBCollection) -> None:
    document0 = readonly_vector_collection.vector_find_one(
        [0.2, 0.6],
    )

    assert document0 is not None
    assert "_id" in document0
    assert "$vector" in document0
    assert "text" in document0
    assert "$similarity" in document0

    document_w_sim = readonly_vector_collection.vector_find_one(
        [0.2, 0.6],
        include_similarity=True,
    )

    assert document_w_sim is not None
    assert "_id" in document_w_sim
    assert "$vector" in document_w_sim
    assert "text" in document_w_sim
    assert "$similarity" in document_w_sim

    document_no_sim = readonly_vector_collection.vector_find_one(
        [0.2, 0.6],
        include_similarity=False,
    )

    assert document_no_sim is not None
    assert "_id" in document_no_sim
    assert "$vector" in document_no_sim
    assert "text" in document_no_sim
    assert "$similarity" not in document_no_sim

    document_w_fields = readonly_vector_collection.vector_find_one(
        [0.2, 0.6], fields=["text"]
    )

    assert document_w_fields is not None
    assert "_id" in document_w_fields
    assert "$vector" not in document_w_fields
    assert "text" in document_w_fields
    assert "$similarity" in document_w_fields

    document_no = readonly_vector_collection.vector_find_one(
        [0.2, 0.6],
        filter={"nonexisting": "gotcha"},
    )

    assert document_no is None


@pytest.mark.describe("vector_find_one_and_update")
def test_vector_find_one_and_update(
    disposable_vector_collection: AstraDBCollection,
) -> None:
    update = {"$set": {"status": "active"}}

    document0 = disposable_vector_collection.vector_find_one(
        vector=[0.1, 0.9],
        filter={"status": "active"},
    )
    assert document0 is None

    update_response = disposable_vector_collection.vector_find_one_and_update(
        vector=[0.1, 0.9],
        update=update,
    )
    assert update_response is not None
    assert update_response["_id"] == "1"

    document1 = disposable_vector_collection.vector_find_one(
        vector=[0.1, 0.9],
        filter={"status": "active"},
    )

    assert document1 is not None
    assert document1["_id"] == update_response["_id"]
    assert document1["status"] == "active"

    update_response_no = disposable_vector_collection.vector_find_one_and_update(
        vector=[0.1, 0.9],
        filter={"nonexisting": "gotcha"},
        update=update,
    )
    assert update_response_no is None


@pytest.mark.describe("vector_find_one_and_replace")
def test_vector_find_one_and_replace(
    disposable_vector_collection: AstraDBCollection,
) -> None:
    replacement0 = {
        "_id": "1",
        "text": "Revised sample entry number <1>",
        "added_field": True,
        "$vector": [0.101, 0.899],
    }

    document0 = disposable_vector_collection.vector_find_one(
        vector=[0.1, 0.9],
        filter={"added_field": True},
    )
    assert document0 is None

    replace_response0 = disposable_vector_collection.vector_find_one_and_replace(
        vector=[0.1, 0.9],
        replacement=replacement0,
    )
    assert replace_response0 is not None
    assert replace_response0["_id"] == "1"

    document1 = disposable_vector_collection.vector_find_one(
        vector=[0.1, 0.9],
        filter={"added_field": True},
    )

    assert document1 is not None
    assert document1["_id"] == replace_response0["_id"]
    assert "otherfield" not in document1
    assert "anotherfield" not in document1
    assert document1["text"] == replacement0["text"]
    assert document1["added_field"] is True

    # no supplying the _id
    replacement1 = {
        "text": "Further revised sample entry number <1>",
        "different_added_field": False,
        "$vector": [0.101, 0.899],
    }

    replace_response1 = disposable_vector_collection.vector_find_one_and_replace(
        vector=[0.1, 0.9],
        replacement=replacement1,
    )
    assert replace_response0 is not None
    assert replace_response0["_id"] == "1"

    document2 = disposable_vector_collection.vector_find_one(
        vector=[0.1, 0.9],
        filter={"different_added_field": False},
    )

    assert document2 is not None
    assert cast(API_DOC, document2)["_id"] == cast(API_DOC, replace_response1)["_id"]
    assert cast(API_DOC, document2)["text"] == replacement1["text"]
    assert "added_field" not in cast(API_DOC, document2)
    assert cast(API_DOC, document2)["different_added_field"] is False

    replace_response_no = disposable_vector_collection.vector_find_one_and_replace(
        vector=[0.1, 0.9],
        filter={"nonexisting": "gotcha"},
        replacement=replacement1,
    )
    assert replace_response_no is None
