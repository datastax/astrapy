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
Tests for the `db.py` parts related to DML & client creation
"""

import os
import logging

import pytest

from ..conftest import AstraDBCredentials
from astrapy.db import AstraDB, AstraDBCollection
from astrapy.defaults import DEFAULT_KEYSPACE_NAME

TEST_CREATE_DELETE_VECTOR_COLLECTION_NAME = "ephemeral_v_col"
TEST_CREATE_DELETE_NONVECTOR_COLLECTION_NAME = "ephemeral_non_v_col"

logger = logging.getLogger(__name__)


@pytest.mark.describe("should confirm path handling in constructor")
def test_path_handling(astra_db_credentials_kwargs: AstraDBCredentials) -> None:
    token = astra_db_credentials_kwargs["token"]
    api_endpoint = astra_db_credentials_kwargs["api_endpoint"]
    namespace = astra_db_credentials_kwargs.get("namespace")

    if token is None or api_endpoint is None:
        raise ValueError("Required ASTRA DB configuration is missing")

    astra_db_1 = AstraDB(token=token, api_endpoint=api_endpoint, namespace=namespace)
    url_1 = astra_db_1.base_path

    astra_db_2 = AstraDB(
        token=token, api_endpoint=api_endpoint, namespace=namespace, api_version="v1"
    )
    url_2 = astra_db_2.base_path

    astra_db_3 = AstraDB(
        token=token, api_endpoint=api_endpoint, namespace=namespace, api_version="/v1"
    )
    url_3 = astra_db_3.base_path

    astra_db_4 = AstraDB(
        token=token, api_endpoint=api_endpoint, namespace=namespace, api_version="/v1/"
    )
    url_4 = astra_db_4.base_path

    assert url_1 == url_2 == url_3 == url_4

    # autofill of the default keyspace name
    unspecified_ks_client = AstraDB(
        token=token, api_endpoint=api_endpoint, namespace=DEFAULT_KEYSPACE_NAME
    )
    explicit_ks_client = AstraDB(token=token, api_endpoint=api_endpoint, namespace=None)

    assert unspecified_ks_client.base_path == explicit_ks_client.base_path


@pytest.mark.skipif(
    int(os.getenv("TEST_SKIP_COLLECTION_DELETE", "0")) == 1,
    reason="collection-deletion tests are suppressed",
)
@pytest.mark.describe("should create, use and destroy a non-vector collection")
def test_create_use_destroy_nonvector_collection(db: AstraDB) -> None:
    col = db.create_collection(TEST_CREATE_DELETE_NONVECTOR_COLLECTION_NAME)
    assert isinstance(col, AstraDBCollection)
    col.insert_one({"_id": "first", "name": "a"})
    col.insert_many(
        [
            {"_id": "second", "name": "b", "room": 7},
            {"name": "c", "room": 7},
            {"_id": "last", "type": "unnamed", "room": 7},
        ]
    )
    docs = col.find(filter={"room": 7}, projection={"name": 1})
    ids = [doc["_id"] for doc in docs["data"]["documents"]]
    assert len(ids) == 3
    assert "second" in ids
    assert "first" not in ids
    auto_id = [id for id in ids if id not in {"second", "last"}][0]
    col.delete(auto_id)
    assert col.find_one(filter={"name": "c"})["data"]["document"] is None
    del_res = db.delete_collection(TEST_CREATE_DELETE_NONVECTOR_COLLECTION_NAME)
    assert del_res["status"]["ok"] == 1


@pytest.mark.skipif(
    int(os.getenv("TEST_SKIP_COLLECTION_DELETE", "0")) == 1,
    reason="collection-deletion tests are suppressed",
)
@pytest.mark.describe("should create and destroy a vector collection")
def test_create_use_destroy_vector_collection(db: AstraDB) -> None:
    col = db.create_collection(
        collection_name=TEST_CREATE_DELETE_VECTOR_COLLECTION_NAME, dimension=2
    )
    assert isinstance(col, AstraDBCollection)
    del_res = db.delete_collection(
        collection_name=TEST_CREATE_DELETE_VECTOR_COLLECTION_NAME
    )
    assert del_res["status"]["ok"] == 1


@pytest.mark.describe("should get all collections")
def test_get_collections(db: AstraDB, readonly_v_collection: AstraDBCollection) -> None:
    res = db.get_collections()
    assert res["status"]["collections"] is not None
    assert readonly_v_collection.collection_name in res["status"]["collections"]
