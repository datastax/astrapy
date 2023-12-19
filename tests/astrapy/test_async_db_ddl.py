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

import logging
from typing import Dict, Optional

import pytest

from astrapy.db import AsyncAstraDB, AsyncAstraDBCollection
from astrapy.defaults import DEFAULT_KEYSPACE_NAME

TEST_CREATE_DELETE_VECTOR_COLLECTION_NAME = "ephemeral_v_col"
TEST_CREATE_DELETE_NONVECTOR_COLLECTION_NAME = "ephemeral_non_v_col"

logger = logging.getLogger(__name__)


@pytest.mark.describe("should confirm path handling in constructor")
async def test_path_handling(
    astra_db_credentials_kwargs: Dict[str, Optional[str]]
) -> None:
    async with AsyncAstraDB(**astra_db_credentials_kwargs) as astra_db_1:
        url_1 = astra_db_1.base_path

    async with AsyncAstraDB(
        **astra_db_credentials_kwargs,
        api_version="v1",
    ) as astra_db_2:
        url_2 = astra_db_2.base_path

    async with AsyncAstraDB(
        **astra_db_credentials_kwargs,
        api_version="/v1",
    ) as astra_db_3:
        url_3 = astra_db_3.base_path

    async with AsyncAstraDB(
        **astra_db_credentials_kwargs,
        api_version="/v1/",
    ) as astra_db_4:
        url_4 = astra_db_4.base_path

    assert url_1 == url_2 == url_3 == url_4

    # autofill of the default keyspace name
    async with AsyncAstraDB(
        **{
            **astra_db_credentials_kwargs,
            **{"namespace": DEFAULT_KEYSPACE_NAME},
        }
    ) as unspecified_ks_client, AsyncAstraDB(
        **{
            **astra_db_credentials_kwargs,
            **{"namespace": None},
        }
    ) as explicit_ks_client:
        assert unspecified_ks_client.base_path == explicit_ks_client.base_path


@pytest.mark.describe("should create, use and destroy a non-vector collection")
async def test_create_use_destroy_nonvector_collection(async_db: AsyncAstraDB) -> None:
    col = await async_db.create_collection(TEST_CREATE_DELETE_NONVECTOR_COLLECTION_NAME)
    assert isinstance(col, AsyncAstraDBCollection)
    await col.insert_one({"_id": "first", "name": "a"})
    await col.insert_many(
        [
            {"_id": "second", "name": "b", "room": 7},
            {"name": "c", "room": 7},
            {"_id": "last", "type": "unnamed", "room": 7},
        ]
    )
    docs = await col.find(filter={"room": 7}, projection={"name": 1})
    ids = [doc["_id"] for doc in docs["data"]["documents"]]
    assert len(ids) == 3
    assert "second" in ids
    assert "first" not in ids
    auto_id = [id for id in ids if id not in {"second", "last"}][0]
    await col.delete_one(auto_id)
    assert (await col.find_one(filter={"name": "c"}))["data"]["document"] is None
    del_res = await async_db.delete_collection(
        TEST_CREATE_DELETE_NONVECTOR_COLLECTION_NAME
    )
    assert del_res["status"]["ok"] == 1


@pytest.mark.describe("should create and destroy a vector collection")
async def test_create_use_destroy_vector_collection(async_db: AsyncAstraDB) -> None:
    col = await async_db.create_collection(
        collection_name=TEST_CREATE_DELETE_VECTOR_COLLECTION_NAME, dimension=2
    )
    assert isinstance(col, AsyncAstraDBCollection)
    del_res = await async_db.delete_collection(
        collection_name=TEST_CREATE_DELETE_VECTOR_COLLECTION_NAME
    )
    assert del_res["status"]["ok"] == 1


@pytest.mark.describe("should get all collections")
async def test_get_collections(async_db: AsyncAstraDB) -> None:
    res = await async_db.get_collections()
    assert res["status"]["collections"] is not None
