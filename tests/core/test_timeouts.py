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

import pytest
import httpx

from ..conftest import AstraDBCredentials
from .conftest import TEST_SKIP_COLLECTION_DELETE
from astrapy.core.db import (
    AstraDB,
    AstraDBCollection,
    AsyncAstraDB,
    AsyncAstraDBCollection,
)
from astrapy.core.defaults import DEFAULT_KEYSPACE_NAME

TEST_CREATE_DELETE_VECTOR_COLLECTION_NAME = "ephemeral_v_col"
TEST_CREATE_DELETE_NONVECTOR_COLLECTION_NAME = "ephemeral_non_v_col"

logger = logging.getLogger(__name__)


@pytest.mark.describe("should obey timeout requirements for databases")
def test_db_timeout_sync(db: AstraDB) -> None:
    db.get_collections()
    db.get_collections(timeout_info=10)
    with pytest.raises(httpx.TimeoutException):
        db.get_collections(timeout_info=0.0001)
    with pytest.raises(httpx.TimeoutException):
        db.get_collections(timeout_info={"read": 0.0001})


@pytest.mark.describe("should obey timeout requirements for databases (async)")
async def test_db_timeout_async(async_db: AsyncAstraDB) -> None:
    await async_db.get_collections()
    await async_db.get_collections(timeout_info=10)
    with pytest.raises(httpx.TimeoutException):
        await async_db.get_collections(timeout_info=0.0001)
    with pytest.raises(httpx.TimeoutException):
        await async_db.get_collections(timeout_info={"read": 0.0001})


@pytest.mark.describe("should obey timeout requirements for collection reads")
def test_collection_read_timeout_sync(
    pagination_v_collection: AstraDBCollection,
) -> None:
    pagination_v_collection.vector_find([0.1, -0.1], limit=200)
    pagination_v_collection.vector_find([0.1, -0.1], limit=200, timeout_info=10)
    with pytest.raises(httpx.TimeoutException):
        pagination_v_collection.vector_find([0.1, -0.1], limit=200, timeout_info=0.0001)
    with pytest.raises(httpx.TimeoutException):
        pagination_v_collection.vector_find(
            [0.1, -0.1], limit=200, timeout_info={"read": 0.0001}
        )


@pytest.mark.describe("should obey timeout requirements for collection reads (async)")
async def test_collection_read_timeout_async(
    async_pagination_v_collection: AsyncAstraDBCollection,
) -> None:
    await async_pagination_v_collection.vector_find([0.1, -0.1], limit=200)
    await async_pagination_v_collection.vector_find(
        [0.1, -0.1], limit=200, timeout_info=10
    )
    with pytest.raises(httpx.TimeoutException):
        await async_pagination_v_collection.vector_find(
            [0.1, -0.1], limit=200, timeout_info=0.0001
        )
    with pytest.raises(httpx.TimeoutException):
        await async_pagination_v_collection.vector_find(
            [0.1, -0.1], limit=200, timeout_info={"read": 0.0001}
        )


@pytest.mark.describe("should obey timeout requirements for collection writes")
def test_collection_write_timeout_sync(
    writable_v_collection: AstraDBCollection,
) -> None:
    documents = [{"a": ["a" * 10] * 1000}] * 20

    writable_v_collection.insert_many(documents)
    writable_v_collection.insert_many(documents, timeout_info=10)
    with pytest.raises(httpx.TimeoutException):
        writable_v_collection.insert_many(documents, timeout_info=0.0001)
    with pytest.raises(httpx.TimeoutException):
        writable_v_collection.insert_many(documents, timeout_info={"write": 0.0001})


@pytest.mark.describe("should obey timeout requirements for collection writes (async)")
async def test_collection_write_timeout_async(
    async_writable_v_collection: AsyncAstraDBCollection,
) -> None:
    documents = [{"a": ["a" * 10] * 1000}] * 20

    await async_writable_v_collection.insert_many(documents)
    await async_writable_v_collection.insert_many(documents, timeout_info=10)
    with pytest.raises(httpx.TimeoutException):
        await async_writable_v_collection.insert_many(documents, timeout_info=0.000001)
    with pytest.raises(httpx.TimeoutException):
        await async_writable_v_collection.insert_many(
            documents, timeout_info={"write": 0.000001}
        )
