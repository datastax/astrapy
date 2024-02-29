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

import pytest

from ..conftest import (
    AstraDBCredentials,
    ASTRA_DB_SECONDARY_KEYSPACE,
    TEST_COLLECTION_NAME,
)
from astrapy import AsyncCollection, AsyncDatabase


class TestDDLAsync:
    @pytest.mark.describe("test of collection creation, get, and then drop, async")
    async def test_collection_lifecycle_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        TEST_LOCAL_COLLECTION_NAME = "test_local_coll"
        col1 = await async_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME,
            dimension=123,
            metric="euclidean",
            indexing={"deny": ["a", "b", "c"]},
        )
        col2 = await async_database.get_collection(TEST_LOCAL_COLLECTION_NAME)
        assert col1 == col2
        await async_database.drop_collection(TEST_LOCAL_COLLECTION_NAME)

    @pytest.mark.describe("test of Database list_collections, async")
    async def test_database_list_collections_async(
        self,
        async_database: AsyncDatabase,
        async_collection: AsyncCollection,
    ) -> None:
        assert TEST_COLLECTION_NAME in await async_database.list_collection_names()

    @pytest.mark.describe("test of Database list_collections unsupported filter, async")
    async def test_database_list_collections_filter_async(
        self,
        async_database: AsyncDatabase,
        async_collection: AsyncCollection,
    ) -> None:
        with pytest.raises(TypeError):
            await async_database.list_collection_names(filter={"k": "v"})

    @pytest.mark.skipif(
        ASTRA_DB_SECONDARY_KEYSPACE is None, reason="No secondary keyspace provided"
    )
    @pytest.mark.describe(
        "test of Database list_collections on cross-namespaces, async"
    )
    async def test_database_list_collections_cross_namespace_async(
        self,
        async_database: AsyncDatabase,
        async_collection: AsyncCollection,
    ) -> None:
        assert TEST_COLLECTION_NAME not in await async_database.list_collection_names(
            namespace=ASTRA_DB_SECONDARY_KEYSPACE
        )

    @pytest.mark.skipif(
        ASTRA_DB_SECONDARY_KEYSPACE is None, reason="No secondary keyspace provided"
    )
    @pytest.mark.describe("test of cross-namespace collection lifecycle, async")
    async def test_collection_namespace_async(
        self,
        async_database: AsyncDatabase,
        astra_db_credentials_kwargs: AstraDBCredentials,
    ) -> None:
        TEST_LOCAL_COLLECTION_NAME1 = "test_crossns_coll1"
        TEST_LOCAL_COLLECTION_NAME2 = "test_crossns_coll2"
        database_on_secondary = AsyncDatabase(
            astra_db_credentials_kwargs["api_endpoint"],
            astra_db_credentials_kwargs["token"],
            namespace=ASTRA_DB_SECONDARY_KEYSPACE,
        )
        await async_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME1,
            namespace=ASTRA_DB_SECONDARY_KEYSPACE,
        )
        col2_on_secondary = await async_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME2,
            namespace=ASTRA_DB_SECONDARY_KEYSPACE,
        )
        assert (
            TEST_LOCAL_COLLECTION_NAME1
            in await database_on_secondary.list_collection_names()
        )
        await database_on_secondary.drop_collection(TEST_LOCAL_COLLECTION_NAME1)
        await async_database.drop_collection(col2_on_secondary)
        assert (
            TEST_LOCAL_COLLECTION_NAME1
            not in await database_on_secondary.list_collection_names()
        )
        assert (
            TEST_LOCAL_COLLECTION_NAME2
            not in await database_on_secondary.list_collection_names()
        )
