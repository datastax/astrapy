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

from ..conftest import ASTRA_DB_SECONDARY_KEYSPACE, TEST_COLLECTION_NAME
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
        ASTRA_DB_SECONDARY_KEYSPACE is None, reason="No secondary keyspace"
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
