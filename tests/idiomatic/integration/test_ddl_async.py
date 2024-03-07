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
from astrapy.api import APIRequestError
from astrapy import AsyncCollection, AsyncDatabase


class TestDDLAsync:
    @pytest.mark.describe("test of collection creation, get, and then drop, async")
    async def test_collection_lifecycle_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        TEST_LOCAL_COLLECTION_NAME = "test_local_coll"
        TEST_LOCAL_COLLECTION_NAME_B = "test_local_coll_b"
        col1 = await async_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME,
            dimension=123,
            metric="euclidean",
            indexing={"deny": ["a", "b", "c"]},
        )
        await async_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME_B,
            indexing={"allow": ["z"]},
        )
        lc_response = [col async for col in async_database.list_collections()]
        #
        expected_coll_dict = {
            "name": TEST_LOCAL_COLLECTION_NAME,
            "dimension": 123,
            "metric": "euclidean",
            "indexing": {"deny": ["a", "b", "c"]},
        }
        expected_coll_dict_b = {
            "name": TEST_LOCAL_COLLECTION_NAME_B,
            "indexing": {"allow": ["z"]},
        }
        assert expected_coll_dict in lc_response
        assert expected_coll_dict_b in lc_response
        #
        col2 = await async_database.get_collection(TEST_LOCAL_COLLECTION_NAME)
        assert col1 == col2
        dc_response = await async_database.drop_collection(TEST_LOCAL_COLLECTION_NAME)
        assert dc_response == {"ok": 1}
        dc_response2 = await async_database.drop_collection(TEST_LOCAL_COLLECTION_NAME)
        assert dc_response2 == {"ok": 1}
        await async_database.drop_collection(TEST_LOCAL_COLLECTION_NAME_B)

    @pytest.mark.describe("test of check_exists for create_collection, async")
    async def test_create_collection_check_exists_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        TEST_LOCAL_COLLECTION_NAME = "test_check_exists"
        await async_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME,
            dimension=3,
        )

        with pytest.raises(ValueError):
            await async_database.create_collection(
                TEST_LOCAL_COLLECTION_NAME,
                dimension=3,
            )
        with pytest.raises(ValueError):
            await async_database.create_collection(
                TEST_LOCAL_COLLECTION_NAME,
                indexing={"deny": ["a"]},
            )
        await async_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME,
            dimension=3,
            check_exists=False,
        )
        with pytest.raises(APIRequestError):
            await async_database.create_collection(
                TEST_LOCAL_COLLECTION_NAME,
                indexing={"deny": ["a"]},
                check_exists=False,
            )

        await async_database.drop_collection(TEST_LOCAL_COLLECTION_NAME)

    @pytest.mark.describe("test of Database list_collections, async")
    async def test_database_list_collections_async(
        self,
        async_database: AsyncDatabase,
        async_collection: AsyncCollection,
    ) -> None:
        assert TEST_COLLECTION_NAME in await async_database.list_collection_names()

    @pytest.mark.describe("test of Collection options, async")
    async def test_collection_options_async(
        self,
        async_collection: AsyncCollection,
    ) -> None:
        options = await async_collection.options()
        assert options["name"] == async_collection.name

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

    @pytest.mark.describe("test of collection command, async")
    async def test_collection_command_async(
        self,
        async_database: AsyncDatabase,
        async_collection: AsyncCollection,
    ) -> None:
        cmd1 = await async_database.command(
            {"countDocuments": {}}, collection_name=async_collection.name
        )
        assert isinstance(cmd1, dict)
        assert isinstance(cmd1["status"]["count"], int)
        cmd2 = await async_database.copy(namespace="...").command(
            {"countDocuments": {}},
            namespace=async_collection.namespace,
            collection_name=async_collection.name,
        )
        assert cmd2 == cmd1

    @pytest.mark.describe("test of database command, async")
    async def test_database_command_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        cmd1 = await async_database.command({"findCollections": {}})
        assert isinstance(cmd1, dict)
        assert isinstance(cmd1["status"]["collections"], list)
        cmd2 = await async_database.copy(namespace="...").command(
            {"findCollections": {}}, namespace=async_database.namespace
        )
        assert cmd2 == cmd1
