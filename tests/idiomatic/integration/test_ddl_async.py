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

from __future__ import annotations

import time

import pytest

from astrapy import AsyncCollection, AsyncDatabase, DataAPIClient
from astrapy.constants import DefaultIdType, VectorMetric
from astrapy.ids import UUID, ObjectId
from astrapy.info import CollectionDescriptor, DatabaseInfo

from ..conftest import (
    IS_ASTRA_DB,
    SECONDARY_NAMESPACE,
    TEST_COLLECTION_NAME,
    DataAPICredentials,
    DataAPICredentialsInfo,
)


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
            metric=VectorMetric.EUCLIDEAN,
            indexing={"deny": ["a", "b", "c"]},
        )
        await async_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME_B,
            indexing={"allow": ["z"]},
        )
        lc_response = [col async for col in async_database.list_collections()]
        #
        expected_coll_descriptor = CollectionDescriptor.from_dict(
            {
                "name": TEST_LOCAL_COLLECTION_NAME,
                "options": {
                    "vector": {
                        "dimension": 123,
                        "metric": "euclidean",
                    },
                    "indexing": {"deny": ["a", "b", "c"]},
                },
            },
        )
        expected_coll_descriptor_b = CollectionDescriptor.from_dict(
            {
                "name": TEST_LOCAL_COLLECTION_NAME_B,
                "options": {
                    "indexing": {"allow": ["z"]},
                },
            },
        )
        assert expected_coll_descriptor in lc_response
        assert expected_coll_descriptor_b in lc_response
        #
        col2 = await async_database.get_collection(TEST_LOCAL_COLLECTION_NAME)
        assert col1 == col2
        dc_response = await async_database.drop_collection(TEST_LOCAL_COLLECTION_NAME)
        assert dc_response == {"ok": 1}
        dc_response2 = await async_database.drop_collection(TEST_LOCAL_COLLECTION_NAME)
        assert dc_response2 == {"ok": 1}
        await async_database.drop_collection(TEST_LOCAL_COLLECTION_NAME_B)

    @pytest.mark.describe("test of default_id_type in creating collections, async")
    async def test_collection_default_id_type_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        ID_TEST_COLLECTION_NAME_ROOT = "id_type_test_"

        acol = await async_database.create_collection(
            ID_TEST_COLLECTION_NAME_ROOT + DefaultIdType.UUID,
            default_id_type=DefaultIdType.UUID,
        )
        assert (await acol.options()).default_id.default_id_type == DefaultIdType.UUID
        i1res = await acol.insert_one({"role": "probe"})
        assert isinstance(i1res.inserted_id, UUID)
        doc = await acol.find_one({})
        assert isinstance(doc["_id"], UUID)
        await acol.drop()

        time.sleep(2)
        acol = await async_database.create_collection(
            ID_TEST_COLLECTION_NAME_ROOT + DefaultIdType.UUIDV6,
            default_id_type=DefaultIdType.UUIDV6,
        )
        assert (await acol.options()).default_id.default_id_type == DefaultIdType.UUIDV6
        i1res = await acol.insert_one({"role": "probe"})
        assert isinstance(i1res.inserted_id, UUID)
        assert i1res.inserted_id.version == 6
        doc = await acol.find_one({})
        assert isinstance(doc["_id"], UUID)
        assert doc["_id"].version == 6
        await acol.drop()

        time.sleep(2)
        acol = await async_database.create_collection(
            ID_TEST_COLLECTION_NAME_ROOT + DefaultIdType.UUIDV7,
            default_id_type=DefaultIdType.UUIDV7,
        )
        assert (await acol.options()).default_id.default_id_type == DefaultIdType.UUIDV7
        i1res = await acol.insert_one({"role": "probe"})
        assert isinstance(i1res.inserted_id, UUID)
        assert i1res.inserted_id.version == 7
        doc = await acol.find_one({})
        assert isinstance(doc["_id"], UUID)
        assert doc["_id"].version == 7
        await acol.drop()

        time.sleep(2)
        acol = await async_database.create_collection(
            ID_TEST_COLLECTION_NAME_ROOT + DefaultIdType.DEFAULT,
            default_id_type=DefaultIdType.DEFAULT,
        )
        assert (
            await acol.options()
        ).default_id.default_id_type == DefaultIdType.DEFAULT
        await acol.drop()

        time.sleep(2)
        acol = await async_database.create_collection(
            ID_TEST_COLLECTION_NAME_ROOT + DefaultIdType.OBJECTID,
            default_id_type=DefaultIdType.OBJECTID,
        )
        assert (
            await acol.options()
        ).default_id.default_id_type == DefaultIdType.OBJECTID
        i1res = await acol.insert_one({"role": "probe"})
        assert isinstance(i1res.inserted_id, ObjectId)
        doc = await acol.find_one({})
        assert isinstance(doc["_id"], ObjectId)
        await acol.drop()

    @pytest.mark.describe("test of collection drop, async")
    async def test_collection_drop_async(self, async_database: AsyncDatabase) -> None:
        col = await async_database.create_collection(
            name="async_collection_to_drop", dimension=2
        )
        del_res = await col.drop()
        assert del_res["ok"] == 1
        assert "async_collection_to_drop" not in (
            await async_database.list_collection_names()
        )

    @pytest.mark.skipif(not IS_ASTRA_DB, reason="Not supported outside of Astra DB")
    @pytest.mark.describe("test of database metainformation, async")
    async def test_get_database_info_async(
        self,
        async_database: AsyncDatabase,
        data_api_credentials_kwargs: DataAPICredentials,
    ) -> None:
        assert isinstance(async_database.id, str)
        assert isinstance(async_database.name(), str)
        assert async_database.namespace == data_api_credentials_kwargs["namespace"]
        assert isinstance(async_database.info(), DatabaseInfo)
        assert isinstance(async_database.info().raw_info, dict)

    @pytest.mark.skipif(not IS_ASTRA_DB, reason="Not supported outside of Astra DB")
    @pytest.mark.describe("test of collection metainformation, async")
    async def test_get_collection_info_async(
        self,
        async_collection: AsyncCollection,
    ) -> None:
        info = async_collection.info()
        assert info.namespace == async_collection.namespace
        assert (
            info.namespace == async_collection._astra_db_collection.astra_db.namespace
        )

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
        assert options.vector is not None
        assert options.vector.dimension == 2

    @pytest.mark.skipif(
        SECONDARY_NAMESPACE is None, reason="No secondary namespace provided"
    )
    @pytest.mark.describe(
        "test of Database list_collections on cross-namespaces, async"
    )
    async def test_database_list_collections_cross_namespace_async(
        self,
        async_database: AsyncDatabase,
        async_collection: AsyncCollection,
        data_api_credentials_info: DataAPICredentialsInfo,
    ) -> None:
        assert TEST_COLLECTION_NAME not in await async_database.list_collection_names(
            namespace=data_api_credentials_info["secondary_namespace"]
        )

    @pytest.mark.skipif(
        SECONDARY_NAMESPACE is None, reason="No secondary namespace provided"
    )
    @pytest.mark.describe("test of cross-namespace collection lifecycle, async")
    async def test_collection_namespace_async(
        self,
        async_database: AsyncDatabase,
        client: DataAPIClient,
        data_api_credentials_kwargs: DataAPICredentials,
        data_api_credentials_info: DataAPICredentialsInfo,
    ) -> None:
        TEST_LOCAL_COLLECTION_NAME1 = "test_crossns_coll1"
        TEST_LOCAL_COLLECTION_NAME2 = "test_crossns_coll2"
        database_on_secondary = client.get_async_database(
            data_api_credentials_kwargs["api_endpoint"],
            token=data_api_credentials_kwargs["token"],
            namespace=data_api_credentials_info["secondary_namespace"],
        )
        await async_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME1,
            namespace=data_api_credentials_info["secondary_namespace"],
        )
        col2_on_secondary = await async_database.create_collection(
            TEST_LOCAL_COLLECTION_NAME2,
            namespace=data_api_credentials_info["secondary_namespace"],
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

    @pytest.mark.describe("test of database command targeting collection, async")
    async def test_database_command_on_collection_async(
        self,
        async_database: AsyncDatabase,
        async_collection: AsyncCollection,
    ) -> None:
        cmd1 = await async_database.command(
            {"countDocuments": {}}, collection_name=async_collection.name
        )
        assert isinstance(cmd1, dict)
        assert isinstance(cmd1["status"]["count"], int)
        cmd2 = await async_database._copy(namespace="...").command(
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
        cmd2 = await async_database._copy(namespace="...").command(
            {"findCollections": {}}, namespace=async_database.namespace
        )
        assert cmd2 == cmd1

    @pytest.mark.describe("test of database command, async")
    async def test_collection_command_async(
        self,
        async_collection: AsyncCollection,
    ) -> None:
        cmd1 = await async_collection.command({"countDocuments": {}})
        assert isinstance(cmd1, dict)
        assert isinstance(cmd1["status"]["count"], int)

    @pytest.mark.describe("test of tokenless client creation, async")
    async def test_tokenless_client_async(
        self,
        data_api_credentials_kwargs: DataAPICredentials,
        data_api_credentials_info: DataAPICredentialsInfo,
    ) -> None:
        api_endpoint = data_api_credentials_kwargs["api_endpoint"]
        token = data_api_credentials_kwargs["token"]
        client = DataAPIClient(environment=data_api_credentials_info["environment"])
        a_database = client.get_async_database(api_endpoint, token=token)
        coll_names = await a_database.list_collection_names()
        assert isinstance(coll_names, list)
