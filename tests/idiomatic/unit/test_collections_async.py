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

from ..conftest import ASTRA_DB_SECONDARY_KEYSPACE
from astrapy import AsyncCollection, AsyncDatabase


class TestCollectionsAsync:
    @pytest.mark.describe("test of instantiating Collection, async")
    async def test_instantiate_collection_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        col1 = AsyncCollection(
            async_database,
            "id_test_collection",
            caller_name="c_n",
            caller_version="c_v",
        )
        col2 = AsyncCollection(
            async_database,
            "id_test_collection",
            caller_name="c_n",
            caller_version="c_v",
        )
        assert col1 == col2

    @pytest.mark.describe("test of Collection conversions, async")
    async def test_convert_collection_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        col1 = AsyncCollection(
            async_database,
            "id_test_collection",
            caller_name="c_n",
            caller_version="c_v",
        )
        assert col1 == col1.copy()
        assert col1 == col1.to_sync().to_async()

    @pytest.mark.describe("test of Collection rich copy, async")
    async def test_rich_copy_collection_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        col1 = AsyncCollection(
            async_database,
            "id_test_collection",
            caller_name="c_n",
            caller_version="c_v",
        )
        assert col1 != col1.copy(database=async_database.copy(token="x_t"))
        assert col1 != col1.copy(name="o")
        assert col1 != col1.copy(namespace="o")
        assert col1 != col1.copy(caller_name="o")
        assert col1 != col1.copy(caller_version="o")

        col2 = col1.copy(
            database=async_database.copy(token="x_t"),
            name="other_name",
            namespace="other_namespace",
            caller_name="x_n",
            caller_version="x_v",
        )
        assert col2 != col1

        col2.set_caller(
            caller_name="c_n",
            caller_version="c_v",
        )
        col3 = col2.copy(
            database=async_database,
            name="id_test_collection",
            namespace=async_database.namespace,
        )
        assert col3 == col1

    @pytest.mark.describe("test of Collection rich conversions, async")
    async def test_rich_convert_collection_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        col1 = AsyncCollection(
            async_database,
            "id_test_collection",
            caller_name="c_n",
            caller_version="c_v",
        )
        assert (
            col1
            != col1.to_sync(
                database=async_database.copy(token="x_t").to_sync()
            ).to_async()
        )
        assert col1 != col1.to_sync(name="o").to_async()
        assert col1 != col1.to_sync(namespace="o").to_async()
        assert col1 != col1.to_sync(caller_name="o").to_async()
        assert col1 != col1.to_sync(caller_version="o").to_async()

        col2s = col1.to_sync(
            database=async_database.copy(token="x_t").to_sync(),
            name="other_name",
            namespace="other_namespace",
            caller_name="x_n",
            caller_version="x_v",
        )
        assert col2s.to_async() != col1

        col2s.set_caller(
            caller_name="c_n",
            caller_version="c_v",
        )
        col3 = col2s.to_async(
            database=async_database,
            name="id_test_collection",
            namespace=async_database.namespace,
        )
        assert col3 == col1

    @pytest.mark.describe("test of Collection database property, async")
    async def test_collection_database_property_async(
        self,
    ) -> None:
        db1 = AsyncDatabase("a", "t", namespace="ns1")
        db2 = AsyncDatabase("a", "t", namespace="ns2")
        col1 = AsyncCollection(db1, "coll")
        col2 = AsyncCollection(db1, "coll", namespace="ns2")
        assert col1.database == db1
        assert col2.database == db2

    @pytest.mark.describe("test of Collection set_caller, async")
    async def test_collection_set_caller_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        col1 = AsyncCollection(
            async_database,
            "id_test_collection",
            caller_name="c_n1",
            caller_version="c_v1",
        )
        col2 = AsyncCollection(
            async_database,
            "id_test_collection",
            caller_name="c_n2",
            caller_version="c_v2",
        )
        col2.set_caller(
            caller_name="c_n1",
            caller_version="c_v1",
        )
        assert col1 == col2

    @pytest.mark.describe("test errors for unsupported Collection methods, async")
    async def test_collection_unsupported_methods_async(
        self,
        async_collection_instance: AsyncCollection,
    ) -> None:
        with pytest.raises(TypeError):
            await async_collection_instance.find_raw_batches(1, "x")
        with pytest.raises(TypeError):
            await async_collection_instance.aggregate(1, "x")
        with pytest.raises(TypeError):
            await async_collection_instance.aggregate_raw_batches(1, "x")
        with pytest.raises(TypeError):
            await async_collection_instance.watch(1, "x")
        with pytest.raises(TypeError):
            await async_collection_instance.rename(1, "x")
        with pytest.raises(TypeError):
            await async_collection_instance.create_index(1, "x")
        with pytest.raises(TypeError):
            await async_collection_instance.create_indexes(1, "x")
        with pytest.raises(TypeError):
            await async_collection_instance.drop_index(1, "x")
        with pytest.raises(TypeError):
            await async_collection_instance.drop_indexes(1, "x")
        with pytest.raises(TypeError):
            await async_collection_instance.list_indexes(1, "x")
        with pytest.raises(TypeError):
            await async_collection_instance.index_information(1, "x")
        with pytest.raises(TypeError):
            await async_collection_instance.create_search_index(1, "x")
        with pytest.raises(TypeError):
            await async_collection_instance.create_search_indexes(1, "x")
        with pytest.raises(TypeError):
            await async_collection_instance.drop_search_index(1, "x")
        with pytest.raises(TypeError):
            await async_collection_instance.list_search_indexes(1, "x")
        with pytest.raises(TypeError):
            await async_collection_instance.update_search_index(1, "x")
        with pytest.raises(TypeError):
            await async_collection_instance.distinct(1, "x")

    @pytest.mark.describe("test collection conversions with caller mutableness, async")
    async def test_collection_conversions_caller_mutableness_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        col1 = AsyncCollection(
            async_database,
            "id_test_collection",
            caller_name="c_n1",
            caller_version="c_v1",
        )
        col1.set_caller(
            caller_name="c_n2",
            caller_version="c_v2",
        )
        col2 = AsyncCollection(
            async_database,
            "id_test_collection",
            caller_name="c_n2",
            caller_version="c_v2",
        )
        assert col1.copy() == col2
        assert col1.to_sync().to_async() == col2

    @pytest.mark.skipif(
        ASTRA_DB_SECONDARY_KEYSPACE is None, reason="No secondary keyspace provided"
    )
    @pytest.mark.describe("test collection namespace property, async")
    async def test_collection_namespace_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        col1 = await async_database.get_collection("id_test_collection")
        assert col1.namespace == async_database.namespace

        col2 = await async_database.get_collection(
            "id_test_collection",
            namespace=ASTRA_DB_SECONDARY_KEYSPACE,
        )
        assert col2.namespace == ASTRA_DB_SECONDARY_KEYSPACE

        col3 = AsyncCollection(async_database, "id_test_collection")
        assert col3.namespace == async_database.namespace

        col4 = AsyncCollection(
            async_database,
            "id_test_collection",
            namespace=ASTRA_DB_SECONDARY_KEYSPACE,
        )
        assert col4.namespace == ASTRA_DB_SECONDARY_KEYSPACE
