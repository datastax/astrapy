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

from astrapy import AsyncCollection, AsyncDatabase

TEST_CREATE_DELETE_VECTOR_COLLECTION_NAME = "ephemeral_v_col"
TEST_CREATE_DELETE_NONVECTOR_COLLECTION_NAME = "ephemeral_non_v_col"

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

    @pytest.mark.describe("should create and destroy a vector collection using collection drop (async)")
    async def test_create_destroy_collection(self, async_database: AsyncDatabase) -> None:
        col = await async_database.create_collection(
            name=TEST_CREATE_DELETE_VECTOR_COLLECTION_NAME, dimension=2
        )
        assert isinstance(col, AsyncCollection)
        del_res = await col.drop(
            TEST_CREATE_DELETE_VECTOR_COLLECTION_NAME
        )
        assert del_res["status"]["ok"] == 1

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
