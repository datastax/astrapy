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

    @pytest.mark.describe("test errors for unsupported Collection methods, async")
    async def test_collection_unsupported_methods_async(
        self,
        async_collection: AsyncCollection,
    ) -> None:
        with pytest.raises(TypeError):
            await async_collection.find_raw_batches(1, "x")
        with pytest.raises(TypeError):
            await async_collection.aggregate(1, "x")
        with pytest.raises(TypeError):
            await async_collection.aggregate_raw_batches(1, "x")
        with pytest.raises(TypeError):
            await async_collection.watch(1, "x")
        with pytest.raises(TypeError):
            await async_collection.rename(1, "x")
        with pytest.raises(TypeError):
            await async_collection.create_index(1, "x")
        with pytest.raises(TypeError):
            await async_collection.create_indexes(1, "x")
        with pytest.raises(TypeError):
            await async_collection.drop_index(1, "x")
        with pytest.raises(TypeError):
            await async_collection.drop_indexes(1, "x")
        with pytest.raises(TypeError):
            await async_collection.list_indexes(1, "x")
        with pytest.raises(TypeError):
            await async_collection.index_information(1, "x")
        with pytest.raises(TypeError):
            await async_collection.create_search_index(1, "x")
        with pytest.raises(TypeError):
            await async_collection.create_search_indexes(1, "x")
        with pytest.raises(TypeError):
            await async_collection.drop_search_index(1, "x")
        with pytest.raises(TypeError):
            await async_collection.list_search_indexes(1, "x")
        with pytest.raises(TypeError):
            await async_collection.update_search_index(1, "x")
        with pytest.raises(TypeError):
            await async_collection.distinct(1, "x")
