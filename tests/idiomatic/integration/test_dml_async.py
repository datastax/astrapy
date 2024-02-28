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

from astrapy import AsyncCollection
from astrapy.results import DeleteResult, InsertOneResult


class TestDMLAsync:
    @pytest.mark.describe("test of collection count_documents, async")
    async def test_collection_count_documents_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        assert await async_empty_collection.count_documents(filter={}) == 0
        await async_empty_collection.insert_one({"doc": 1, "group": "A"})
        await async_empty_collection.insert_one({"doc": 2, "group": "B"})
        await async_empty_collection.insert_one({"doc": 3, "group": "A"})
        assert await async_empty_collection.count_documents(filter={}) == 3
        assert await async_empty_collection.count_documents(filter={"group": "A"}) == 2

    @pytest.mark.describe("test of collection insert_one, async")
    async def test_collection_insert_one_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        io_result1 = await async_empty_collection.insert_one({"doc": 1, "group": "A"})
        assert isinstance(io_result1, InsertOneResult)
        assert io_result1.acknowledged is True
        io_result2 = await async_empty_collection.insert_one(
            {"_id": "xxx", "doc": 2, "group": "B"}
        )
        assert io_result2.inserted_id == "xxx"
        assert await async_empty_collection.count_documents(filter={"group": "A"}) == 1

    @pytest.mark.describe("test of collection delete_one, async")
    async def test_collection_delete_one_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        await async_empty_collection.insert_one({"doc": 1, "group": "A"})
        await async_empty_collection.insert_one({"doc": 2, "group": "B"})
        await async_empty_collection.insert_one({"doc": 3, "group": "A"})
        assert await async_empty_collection.count_documents(filter={}) == 3
        do_result1 = await async_empty_collection.delete_one({"group": "A"})
        assert isinstance(do_result1, DeleteResult)
        assert do_result1.acknowledged is True
        assert do_result1.deleted_count == 1
        assert await async_empty_collection.count_documents(filter={}) == 2

    @pytest.mark.describe("test of collection delete_many, async")
    async def test_collection_delete_many_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        await async_empty_collection.insert_one({"doc": 1, "group": "A"})
        await async_empty_collection.insert_one({"doc": 2, "group": "B"})
        await async_empty_collection.insert_one({"doc": 3, "group": "A"})
        assert await async_empty_collection.count_documents(filter={}) == 3
        do_result1 = await async_empty_collection.delete_many({"group": "A"})
        assert isinstance(do_result1, DeleteResult)
        assert do_result1.acknowledged is True
        assert do_result1.deleted_count == 2
        assert await async_empty_collection.count_documents(filter={}) == 1
