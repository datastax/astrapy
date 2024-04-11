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

import asyncio
import pytest

from astrapy import AsyncCollection, AsyncDatabase

from astrapy.exceptions import DataAPITimeoutException
from astrapy.operations import AsyncDeleteMany, AsyncInsertMany
from astrapy.admin import fetch_database_info


class TestTimeoutAsync:
    @pytest.mark.describe("test of collection count_documents timeout, async")
    async def test_collection_count_documents_timeout_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        await async_empty_collection.insert_many([{"a": 1}] * 100)
        await asyncio.sleep(10)
        assert await async_empty_collection.count_documents({}, upper_bound=150) >= 100

        with pytest.raises(DataAPITimeoutException) as exc:
            await async_empty_collection.count_documents(
                {}, upper_bound=150, max_time_ms=1
            )
        assert exc.value.timeout_type in {"connect", "read"}
        assert exc.value.endpoint is not None
        assert exc.value.raw_payload is not None

    @pytest.mark.describe("test of database info timeout, async")
    async def test_database_info_timeout_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        fetch_database_info(
            async_database._astra_db.api_endpoint,
            token=async_database._astra_db.token,
            namespace=async_database.namespace,
        )

        with pytest.raises(DataAPITimeoutException) as exc:
            fetch_database_info(
                async_database._astra_db.api_endpoint,
                token=async_database._astra_db.token,
                namespace=async_database.namespace,
                max_time_ms=1,
            )
        assert exc.value.timeout_type in {"connect", "read"}
        assert exc.value.endpoint is not None
        assert exc.value.raw_payload is not None

    @pytest.mark.describe("test of cursor-based timeouts, async")
    async def test_cursor_timeouts_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        await async_empty_collection.insert_one({"a": 1})

        cur0 = async_empty_collection.find({})
        cur1 = async_empty_collection.find({}, max_time_ms=1)
        await cur0.__anext__()
        with pytest.raises(DataAPITimeoutException):
            await cur1.__anext__()

        await async_empty_collection.find_one({})
        with pytest.raises(DataAPITimeoutException):
            await async_empty_collection.find_one({}, max_time_ms=1)

    @pytest.mark.describe("test of cursor-based overall timeouts, async")
    async def test_cursor_overalltimeout_exceptions_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        acol = async_empty_collection
        await acol.insert_many([{"a": 1}] * 1000)

        await acol.distinct("a", max_time_ms=20000)
        with pytest.raises(DataAPITimeoutException):
            await acol.distinct("a", max_time_ms=1)

        cur1 = acol.find({})
        cur2 = acol.find({})
        await cur1.distinct("a", max_time_ms=20000)
        with pytest.raises(DataAPITimeoutException):
            await cur2.distinct("a", max_time_ms=1)

    @pytest.mark.describe("test of insert_many timeouts, async")
    async def test_insert_many_timeout_exceptions_async(
        self,
        async_collection: AsyncCollection,
    ) -> None:
        fifty_docs = [{"seq": i} for i in range(50)]
        await async_collection.insert_many(fifty_docs, ordered=True, max_time_ms=20000)
        await async_collection.insert_many(
            fifty_docs, ordered=False, concurrency=1, max_time_ms=20000
        )
        await async_collection.insert_many(
            fifty_docs, ordered=False, concurrency=2, max_time_ms=20000
        )

        with pytest.raises(DataAPITimeoutException):
            await async_collection.insert_many(fifty_docs, ordered=True, max_time_ms=2)
        with pytest.raises(DataAPITimeoutException):
            await async_collection.insert_many(
                fifty_docs, ordered=False, concurrency=1, max_time_ms=2
            )
        with pytest.raises(DataAPITimeoutException):
            await async_collection.insert_many(
                fifty_docs, ordered=False, concurrency=2, max_time_ms=2
            )

    @pytest.mark.describe("test of update_many timeouts, async")
    async def test_update_many_timeout_exceptions_async(
        self,
        async_collection: AsyncCollection,
    ) -> None:
        fifty_docs = [{"seq": i, "f": "update_many"} for i in range(50)]
        await async_collection.insert_many(fifty_docs, ordered=False, concurrency=3)

        await async_collection.update_many({"f": "update_many"}, {"$inc": {"seq": 100}})
        await async_collection.update_many(
            {"f": "update_many"}, {"$inc": {"seq": 100}}, max_time_ms=20000
        )

        with pytest.raises(DataAPITimeoutException):
            await async_collection.update_many(
                {"f": "update_many"}, {"$inc": {"seq": 100}}, max_time_ms=2
            )

    @pytest.mark.describe("test of delete_many timeouts, async")
    async def test_delete_many_timeout_exceptions_async(
        self,
        async_collection: AsyncCollection,
    ) -> None:
        fifty_docs1 = [{"seq": i, "f": "delete_many1"} for i in range(50)]
        fifty_docs2 = [{"seq": i, "f": "delete_many2"} for i in range(50)]
        fifty_docs3 = [{"seq": i, "f": "delete_many3"} for i in range(50)]
        await async_collection.insert_many(
            fifty_docs1 + fifty_docs2 + fifty_docs3,
            ordered=False,
            concurrency=5,
        )

        await async_collection.delete_many({"f": "delete_many1"})
        await async_collection.delete_many({"f": "delete_many2"}, max_time_ms=20000)
        with pytest.raises(DataAPITimeoutException):
            await async_collection.delete_many({"f": "delete_many3"}, max_time_ms=2)

    @pytest.mark.describe("test of bulk_write timeouts, async")
    async def test_bulk_write_ordered_timeout_exceptions_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        im_a = AsyncInsertMany([{"seq": i, "group": "A"} for i in range(100)])
        im_b = AsyncInsertMany([{"seq": i, "group": "B"} for i in range(100)])
        dm = AsyncDeleteMany(filter={"group": "A"})

        await async_empty_collection.bulk_write([im_a, im_b, dm], ordered=True)
        await async_empty_collection.bulk_write(
            [im_a, im_b, dm], ordered=True, max_time_ms=50000
        )
        with pytest.raises(DataAPITimeoutException):
            await async_empty_collection.bulk_write(
                [im_a, im_b, dm], ordered=True, max_time_ms=5
            )

    @pytest.mark.describe("test of bulk_write timeouts, async")
    async def test_bulk_write_unordered_timeout_exceptions_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        im_a = AsyncInsertMany([{"seq": i, "group": "A"} for i in range(100)])
        im_b = AsyncInsertMany([{"seq": i, "group": "B"} for i in range(100)])
        dm = AsyncDeleteMany(filter={"group": "A"})

        await async_empty_collection.bulk_write([im_a, im_b, dm], ordered=False)
        await async_empty_collection.bulk_write(
            [im_a, im_b, dm], ordered=False, max_time_ms=50000
        )
        with pytest.raises(DataAPITimeoutException):
            await async_empty_collection.bulk_write(
                [im_a, im_b, dm], ordered=False, max_time_ms=5
            )
