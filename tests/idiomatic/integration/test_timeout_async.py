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

import asyncio
from typing import TYPE_CHECKING

import pytest

from astrapy import AsyncDatabase
from astrapy.admin.admin import async_fetch_database_info
from astrapy.exceptions import DataAPITimeoutException, DevOpsAPITimeoutException

from ..conftest import IS_ASTRA_DB

if TYPE_CHECKING:
    from ..conftest import DefaultAsyncCollection


class TestTimeoutAsync:
    @pytest.mark.describe("test of collection count_documents timeout, async")
    async def test_collection_count_documents_timeout_async(
        self,
        async_empty_collection: DefaultAsyncCollection,
    ) -> None:
        await async_empty_collection.insert_many([{"a": 1}] * 500)
        await asyncio.sleep(2)

        with pytest.raises(DataAPITimeoutException) as exc:
            await async_empty_collection.count_documents(
                {}, upper_bound=800, timeout_ms=1
            )
        assert await async_empty_collection.count_documents({}, upper_bound=800) >= 500
        assert exc.value.timeout_type in {"connect", "read"}
        assert exc.value.endpoint is not None
        assert exc.value.raw_payload is not None

    @pytest.mark.skipif(not IS_ASTRA_DB, reason="Not supported outside of Astra DB")
    @pytest.mark.describe("test of database info timeout, async")
    async def test_database_info_timeout_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        info = await async_fetch_database_info(
            async_database.api_endpoint,
            token=async_database.api_options.token,
            keyspace=async_database.keyspace,
        )
        assert info is not None

        with pytest.raises(DevOpsAPITimeoutException) as exc:
            info = await async_fetch_database_info(
                async_database.api_endpoint,
                token=async_database.api_options.token,
                keyspace=async_database.keyspace,
                timeout_ms=1,
            )
            assert info is not None
        assert exc.value.timeout_type in {"connect", "read"}
        assert exc.value.endpoint is not None
        assert exc.value.raw_payload is not None

    @pytest.mark.describe("test of cursor-based overall timeouts, async")
    async def test_cursor_overalltimeout_exceptions_async(
        self,
        async_empty_collection: DefaultAsyncCollection,
    ) -> None:
        acol = async_empty_collection
        await acol.insert_many([{"a": 1}] * 1000)

        await acol.distinct("a", timeout_ms=20000)
        with pytest.raises(DataAPITimeoutException):
            await acol.distinct("a", timeout_ms=1)

        await acol.distinct("a", timeout_ms=20000)
        with pytest.raises(DataAPITimeoutException):
            await acol.distinct("a", timeout_ms=1)

    @pytest.mark.describe("test of insert_many timeouts, async")
    async def test_insert_many_timeout_exceptions_async(
        self,
        async_collection: DefaultAsyncCollection,
    ) -> None:
        fifty_docs = [{"seq": i} for i in range(50)]
        await async_collection.insert_many(fifty_docs, ordered=True, timeout_ms=20000)
        await async_collection.insert_many(
            fifty_docs, ordered=False, concurrency=1, timeout_ms=20000
        )
        await async_collection.insert_many(
            fifty_docs, ordered=False, concurrency=2, timeout_ms=20000
        )

        with pytest.raises(DataAPITimeoutException):
            await async_collection.insert_many(fifty_docs, ordered=True, timeout_ms=2)
        with pytest.raises(DataAPITimeoutException):
            await async_collection.insert_many(
                fifty_docs, ordered=False, concurrency=1, timeout_ms=2
            )
        with pytest.raises(DataAPITimeoutException):
            await async_collection.insert_many(
                fifty_docs, ordered=False, concurrency=2, timeout_ms=2
            )

    @pytest.mark.describe("test of update_many timeouts, async")
    async def test_update_many_timeout_exceptions_async(
        self,
        async_collection: DefaultAsyncCollection,
    ) -> None:
        fifty_docs = [{"seq": i, "f": "update_many"} for i in range(50)]
        await async_collection.insert_many(fifty_docs, ordered=False, concurrency=3)

        await async_collection.update_many({"f": "update_many"}, {"$inc": {"seq": 100}})
        await async_collection.update_many(
            {"f": "update_many"}, {"$inc": {"seq": 100}}, timeout_ms=20000
        )

        with pytest.raises(DataAPITimeoutException):
            await async_collection.update_many(
                {"f": "update_many"}, {"$inc": {"seq": 100}}, timeout_ms=2
            )

    @pytest.mark.describe("test of delete_many timeouts, async")
    async def test_delete_many_timeout_exceptions_async(
        self,
        async_collection: DefaultAsyncCollection,
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
        await async_collection.delete_many({"f": "delete_many2"}, timeout_ms=20000)
        with pytest.raises(DataAPITimeoutException):
            await async_collection.delete_many({"f": "delete_many3"}, timeout_ms=2)

    @pytest.mark.describe("test of collection find-with-collective timeout, async")
    async def test_collection_find_with_collective_timeout_async(
        self,
        async_empty_collection: DefaultAsyncCollection,
    ) -> None:
        await async_empty_collection.insert_many([{"a": 1}] * 55)
        await asyncio.sleep(1)

        with pytest.raises(DataAPITimeoutException):
            await async_empty_collection.distinct("a", general_method_timeout_ms=1)

        cur_tl = async_empty_collection.find()
        with pytest.raises(DataAPITimeoutException):
            await cur_tl.to_list(general_method_timeout_ms=1)

        cur_fe = async_empty_collection.find()
        with pytest.raises(DataAPITimeoutException):
            await cur_fe.for_each(lambda doc: None, general_method_timeout_ms=1)
