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

from astrapy.exceptions import DataAPITimeoutException
from astrapy.info import get_database_info


class TestTimeoutAsync:
    @pytest.mark.describe("test of collection count_documents timeout, async")
    async def test_collection_count_documents_timeout_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        await async_empty_collection.insert_many([{"a": 1}] * 100)
        assert await async_empty_collection.count_documents({}, upper_bound=150) == 100

        with pytest.raises(DataAPITimeoutException) as exc:
            await async_empty_collection.count_documents(
                {}, upper_bound=150, max_time_ms=1
            )
        assert exc.value.timeout_type == "read"
        assert exc.value.endpoint is not None
        assert exc.value.raw_payload is not None

    @pytest.mark.describe("test of database info timeout, async")
    async def test_database_info_timeout_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        get_database_info(
            async_database._astra_db.api_endpoint,
            token=async_database._astra_db.token,
            namespace=async_database.namespace,
        )

        with pytest.raises(DataAPITimeoutException) as exc:
            get_database_info(
                async_database._astra_db.api_endpoint,
                token=async_database._astra_db.token,
                namespace=async_database.namespace,
                max_time_ms=1,
            )
        assert exc.value.timeout_type == "read"
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

        await acol.distinct("a", max_time_ms=5000)
        with pytest.raises(DataAPITimeoutException):
            await acol.distinct("a", max_time_ms=1)

        cur1 = acol.find({})
        cur2 = acol.find({})
        await cur1.distinct("a", max_time_ms=5000)
        with pytest.raises(DataAPITimeoutException):
            await cur2.distinct("a", max_time_ms=1)
