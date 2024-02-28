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

from ..conftest import AstraDBCredentials, TEST_COLLECTION_INSTANCE_NAME
from astrapy import AsyncCollection, AsyncDatabase


class TestDatabasesAsync:
    @pytest.mark.describe("test of instantiating Database, async")
    async def test_instantiate_database_async(
        self,
        astra_db_credentials_kwargs: AstraDBCredentials,
    ) -> None:
        db1 = AsyncDatabase(
            caller_name="c_n",
            caller_version="c_v",
            **astra_db_credentials_kwargs,
        )
        db2 = AsyncDatabase(
            caller_name="c_n",
            caller_version="c_v",
            **astra_db_credentials_kwargs,
        )
        assert db1 == db2

    @pytest.mark.describe("test of Database conversions, async")
    async def test_convert_database_async(
        self,
        astra_db_credentials_kwargs: AstraDBCredentials,
    ) -> None:
        db1 = AsyncDatabase(
            caller_name="c_n",
            caller_version="c_v",
            **astra_db_credentials_kwargs,
        )
        assert db1 == db1.copy()
        assert db1 == db1.to_sync().to_async()

    @pytest.mark.describe("test of Database set_caller, async")
    async def test_database_set_caller_async(
        self,
        astra_db_credentials_kwargs: AstraDBCredentials,
    ) -> None:
        db1 = AsyncDatabase(
            caller_name="c_n1",
            caller_version="c_v1",
            **astra_db_credentials_kwargs,
        )
        db2 = AsyncDatabase(
            caller_name="c_n2",
            caller_version="c_v2",
            **astra_db_credentials_kwargs,
        )
        db2.set_caller(
            caller_name="c_n1",
            caller_version="c_v1",
        )
        assert db1 == db2

    @pytest.mark.describe("test errors for unsupported Database methods, async")
    async def test_database_unsupported_methods_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        with pytest.raises(TypeError):
            await async_database.aggregate(1, "x")
        with pytest.raises(TypeError):
            await async_database.cursor_command(1, "x")
        with pytest.raises(TypeError):
            await async_database.dereference(1, "x")
        with pytest.raises(TypeError):
            await async_database.watch(1, "x")
        with pytest.raises(TypeError):
            await async_database.validate_collection(1, "x")

    @pytest.mark.describe("test get_collection method, async")
    async def test_database_get_collection_async(
        self,
        async_database: AsyncDatabase,
        async_collection_instance: AsyncCollection,
    ) -> None:
        collection = await async_database.get_collection(TEST_COLLECTION_INSTANCE_NAME)
        assert collection == async_collection_instance

        NAMESPACE_2 = "other_namespace"
        collection_ns2 = await async_database.get_collection(
            TEST_COLLECTION_INSTANCE_NAME, namespace=NAMESPACE_2
        )
        assert collection_ns2 == AsyncCollection(
            async_database, TEST_COLLECTION_INSTANCE_NAME, namespace=NAMESPACE_2
        )
        assert collection_ns2._astra_db_collection.astra_db.namespace == NAMESPACE_2
