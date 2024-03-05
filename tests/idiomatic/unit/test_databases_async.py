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
    TEST_COLLECTION_INSTANCE_NAME,
)
from astrapy.defaults import DEFAULT_KEYSPACE_NAME
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

    @pytest.mark.describe("test of Database rich copy, async")
    async def test_rich_copy_database_async(
        self,
    ) -> None:
        db1 = AsyncDatabase(
            api_endpoint="api_endpoint",
            token="token",
            namespace="namespace",
            caller_name="c_n",
            caller_version="c_v",
            api_path="api_path",
            api_version="api_version",
        )
        assert db1 != db1.copy(api_endpoint="x")
        assert db1 != db1.copy(token="x")
        assert db1 != db1.copy(namespace="x")
        assert db1 != db1.copy(caller_name="x")
        assert db1 != db1.copy(caller_version="x")
        assert db1 != db1.copy(api_path="x")
        assert db1 != db1.copy(api_version="x")

        db2 = db1.copy(
            api_endpoint="x",
            token="x",
            namespace="x",
            caller_name="x_n",
            caller_version="x_v",
            api_path="x",
            api_version="x",
        )
        assert db2 != db1

        db2.set_caller(
            caller_name="c_n",
            caller_version="c_v",
        )
        db3 = db2.copy(
            api_endpoint="api_endpoint",
            token="token",
            namespace="namespace",
            api_path="api_path",
            api_version="api_version",
        )
        assert db3 == db1

    @pytest.mark.describe("test of Database rich conversions, async")
    async def test_rich_convert_database_async(
        self,
    ) -> None:
        db1 = AsyncDatabase(
            api_endpoint="api_endpoint",
            token="token",
            namespace="namespace",
            caller_name="c_n",
            caller_version="c_v",
            api_path="api_path",
            api_version="api_version",
        )
        assert db1 != db1.to_sync(api_endpoint="o").to_async()
        assert db1 != db1.to_sync(token="o").to_async()
        assert db1 != db1.to_sync(namespace="o").to_async()
        assert db1 != db1.to_sync(caller_name="o").to_async()
        assert db1 != db1.to_sync(caller_version="o").to_async()
        assert db1 != db1.to_sync(api_path="o").to_async()
        assert db1 != db1.to_sync(api_version="o").to_async()

        db2s = db1.to_sync(
            api_endpoint="x",
            token="x",
            namespace="x",
            caller_name="x_n",
            caller_version="x_v",
            api_path="x",
            api_version="x",
        )
        assert db2s.to_async() != db1

        db2s.set_caller(
            caller_name="c_n",
            caller_version="c_v",
        )
        db3 = db2s.to_async(
            api_endpoint="api_endpoint",
            token="token",
            namespace="namespace",
            api_path="api_path",
            api_version="api_version",
        )
        assert db3 == db1

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

    @pytest.mark.describe("test get_collection method, async")
    async def test_database_get_collection_async(
        self,
        async_database: AsyncDatabase,
        async_collection_instance: AsyncCollection,
    ) -> None:
        collection = await async_database.get_collection(TEST_COLLECTION_INSTANCE_NAME)
        assert collection == async_collection_instance

        assert (
            await getattr(async_database, TEST_COLLECTION_INSTANCE_NAME) == collection
        )
        assert await async_database[TEST_COLLECTION_INSTANCE_NAME] == collection

        NAMESPACE_2 = "other_namespace"
        collection_ns2 = await async_database.get_collection(
            TEST_COLLECTION_INSTANCE_NAME, namespace=NAMESPACE_2
        )
        assert collection_ns2 == AsyncCollection(
            async_database, TEST_COLLECTION_INSTANCE_NAME, namespace=NAMESPACE_2
        )
        assert collection_ns2._astra_db_collection.astra_db.namespace == NAMESPACE_2

    @pytest.mark.describe("test database conversions with caller mutableness, async")
    async def test_database_conversions_caller_mutableness_async(
        self,
        astra_db_credentials_kwargs: AstraDBCredentials,
    ) -> None:
        db1 = AsyncDatabase(
            caller_name="c_n1",
            caller_version="c_v1",
            **astra_db_credentials_kwargs,
        )
        db1.set_caller(
            caller_name="c_n2",
            caller_version="c_v2",
        )
        db2 = AsyncDatabase(
            caller_name="c_n2",
            caller_version="c_v2",
            **astra_db_credentials_kwargs,
        )
        assert db1.to_sync().to_async() == db2
        assert db1.copy() == db2

    @pytest.mark.skipif(
        ASTRA_DB_SECONDARY_KEYSPACE is None, reason="No secondary keyspace provided"
    )
    @pytest.mark.describe("test database namespace property, async")
    async def test_database_namespace_async(
        self,
        astra_db_credentials_kwargs: AstraDBCredentials,
    ) -> None:
        db1 = AsyncDatabase(
            **astra_db_credentials_kwargs,
        )
        assert db1.namespace == DEFAULT_KEYSPACE_NAME

        db2 = AsyncDatabase(
            token=astra_db_credentials_kwargs["token"],
            api_endpoint=astra_db_credentials_kwargs["api_endpoint"],
            namespace=ASTRA_DB_SECONDARY_KEYSPACE,
        )
        assert db2.namespace == ASTRA_DB_SECONDARY_KEYSPACE
