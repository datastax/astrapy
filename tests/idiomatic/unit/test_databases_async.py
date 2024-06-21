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
    DataAPICredentials,
    DataAPICredentialsInfo,
    SECONDARY_NAMESPACE,
    TEST_COLLECTION_INSTANCE_NAME,
)
from astrapy.core.defaults import DEFAULT_KEYSPACE_NAME
from astrapy.exceptions import DevOpsAPIException
from astrapy import AsyncCollection, AsyncDatabase


class TestDatabasesAsync:
    @pytest.mark.describe("test of instantiating Database, async")
    async def test_instantiate_database_async(
        self,
        data_api_credentials_kwargs: DataAPICredentials,
    ) -> None:
        db1 = AsyncDatabase(
            caller_name="c_n",
            caller_version="c_v",
            **data_api_credentials_kwargs,
        )
        db2 = AsyncDatabase(
            caller_name="c_n",
            caller_version="c_v",
            **data_api_credentials_kwargs,
        )
        assert db1 == db2

    @pytest.mark.describe("test of Database conversions, async")
    async def test_convert_database_async(
        self,
        data_api_credentials_kwargs: DataAPICredentials,
    ) -> None:
        db1 = AsyncDatabase(
            caller_name="c_n",
            caller_version="c_v",
            **data_api_credentials_kwargs,
        )
        assert db1 == db1._copy()
        assert db1 == db1.with_options()
        assert db1 == db1.to_sync().to_async()

    @pytest.mark.describe("test of Database rich _copy, async")
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
        assert db1 != db1._copy(api_endpoint="x")
        assert db1 != db1._copy(token="x")
        assert db1 != db1._copy(namespace="x")
        assert db1 != db1._copy(caller_name="x")
        assert db1 != db1._copy(caller_version="x")
        assert db1 != db1._copy(api_path="x")
        assert db1 != db1._copy(api_version="x")

        db2 = db1._copy(
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
        db3 = db2._copy(
            api_endpoint="api_endpoint",
            token="token",
            namespace="namespace",
            api_path="api_path",
            api_version="api_version",
        )
        assert db3 == db1

        assert db1.with_options(namespace="x") != db1
        assert (
            db1.with_options(namespace="x").with_options(namespace="namespace") == db1
        )
        assert db1.with_options(caller_name="x") != db1
        assert db1.with_options(caller_name="x").with_options(caller_name="c_n") == db1
        assert db1.with_options(caller_version="x") != db1
        assert (
            db1.with_options(caller_version="x").with_options(caller_version="c_v")
            == db1
        )

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
        data_api_credentials_kwargs: DataAPICredentials,
    ) -> None:
        db1 = AsyncDatabase(
            caller_name="c_n1",
            caller_version="c_v1",
            **data_api_credentials_kwargs,
        )
        db2 = AsyncDatabase(
            caller_name="c_n2",
            caller_version="c_v2",
            **data_api_credentials_kwargs,
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

        assert getattr(async_database, TEST_COLLECTION_INSTANCE_NAME) == collection
        assert async_database[TEST_COLLECTION_INSTANCE_NAME] == collection

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
        data_api_credentials_kwargs: DataAPICredentials,
    ) -> None:
        db1 = AsyncDatabase(
            caller_name="c_n1",
            caller_version="c_v1",
            **data_api_credentials_kwargs,
        )
        db1.set_caller(
            caller_name="c_n2",
            caller_version="c_v2",
        )
        db2 = AsyncDatabase(
            caller_name="c_n2",
            caller_version="c_v2",
            **data_api_credentials_kwargs,
        )
        assert db1.to_sync().to_async() == db2
        assert db1._copy() == db2

    @pytest.mark.skipif(
        SECONDARY_NAMESPACE is None, reason="No secondary namespace provided"
    )
    @pytest.mark.describe("test database namespace property, async")
    async def test_database_namespace_async(
        self,
        data_api_credentials_kwargs: DataAPICredentials,
        data_api_credentials_info: DataAPICredentialsInfo,
    ) -> None:
        db1 = AsyncDatabase(
            **data_api_credentials_kwargs,
        )
        assert db1.namespace == DEFAULT_KEYSPACE_NAME

        db2 = AsyncDatabase(
            token=data_api_credentials_kwargs["token"],
            api_endpoint=data_api_credentials_kwargs["api_endpoint"],
            namespace=data_api_credentials_info["secondary_namespace"],
        )
        assert db2.namespace == data_api_credentials_info["secondary_namespace"]

    @pytest.mark.describe("test database id, async")
    async def test_database_id_async(self) -> None:
        db1 = AsyncDatabase(
            token="t",
            api_endpoint="https://a1234567-89ab-cdef-0123-456789abcdef-us-central1.apps.astra-dev.datastax.com",
        )
        assert db1.id == "a1234567-89ab-cdef-0123-456789abcdef"

        db2 = AsyncDatabase(
            token="t",
            api_endpoint="http://localhost:12345",
        )
        with pytest.raises(DevOpsAPIException):
            db2.id
