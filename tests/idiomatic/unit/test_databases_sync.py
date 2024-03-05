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
from astrapy import Collection, Database


class TestDatabasesSync:
    @pytest.mark.describe("test of instantiating Database, sync")
    def test_instantiate_database_sync(
        self,
        astra_db_credentials_kwargs: AstraDBCredentials,
    ) -> None:
        db1 = Database(
            caller_name="c_n",
            caller_version="c_v",
            **astra_db_credentials_kwargs,
        )
        db2 = Database(
            caller_name="c_n",
            caller_version="c_v",
            **astra_db_credentials_kwargs,
        )
        assert db1 == db2

    @pytest.mark.describe("test of Database conversions, sync")
    def test_convert_database_sync(
        self,
        astra_db_credentials_kwargs: AstraDBCredentials,
    ) -> None:
        db1 = Database(
            caller_name="c_n",
            caller_version="c_v",
            **astra_db_credentials_kwargs,
        )
        assert db1 == db1.copy()
        assert db1 == db1.to_async().to_sync()

    @pytest.mark.describe("test of Database rich copy, sync")
    def test_rich_copy_database_sync(
        self,
    ) -> None:
        db1 = Database(
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

    @pytest.mark.describe("test of Database rich conversions, sync")
    def test_rich_convert_database_sync(
        self,
    ) -> None:
        db1 = Database(
            api_endpoint="api_endpoint",
            token="token",
            namespace="namespace",
            caller_name="c_n",
            caller_version="c_v",
            api_path="api_path",
            api_version="api_version",
        )
        assert db1 != db1.to_async(api_endpoint="o").to_sync()
        assert db1 != db1.to_async(token="o").to_sync()
        assert db1 != db1.to_async(namespace="o").to_sync()
        assert db1 != db1.to_async(caller_name="o").to_sync()
        assert db1 != db1.to_async(caller_version="o").to_sync()
        assert db1 != db1.to_async(api_path="o").to_sync()
        assert db1 != db1.to_async(api_version="o").to_sync()

        db2a = db1.to_async(
            api_endpoint="x",
            token="x",
            namespace="x",
            caller_name="x_n",
            caller_version="x_v",
            api_path="x",
            api_version="x",
        )
        assert db2a.to_sync() != db1

        db2a.set_caller(
            caller_name="c_n",
            caller_version="c_v",
        )
        db3 = db2a.to_sync(
            api_endpoint="api_endpoint",
            token="token",
            namespace="namespace",
            api_path="api_path",
            api_version="api_version",
        )
        assert db3 == db1

    @pytest.mark.describe("test of Database set_caller, sync")
    def test_database_set_caller_sync(
        self,
        astra_db_credentials_kwargs: AstraDBCredentials,
    ) -> None:
        db1 = Database(
            caller_name="c_n1",
            caller_version="c_v1",
            **astra_db_credentials_kwargs,
        )
        db2 = Database(
            caller_name="c_n2",
            caller_version="c_v2",
            **astra_db_credentials_kwargs,
        )
        db2.set_caller(
            caller_name="c_n1",
            caller_version="c_v1",
        )
        assert db1 == db2

    @pytest.mark.describe("test get_collection method, sync")
    def test_database_get_collection_sync(
        self,
        sync_database: Database,
        sync_collection_instance: Collection,
        astra_db_credentials_kwargs: AstraDBCredentials,
    ) -> None:
        collection = sync_database.get_collection(TEST_COLLECTION_INSTANCE_NAME)
        assert collection == sync_collection_instance

        assert getattr(sync_database, TEST_COLLECTION_INSTANCE_NAME) == collection
        assert sync_database[TEST_COLLECTION_INSTANCE_NAME] == collection

        NAMESPACE_2 = "other_namespace"
        collection_ns2 = sync_database.get_collection(
            TEST_COLLECTION_INSTANCE_NAME, namespace=NAMESPACE_2
        )
        assert collection_ns2 == Collection(
            sync_database, TEST_COLLECTION_INSTANCE_NAME, namespace=NAMESPACE_2
        )
        assert collection_ns2._astra_db_collection.astra_db.namespace == NAMESPACE_2

    @pytest.mark.describe("test database conversions with caller mutableness, sync")
    def test_database_conversions_caller_mutableness_sync(
        self,
        astra_db_credentials_kwargs: AstraDBCredentials,
    ) -> None:
        db1 = Database(
            caller_name="c_n1",
            caller_version="c_v1",
            **astra_db_credentials_kwargs,
        )
        db1.set_caller(
            caller_name="c_n2",
            caller_version="c_v2",
        )
        db2 = Database(
            caller_name="c_n2",
            caller_version="c_v2",
            **astra_db_credentials_kwargs,
        )
        assert db1.to_async().to_sync() == db2
        assert db1.copy() == db2

    @pytest.mark.skipif(
        ASTRA_DB_SECONDARY_KEYSPACE is None, reason="No secondary keyspace provided"
    )
    @pytest.mark.describe("test database namespace property, sync")
    def test_database_namespace_sync(
        self,
        astra_db_credentials_kwargs: AstraDBCredentials,
    ) -> None:
        db1 = Database(
            **astra_db_credentials_kwargs,
        )
        assert db1.namespace == DEFAULT_KEYSPACE_NAME

        db2 = Database(
            token=astra_db_credentials_kwargs["token"],
            api_endpoint=astra_db_credentials_kwargs["api_endpoint"],
            namespace=ASTRA_DB_SECONDARY_KEYSPACE,
        )
        assert db2.namespace == ASTRA_DB_SECONDARY_KEYSPACE
