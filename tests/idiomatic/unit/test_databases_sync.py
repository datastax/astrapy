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

from astrapy import Collection, Database
from astrapy.core.defaults import DEFAULT_KEYSPACE_NAME
from astrapy.exceptions import DevOpsAPIException

from ..conftest import (
    SECONDARY_NAMESPACE,
    TEST_COLLECTION_INSTANCE_NAME,
    DataAPICredentials,
    DataAPICredentialsInfo,
)


class TestDatabasesSync:
    @pytest.mark.describe("test of instantiating Database, sync")
    def test_instantiate_database_sync(
        self,
        data_api_credentials_kwargs: DataAPICredentials,
    ) -> None:
        db1 = Database(
            caller_name="c_n",
            caller_version="c_v",
            **data_api_credentials_kwargs,
        )
        db2 = Database(
            caller_name="c_n",
            caller_version="c_v",
            **data_api_credentials_kwargs,
        )
        assert db1 == db2

    @pytest.mark.describe("test of Database conversions, sync")
    def test_convert_database_sync(
        self,
        data_api_credentials_kwargs: DataAPICredentials,
    ) -> None:
        db1 = Database(
            caller_name="c_n",
            caller_version="c_v",
            **data_api_credentials_kwargs,
        )
        assert db1 == db1._copy()
        assert db1 == db1.with_options()
        assert db1 == db1.to_async().to_sync()

    @pytest.mark.describe("test of Database rich _copy, sync")
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
        data_api_credentials_kwargs: DataAPICredentials,
    ) -> None:
        db1 = Database(
            caller_name="c_n1",
            caller_version="c_v1",
            **data_api_credentials_kwargs,
        )
        db2 = Database(
            caller_name="c_n2",
            caller_version="c_v2",
            **data_api_credentials_kwargs,
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
        data_api_credentials_kwargs: DataAPICredentials,
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
        data_api_credentials_kwargs: DataAPICredentials,
    ) -> None:
        db1 = Database(
            caller_name="c_n1",
            caller_version="c_v1",
            **data_api_credentials_kwargs,
        )
        db1.set_caller(
            caller_name="c_n2",
            caller_version="c_v2",
        )
        db2 = Database(
            caller_name="c_n2",
            caller_version="c_v2",
            **data_api_credentials_kwargs,
        )
        assert db1.to_async().to_sync() == db2
        assert db1._copy() == db2

    @pytest.mark.skipif(
        SECONDARY_NAMESPACE is None, reason="No secondary namespace provided"
    )
    @pytest.mark.describe("test database namespace property, sync")
    def test_database_namespace_sync(
        self,
        data_api_credentials_kwargs: DataAPICredentials,
        data_api_credentials_info: DataAPICredentialsInfo,
    ) -> None:
        db1 = Database(
            **data_api_credentials_kwargs,
        )
        assert db1.namespace == DEFAULT_KEYSPACE_NAME

        db2 = Database(
            token=data_api_credentials_kwargs["token"],
            api_endpoint=data_api_credentials_kwargs["api_endpoint"],
            namespace=data_api_credentials_info["secondary_namespace"],
        )
        assert db2.namespace == data_api_credentials_info["secondary_namespace"]

    @pytest.mark.describe("test database id, sync")
    def test_database_id_sync(self) -> None:
        db1 = Database(
            token="t",
            api_endpoint="https://a1234567-89ab-cdef-0123-456789abcdef-us-central1.apps.astra-dev.datastax.com",
        )
        assert db1.id == "a1234567-89ab-cdef-0123-456789abcdef"

        db2 = Database(
            token="t",
            api_endpoint="http://localhost:12345",
        )
        with pytest.raises(DevOpsAPIException):
            db2.id
