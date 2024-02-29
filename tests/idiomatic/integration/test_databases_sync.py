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

    @pytest.mark.describe("test errors for unsupported Database methods, sync")
    def test_database_unsupported_methods_sync(
        self,
        sync_database: Database,
    ) -> None:
        with pytest.raises(TypeError):
            sync_database.aggregate(1, "x")
        with pytest.raises(TypeError):
            sync_database.cursor_command(1, "x")
        with pytest.raises(TypeError):
            sync_database.dereference(1, "x")
        with pytest.raises(TypeError):
            sync_database.watch(1, "x")
        with pytest.raises(TypeError):
            sync_database.validate_collection(1, "x")

    @pytest.mark.describe("test get_collection method, sync")
    def test_database_get_collection_sync(
        self,
        sync_database: Database,
        sync_collection_instance: Collection,
        astra_db_credentials_kwargs: AstraDBCredentials,
    ) -> None:
        collection = sync_database.get_collection(TEST_COLLECTION_INSTANCE_NAME)
        assert collection == sync_collection_instance

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
