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

from ..conftest import ASTRA_DB_SECONDARY_KEYSPACE
from astrapy import Collection, Database


class TestCollectionsSync:
    @pytest.mark.describe("test of instantiating Collection, sync")
    def test_instantiate_collection_sync(
        self,
        sync_database: Database,
    ) -> None:
        col1 = Collection(
            sync_database,
            "id_test_collection",
            caller_name="c_n",
            caller_version="c_v",
        )
        col2 = Collection(
            sync_database,
            "id_test_collection",
            caller_name="c_n",
            caller_version="c_v",
        )
        assert col1 == col2

    @pytest.mark.describe("test of Collection conversions, sync")
    def test_convert_collection_sync(
        self,
        sync_database: Database,
    ) -> None:
        col1 = Collection(
            sync_database,
            "id_test_collection",
            caller_name="c_n",
            caller_version="c_v",
        )
        assert col1 == col1.copy()
        assert col1 == col1.with_options()
        assert col1 == col1.to_async().to_sync()

    @pytest.mark.describe("test of Collection rich copy, sync")
    def test_rich_copy_collection_sync(
        self,
        sync_database: Database,
    ) -> None:
        col1 = Collection(
            sync_database,
            "id_test_collection",
            caller_name="c_n",
            caller_version="c_v",
        )
        assert col1 != col1.copy(database=sync_database.copy(token="x_t"))
        assert col1 != col1.copy(name="o")
        assert col1 != col1.copy(namespace="o")
        assert col1 != col1.copy(caller_name="o")
        assert col1 != col1.copy(caller_version="o")

        col2 = col1.copy(
            database=sync_database.copy(token="x_t"),
            name="other_name",
            namespace="other_namespace",
            caller_name="x_n",
            caller_version="x_v",
        )
        assert col2 != col1

        col2.set_caller(
            caller_name="c_n",
            caller_version="c_v",
        )
        col3 = col2.copy(
            database=sync_database,
            name="id_test_collection",
            namespace=sync_database.namespace,
        )
        assert col3 == col1

        assert col1.with_options(name="x") != col1
        assert (
            col1.with_options(name="x").with_options(name="id_test_collection") == col1
        )
        assert col1.with_options(caller_name="x") != col1
        assert (
            col1.with_options(caller_name="x").with_options(caller_name="c_n") == col1
        )
        assert col1.with_options(caller_version="x") != col1
        assert (
            col1.with_options(caller_version="x").with_options(caller_version="c_v")
            == col1
        )

    @pytest.mark.describe("test of Collection rich conversions, sync")
    def test_rich_convert_collection_sync(
        self,
        sync_database: Database,
    ) -> None:
        col1 = Collection(
            sync_database,
            "id_test_collection",
            caller_name="c_n",
            caller_version="c_v",
        )
        assert (
            col1
            != col1.to_async(
                database=sync_database.copy(token="x_t").to_async()
            ).to_sync()
        )
        assert col1 != col1.to_async(name="o").to_sync()
        assert col1 != col1.to_async(namespace="o").to_sync()
        assert col1 != col1.to_async(caller_name="o").to_sync()
        assert col1 != col1.to_async(caller_version="o").to_sync()

        col2a = col1.to_async(
            database=sync_database.copy(token="x_t").to_async(),
            name="other_name",
            namespace="other_namespace",
            caller_name="x_n",
            caller_version="x_v",
        )
        assert col2a.to_sync() != col1

        col2a.set_caller(
            caller_name="c_n",
            caller_version="c_v",
        )
        col3 = col2a.to_sync(
            database=sync_database,
            name="id_test_collection",
            namespace=sync_database.namespace,
        )
        assert col3 == col1

    @pytest.mark.describe("test of Collection database property, sync")
    def test_collection_database_property_sync(
        self,
    ) -> None:
        db1 = Database("a", "t", namespace="ns1")
        db2 = Database("a", "t", namespace="ns2")
        col1 = Collection(db1, "coll")
        col2 = Collection(db1, "coll", namespace="ns2")
        assert col1.database == db1
        assert col2.database == db2

    @pytest.mark.describe("test of Collection name property, sync")
    def test_collection_name_property_sync(
        self,
    ) -> None:
        db1 = Database("a", "t", namespace="ns1")
        col1 = Collection(db1, "coll")
        assert col1.name == "coll"

    @pytest.mark.describe("test of Collection set_caller, sync")
    def test_collection_set_caller_sync(
        self,
        sync_database: Database,
    ) -> None:
        col1 = Collection(
            sync_database,
            "id_test_collection",
            caller_name="c_n1",
            caller_version="c_v1",
        )
        col2 = Collection(
            sync_database,
            "id_test_collection",
            caller_name="c_n2",
            caller_version="c_v2",
        )
        col2.set_caller(
            caller_name="c_n1",
            caller_version="c_v1",
        )
        assert col1 == col2

    @pytest.mark.describe("test collection conversions with caller mutableness, sync")
    def test_collection_conversions_caller_mutableness_sync(
        self,
        sync_database: Database,
    ) -> None:
        col1 = Collection(
            sync_database,
            "id_test_collection",
            caller_name="c_n1",
            caller_version="c_v1",
        )
        col1.set_caller(
            caller_name="c_n2",
            caller_version="c_v2",
        )
        col2 = Collection(
            sync_database,
            "id_test_collection",
            caller_name="c_n2",
            caller_version="c_v2",
        )
        assert col1.copy() == col2
        assert col1.to_async().to_sync() == col2

    @pytest.mark.skipif(
        ASTRA_DB_SECONDARY_KEYSPACE is None, reason="No secondary keyspace provided"
    )
    @pytest.mark.describe("test collection namespace property, sync")
    def test_collection_namespace_sync(
        self,
        sync_database: Database,
    ) -> None:
        col1 = sync_database.get_collection("id_test_collection")
        assert col1.namespace == sync_database.namespace

        col2 = sync_database.get_collection(
            "id_test_collection",
            namespace=ASTRA_DB_SECONDARY_KEYSPACE,
        )
        assert col2.namespace == ASTRA_DB_SECONDARY_KEYSPACE

        col3 = Collection(sync_database, "id_test_collection")
        assert col3.namespace == sync_database.namespace

        col4 = Collection(
            sync_database,
            "id_test_collection",
            namespace=ASTRA_DB_SECONDARY_KEYSPACE,
        )
        assert col4.namespace == ASTRA_DB_SECONDARY_KEYSPACE
