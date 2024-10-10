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

import pytest

from astrapy import Collection, Database

from ..conftest import (
    SECONDARY_KEYSPACE,
    DataAPICredentialsInfo,
)


class TestCollectionsSync:
    @pytest.mark.describe("test of instantiating Collection, sync")
    def test_instantiate_collection_sync(
        self,
        sync_database: Database,
    ) -> None:
        col1 = Collection(
            sync_database,
            "id_test_collection",
            callers=[("cn", "cv")],
        )
        col2 = Collection(
            sync_database,
            "id_test_collection",
            callers=[("cn", "cv")],
        )
        assert col1 == col2

    @pytest.mark.describe("test of Collection conversions, sync")
    def test_convert_collection_sync(
        self,
        sync_database: Database,
    ) -> None:
        col1 = Collection(sync_database, "id_test_collection", callers=[("cn", "cv")])
        assert col1 == col1._copy()
        assert col1 == col1.with_options()
        assert col1 == col1.to_async().to_sync()

    @pytest.mark.describe("test of Collection rich _copy, sync")
    def test_rich_copy_collection_sync(
        self,
        sync_database: Database,
    ) -> None:
        callers0 = [("cn", "cv"), ("dn", "dv")]
        callers1 = [("x", "y")]
        col1 = Collection(
            sync_database,
            "id_test_collection",
            callers=callers0,
        )
        assert col1 != col1._copy(database=sync_database._copy(token="x_t"))
        assert col1 != col1._copy(name="o")
        assert col1 != col1._copy(keyspace="o")
        assert col1 != col1._copy(callers=callers1)

        col2 = col1._copy(
            database=sync_database._copy(token="x_t"),
            name="other_name",
            keyspace="other_keyspace",
            callers=callers1,
        )
        assert col2 != col1

        assert col1.with_options(name="x") != col1
        assert col1.with_options(callers=callers1) != col1

        assert (
            col1.with_options(name="x").with_options(name="id_test_collection") == col1
        )
        assert (
            col1.with_options(callers=callers1).with_options(callers=callers0) == col1
        )

    @pytest.mark.describe("test of Collection rich conversions, sync")
    def test_rich_convert_collection_sync(
        self,
        sync_database: Database,
    ) -> None:
        callers0 = [("cn", "cv"), ("dn", "dv")]
        callers1 = [("x", "y")]
        col1 = Collection(
            sync_database,
            "id_test_collection",
            keyspace="the_ks",
            callers=callers0,
        )
        assert (
            col1
            != col1.to_async(
                database=sync_database._copy(token="x_t").to_async()
            ).to_sync()
        )
        assert col1 != col1.to_async(name="o").to_sync()
        assert col1 != col1.to_async(keyspace="o").to_sync()
        assert col1 != col1.to_async(callers=callers1).to_sync()

        col2a = col1.to_async(
            database=sync_database._copy(token="x_t").to_async(),
            name="other_name",
            keyspace="other_keyspace",
            callers=callers1,
        )
        assert col2a.to_sync() != col1

        col3 = col2a.to_sync(
            database=sync_database._copy(),
            name="id_test_collection",
            keyspace="the_ks",
            callers=callers0,
        )
        assert col3 == col1

    @pytest.mark.describe("test of Collection database property, sync")
    def test_collection_database_property_sync(
        self,
    ) -> None:
        db1 = Database("a", "t", keyspace="ns1")
        db2 = Database("a", "t", keyspace="ns2")
        col1 = Collection(db1, "coll")
        col2 = Collection(db1, "coll", keyspace="ns2")
        assert col1.database == db1
        assert col2.database == db2

    @pytest.mark.describe("test of Collection name property, sync")
    def test_collection_name_property_sync(
        self,
    ) -> None:
        db1 = Database("a", "t", keyspace="ns1")
        col1 = Collection(db1, "coll")
        assert col1.name == "coll"

    @pytest.mark.skipif(
        SECONDARY_KEYSPACE is None, reason="No secondary keyspace provided"
    )
    @pytest.mark.describe("test collection keyspace property, sync")
    def test_collection_keyspace_sync(
        self,
        sync_database: Database,
        data_api_credentials_info: DataAPICredentialsInfo,
    ) -> None:
        col1 = sync_database.get_collection("id_test_collection")
        assert col1.keyspace == sync_database.keyspace

        col2 = sync_database.get_collection(
            "id_test_collection",
            keyspace=data_api_credentials_info["secondary_keyspace"],
        )
        assert col2.keyspace == data_api_credentials_info["secondary_keyspace"]

        col3 = Collection(sync_database, "id_test_collection")
        assert col3.keyspace == sync_database.keyspace

        col4 = Collection(
            sync_database,
            "id_test_collection",
            keyspace=data_api_credentials_info["secondary_keyspace"],
        )
        assert col4.keyspace == data_api_credentials_info["secondary_keyspace"]
        assert col1 == col3
        assert col2 == col4
