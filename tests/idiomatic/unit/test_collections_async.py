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

from astrapy import AsyncCollection, AsyncDatabase

from ..conftest import (
    SECONDARY_NAMESPACE,
    DataAPICredentialsInfo,
    async_fail_if_not_removed,
)


class TestCollectionsAsync:
    @pytest.mark.describe("test of instantiating Collection, async")
    async def test_instantiate_collection_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        col1 = AsyncCollection(
            async_database,
            "id_test_collection",
            callers=[("cn", "cv")],
        )
        col2 = AsyncCollection(
            async_database,
            "id_test_collection",
            callers=[("cn", "cv")],
        )
        assert col1 == col2

    @pytest.mark.describe("test of Collection conversions, async")
    async def test_convert_collection_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        col1 = AsyncCollection(
            async_database,
            "id_test_collection",
            callers=[("cn", "cv")],
        )
        assert col1 == col1._copy()
        assert col1 == col1.with_options()
        assert col1 == col1.to_sync().to_async()

    @pytest.mark.describe("test of Collection rich _copy, async")
    async def test_rich_copy_collection_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        callers0 = [("cn", "cv"), ("dn", "dv")]
        callers1 = [("x", "y")]
        col1 = AsyncCollection(
            async_database,
            "id_test_collection",
            callers=callers0,
        )
        assert col1 != col1._copy(database=async_database._copy(token="x_t"))
        assert col1 != col1._copy(name="o")
        assert col1 != col1._copy(keyspace="o")
        assert col1 != col1._copy(callers=callers1)

        col2 = col1._copy(
            database=async_database._copy(token="x_t"),
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

    @pytest.mark.describe("test of Collection rich conversions, async")
    async def test_rich_convert_collection_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        callers0 = [("cn", "cv"), ("dn", "dv")]
        callers1 = [("x", "y")]
        col1 = AsyncCollection(
            async_database,
            "id_test_collection",
            keyspace="the_ks",
            callers=callers0,
        )
        assert (
            col1
            != col1.to_sync(
                database=async_database._copy(token="x_t").to_sync()
            ).to_async()
        )
        assert col1 != col1.to_sync(name="o").to_async()
        assert col1 != col1.to_sync(keyspace="o").to_async()
        assert col1 != col1.to_sync(callers=callers1).to_async()

        col2s = col1.to_sync(
            database=async_database._copy(token="x_t").to_sync(),
            name="other_name",
            keyspace="other_keyspace",
            callers=callers1,
        )
        assert col2s.to_async() != col1

        col3 = col2s.to_async(
            database=async_database._copy(),
            name="id_test_collection",
            keyspace="the_ks",
            callers=callers0,
        )
        assert col3 == col1

    @pytest.mark.describe("test of Collection database property, async")
    async def test_collection_database_property_async(
        self,
    ) -> None:
        db1 = AsyncDatabase("a", "t", keyspace="ns1")
        db2 = AsyncDatabase("a", "t", keyspace="ns2")
        col1 = AsyncCollection(db1, "coll")
        col2 = AsyncCollection(db1, "coll", keyspace="ns2")
        assert col1.database == db1
        assert col2.database == db2

    @pytest.mark.describe("test of Collection name property, async")
    async def test_collection_name_property_async(
        self,
    ) -> None:
        db1 = AsyncDatabase("a", "t", keyspace="ns1")
        col1 = AsyncCollection(db1, "coll")
        assert col1.name == "coll"

    @async_fail_if_not_removed
    @pytest.mark.skipif(
        SECONDARY_NAMESPACE is None, reason="No secondary keyspace provided"
    )
    @pytest.mark.describe("test collection namespace property, async")
    async def test_collection_namespace_async(
        self,
        async_database: AsyncDatabase,
        data_api_credentials_info: DataAPICredentialsInfo,
    ) -> None:
        col1 = await async_database.get_collection("id_test_collection")
        with pytest.warns(DeprecationWarning):
            col1.namespace
        assert col1.namespace == async_database.namespace

        col2 = await async_database.get_collection(
            "id_test_collection",
            namespace=data_api_credentials_info["secondary_namespace"],
        )
        assert col2.namespace == data_api_credentials_info["secondary_namespace"]

        col3 = AsyncCollection(async_database, "id_test_collection")
        assert col3.namespace == async_database.namespace

        col4 = AsyncCollection(
            async_database,
            "id_test_collection",
            namespace=data_api_credentials_info["secondary_namespace"],
        )
        assert col4.namespace == data_api_credentials_info["secondary_namespace"]

    @pytest.mark.skipif(
        SECONDARY_NAMESPACE is None, reason="No secondary keyspace provided"
    )
    @pytest.mark.describe("test collection keyspace property, async")
    async def test_collection_keyspace_async(
        self,
        async_database: AsyncDatabase,
        data_api_credentials_info: DataAPICredentialsInfo,
    ) -> None:
        col1 = await async_database.get_collection("id_test_collection")
        assert col1.keyspace == async_database.keyspace

        col2 = await async_database.get_collection(
            "id_test_collection",
            namespace=data_api_credentials_info["secondary_namespace"],
        )
        assert col2.keyspace == data_api_credentials_info["secondary_namespace"]

        col3 = AsyncCollection(async_database, "id_test_collection")
        assert col3.keyspace == async_database.keyspace

        col4 = AsyncCollection(
            async_database,
            "id_test_collection",
            namespace=data_api_credentials_info["secondary_namespace"],
        )
        assert col4.keyspace == data_api_credentials_info["secondary_namespace"]
