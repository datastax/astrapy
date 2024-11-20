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

from typing import Sequence

import pytest

from astrapy import AsyncCollection, AsyncDatabase
from astrapy.constants import CallerType
from astrapy.utils.api_options import APIOptions, FullAPIOptions, defaultAPIOptions
from astrapy.utils.unset import _UNSET, UnsetType

from ..conftest import (
    SECONDARY_KEYSPACE,
    DataAPICredentialsInfo,
    DefaultAsyncCollection,
)


def _wrapCallers(
    asrc_database: AsyncDatabase,
    callers: Sequence[CallerType] | UnsetType = _UNSET,
) -> FullAPIOptions:
    return defaultAPIOptions(
        environment=asrc_database.api_options.environment
    ).with_override(APIOptions(callers=callers))


class TestCollectionsAsync:
    @pytest.mark.describe("test of instantiating Collection, async")
    async def test_instantiate_collection_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        col1: DefaultAsyncCollection = AsyncCollection(
            database=async_database,
            name="id_test_collection",
            keyspace=None,
            api_options=_wrapCallers(async_database, callers=[("cn", "cv")]),
        )
        col2: DefaultAsyncCollection = AsyncCollection(
            database=async_database,
            name="id_test_collection",
            keyspace=None,
            api_options=_wrapCallers(async_database, callers=[("cn", "cv")]),
        )
        assert col1 == col2

    @pytest.mark.describe("test of Collection conversions, async")
    async def test_convert_collection_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        col1: DefaultAsyncCollection = AsyncCollection(
            database=async_database,
            name="id_test_collection",
            keyspace=None,
            api_options=_wrapCallers(async_database, callers=[("cn", "cv")]),
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
        col1: DefaultAsyncCollection = AsyncCollection(
            database=async_database,
            name="id_test_collection",
            keyspace=None,
            api_options=_wrapCallers(async_database, callers=callers0),
        )
        assert col1 != col1._copy(name="o")
        assert col1 != col1._copy(keyspace="o")
        assert col1 != col1._copy(callers=callers1)

        col2 = col1._copy(
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

        assert (
            col1.with_options(callers=callers1).with_options(
                api_options=APIOptions(callers=callers0)
            )
            == col1
        )
        assert (
            col1.with_options(callers=callers1).with_options(
                callers=callers0, api_options=APIOptions(callers=callers1)
            )
            == col1
        )

    @pytest.mark.describe("test of Collection rich conversions, async")
    async def test_rich_convert_collection_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        callers0 = [("cn", "cv"), ("dn", "dv")]
        callers1 = [("x", "y")]
        col1: DefaultAsyncCollection = AsyncCollection(
            database=async_database,
            name="id_test_collection",
            keyspace="the_ks",
            api_options=_wrapCallers(async_database, callers=callers0),
        )
        assert col1 != col1.to_sync(name="o").to_async()
        assert col1 != col1.to_sync(keyspace="o").to_async()
        assert col1 != col1.to_sync(callers=callers1).to_async()

        col2s = col1.to_sync(
            name="other_name",
            keyspace="other_keyspace",
            callers=callers1,
        )
        assert col2s.to_async() != col1

        col3 = col2s.to_async(
            name="id_test_collection",
            keyspace="the_ks",
            callers=callers0,
        )
        assert col3 == col1

        col1_a = col1.to_sync(callers=callers1)
        assert col1_a.to_async() != col1
        assert col1_a.to_async(api_options=APIOptions(callers=callers0)) == col1
        assert (
            col1_a.to_async(callers=callers0, api_options=APIOptions(callers=callers1))
            == col1
        )

    @pytest.mark.describe("test of Collection database property, async")
    async def test_collection_database_property_async(
        self,
    ) -> None:
        opts0 = defaultAPIOptions(environment="other")
        db1 = AsyncDatabase(api_endpoint="a", keyspace="ns1", api_options=opts0)
        db2 = AsyncDatabase(api_endpoint="a", keyspace="ns2", api_options=opts0)
        col1: DefaultAsyncCollection = AsyncCollection(
            database=db1,
            name="coll",
            keyspace=None,
            api_options=opts0,
        )
        col2: DefaultAsyncCollection = AsyncCollection(
            database=db1, name="coll", api_options=opts0, keyspace="ns2"
        )
        assert col1.database == db1
        assert col2.database == db2

    @pytest.mark.describe("test of Collection name property, async")
    async def test_collection_name_property_async(
        self,
    ) -> None:
        opts0 = defaultAPIOptions(environment="other")
        db1 = AsyncDatabase(api_endpoint="a", keyspace="ns1", api_options=opts0)
        col1: DefaultAsyncCollection = AsyncCollection(
            database=db1,
            name="coll",
            keyspace=None,
            api_options=opts0,
        )
        assert col1.name == "coll"

    @pytest.mark.skipif(
        SECONDARY_KEYSPACE is None, reason="No secondary keyspace provided"
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
            keyspace=data_api_credentials_info["secondary_keyspace"],
        )
        assert col2.keyspace == data_api_credentials_info["secondary_keyspace"]

        col3: DefaultAsyncCollection = AsyncCollection(
            database=async_database,
            name="id_test_collection",
            keyspace=None,
            api_options=async_database.api_options,
        )
        assert col3.keyspace == async_database.keyspace

        col4: DefaultAsyncCollection = AsyncCollection(
            database=async_database,
            name="id_test_collection",
            keyspace=data_api_credentials_info["secondary_keyspace"],
            api_options=async_database.api_options,
        )
        assert col4.keyspace == data_api_credentials_info["secondary_keyspace"]
        assert col1 == col3
        assert col2 == col4
