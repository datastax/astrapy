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

from collections.abc import Sequence

import pytest

from astrapy import AsyncCollection, AsyncDatabase
from astrapy.constants import CallerType
from astrapy.settings.defaults import RERANKING_HEADER_API_KEY
from astrapy.utils.api_options import APIOptions, FullAPIOptions, defaultAPIOptions
from astrapy.utils.unset import _UNSET, UnsetType

from ..conftest import (
    DataAPICredentialsInfo,
    DefaultAsyncCollection,
)


def _wrapSomeOptions(
    asrc_database: AsyncDatabase,
    *,
    callers: Sequence[CallerType] | UnsetType = _UNSET,
    embedding_api_key: str | UnsetType = _UNSET,
    reranking_api_key: str | UnsetType = _UNSET,
) -> FullAPIOptions:
    return defaultAPIOptions(
        environment=asrc_database.api_options.environment
    ).with_override(
        APIOptions(
            callers=callers,
            embedding_api_key=embedding_api_key,
            reranking_api_key=reranking_api_key,
        )
    )


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
            api_options=_wrapSomeOptions(async_database, callers=[("cn", "cv")]),
        )
        col2: DefaultAsyncCollection = AsyncCollection(
            database=async_database,
            name="id_test_collection",
            keyspace=None,
            api_options=_wrapSomeOptions(async_database, callers=[("cn", "cv")]),
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
            api_options=_wrapSomeOptions(async_database, callers=[("cn", "cv")]),
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
        col1: DefaultAsyncCollection = AsyncCollection(
            database=async_database,
            name="id_test_collection",
            keyspace=None,
            api_options=_wrapSomeOptions(
                async_database,
                callers=callers0,
                embedding_api_key="eak",
                reranking_api_key="rak",
            ),
        )

        col2 = col1._copy(
            embedding_api_key="zak",
        )
        assert col2 != col1
        col2b = col1._copy(
            reranking_api_key="zak",
        )
        assert col2b != col1

        assert col1.with_options(embedding_api_key="zak") != col1
        assert col1.with_options(reranking_api_key="zak") != col1

        assert (
            col1.with_options(embedding_api_key="zak").with_options(
                embedding_api_key="eak"
            )
            == col1
        )
        assert (
            col1.with_options(reranking_api_key="zak").with_options(
                reranking_api_key="rak"
            )
            == col1
        )

    @pytest.mark.describe("test of Collection rich conversions, async")
    async def test_rich_convert_collection_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        callers0 = [("cn", "cv"), ("dn", "dv")]
        col1: DefaultAsyncCollection = AsyncCollection(
            database=async_database,
            name="id_test_collection",
            keyspace="the_ks",
            api_options=_wrapSomeOptions(
                async_database,
                callers=callers0,
                embedding_api_key="eak",
                reranking_api_key="rak",
            ),
        )
        assert col1 != col1.to_sync(embedding_api_key="zak").to_async()
        assert col1 != col1.to_sync(reranking_api_key="zak").to_async()

        col2s = col1.to_sync(
            embedding_api_key="zak",
        )
        assert col2s.to_async() != col1
        col2bs = col1.to_sync(
            reranking_api_key="zak",
        )
        assert col2bs.to_async() != col1

        col3 = col2s.to_async(
            embedding_api_key="eak",
        )
        assert col3 == col1
        col3b = col2bs.to_async(
            reranking_api_key="rak",
        )
        assert col3b == col1

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

    @pytest.mark.describe("test collection keyspace property, async")
    async def test_collection_keyspace_async(
        self,
        async_database: AsyncDatabase,
        data_api_credentials_info: DataAPICredentialsInfo,
    ) -> None:
        col1 = async_database.get_collection("id_test_collection")
        assert col1.keyspace == async_database.keyspace

        col2 = async_database.get_collection(
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

    @pytest.mark.describe(
        "test collection reranking API key in commander headers, async"
    )
    async def test_collection_rerankingapikey_in_headers_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        col_0 = async_database.get_collection("q")
        col_1 = async_database.get_collection("q", reranking_api_key="RAK")
        assert RERANKING_HEADER_API_KEY not in col_0._api_commander.full_headers
        assert col_1._api_commander.full_headers[RERANKING_HEADER_API_KEY] == "RAK"
