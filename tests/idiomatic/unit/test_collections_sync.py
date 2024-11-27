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

from astrapy import Collection, Database
from astrapy.constants import CallerType
from astrapy.utils.api_options import APIOptions, FullAPIOptions, defaultAPIOptions
from astrapy.utils.unset import _UNSET, UnsetType

from ..conftest import (
    SECONDARY_KEYSPACE,
    DataAPICredentialsInfo,
    DefaultCollection,
)


def _wrapSomeOptions(
    src_database: Database,
    *,
    callers: Sequence[CallerType] | UnsetType = _UNSET,
    embedding_api_key: str | UnsetType = _UNSET,
) -> FullAPIOptions:
    return defaultAPIOptions(
        environment=src_database.api_options.environment
    ).with_override(
        APIOptions(
            callers=callers,
            embedding_api_key=embedding_api_key,
        )
    )


class TestCollectionsSync:
    @pytest.mark.describe("test of instantiating Collection, sync")
    def test_instantiate_collection_sync(
        self,
        sync_database: Database,
    ) -> None:
        col1: DefaultCollection = Collection(
            database=sync_database,
            name="id_test_collection",
            keyspace=None,
            api_options=_wrapSomeOptions(sync_database, callers=[("cn", "cv")]),
        )
        col2: DefaultCollection = Collection(
            database=sync_database,
            name="id_test_collection",
            keyspace=None,
            api_options=_wrapSomeOptions(sync_database, callers=[("cn", "cv")]),
        )
        assert col1 == col2

    @pytest.mark.describe("test of Collection conversions, sync")
    def test_convert_collection_sync(
        self,
        sync_database: Database,
    ) -> None:
        col1: DefaultCollection = Collection(
            database=sync_database,
            name="id_test_collection",
            keyspace=None,
            api_options=_wrapSomeOptions(sync_database, callers=[("cn", "cv")]),
        )
        assert col1 == col1._copy()
        assert col1 == col1.with_options()
        assert col1 == col1.to_async().to_sync()

    @pytest.mark.describe("test of Collection rich _copy, sync")
    def test_rich_copy_collection_sync(
        self,
        sync_database: Database,
    ) -> None:
        callers0 = [("cn", "cv"), ("dn", "dv")]
        col1: DefaultCollection = Collection(
            database=sync_database,
            name="id_test_collection",
            keyspace=None,
            api_options=_wrapSomeOptions(
                sync_database,
                callers=callers0,
                embedding_api_key="eak",
            ),
        )
        assert col1 != col1._copy(embedding_api_key="zak")

        col2 = col1._copy(
            embedding_api_key="zak",
        )
        assert col2 != col1

        assert col1.with_options(embedding_api_key="zak") != col1

        assert (
            col1.with_options(embedding_api_key="zak").with_options(
                embedding_api_key="eak"
            )
            == col1
        )

    @pytest.mark.describe("test of Collection rich conversions, sync")
    def test_rich_convert_collection_sync(
        self,
        sync_database: Database,
    ) -> None:
        callers0 = [("cn", "cv"), ("dn", "dv")]
        col1: DefaultCollection = Collection(
            database=sync_database,
            name="id_test_collection",
            keyspace="the_ks",
            api_options=_wrapSomeOptions(
                sync_database,
                callers=callers0,
                embedding_api_key="eak",
            ),
        )
        assert col1 != col1.to_async(embedding_api_key="zak").to_sync()

        col2a = col1.to_async(
            embedding_api_key="zak",
        )
        assert col2a.to_sync() != col1

        col3 = col2a.to_sync(
            embedding_api_key="eak",
        )
        assert col3 == col1

    @pytest.mark.describe("test of Collection database property, sync")
    def test_collection_database_property_sync(
        self,
    ) -> None:
        opts0 = defaultAPIOptions(environment="other")
        db1 = Database(api_endpoint="a", keyspace="ns1", api_options=opts0)
        db2 = Database(api_endpoint="a", keyspace="ns2", api_options=opts0)
        col1: DefaultCollection = Collection(
            database=db1,
            name="coll",
            keyspace=None,
            api_options=opts0,
        )
        col2: DefaultCollection = Collection(
            database=db1, name="coll", api_options=opts0, keyspace="ns2"
        )
        assert col1.database == db1
        assert col2.database == db2

    @pytest.mark.describe("test of Collection name property, sync")
    def test_collection_name_property_sync(
        self,
    ) -> None:
        opts0 = defaultAPIOptions(environment="other")
        db1 = Database(api_endpoint="a", keyspace="ns1", api_options=opts0)
        col1: DefaultCollection = Collection(
            database=db1,
            name="coll",
            keyspace=None,
            api_options=opts0,
        )
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

        col3: DefaultCollection = Collection(
            database=sync_database,
            name="id_test_collection",
            keyspace=None,
            api_options=sync_database.api_options,
        )
        assert col3.keyspace == sync_database.keyspace

        col4: DefaultCollection = Collection(
            database=sync_database,
            name="id_test_collection",
            keyspace=data_api_credentials_info["secondary_keyspace"],
            api_options=sync_database.api_options,
        )
        assert col4.keyspace == data_api_credentials_info["secondary_keyspace"]
        assert col1 == col3
        assert col2 == col4
