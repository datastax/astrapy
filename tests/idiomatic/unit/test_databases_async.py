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

from astrapy import AsyncCollection, AsyncDatabase, DataAPIClient
from astrapy.constants import Environment
from astrapy.defaults import DEFAULT_ASTRA_DB_KEYSPACE
from astrapy.exceptions import DevOpsAPIException

from ..conftest import (
    TEST_COLLECTION_INSTANCE_NAME,
    DataAPICredentials,
    async_fail_if_not_removed,
    sync_fail_if_not_removed,
)


class TestDatabasesAsync:
    @pytest.mark.describe("test of instantiating Database, async")
    async def test_instantiate_database_async(
        self,
        data_api_credentials_kwargs: DataAPICredentials,
    ) -> None:
        db1 = AsyncDatabase(
            callers=[("c_n", "c_v")],
            **data_api_credentials_kwargs,
        )
        db2 = AsyncDatabase(
            callers=[("c_n", "c_v")],
            **data_api_credentials_kwargs,
        )
        assert db1 == db2

    @pytest.mark.describe("test of Database conversions, async")
    async def test_convert_database_async(
        self,
        data_api_credentials_kwargs: DataAPICredentials,
    ) -> None:
        db1 = AsyncDatabase(
            callers=[("c_n", "c_v")],
            **data_api_credentials_kwargs,
        )
        assert db1 == db1._copy()
        assert db1 == db1.with_options()
        assert db1 == db1.to_sync().to_async()

    @async_fail_if_not_removed
    @pytest.mark.describe("test of caller deprecation in Database rich _copy, async")
    async def test_caller_deprecation_in_rich_copy_database_async(
        self,
    ) -> None:
        with pytest.warns(DeprecationWarning):
            db1 = AsyncDatabase(
                api_endpoint="api_endpoint",
                token="token",
                keyspace="keyspace",
                caller_name="c_n",
                caller_version="c_v",
                api_path="api_path",
                api_version="api_version",
            )
        assert db1 == AsyncDatabase(
            api_endpoint="api_endpoint",
            token="token",
            keyspace="keyspace",
            callers=[("c_n", "c_v")],
            api_path="api_path",
            api_version="api_version",
        )
        with pytest.raises(ValueError, match="`caller_name` and `caller_version`"):
            assert db1 == AsyncDatabase(
                api_endpoint="api_endpoint",
                token="token",
                keyspace="keyspace",
                callers=[("c_n", "c_v")],
                caller_name="c_n",
            )
        with pytest.raises(ValueError, match="`caller_name` and `caller_version`"):
            assert db1 == AsyncDatabase(
                api_endpoint="api_endpoint",
                token="token",
                keyspace="keyspace",
                callers=[("c_n", "c_v")],
                caller_version="c_v",
            )
        with pytest.warns(DeprecationWarning):
            assert db1 != db1._copy(caller_name="x", caller_version="x")
        with pytest.warns(DeprecationWarning):
            assert db1 != db1._copy(caller_name="x")
        with pytest.warns(DeprecationWarning):
            assert db1 != db1._copy(caller_version="x")

        with pytest.warns(DeprecationWarning):
            db2 = db1._copy(
                api_endpoint="x",
                token="x",
                keyspace="x",
                caller_name="x_n",
                caller_version="x_v",
                api_path="x",
                api_version="x",
            )
        assert db2 != db1

        with pytest.warns(DeprecationWarning):
            db2.set_caller(
                caller_name="c_n",
                caller_version="c_v",
            )
        db3 = db2._copy(
            api_endpoint="api_endpoint",
            token="token",
            keyspace="keyspace",
            api_path="api_path",
            api_version="api_version",
        )
        assert db3 == db1

        with pytest.warns(DeprecationWarning):
            assert db1.with_options(caller_name="x") != db1
        with pytest.warns(DeprecationWarning):
            assert db1.with_options(caller_version="x") != db1

        with pytest.warns(DeprecationWarning):
            assert (
                db1.with_options(caller_name="x", caller_version="x").with_options(
                    caller_name="c_n", caller_version="c_v"
                )
                == db1
            )

    @pytest.mark.describe("test of Database rich _copy, async")
    async def test_rich_copy_database_async(
        self,
    ) -> None:
        callers0 = [("cn", "cv"), ("dn", "dv")]
        callers1 = [("x", "y")]
        db1 = AsyncDatabase(
            api_endpoint="api_endpoint",
            token="token",
            keyspace="keyspace",
            callers=callers0,
            api_path="api_path",
            api_version="api_version",
        )
        assert db1 != db1._copy(api_endpoint="x")
        assert db1 != db1._copy(token="x")
        assert db1 != db1._copy(keyspace="x")
        assert db1 != db1._copy(callers=callers1)
        assert db1 != db1._copy(api_path="x")
        assert db1 != db1._copy(api_version="x")

        db2 = db1._copy(
            api_endpoint="x",
            token="x",
            keyspace="x",
            callers=callers1,
            api_path="x",
            api_version="x",
        )
        assert db2 != db1

        assert db1.with_options(keyspace="x") != db1
        assert db1.with_options(callers=callers1) != db1

        assert db1.with_options(keyspace="x").with_options(keyspace="keyspace") == db1
        assert db1.with_options(callers=callers1).with_options(callers=callers0) == db1

    @async_fail_if_not_removed
    @pytest.mark.describe(
        "test of caller deprecation in Database rich conversions, async"
    )
    async def test_caller_deprecation_in_rich_convert_database_async(
        self,
    ) -> None:
        with pytest.warns(DeprecationWarning):
            db1 = AsyncDatabase(
                api_endpoint="api_endpoint",
                token="token",
                keyspace="keyspace",
                caller_name="c_n",
                caller_version="c_v",
                api_path="api_path",
                api_version="api_version",
            )
        with pytest.warns(DeprecationWarning):
            assert db1 != db1.to_sync(caller_name="o").to_async()
        with pytest.warns(DeprecationWarning):
            assert db1 != db1.to_sync(caller_version="o").to_async()

        with pytest.warns(DeprecationWarning):
            db2s = db1.to_sync(
                api_endpoint="x",
                token="x",
                keyspace="x",
                caller_name="x_n",
                caller_version="x_v",
                api_path="x",
                api_version="x",
            )
        assert db2s.to_async() != db1

        with pytest.warns(DeprecationWarning):
            db2s.set_caller(
                caller_name="c_n",
                caller_version="c_v",
            )
        db3 = db2s.to_async(
            api_endpoint="api_endpoint",
            token="token",
            keyspace="keyspace",
            api_path="api_path",
            api_version="api_version",
        )
        assert db3 == db1

    @pytest.mark.describe("test of Database rich conversions, async")
    async def test_rich_convert_database_async(
        self,
    ) -> None:
        callers0 = [("cn", "cv"), ("dn", "dv")]
        callers1 = [("x", "y")]
        db1 = AsyncDatabase(
            api_endpoint="api_endpoint",
            token="token",
            keyspace="keyspace",
            callers=callers0,
            api_path="api_path",
            api_version="api_version",
        )
        assert db1 != db1.to_sync(api_endpoint="o").to_async()
        assert db1 != db1.to_sync(token="o").to_async()
        assert db1 != db1.to_sync(keyspace="o").to_async()
        assert db1 != db1.to_sync(callers=callers1).to_async()
        assert db1 != db1.to_sync(api_path="o").to_async()
        assert db1 != db1.to_sync(api_version="o").to_async()

        db2s = db1.to_sync(
            api_endpoint="x",
            token="x",
            keyspace="x",
            callers=callers1,
            api_path="x",
            api_version="x",
        )
        assert db2s.to_async() != db1

        db3 = db2s.to_async(
            api_endpoint="api_endpoint",
            token="token",
            keyspace="keyspace",
            callers=callers0,
            api_path="api_path",
            api_version="api_version",
        )
        assert db3 == db1

    @async_fail_if_not_removed
    @pytest.mark.describe("test of Database set_caller, async")
    async def test_database_set_caller_async(
        self,
        data_api_credentials_kwargs: DataAPICredentials,
    ) -> None:
        with pytest.warns(DeprecationWarning):
            db1 = AsyncDatabase(
                caller_name="c_n1",
                caller_version="c_v1",
                **data_api_credentials_kwargs,
            )
        with pytest.warns(DeprecationWarning):
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

        NAMESPACE_2 = "other_keyspace"
        collection_ks2 = await async_database.get_collection(
            TEST_COLLECTION_INSTANCE_NAME, keyspace=NAMESPACE_2
        )
        assert collection_ks2 == AsyncCollection(
            async_database, TEST_COLLECTION_INSTANCE_NAME, keyspace=NAMESPACE_2
        )
        assert collection_ks2.database.keyspace == NAMESPACE_2

    @async_fail_if_not_removed
    @pytest.mark.describe("test database conversions with caller mutableness, async")
    async def test_database_conversions_caller_mutableness_async(
        self,
        data_api_credentials_kwargs: DataAPICredentials,
    ) -> None:
        with pytest.warns(DeprecationWarning):
            db1 = AsyncDatabase(
                caller_name="c_n1",
                caller_version="c_v1",
                **data_api_credentials_kwargs,
            )
        db1.set_caller(
            caller_name="c_n2",
            caller_version="c_v2",
        )
        with pytest.warns(DeprecationWarning):
            db2 = AsyncDatabase(
                caller_name="c_n2",
                caller_version="c_v2",
                **data_api_credentials_kwargs,
            )
        assert db1.to_sync().to_async() == db2
        assert db1._copy() == db2

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

    @pytest.mark.describe("test database default keyspace per environment, async")
    async def test_database_default_keyspace_per_environment_async(self) -> None:
        db_a_m = AsyncDatabase(
            "ep", token="t", keyspace="M", environment=Environment.PROD
        )
        assert db_a_m.keyspace == "M"
        db_o_m = AsyncDatabase(
            "ep", token="t", keyspace="M", environment=Environment.OTHER
        )
        assert db_o_m.keyspace == "M"
        db_a_n = AsyncDatabase("ep", token="t", environment=Environment.PROD)
        assert db_a_n.keyspace == DEFAULT_ASTRA_DB_KEYSPACE
        db_o_n = AsyncDatabase("ep", token="t", environment=Environment.OTHER)
        assert db_o_n.keyspace is None

    @pytest.mark.describe(
        "test database-from-client default keyspace per environment, async"
    )
    async def test_database_from_client_default_keyspace_per_environment_async(
        self,
    ) -> None:
        client_a = DataAPIClient(environment=Environment.PROD)
        db_a_m = client_a.get_async_database("ep", region="r", keyspace="M")
        assert db_a_m.keyspace == "M"
        db_a_n = client_a.get_async_database("ep", region="r")
        assert db_a_n.keyspace == DEFAULT_ASTRA_DB_KEYSPACE

        client_o = DataAPIClient(environment=Environment.OTHER)
        db_a_m = client_o.get_async_database("http://a", keyspace="M")
        assert db_a_m.keyspace == "M"
        db_a_n = client_o.get_async_database("http://a")
        assert db_a_n.keyspace is None

    @pytest.mark.describe(
        "test database-from-dataapidbadmin default keyspace per environment, async"
    )
    async def test_database_from_dataapidbadmin_default_keyspace_per_environment_async(
        self,
    ) -> None:
        client = DataAPIClient(environment=Environment.OTHER)
        db_admin = client.get_async_database("http://a").get_database_admin()
        db_m = db_admin.get_async_database(keyspace="M")
        assert db_m.keyspace == "M"
        db_n = db_admin.get_async_database()
        assert db_n.keyspace is None

    @sync_fail_if_not_removed
    @pytest.mark.describe("test of database keyspace property, async")
    def test_database_keyspace_property_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        with pytest.warns(DeprecationWarning):
            async_database.namespace
        assert async_database.namespace == async_database.keyspace
