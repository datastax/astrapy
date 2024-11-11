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

from astrapy import Collection, DataAPIClient, Database
from astrapy.authentication import StaticTokenProvider
from astrapy.constants import Environment
from astrapy.exceptions import DevOpsAPIException
from astrapy.settings.defaults import DEFAULT_ASTRA_DB_KEYSPACE
from astrapy.utils.api_options import (
    APIOptions,
    DataAPIURLOptions,
    defaultAPIOptions,
)

from ..conftest import (
    TEST_COLLECTION_INSTANCE_NAME,
    DataAPICredentials,
    DataAPICredentialsInfo,
    DefaultCollection,
)

api_ep5643_prod = (
    "https://56439999-89ab-cdef-0123-456789abcdef-region.apps.astra.datastax.com"
)


class TestDatabasesSync:
    @pytest.mark.describe("test of instantiating Database, sync")
    def test_instantiate_database_sync(
        self,
        data_api_credentials_kwargs: DataAPICredentials,
        data_api_credentials_info: DataAPICredentialsInfo,
    ) -> None:
        opts0 = defaultAPIOptions(environment=data_api_credentials_info["environment"])
        opts = opts0.with_override(
            APIOptions(
                token=data_api_credentials_kwargs["token"],
            )
        )

        db1 = Database(
            api_endpoint=data_api_credentials_kwargs["api_endpoint"],
            keyspace=data_api_credentials_kwargs["keyspace"],
            api_options=opts,
        )
        db2 = Database(
            api_endpoint=data_api_credentials_kwargs["api_endpoint"],
            keyspace=data_api_credentials_kwargs["keyspace"],
            api_options=opts,
        )
        assert db1 == db2

    @pytest.mark.describe("test of Database conversions, sync")
    def test_convert_database_sync(
        self,
        data_api_credentials_kwargs: DataAPICredentials,
        data_api_credentials_info: DataAPICredentialsInfo,
    ) -> None:
        opts0 = defaultAPIOptions(environment=data_api_credentials_info["environment"])
        opts = opts0.with_override(
            APIOptions(
                token=data_api_credentials_kwargs["token"],
            )
        )

        db1 = Database(
            api_endpoint=data_api_credentials_kwargs["api_endpoint"],
            keyspace=data_api_credentials_kwargs["keyspace"],
            api_options=opts,
        )
        assert db1 == db1._copy()
        assert db1 == db1.with_options()
        assert db1 == db1.to_async().to_sync()

    @pytest.mark.describe("test of Database rich _copy, sync")
    def test_rich_copy_database_sync(
        self,
    ) -> None:
        callers0 = [("cn", "cv"), ("dn", "dv")]
        callers1 = [("x", "y")]
        db1 = Database(
            api_endpoint="api_endpoint",
            keyspace="keyspace",
            api_options=defaultAPIOptions(environment="other").with_override(
                APIOptions(
                    token=StaticTokenProvider("token"),
                    callers=callers0,
                    data_api_url_options=DataAPIURLOptions(
                        api_path="api_path",
                        api_version="api_version",
                    ),
                ),
            ),
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

    @pytest.mark.describe("test of Database rich conversions, sync")
    def test_rich_convert_database_sync(
        self,
    ) -> None:
        callers0 = [("cn", "cv"), ("dn", "dv")]
        callers1 = [("x", "y")]
        db1 = Database(
            api_endpoint="api_endpoint",
            keyspace="keyspace",
            api_options=defaultAPIOptions(environment="other").with_override(
                APIOptions(
                    token=StaticTokenProvider("token"),
                    callers=callers0,
                    data_api_url_options=DataAPIURLOptions(
                        api_path="api_path",
                        api_version="api_version",
                    ),
                ),
            ),
        )
        assert db1 != db1.to_async(api_endpoint="o").to_sync()
        assert db1 != db1.to_async(token="o").to_sync()
        assert db1 != db1.to_async(keyspace="o").to_sync()
        assert db1 != db1.to_async(callers=callers1).to_sync()
        assert db1 != db1.to_async(api_path="o").to_sync()
        assert db1 != db1.to_async(api_version="o").to_sync()

        db2a = db1.to_async(
            api_endpoint="x",
            token="x",
            keyspace="x",
            callers=callers1,
            api_path="x",
            api_version="x",
        )
        assert db2a.to_sync() != db1

        db3 = db2a.to_sync(
            api_endpoint="api_endpoint",
            token="token",
            keyspace="keyspace",
            callers=callers0,
            api_path="api_path",
            api_version="api_version",
        )
        assert db3 == db1

    @pytest.mark.describe("test get_collection method, sync")
    def test_database_get_collection_sync(
        self,
        sync_database: Database,
        sync_collection_instance: DefaultCollection,
        data_api_credentials_kwargs: DataAPICredentials,
    ) -> None:
        collection = sync_database.get_collection(TEST_COLLECTION_INSTANCE_NAME)
        assert collection == sync_collection_instance

        assert getattr(sync_database, TEST_COLLECTION_INSTANCE_NAME) == collection
        assert sync_database[TEST_COLLECTION_INSTANCE_NAME] == collection

        KEYSPACE_2 = "other_keyspace"
        collection_ks2 = sync_database.get_collection(
            TEST_COLLECTION_INSTANCE_NAME, keyspace=KEYSPACE_2
        )
        assert collection_ks2 == Collection(
            database=sync_database,
            name=TEST_COLLECTION_INSTANCE_NAME,
            keyspace=KEYSPACE_2,
            api_options=sync_database.api_options,
        )
        assert collection_ks2.database.keyspace == KEYSPACE_2

    @pytest.mark.describe("test database id and region, sync")
    def test_database_id_region_sync(self) -> None:
        db1 = Database(
            api_endpoint="https://a1234567-89ab-cdef-0123-456789abcdef-us-central1.apps.astra-dev.datastax.com",
            keyspace="k",
            api_options=defaultAPIOptions(environment="dev"),
        )
        assert db1.id == "a1234567-89ab-cdef-0123-456789abcdef"
        assert db1.region == "us-central1"

        db2 = Database(
            api_endpoint="http://localhost:12345",
            keyspace="k",
            api_options=defaultAPIOptions(environment="dev"),
        )
        with pytest.raises(DevOpsAPIException):
            db2.id
        with pytest.raises(DevOpsAPIException):
            db2.region

    @pytest.mark.describe("test database default keyspace per environment, sync")
    def test_database_default_keyspace_per_environment_sync(self) -> None:
        opts_p = defaultAPIOptions(environment=Environment.PROD)
        opts_o = defaultAPIOptions(environment=Environment.OTHER)
        db_a_m = Database(
            api_endpoint="ep",
            keyspace="M",
            api_options=opts_p,
        )
        assert db_a_m.keyspace == "M"
        db_o_m = Database(
            api_endpoint="ep",
            keyspace="M",
            api_options=opts_o,
        )
        assert db_o_m.keyspace == "M"
        db_a_n = Database(
            api_endpoint="ep",
            keyspace=None,
            api_options=opts_p,
        )
        assert db_a_n.keyspace == DEFAULT_ASTRA_DB_KEYSPACE
        db_o_n = Database(
            api_endpoint="ep",
            keyspace=None,
            api_options=opts_o,
        )
        assert db_o_n.keyspace is None

    @pytest.mark.describe(
        "test database-from-client default keyspace per environment, sync"
    )
    def test_database_from_client_default_keyspace_per_environment_sync(self) -> None:
        client_a = DataAPIClient(environment=Environment.PROD)
        db_a_me = client_a.get_database(api_ep5643_prod, keyspace="M")
        assert db_a_me.keyspace == "M"
        db_a_ne = client_a.get_database(api_ep5643_prod)
        assert db_a_ne.keyspace == DEFAULT_ASTRA_DB_KEYSPACE

        client_o = DataAPIClient(environment=Environment.OTHER)
        db_a_m = client_o.get_database("http://a", keyspace="M")
        assert db_a_m.keyspace == "M"
        db_a_n = client_o.get_database("http://a")
        assert db_a_n.keyspace is None

    @pytest.mark.describe(
        "test database-from-dataapidbadmin default keyspace per environment, sync"
    )
    def test_database_from_dataapidbadmin_default_keyspace_per_environment_sync(
        self,
    ) -> None:
        client = DataAPIClient(environment=Environment.OTHER)
        db_admin = client.get_database("http://a").get_database_admin()
        db_m = db_admin.get_database(keyspace="M")
        assert db_m.keyspace == "M"
        db_n = db_admin.get_database()
        assert db_n.keyspace is None

    @pytest.mark.describe("test of database keyspace property, sync")
    def test_database_keyspace_property_sync(
        self,
        sync_database: Database,
    ) -> None:
        assert isinstance(sync_database.keyspace, str)
