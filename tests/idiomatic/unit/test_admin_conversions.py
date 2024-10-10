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

from astrapy import (
    AstraDBAdmin,
    AstraDBDatabaseAdmin,
    AsyncDatabase,
    DataAPIClient,
    DataAPIDatabaseAdmin,
    Database,
)
from astrapy.authentication import StaticTokenProvider, UsernamePasswordTokenProvider
from astrapy.constants import Environment

api_ep0123_dev = (
    "https://01234567-89ab-cdef-0123-456789abcdef-region.apps.astra-dev.datastax.com"
)
api_ep7777_dev = (
    "https://77777777-89ab-cdef-0123-456789abcdef-region.apps.astra-dev.datastax.com"
)
api_ep9999_test = (
    "https://99999999-89ab-cdef-0123-456789abcdef-region.apps.astra-test.datastax.com"
)


class TestAdminConversions:
    @pytest.mark.describe("test of DataAPIClient conversions and comparison functions")
    def test_dataapiclient_conversions(self) -> None:
        callers0 = [("cn", "cv"), ("dn", "dv")]
        callers1 = [("x", "y")]
        dac1 = DataAPIClient(
            "t1",
            environment="dev",
            callers=callers0,
        )
        dac2 = DataAPIClient(
            "t1",
            environment="dev",
            callers=callers0,
        )
        assert dac1 == dac2

        assert dac1 != dac1._copy(token="x")
        assert dac1 != dac1._copy(environment="test")
        assert dac1 != dac1._copy(callers=callers1)

        assert dac1 == dac1._copy(token="x")._copy(token="t1")
        assert dac1 == dac1._copy(environment="test")._copy(environment="dev")
        assert dac1 == dac1._copy(callers=callers1)._copy(callers=callers0)

        assert dac1 != dac1.with_options(token="x")
        assert dac1 != dac1.with_options(callers=callers1)

        assert dac1 == dac1.with_options(token="x").with_options(token="t1")
        assert dac1 == dac1.with_options(callers=callers1).with_options(
            callers=callers0
        )

        a_e_string = (
            "https://01234567-89ab-cdef-0123-456789abcdef-us-east1"
            ".apps.astra-dev.datastax.com"
        )
        db1 = dac1[a_e_string]
        expected_db_1 = Database(
            api_endpoint=a_e_string,
            token="t1",
            callers=callers0,
        )
        assert db1 == expected_db_1
        with pytest.raises(ValueError):
            dac1["abc"]

    @pytest.mark.describe("test of spawning databases from a DataAPIClient")
    def test_dataapiclient_spawning_databases(self) -> None:
        token = "the-token"
        database_id = "00000000-1111-2222-3333-444444444444"
        database_region = "the-region"
        endpoint = f"https://{database_id}-{database_region}.apps.astra.datastax.com"

        client = DataAPIClient(
            token=token,
            environment=Environment.PROD,
            callers=[("cn", "cv")],
        )

        db1 = client.get_database(endpoint)
        db3 = client.get_database(endpoint)

        assert db1 == db3

    @pytest.mark.describe("test of AstraDBAdmin conversions and comparison functions")
    def test_astradbadmin_conversions(self) -> None:
        callers0 = [("cn", "cv"), ("dn", "dv")]
        callers1 = [("x", "y")]
        adm1 = AstraDBAdmin(
            "t1",
            environment="dev",
            callers=callers0,
            dev_ops_url="dou",
            dev_ops_api_version="dvv",
        )
        adm2 = AstraDBAdmin(
            "t1",
            environment="dev",
            callers=callers0,
            dev_ops_url="dou",
            dev_ops_api_version="dvv",
        )
        assert adm1 == adm2

        assert adm1 != adm1._copy(token="x")
        assert adm1 != adm1._copy(environment="test")
        assert adm1 != adm1._copy(callers=callers1)
        assert adm1 != adm1._copy(dev_ops_url="x")
        assert adm1 != adm1._copy(dev_ops_api_version="x")

        assert adm1 == adm1._copy(token="x")._copy(token="t1")
        assert adm1 == adm1._copy(environment="test")._copy(environment="dev")
        assert adm1 == adm1._copy(callers=callers1)._copy(callers=callers0)
        assert adm1 == adm1._copy(dev_ops_url="x")._copy(dev_ops_url="dou")
        assert adm1 == adm1._copy(dev_ops_api_version="x")._copy(
            dev_ops_api_version="dvv"
        )

        assert adm1 != adm1.with_options(token="x")
        assert adm1 != adm1.with_options(callers=callers1)

        assert adm1 == adm1.with_options(token="x").with_options(token="t1")
        assert adm1 == adm1.with_options(callers=callers1).with_options(
            callers=callers0
        )

    @pytest.mark.describe(
        "test of AstraDBDatabaseAdmin conversions and comparison functions"
    )
    def test_astradbdatabaseadmin_conversions(self) -> None:
        callers0 = [("cn", "cv"), ("dn", "dv")]
        callers1 = [("x", "y")]
        adda1 = AstraDBDatabaseAdmin(
            api_ep0123_dev,
            token="t1",
            environment="dev",
            callers=callers0,
            dev_ops_url="dou",
            dev_ops_api_version="dvv",
            api_path="appi",
            api_version="vX",
        )
        adda2 = AstraDBDatabaseAdmin(
            api_ep0123_dev,
            token="t1",
            environment="dev",
            callers=callers0,
            dev_ops_url="dou",
            dev_ops_api_version="dvv",
            api_path="appi",
            api_version="vX",
        )
        assert adda1 == adda2

        assert adda1 != adda1._copy(api_ep9999_test, environment="test")
        assert adda1 != adda1._copy(token="x")
        assert adda1 != adda1._copy(callers=callers1)
        assert adda1 != adda1._copy(dev_ops_url="x")
        assert adda1 != adda1._copy(dev_ops_api_version="x")
        assert adda1 != adda1._copy(api_path="x")
        assert adda1 != adda1._copy(api_version="x")

        assert adda1 == adda1._copy(token="x")._copy(token="t1")
        assert adda1 == adda1._copy(api_ep9999_test, environment="test")._copy(
            api_ep0123_dev, environment="dev"
        )
        assert adda1 == adda1._copy(callers=callers1)._copy(callers=callers0)
        assert adda1 == adda1._copy(dev_ops_url="x")._copy(dev_ops_url="dou")
        assert adda1 == adda1._copy(dev_ops_api_version="x")._copy(
            dev_ops_api_version="dvv"
        )
        assert adda1 == adda1._copy(api_path="x")._copy(api_path="appi")
        assert adda1 == adda1._copy(api_version="x")._copy(api_version="vX")

        assert adda1 != adda1.with_options(api_ep7777_dev)
        assert adda1 != adda1.with_options(token="x")
        assert adda1 != adda1.with_options(callers=callers1)

        assert adda1 == adda1.with_options(api_ep7777_dev).with_options(api_ep0123_dev)
        assert adda1 == adda1.with_options(token="x").with_options(token="t1")
        assert adda1 == adda1.with_options(callers=callers1).with_options(
            callers=callers0
        )

    @pytest.mark.describe(
        "test of DataAPIDBDatabaseAdmin conversions and comparison functions"
    )
    def test_dataapidatabaseadmin_conversions(self) -> None:
        callers0 = [("cn", "cv"), ("dn", "dv")]
        callers1 = [("x", "y")]
        dada1 = DataAPIDatabaseAdmin(
            "http://a.b.c:1234",
            token="t1",
            environment="hcd",
            api_path="appi",
            api_version="v9",
            callers=callers0,
        )
        dada2 = DataAPIDatabaseAdmin(
            "http://a.b.c:1234",
            token="t1",
            environment="hcd",
            api_path="appi",
            api_version="v9",
            callers=callers0,
        )
        assert dada1 == dada2

        assert dada1 != dada1._copy(api_endpoint="https://x.y.z:9876")
        assert dada1 != dada1._copy(token="tx")
        assert dada1 != dada1._copy(environment="en")
        assert dada1 != dada1._copy(api_path="ap")
        assert dada1 != dada1._copy(api_version="av")
        assert dada1 != dada1._copy(callers=callers1)

        assert dada1 == dada1._copy(api_endpoint="x")._copy(
            api_endpoint="http://a.b.c:1234"
        )
        assert dada1 == dada1._copy(token="x")._copy(token="t1")
        assert dada1 == dada1._copy(environment="x")._copy(environment="hcd")
        assert dada1 == dada1._copy(api_path="x")._copy(api_path="appi")
        assert dada1 == dada1._copy(api_version="x")._copy(api_version="v9")
        assert dada1 == dada1._copy(callers=callers1)._copy(callers=callers0)

        assert dada1 != dada1.with_options(api_endpoint="https://x.y.z:9876")
        assert dada1 != dada1.with_options(token="x")
        assert dada1 != dada1.with_options(callers=callers1)

        assert dada1 == dada1.with_options(
            api_endpoint="https://x.y.z:9876"
        ).with_options(api_endpoint="http://a.b.c:1234")
        assert dada1 == dada1.with_options(token="x").with_options(token="t1")
        assert dada1 == dada1.with_options(callers=callers1).with_options(
            callers=callers0
        )

    @pytest.mark.describe("test of token inheritance in spawning from DataAPIClient")
    def test_dataapiclient_token_inheritance(self) -> None:
        client_t = DataAPIClient(token=StaticTokenProvider("static"))
        client_0 = DataAPIClient()
        token_f = UsernamePasswordTokenProvider(username="u", password="p")
        client_f = DataAPIClient(token=token_f)
        a_e_string = (
            "https://01234567-89ab-cdef-0123-456789abcdef-us-east1"
            ".apps.astra.datastax.com"
        )

        assert client_t.get_database(
            a_e_string, token=token_f
        ) == client_f.get_database(a_e_string)
        assert client_0.get_database(
            a_e_string, token=token_f
        ) == client_f.get_database(a_e_string)

        assert client_t.get_database(
            a_e_string, token=token_f
        ) == client_f.get_database(a_e_string)
        assert client_0.get_database(
            a_e_string, token=token_f
        ) == client_f.get_database(a_e_string)

        assert client_t.get_admin(token=token_f) == client_f.get_admin()
        assert client_0.get_admin(token=token_f) == client_f.get_admin()

    @pytest.mark.describe("test of token inheritance in spawning from AstraDBAdmin")
    def test_astradbadmin_token_inheritance(self) -> None:
        admin_t = AstraDBAdmin(token=StaticTokenProvider("static"))
        admin_0 = AstraDBAdmin()
        token_f = UsernamePasswordTokenProvider(username="u", password="p")
        admin_f = AstraDBAdmin(token=token_f)
        db_id_string = "01234567-89ab-cdef-0123-456789abcdef"

        assert admin_t.get_database(
            db_id_string, token=token_f, keyspace="n", region="r"
        ) == admin_f.get_database(db_id_string, keyspace="n", region="r")
        assert admin_0.get_database(
            db_id_string, token=token_f, keyspace="n", region="r"
        ) == admin_f.get_database(db_id_string, keyspace="n", region="r")

    @pytest.mark.describe(
        "test of token inheritance in spawning from AstraDBDatabaseAdmin"
    )
    def test_astradbdatabaseadmin_token_inheritance(self) -> None:
        adbadmin_t = AstraDBDatabaseAdmin(
            api_ep0123_dev,
            token=StaticTokenProvider("static"),
            environment=Environment.DEV,
        )
        adbadmin_0 = AstraDBDatabaseAdmin(
            api_ep0123_dev,
            environment=Environment.DEV,
        )
        token_f = UsernamePasswordTokenProvider(username="u", password="p")
        adbadmin_f = AstraDBDatabaseAdmin(
            api_ep0123_dev,
            token=token_f,
            environment=Environment.DEV,
        )

        assert adbadmin_t.get_database(
            token=token_f,
            keyspace="n",
        ) == adbadmin_f.get_database(keyspace="n")
        assert adbadmin_0.get_database(
            token=token_f,
            keyspace="n",
        ) == adbadmin_f.get_database(keyspace="n")

    @pytest.mark.describe(
        "test of token inheritance in spawning from DataAPIDatabaseAdmin"
    )
    def test_dataapidatabaseadmin_token_inheritance(self) -> None:
        a_e_string = "http://x.y:123"
        dadbadmin_t = DataAPIDatabaseAdmin(
            a_e_string, token=StaticTokenProvider("static")
        )
        dadbadmin_0 = DataAPIDatabaseAdmin(a_e_string)
        token_f = UsernamePasswordTokenProvider(username="u", password="p")
        dadbadmin_f = DataAPIDatabaseAdmin(a_e_string, token=token_f)

        assert dadbadmin_t.get_database(
            token=token_f, keyspace="n"
        ) == dadbadmin_f.get_database(keyspace="n")
        assert dadbadmin_0.get_database(
            token=token_f, keyspace="n"
        ) == dadbadmin_f.get_database(keyspace="n")

    @pytest.mark.describe("test of token inheritance in spawning from Database")
    def test_database_token_inheritance(self) -> None:
        a_e_string = "http://x.y:123"
        database_t = Database(
            a_e_string,
            token=StaticTokenProvider("static"),
            environment=Environment.OTHER,
        )
        a_database_t = AsyncDatabase(
            a_e_string,
            token=StaticTokenProvider("static"),
            environment=Environment.OTHER,
        )
        database_0 = Database(a_e_string, environment=Environment.OTHER)
        a_database_0 = AsyncDatabase(a_e_string, environment=Environment.OTHER)
        token_f = UsernamePasswordTokenProvider(username="u", password="p")
        database_f = Database(a_e_string, token=token_f, environment=Environment.OTHER)
        a_database_f = AsyncDatabase(
            a_e_string, token=token_f, environment=Environment.OTHER
        )

        assert (
            database_t.get_database_admin(token=token_f)
            == database_f.get_database_admin()
        )
        assert (
            database_0.get_database_admin(token=token_f)
            == database_f.get_database_admin()
        )

        assert (
            a_database_t.get_database_admin(token=token_f)
            == a_database_f.get_database_admin()
        )
        assert (
            a_database_0.get_database_admin(token=token_f)
            == a_database_f.get_database_admin()
        )

    @pytest.mark.describe(
        "test of id, endpoint, region normalization in get_database(_admin)"
    )
    def test_param_normalize_getdatabase(self) -> None:
        # the case of ID only is deferred to an integration test (it's impure)
        api_ep = "https://01234567-89ab-cdef-0123-456789abcdef-the-region.apps.astra.datastax.com"
        db_id = "01234567-89ab-cdef-0123-456789abcdef"
        db_reg = "the-region"

        adm = AstraDBAdmin("t1")

        db_adm1 = adm.get_database_admin(db_id, region=db_reg)
        with pytest.raises(ValueError):
            adm.get_database_admin(api_ep, region=db_reg)
        db_adm3 = adm.get_database_admin(api_ep)
        with pytest.raises(ValueError):
            adm.get_database_admin(api_ep, region="not-that-one")

        assert db_adm1 == db_adm3

        db_1 = adm.get_database(db_id, region=db_reg, keyspace="the_ks")
        with pytest.raises(ValueError):
            adm.get_database(api_ep, region=db_reg, keyspace="the_ks")
        db_3 = adm.get_database(api_ep, keyspace="the_ks")
        with pytest.raises(ValueError):
            adm.get_database(api_ep, region="not-that-one", keyspace="the_ks")

        assert db_1 == db_3

    @pytest.mark.describe(
        "test of spawner_database for AstraDBDatabaseAdmin if not provided"
    )
    def test_spawnerdatabase_astradbdatabaseadmin_notprovided(self) -> None:
        api_ep = "https://01234567-89ab-cdef-0123-456789abcdef-the-region.apps.astra.datastax.com"
        db_adm = AstraDBDatabaseAdmin(api_ep)
        assert db_adm.spawner_database.api_endpoint == api_ep

    @pytest.mark.describe(
        "test of spawner_database for DataAPIDatabaseAdmin if not provided"
    )
    def test_spawnerdatabase_dataapidatabaseadmin_notprovided(self) -> None:
        api_ep = "http://aa"
        db_adm = DataAPIDatabaseAdmin(api_ep)
        assert db_adm.spawner_database.api_endpoint == api_ep

    @pytest.mark.describe(
        "test of spawner_database for AstraDBDatabaseAdmin, sync db provided"
    )
    def test_spawnerdatabase_astradbdatabaseadmin_syncprovided(self) -> None:
        api_ep = "https://01234567-89ab-cdef-0123-456789abcdef-the-region.apps.astra.datastax.com"
        db = Database(api_ep, keyspace="M")
        db_adm = AstraDBDatabaseAdmin(api_ep, spawner_database=db)
        assert db_adm.spawner_database is db

    @pytest.mark.describe(
        "test of spawner_database for AstraDBDatabaseAdmin, async db provided"
    )
    def test_spawnerdatabase_astradbdatabaseadmin_asyncprovided(self) -> None:
        api_ep = "https://01234567-89ab-cdef-0123-456789abcdef-the-region.apps.astra.datastax.com"
        adb = AsyncDatabase(api_ep, keyspace="M")
        db_adm = AstraDBDatabaseAdmin(api_ep, spawner_database=adb)
        assert db_adm.spawner_database is adb

    @pytest.mark.describe(
        "test of spawner_database for DataAPIDatabaseAdmin, sync db provided"
    )
    def test_spawnerdatabase_dataapidatabaseadmin_syncprovided(self) -> None:
        api_ep = "http://aa"
        db = Database(api_ep)
        db_adm = DataAPIDatabaseAdmin(api_ep, spawner_database=db)
        assert db_adm.spawner_database is db

    @pytest.mark.describe(
        "test of spawner_database for DataAPIDatabaseAdmin, async db provided"
    )
    def test_spawnerdatabase_dataapidatabaseadmin_asyncprovided(self) -> None:
        api_ep = "http://aa"
        adb = AsyncDatabase(api_ep)
        db_adm = DataAPIDatabaseAdmin(api_ep, spawner_database=adb)
        assert db_adm.spawner_database is adb

    @pytest.mark.describe("test of from_api_endpoint for AstraDBDatabaseAdmin")
    def test_fromapiendpoint_astradbdatabaseadmin(self) -> None:
        api_ep = "https://01234567-89ab-cdef-0123-456789abcdef-the-region.apps.astra.datastax.com"
        db_adm = AstraDBDatabaseAdmin.from_api_endpoint(api_ep, token="t")
        assert db_adm.get_database(keyspace="M").api_endpoint == api_ep
