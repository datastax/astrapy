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
from astrapy.authentication import (
    StaticTokenProvider,
    UsernamePasswordTokenProvider,
)
from astrapy.constants import Environment
from astrapy.utils.api_options import (
    APIOptions,
    DataAPIURLOptions,
    DevOpsAPIURLOptions,
    defaultAPIOptions,
)

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
        opts0 = defaultAPIOptions(environment="dev").with_override(
            APIOptions(
                token="t1",
                callers=callers0,
            )
        )
        expected_db_1 = Database(
            api_endpoint=a_e_string,
            keyspace=None,
            api_options=opts0,
        )

        assert db1 == expected_db_1
        with pytest.raises(ValueError):
            dac1["abc"]

        # equivalence between passing api_options and named parameters; option override
        dac1_opt = DataAPIClient(
            environment="dev",
            api_options=APIOptions(
                token="t1",
                callers=callers0,
            ),
        )
        dac1_opt_ovr = DataAPIClient(
            "t1",
            environment="dev",
            callers=callers0,
            api_options=APIOptions(
                token="t_another",
                callers=callers1,
            ),
        )
        assert dac1 == dac1_opt
        assert dac1 == dac1_opt_ovr

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
        db2 = client.get_database(endpoint)
        assert db1 == db2

        # api_options and override check
        db2_param = client.get_database(endpoint, token="t1")
        db2_opt = client.get_database(
            endpoint,
            api_options=APIOptions(
                token="t1",
            ),
        )
        db2_opt_override = client.get_database(
            endpoint,
            token="t1",
            api_options=APIOptions(
                token="t_another",
            ),
        )
        assert db2_param == db2_opt
        assert db2_param == db2_opt_override

    @pytest.mark.describe("test of AstraDBAdmin conversions and comparison functions")
    def test_astradbadmin_conversions(self) -> None:
        callers0 = [("cn", "cv"), ("dn", "dv")]
        callers1 = [("x", "y")]
        opts0 = defaultAPIOptions(environment="dev").with_override(
            APIOptions(
                token="t1",
                environment="dev",
                callers=callers0,
                dev_ops_api_url_options=DevOpsAPIURLOptions(
                    dev_ops_url="dou",
                    dev_ops_api_version="dvv",
                ),
            ),
        )
        adm0 = AstraDBAdmin(api_options=opts0)
        adm1 = AstraDBAdmin(api_options=opts0)

        assert adm0 == adm1
        assert adm0 != adm0._copy(token="x")
        assert adm0 != adm0._copy(environment="test")
        assert adm0 != adm0._copy(callers=callers1)
        assert adm0 != adm0._copy(dev_ops_url="x")
        assert adm0 != adm0._copy(dev_ops_api_version="x")

        assert adm0 == adm0._copy(token="x")._copy(token="t1")
        assert adm0 == adm0._copy(environment="test")._copy(environment="dev")
        assert adm0 == adm0._copy(callers=callers1)._copy(callers=callers0)
        assert adm0 == adm0._copy(dev_ops_url="x")._copy(dev_ops_url="dou")
        assert adm0 == adm0._copy(dev_ops_api_version="x")._copy(
            dev_ops_api_version="dvv"
        )
        assert adm0 != adm0.with_options(token="x")
        assert adm0 != adm0.with_options(callers=callers1)
        assert adm0 == adm0.with_options(token="x").with_options(token="t1")
        assert adm0 == adm0.with_options(callers=callers1).with_options(
            callers=callers0
        )

    @pytest.mark.describe(
        "test of AstraDBDatabaseAdmin conversions and comparison functions"
    )
    def test_astradbdatabaseadmin_conversions(self) -> None:
        callers0 = [("cn", "cv"), ("dn", "dv")]
        callers1 = [("x", "y")]
        opts0 = defaultAPIOptions(environment="dev").with_override(
            APIOptions(
                token="t1",
                environment="dev",
                callers=callers0,
                dev_ops_api_url_options=DevOpsAPIURLOptions(
                    dev_ops_url="dou",
                    dev_ops_api_version="dvv",
                ),
                data_api_url_options=DataAPIURLOptions(
                    api_path="appi",
                    api_version="vX",
                ),
            ),
        )
        adda1 = AstraDBDatabaseAdmin(
            api_endpoint=api_ep0123_dev,
            api_options=opts0,
        )
        adda2 = AstraDBDatabaseAdmin(
            api_endpoint=api_ep0123_dev,
            api_options=opts0,
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

        assert adda1 != adda1.with_options(api_endpoint=api_ep7777_dev)
        assert adda1 != adda1.with_options(token="x")
        assert adda1 != adda1.with_options(callers=callers1)

        assert adda1 == adda1.with_options(api_endpoint=api_ep7777_dev).with_options(
            api_endpoint=api_ep0123_dev
        )
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
        opts0 = defaultAPIOptions(environment="hcd").with_override(
            APIOptions(
                token="t1",
                environment="hcd",
                callers=callers0,
                data_api_url_options=DataAPIURLOptions(
                    api_path="appi",
                    api_version="v9",
                ),
            ),
        )
        dada1 = DataAPIDatabaseAdmin(
            api_endpoint="http://a.b.c:1234",
            api_options=opts0,
        )
        dada2 = DataAPIDatabaseAdmin(
            api_endpoint="http://a.b.c:1234",
            api_options=opts0,
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

    @pytest.mark.describe("test of client.get_admin option passing")
    def test_client_get_admin_option_passing(self) -> None:
        client_0 = DataAPIClient()
        admin_opt = client_0.get_admin(
            api_options=APIOptions(
                token="tx",
            ),
        )
        admin_param = client_0.get_admin(token="tx")
        admin_opt_param = client_0.get_admin(
            token="tx",
            api_options=APIOptions(
                token="t_another",
            ),
        )

        assert admin_opt == admin_param
        assert admin_opt == admin_opt_param

    @pytest.mark.describe("test of token inheritance in spawning from AstraDBAdmin")
    def test_astradbadmin_token_inheritance(self) -> None:
        opts_0 = defaultAPIOptions(environment=Environment.PROD)
        opts_t = opts_0.with_override(
            APIOptions(token=StaticTokenProvider("static")),
        )
        token_f = UsernamePasswordTokenProvider(username="u", password="p")
        opts_f = opts_0.with_override(APIOptions(token=token_f))
        admin_t = AstraDBAdmin(api_options=opts_t)
        admin_0 = AstraDBAdmin(api_options=opts_0)
        admin_f = AstraDBAdmin(api_options=opts_f)
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
        opts_0 = defaultAPIOptions(environment=Environment.DEV)
        opts_t = opts_0.with_override(
            APIOptions(token=StaticTokenProvider("static")),
        )
        token_f = UsernamePasswordTokenProvider(username="u", password="p")
        opts_f = opts_0.with_override(APIOptions(token=token_f))
        adbadmin_t = AstraDBDatabaseAdmin(
            api_endpoint=api_ep0123_dev, api_options=opts_t
        )
        adbadmin_0 = AstraDBDatabaseAdmin(
            api_endpoint=api_ep0123_dev, api_options=opts_0
        )
        adbadmin_f = AstraDBDatabaseAdmin(
            api_endpoint=api_ep0123_dev, api_options=opts_f
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
        opts_0 = defaultAPIOptions(environment=Environment.PROD)
        opts_t = opts_0.with_override(
            APIOptions(token=StaticTokenProvider("static")),
        )
        token_f = UsernamePasswordTokenProvider(username="u", password="p")
        opts_f = opts_0.with_override(
            APIOptions(token=token_f),
        )
        dadbadmin_t = DataAPIDatabaseAdmin(api_endpoint=a_e_string, api_options=opts_t)
        dadbadmin_0 = DataAPIDatabaseAdmin(api_endpoint=a_e_string, api_options=opts_0)
        dadbadmin_f = DataAPIDatabaseAdmin(api_endpoint=a_e_string, api_options=opts_f)

        assert dadbadmin_t.get_database(
            token=token_f, keyspace="n"
        ) == dadbadmin_f.get_database(keyspace="n")
        assert dadbadmin_0.get_database(
            token=token_f, keyspace="n"
        ) == dadbadmin_f.get_database(keyspace="n")

    @pytest.mark.describe("test of token inheritance in spawning from Database, Astra")
    def test_database_token_inheritance_astra(self) -> None:
        opts_0 = defaultAPIOptions(environment=Environment.TEST)
        opts_t = opts_0.with_override(
            APIOptions(token=StaticTokenProvider("static")),
        )
        token_f = UsernamePasswordTokenProvider(username="u", password="p")
        opts_f = opts_0.with_override(
            APIOptions(token=token_f),
        )
        database_t = Database(
            api_endpoint=api_ep9999_test, keyspace="k", api_options=opts_t
        )
        a_database_t = AsyncDatabase(
            api_endpoint=api_ep9999_test, keyspace="k", api_options=opts_t
        )
        database_0 = Database(
            api_endpoint=api_ep9999_test, keyspace="k", api_options=opts_0
        )
        a_database_0 = AsyncDatabase(
            api_endpoint=api_ep9999_test, keyspace="k", api_options=opts_0
        )
        database_f = Database(
            api_endpoint=api_ep9999_test, keyspace="k", api_options=opts_f
        )
        a_database_f = AsyncDatabase(
            api_endpoint=api_ep9999_test, keyspace="k", api_options=opts_f
        )

        assert isinstance(a_database_0.get_database_admin(), AstraDBDatabaseAdmin)
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

    @pytest.mark.describe("test of option override in getting a DB admin from a DB")
    def test_database_get_database_admin_options(self) -> None:
        opts_0 = defaultAPIOptions(environment=Environment.TEST)
        database_0 = Database(
            api_endpoint=api_ep9999_test, keyspace="k", api_options=opts_0
        )

        db_admin_param = database_0.get_database_admin(token="tx")
        db_admin_opt = database_0.get_database_admin(
            admin_api_options=APIOptions(token="tx"),
        )
        db_admin_opt_param = database_0.get_database_admin(
            token="tx",
            admin_api_options=APIOptions(token="t_another"),
        )
        assert db_admin_param == db_admin_opt
        assert db_admin_param == db_admin_opt_param

        adatabase_0 = database_0.to_async()

        adb_admin_param = adatabase_0.get_database_admin(token="tx")
        adb_admin_opt = adatabase_0.get_database_admin(
            admin_api_options=APIOptions(token="tx"),
        )
        adb_admin_opt_param = adatabase_0.get_database_admin(
            token="tx",
            admin_api_options=APIOptions(token="t_another"),
        )
        assert adb_admin_param == adb_admin_opt
        assert adb_admin_param == adb_admin_opt_param

        assert adb_admin_param == db_admin_param

    @pytest.mark.describe(
        "test of token inheritance in spawning from Database, non-Astra"
    )
    def test_database_token_inheritance_nonastra(self) -> None:
        a_e_string = "http://x.y:123"
        opts_0 = defaultAPIOptions(environment=Environment.OTHER)
        opts_t = opts_0.with_override(
            APIOptions(token=StaticTokenProvider("static")),
        )
        token_f = UsernamePasswordTokenProvider(username="u", password="p")
        opts_f = opts_0.with_override(
            APIOptions(token=token_f),
        )
        database_t = Database(api_endpoint=a_e_string, keyspace="k", api_options=opts_t)
        a_database_t = AsyncDatabase(
            api_endpoint=a_e_string, keyspace="k", api_options=opts_t
        )
        database_0 = Database(api_endpoint=a_e_string, keyspace="k", api_options=opts_0)
        a_database_0 = AsyncDatabase(
            api_endpoint=a_e_string, keyspace="k", api_options=opts_0
        )
        database_f = Database(api_endpoint=a_e_string, keyspace="k", api_options=opts_f)
        a_database_f = AsyncDatabase(
            api_endpoint=a_e_string, keyspace="k", api_options=opts_f
        )

        assert isinstance(a_database_0.get_database_admin(), DataAPIDatabaseAdmin)
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
        opts = defaultAPIOptions(environment=Environment.PROD).with_override(
            APIOptions(token=StaticTokenProvider("t1")),
        )
        adm = AstraDBAdmin(api_options=opts)

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
        db_adm = AstraDBDatabaseAdmin(
            api_endpoint=api_ep,
            api_options=defaultAPIOptions(environment=Environment.PROD),
        )
        assert db_adm.spawner_database.api_endpoint == api_ep

    @pytest.mark.describe(
        "test of spawner_database for DataAPIDatabaseAdmin if not provided"
    )
    def test_spawnerdatabase_dataapidatabaseadmin_notprovided(self) -> None:
        api_ep = "http://aa"
        db_adm = DataAPIDatabaseAdmin(
            api_endpoint=api_ep,
            api_options=defaultAPIOptions(environment=Environment.OTHER),
        )
        assert db_adm.spawner_database.api_endpoint == api_ep

    @pytest.mark.describe(
        "test of spawner_database for AstraDBDatabaseAdmin, sync db provided"
    )
    def test_spawnerdatabase_astradbdatabaseadmin_syncprovided(self) -> None:
        api_ep = "https://01234567-89ab-cdef-0123-456789abcdef-the-region.apps.astra.datastax.com"
        db = Database(
            api_endpoint=api_ep,
            api_options=defaultAPIOptions(environment=Environment.PROD),
            keyspace="M",
        )
        db_adm = AstraDBDatabaseAdmin(
            api_endpoint=api_ep,
            api_options=defaultAPIOptions(environment=Environment.PROD),
            spawner_database=db,
        )
        assert db_adm.spawner_database is db

    @pytest.mark.describe(
        "test of spawner_database for AstraDBDatabaseAdmin, async db provided"
    )
    def test_spawnerdatabase_astradbdatabaseadmin_asyncprovided(self) -> None:
        api_ep = "https://01234567-89ab-cdef-0123-456789abcdef-the-region.apps.astra.datastax.com"
        adb = AsyncDatabase(
            api_endpoint=api_ep,
            api_options=defaultAPIOptions(environment=Environment.PROD),
            keyspace="M",
        )
        db_adm = AstraDBDatabaseAdmin(
            api_endpoint=api_ep,
            api_options=defaultAPIOptions(environment=Environment.PROD),
            spawner_database=adb,
        )
        assert db_adm.spawner_database is adb

    @pytest.mark.describe(
        "test of spawner_database for DataAPIDatabaseAdmin, sync db provided"
    )
    def test_spawnerdatabase_dataapidatabaseadmin_syncprovided(self) -> None:
        api_ep = "http://aa"
        db = Database(
            api_endpoint=api_ep,
            api_options=defaultAPIOptions(environment=Environment.OTHER),
            keyspace="k",
        )
        db_adm = DataAPIDatabaseAdmin(
            api_endpoint=api_ep,
            api_options=defaultAPIOptions(environment=Environment.OTHER),
            spawner_database=db,
        )
        assert db_adm.spawner_database is db

    @pytest.mark.describe(
        "test of spawner_database for DataAPIDatabaseAdmin, async db provided"
    )
    def test_spawnerdatabase_dataapidatabaseadmin_asyncprovided(self) -> None:
        api_ep = "http://aa"
        adb = AsyncDatabase(
            api_endpoint=api_ep,
            api_options=defaultAPIOptions(environment=Environment.OTHER),
            keyspace="k",
        )
        db_adm = DataAPIDatabaseAdmin(
            api_endpoint=api_ep,
            api_options=defaultAPIOptions(environment=Environment.OTHER),
            spawner_database=adb,
        )
        assert db_adm.spawner_database is adb
