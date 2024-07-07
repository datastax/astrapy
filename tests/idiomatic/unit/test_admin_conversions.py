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

import httpx
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


class TestAdminConversions:
    @pytest.mark.describe("test of DataAPIClient conversions and comparison functions")
    def test_dataapiclient_conversions(self) -> None:
        dac1 = DataAPIClient(
            "t1", environment="dev", caller_name="cn", caller_version="cv"
        )
        dac2 = DataAPIClient(
            "t1", environment="dev", caller_name="cn", caller_version="cv"
        )
        assert dac1 == dac2

        assert dac1 != dac1._copy(token="x")
        assert dac1 != dac1._copy(environment="test")
        assert dac1 != dac1._copy(caller_name="x")
        assert dac1 != dac1._copy(caller_version="x")

        assert dac1 == dac1._copy(token="x")._copy(token="t1")
        assert dac1 == dac1._copy(environment="test")._copy(environment="dev")
        assert dac1 == dac1._copy(caller_name="x")._copy(caller_name="cn")
        assert dac1 == dac1._copy(caller_version="x")._copy(caller_version="cv")

        assert dac1 != dac1.with_options(token="x")
        assert dac1 != dac1.with_options(caller_name="x")
        assert dac1 != dac1.with_options(caller_version="x")

        assert dac1 == dac1.with_options(token="x").with_options(token="t1")
        assert dac1 == dac1.with_options(caller_name="x").with_options(caller_name="cn")
        assert dac1 == dac1.with_options(caller_version="x").with_options(
            caller_version="cv"
        )

        dac1b = dac1._copy()
        dac1b.set_caller("cn2", "cv2")
        assert dac1b != dac1
        dac1b.set_caller("cn", "cv")
        assert dac1b == dac1

        a_e_string = (
            "https://01234567-89ab-cdef-0123-456789abcdef-us-east1"
            ".apps.astra-dev.datastax.com"
        )
        d_id_string = "01234567-89ab-cdef-0123-456789abcdef"
        db1 = dac1[a_e_string]
        expected_db_1 = Database(
            api_endpoint=a_e_string,
            token="t1",
            caller_name="cn",
            caller_version="cv",
        )
        assert db1 == expected_db_1
        with pytest.raises(httpx.HTTPStatusError):
            dac1[d_id_string]
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
            caller_name="cn",
            caller_version="cv",
        )

        db1 = client.get_database(endpoint)
        db2 = client.get_database(database_id, region=database_region)
        db3 = client.get_database(endpoint)

        assert db1 == db2
        assert db1 == db3

        with pytest.raises(ValueError):
            client.get_database(endpoint, region=database_region)

    @pytest.mark.describe("test of AstraDBAdmin conversions and comparison functions")
    def test_astradbadmin_conversions(self) -> None:
        adm1 = AstraDBAdmin(
            "t1",
            environment="dev",
            caller_name="cn",
            caller_version="cv",
            dev_ops_url="dou",
            dev_ops_api_version="dvv",
        )
        adm2 = AstraDBAdmin(
            "t1",
            environment="dev",
            caller_name="cn",
            caller_version="cv",
            dev_ops_url="dou",
            dev_ops_api_version="dvv",
        )
        assert adm1 == adm2

        assert adm1 != adm1._copy(token="x")
        assert adm1 != adm1._copy(environment="test")
        assert adm1 != adm1._copy(caller_name="x")
        assert adm1 != adm1._copy(caller_version="x")
        assert adm1 != adm1._copy(dev_ops_url="x")
        assert adm1 != adm1._copy(dev_ops_api_version="x")

        assert adm1 == adm1._copy(token="x")._copy(token="t1")
        assert adm1 == adm1._copy(environment="test")._copy(environment="dev")
        assert adm1 == adm1._copy(caller_name="x")._copy(caller_name="cn")
        assert adm1 == adm1._copy(caller_version="x")._copy(caller_version="cv")
        assert adm1 == adm1._copy(dev_ops_url="x")._copy(dev_ops_url="dou")
        assert adm1 == adm1._copy(dev_ops_api_version="x")._copy(
            dev_ops_api_version="dvv"
        )

        assert adm1 != adm1.with_options(token="x")
        assert adm1 != adm1.with_options(caller_name="x")
        assert adm1 != adm1.with_options(caller_version="x")

        assert adm1 == adm1.with_options(token="x").with_options(token="t1")
        assert adm1 == adm1.with_options(caller_name="x").with_options(caller_name="cn")
        assert adm1 == adm1.with_options(caller_version="x").with_options(
            caller_version="cv"
        )

        adm1b = adm1._copy()
        adm1b.set_caller("cn2", "cv2")
        assert adm1b != adm1
        adm1b.set_caller("cn", "cv")
        assert adm1b == adm1

    @pytest.mark.describe(
        "test of AstraDBDatabaseAdmin conversions and comparison functions"
    )
    def test_astradbdatabaseadmin_conversions(self) -> None:
        adda1 = AstraDBDatabaseAdmin(
            "01234567-89ab-cdef-0123-456789abcdef",
            token="t1",
            region="reg",
            environment="dev",
            caller_name="cn",
            caller_version="cv",
            dev_ops_url="dou",
            dev_ops_api_version="dvv",
        )
        adda2 = AstraDBDatabaseAdmin(
            "01234567-89ab-cdef-0123-456789abcdef",
            token="t1",
            region="reg",
            environment="dev",
            caller_name="cn",
            caller_version="cv",
            dev_ops_url="dou",
            dev_ops_api_version="dvv",
        )
        assert adda1 == adda2

        assert adda1 != adda1._copy(id="99999999-89ab-cdef-0123-456789abcdef")
        assert adda1 != adda1._copy(token="x")
        assert adda1 != adda1._copy(region="x")
        assert adda1 != adda1._copy(environment="test")
        assert adda1 != adda1._copy(caller_name="x")
        assert adda1 != adda1._copy(caller_version="x")
        assert adda1 != adda1._copy(dev_ops_url="x")
        assert adda1 != adda1._copy(dev_ops_api_version="x")

        assert adda1 == adda1._copy(id="99999999-89ab-cdef-0123-456789abcdef")._copy(
            id="01234567-89ab-cdef-0123-456789abcdef"
        )
        assert adda1 == adda1._copy(token="x")._copy(token="t1")
        assert adda1 == adda1._copy(region="x")._copy(region="reg")
        assert adda1 == adda1._copy(environment="test")._copy(environment="dev")
        assert adda1 == adda1._copy(caller_name="x")._copy(caller_name="cn")
        assert adda1 == adda1._copy(caller_version="x")._copy(caller_version="cv")
        assert adda1 == adda1._copy(dev_ops_url="x")._copy(dev_ops_url="dou")
        assert adda1 == adda1._copy(dev_ops_api_version="x")._copy(
            dev_ops_api_version="dvv"
        )

        assert adda1 != adda1.with_options(id="99999999-89ab-cdef-0123-456789abcdef")
        assert adda1 != adda1.with_options(token="x")
        assert adda1 != adda1.with_options(caller_name="x")
        assert adda1 != adda1.with_options(caller_version="x")

        assert adda1 == adda1.with_options(
            id="99999999-89ab-cdef-0123-456789abcdef"
        ).with_options(id="01234567-89ab-cdef-0123-456789abcdef")
        assert adda1 == adda1.with_options(token="x").with_options(token="t1")
        assert adda1 == adda1.with_options(caller_name="x").with_options(
            caller_name="cn"
        )
        assert adda1 == adda1.with_options(caller_version="x").with_options(
            caller_version="cv"
        )

        adda1b = adda1._copy()
        adda1b.set_caller("cn2", "cv2")
        assert adda1b != adda1
        adda1b.set_caller("cn", "cv")
        assert adda1b == adda1

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
            db_id_string, token=token_f, namespace="n", region="r"
        ) == admin_f.get_database(db_id_string, namespace="n", region="r")
        assert admin_0.get_database(
            db_id_string, token=token_f, namespace="n", region="r"
        ) == admin_f.get_database(db_id_string, namespace="n", region="r")

    @pytest.mark.describe(
        "test of token inheritance in spawning from AstraDBDatabaseAdmin"
    )
    def test_astradbdatabaseadmin_token_inheritance(self) -> None:
        db_id_string = "01234567-89ab-cdef-0123-456789abcdef"
        adbadmin_t = AstraDBDatabaseAdmin(
            db_id_string,
            token=StaticTokenProvider("static"),
            region="reg",
        )
        adbadmin_0 = AstraDBDatabaseAdmin(db_id_string, region="reg")
        token_f = UsernamePasswordTokenProvider(username="u", password="p")
        adbadmin_f = AstraDBDatabaseAdmin(db_id_string, region="reg", token=token_f)

        assert adbadmin_t.get_database(
            token=token_f, namespace="n", region="r"
        ) == adbadmin_f.get_database(namespace="n", region="r")
        assert adbadmin_0.get_database(
            token=token_f, namespace="n", region="r"
        ) == adbadmin_f.get_database(namespace="n", region="r")

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
            token=token_f, namespace="n"
        ) == dadbadmin_f.get_database(namespace="n")
        assert dadbadmin_0.get_database(
            token=token_f, namespace="n"
        ) == dadbadmin_f.get_database(namespace="n")

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
        db_adm2 = adm.get_database_admin(api_ep, region=db_reg)
        db_adm3 = adm.get_database_admin(api_ep)
        with pytest.raises(ValueError):
            adm.get_database_admin(api_ep, region="not-that-one")

        assert db_adm1 == db_adm2
        assert db_adm2 == db_adm3

        db_1 = adm.get_database(db_id, region=db_reg, namespace="the_ns")
        db_2 = adm.get_database(api_ep, region=db_reg, namespace="the_ns")
        db_3 = adm.get_database(api_ep, namespace="the_ns")
        with pytest.raises(ValueError):
            adm.get_database(api_ep, region="not-that-one", namespace="the_ns")

        assert db_1 == db_2
        assert db_2 == db_3

        db_adm_m1 = AstraDBDatabaseAdmin(db_id, token="t", region=db_reg)
        db_adm_m2 = AstraDBDatabaseAdmin(api_ep, token="t", region=db_reg)
        db_adm_m3 = AstraDBDatabaseAdmin(api_ep, token="t")
        with pytest.raises(ValueError):
            AstraDBDatabaseAdmin(api_ep, token="t", region="not-that-one")

        assert db_adm_m1 == db_adm_m2
        assert db_adm_m1 == db_adm_m3

    @pytest.mark.describe(
        "test of region being deprecated in AstraDBDatabaseAdmin.get_database"
    )
    def test_region_deprecation_astradbdatabaseadmin_getdatabase(self) -> None:
        api_ep = "https://01234567-89ab-cdef-0123-456789abcdef-the-region.apps.astra.datastax.com"
        db_adm = AstraDBDatabaseAdmin(api_ep)
        with pytest.warns(DeprecationWarning):
            db1 = db_adm.get_database(
                region="another-region", namespace="the-namespace"
            )
            # it's ignored anyway
            assert db1 == db_adm.get_database(namespace="the-namespace")
