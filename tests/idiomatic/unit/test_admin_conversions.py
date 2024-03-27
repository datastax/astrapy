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

import pytest
import httpx

from astrapy import AstraDBAdmin, AstraDBDatabaseAdmin, DataAPIClient, Database


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
        assert dac1 != dac1._copy(environment="x")
        assert dac1 != dac1._copy(caller_name="x")
        assert dac1 != dac1._copy(caller_version="x")

        assert dac1 == dac1._copy(token="x")._copy(token="t1")
        assert dac1 == dac1._copy(environment="x")._copy(environment="dev")
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
        assert adm1 != adm1._copy(environment="x")
        assert adm1 != adm1._copy(caller_name="x")
        assert adm1 != adm1._copy(caller_version="x")
        assert adm1 != adm1._copy(dev_ops_url="x")
        assert adm1 != adm1._copy(dev_ops_api_version="x")

        assert adm1 == adm1._copy(token="x")._copy(token="t1")
        assert adm1 == adm1._copy(environment="x")._copy(environment="dev")
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
            "i1",
            token="t1",
            environment="dev",
            caller_name="cn",
            caller_version="cv",
            dev_ops_url="dou",
            dev_ops_api_version="dvv",
        )
        adda2 = AstraDBDatabaseAdmin(
            "i1",
            token="t1",
            environment="dev",
            caller_name="cn",
            caller_version="cv",
            dev_ops_url="dou",
            dev_ops_api_version="dvv",
        )
        assert adda1 == adda2

        assert adda1 != adda1._copy(id="x")
        assert adda1 != adda1._copy(token="x")
        assert adda1 != adda1._copy(environment="x")
        assert adda1 != adda1._copy(caller_name="x")
        assert adda1 != adda1._copy(caller_version="x")
        assert adda1 != adda1._copy(dev_ops_url="x")
        assert adda1 != adda1._copy(dev_ops_api_version="x")

        assert adda1 == adda1._copy(id="x")._copy(id="i1")
        assert adda1 == adda1._copy(token="x")._copy(token="t1")
        assert adda1 == adda1._copy(environment="x")._copy(environment="dev")
        assert adda1 == adda1._copy(caller_name="x")._copy(caller_name="cn")
        assert adda1 == adda1._copy(caller_version="x")._copy(caller_version="cv")
        assert adda1 == adda1._copy(dev_ops_url="x")._copy(dev_ops_url="dou")
        assert adda1 == adda1._copy(dev_ops_api_version="x")._copy(
            dev_ops_api_version="dvv"
        )

        assert adda1 != adda1.with_options(id="x")
        assert adda1 != adda1.with_options(token="x")
        assert adda1 != adda1.with_options(caller_name="x")
        assert adda1 != adda1.with_options(caller_version="x")

        assert adda1 == adda1.with_options(id="x").with_options(id="i1")
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
