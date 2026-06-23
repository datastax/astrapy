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
from pytest_httpserver import HTTPServer

from astrapy import AstraDBAdmin, DataAPIClient
from astrapy.api_options import APIOptions, DevOpsAPIURLOptions
from astrapy.info import DatabaseDefinition
from astrapy.settings.defaults import (
    DEFAULT_CREATE_DB_CAPACITY_UNITS,
    DEFAULT_CREATE_DB_DB_TYPE,
    DEFAULT_CREATE_DB_TIER,
    DEV_OPS_RESPONSE_HTTP_CREATED,
)
from astrapy.utils.request_tools import HttpMethod

from ..admin_assets import SOME_PCU_GROUP_DESC_JSON


@pytest.fixture
def mock_astra_admin(httpserver: HTTPServer) -> AstraDBAdmin:
    base_endpoint = httpserver.url_for("/")
    client = DataAPIClient(
        environment="test",
        api_options=APIOptions(
            dev_ops_api_url_options=DevOpsAPIURLOptions(
                dev_ops_url=base_endpoint,
                dev_ops_api_version="vx",
            )
        ),
    )

    return client.get_admin()


class TestAdminCreateDatabaseSync:
    @pytest.mark.describe("test of admin create database simple, sync")
    def test_admin_create_database_simple_sync(
        self,
        httpserver: HTTPServer,
        mock_astra_admin: AstraDBAdmin,
    ) -> None:
        base_payload = {
            "name": "the_db_name",
            "cloudProvider": "cp",
            "region": "r",
            "tier": DEFAULT_CREATE_DB_TIER,
            "capacityUnits": DEFAULT_CREATE_DB_CAPACITY_UNITS,
            "dbType": DEFAULT_CREATE_DB_DB_TYPE,
        }

        httpserver.expect_oneshot_request(
            "/vx/databases",
            method=HttpMethod.POST,
            json=base_payload,
        ).respond_with_data(
            "", headers={"Location": "xyz"}, status=DEV_OPS_RESPONSE_HTTP_CREATED
        )

        # We prepare for failure here, but it's a good failure: we want to ensure the httpserver
        # gets the right payload and the error is when the client instantiates the db admin all right.
        with pytest.raises(ValueError, match="Cannot parse the supplied API endpoint"):
            mock_astra_admin.create_database(
                "the_db_name",
                cloud_provider="cp",
                region="r",
                wait_until_active=False,
            )

    @pytest.mark.describe("test of admin create database rich, sync")
    def test_admin_create_database_rich_sync(
        self,
        httpserver: HTTPServer,
        mock_astra_admin: AstraDBAdmin,
    ) -> None:
        rich_payload = {
            "name": "the_db_name",
            "cloudProvider": "cp",
            "region": "r",
            "keyspace": "ks",
            "tier": DEFAULT_CREATE_DB_TIER,
            "capacityUnits": DEFAULT_CREATE_DB_CAPACITY_UNITS,
            "dbType": DEFAULT_CREATE_DB_DB_TYPE,
        }

        httpserver.expect_oneshot_request(
            "/vx/databases",
            method=HttpMethod.POST,
            json=rich_payload,
        ).respond_with_data(
            "", headers={"Location": "xyz"}, status=DEV_OPS_RESPONSE_HTTP_CREATED
        )

        # We prepare for failure here, but it's a good failure: we want to ensure the httpserver
        # gets the right payload and the error is when the client instantiates the db admin all right.
        with pytest.raises(ValueError, match="Cannot parse the supplied API endpoint"):
            mock_astra_admin.create_database(
                "the_db_name",
                cloud_provider="cp",
                region="r",
                keyspace="ks",
                wait_until_active=False,
            )

    @pytest.mark.describe("test of admin create database bad patterns, sync")
    def test_admin_create_database_badpatterns_sync(
        self,
        httpserver: HTTPServer,
        mock_astra_admin: AstraDBAdmin,
    ) -> None:
        with pytest.raises(ValueError, match="must be provided"):
            mock_astra_admin.create_database("the_db_name", wait_until_active=False)  # type: ignore[call-overload]

        with pytest.raises(ValueError, match="Cannot specify both"):
            mock_astra_admin.create_database(  # type: ignore[call-overload]
                "the_db_name",
                definition=DatabaseDefinition(cloud_provider="cp", region="r"),
                keyspace="ks",
                wait_until_active=False,
            )

    @pytest.mark.describe("test of admin create database definition nopcucheck, sync")
    def test_admin_create_database_definition_nopcucheck_sync(
        self,
        httpserver: HTTPServer,
        mock_astra_admin: AstraDBAdmin,
    ) -> None:
        full_payload = {
            "name": "the_db_name",
            "cloudProvider": "cp",
            "region": "r",
            "keyspace": "ks",
            "tier": "tr",
            "capacityUnits": 5,
            "dbType": "ty",
        }
        # No PCU check as we don't provide a pcu id

        httpserver.expect_oneshot_request(
            "/vx/databases",
            method=HttpMethod.POST,
            json=full_payload,
        ).respond_with_data(
            "", headers={"Location": "xyz"}, status=DEV_OPS_RESPONSE_HTTP_CREATED
        )

        db_definition = DatabaseDefinition(
            cloud_provider="cp",
            region="r",
            keyspace="ks",
            tier="tr",
            capacity_units=5,
            db_type="ty",
        )

        # We prepare for failure here, but it's a good failure: we want to ensure the httpserver
        # gets the right payload and the error is when the client instantiates the db admin all right.
        with pytest.raises(ValueError, match="Cannot parse the supplied API endpoint"):
            mock_astra_admin.create_database(
                "the_db_name",
                definition=db_definition,
                wait_until_active=False,
            )

    @pytest.mark.describe("test of admin create database definition failpcucheck, sync")
    def test_admin_create_database_definition_failpcucheck_sync(
        self,
        httpserver: HTTPServer,
        mock_astra_admin: AstraDBAdmin,
    ) -> None:
        full_payload = {
            "name": "the_db_name",
            "cloudProvider": "cp",
            "region": "r",
            "keyspace": "ks",
            "tier": "tr",
            "capacityUnits": 5,
            "dbType": "ty",
            "pcuGroupUUID": "d",
        }
        # We pass a pcu ID but let the httpserver fail the list-pcu-groups request: creation must proceed

        httpserver.expect_oneshot_request(
            "/vx/databases",
            method=HttpMethod.POST,
            json=full_payload,
        ).respond_with_data(
            "", headers={"Location": "xyz"}, status=DEV_OPS_RESPONSE_HTTP_CREATED
        )

        httpserver.expect_oneshot_request(
            "/vx/pcus/actions/get",
            method=HttpMethod.POST,
        ).respond_with_json(
            response_json={"0123": 321},
        )

        db_definition = DatabaseDefinition(
            cloud_provider="cp",
            region="r",
            keyspace="ks",
            tier="tr",
            capacity_units=5,
            db_type="ty",
            pcu_group_id="d",
        )

        # We prepare for failure here, but it's a good failure: we want to ensure the httpserver
        # gets the right payload and the error is when the client instantiates the db admin all right.
        with pytest.raises(ValueError, match="Cannot parse the supplied API endpoint"):
            mock_astra_admin.create_database(
                "the_db_name",
                definition=db_definition,
                wait_until_active=False,
            )

    @pytest.mark.describe(
        "test of admin create database definition missedpcucheck, sync"
    )
    def test_admin_create_database_definition_missedpcucheck_sync(
        self,
        httpserver: HTTPServer,
        mock_astra_admin: AstraDBAdmin,
    ) -> None:
        full_payload = {
            "name": "the_db_name",
            "cloudProvider": "cp",
            "region": "r",
            "keyspace": "ks",
            "tier": "tr",
            "capacityUnits": 5,
            "dbType": "ty",
            "pcuGroupUUID": "requested_pcu_g_id",
        }
        # We pass a pcu ID which the listing endpoint does not return and expect the createDB to fail pre-check

        httpserver.expect_oneshot_request(
            "/vx/databases",
            method=HttpMethod.POST,
            json=full_payload,
        ).respond_with_data(
            "", headers={"Location": "xyz"}, status=DEV_OPS_RESPONSE_HTTP_CREATED
        )

        httpserver.expect_oneshot_request(
            "/vx/pcus/actions/get",
            method=HttpMethod.POST,
        ).respond_with_json(
            response_json=[
                {
                    "uuid": "another_pcu_group_id",
                    **SOME_PCU_GROUP_DESC_JSON,
                },
            ],
        )

        db_definition = DatabaseDefinition(
            cloud_provider="cp",
            region="r",
            keyspace="ks",
            tier="tr",
            capacity_units=5,
            db_type="ty",
            pcu_group_id="d",
        )

        # Expected: PCU not found in the listing, creation aborted.
        with pytest.raises(ValueError, match="Requested PCU Group ID 'd' not found"):
            mock_astra_admin.create_database(
                "the_db_name",
                definition=db_definition,
                wait_until_active=False,
            )

    @pytest.mark.describe(
        "test of admin create database definition matchedpcucheck, sync"
    )
    def test_admin_create_database_definition_matchedpcucheck_sync(
        self,
        httpserver: HTTPServer,
        mock_astra_admin: AstraDBAdmin,
    ) -> None:
        pcu_group_id = "requested_pcu_g_id"

        full_payload = {
            "name": "the_db_name",
            "cloudProvider": "cp",
            "region": "r",
            "keyspace": "ks",
            "tier": "tr",
            "capacityUnits": 5,
            "dbType": "ty",
            "pcuGroupUUID": pcu_group_id,
        }
        # We pass a pcu ID matched in the listing endpoint: DB creation must proceed.

        httpserver.expect_oneshot_request(
            "/vx/databases",
            method=HttpMethod.POST,
            json=full_payload,
        ).respond_with_data(
            "", headers={"Location": "xyz"}, status=DEV_OPS_RESPONSE_HTTP_CREATED
        )

        httpserver.expect_oneshot_request(
            "/vx/pcus/actions/get",
            method=HttpMethod.POST,
        ).respond_with_json(
            response_json=[
                {
                    "uuid": pcu_group_id,
                    **SOME_PCU_GROUP_DESC_JSON,
                    **{
                        "cloudProvider": "cp",
                        "region": "r",
                    },
                },
            ],
        )

        db_definition = DatabaseDefinition(
            cloud_provider="cp",
            region="r",
            keyspace="ks",
            tier="tr",
            capacity_units=5,
            db_type="ty",
            pcu_group_id=pcu_group_id,
        )

        # We prepare for failure here, but it's a good failure: we want to ensure the httpserver
        # gets the right payload and the error is when the client instantiates the db admin all right.
        with pytest.raises(ValueError, match="Cannot parse the supplied API endpoint"):
            mock_astra_admin.create_database(
                "the_db_name",
                definition=db_definition,
                wait_until_active=False,
            )
