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
from astrapy.settings.defaults import (
    DEFAULT_CREATE_DB_CAPACITY_UNITS,
    DEFAULT_CREATE_DB_DB_TYPE,
    DEFAULT_CREATE_DB_TIER,
    DEV_OPS_RESPONSE_HTTP_CREATED,
)
from astrapy.utils.request_tools import HttpMethod


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


class TestAdminCreateDatabaseAsync:
    @pytest.mark.describe("test of admin create database simple, async")
    async def test_admin_create_database_simple_async(
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
            await mock_astra_admin.async_create_database(
                "the_db_name",
                cloud_provider="cp",
                region="r",
                wait_until_active=False,
            )

    @pytest.mark.describe("test of admin create database rich, async")
    async def test_admin_create_database_rich_async(
        self,
        httpserver: HTTPServer,
        mock_astra_admin: AstraDBAdmin,
    ) -> None:
        # TODO add all of the optional params here
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
            await mock_astra_admin.async_create_database(
                "the_db_name",
                cloud_provider="cp",
                region="r",
                keyspace="ks",
                wait_until_active=False,
            )
