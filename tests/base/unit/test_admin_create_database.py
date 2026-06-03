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

"""
Unit tests for the request payload built by the database-creation admin methods.

These do not hit Astra DB: a mock DevOps API (``pytest_httpserver``) is pointed
at by the admin object, and the ``create_database`` payload is asserted through
the ``json`` request matcher (a mismatch yields a non-201 response, which in turn
surfaces as a ``DevOpsAPIException``). ``wait_until_active=False`` keeps each call
to a single DevOps request, with the new database id taken from the
``Location`` response header.
"""

from __future__ import annotations

from typing import Any

import pytest
from pytest_httpserver import HTTPServer

from astrapy.admin.admin import AstraDBAdmin, AstraDBDatabaseAdmin
from astrapy.utils.api_options import (
    APIOptions,
    DevOpsAPIURLOptions,
    defaultAPIOptions,
)
from astrapy.utils.request_tools import HttpMethod

DEV_OPS_API_VERSION = "v2"
CREATE_DB_PATH = f"/{DEV_OPS_API_VERSION}/databases"

DATABASE_ID = "01234567-89ab-cdef-0123-456789abcdef"
DATABASE_NAME = "test_database"
CLOUD_PROVIDER = "aws"
REGION = "us-east-1"
KEYSPACE = "the_keyspace"
PCU_GROUP_ID = "f5e6d7c8-1234-5678-9abc-def012345678"

# Default invocation: a vector database, no keyspace, no PCU group.
VECTOR_DEFAULT_PAYLOAD = {
    "name": DATABASE_NAME,
    "tier": "serverless",
    "cloudProvider": CLOUD_PROVIDER,
    "region": REGION,
    "capacityUnits": 1,
    "dbType": "vector",
}
# database_type=None: the "dbType" key is omitted entirely.
NON_VECTOR_PAYLOAD = {
    "name": DATABASE_NAME,
    "tier": "serverless",
    "cloudProvider": CLOUD_PROVIDER,
    "region": REGION,
    "capacityUnits": 1,
}
# All the new knobs supplied at once.
FULL_PAYLOAD = {
    "name": DATABASE_NAME,
    "tier": "serverless",
    "cloudProvider": CLOUD_PROVIDER,
    "region": REGION,
    "capacityUnits": 1,
    "dbType": "tabular",
    "keyspace": KEYSPACE,
    "pcuGroupUUID": PCU_GROUP_ID,
}


@pytest.fixture
def mock_astra_db_admin(httpserver: HTTPServer) -> AstraDBAdmin:
    base_endpoint = httpserver.url_for("/")
    api_options = defaultAPIOptions(environment="prod").with_override(
        APIOptions(
            token="t1",
            dev_ops_api_url_options=DevOpsAPIURLOptions(
                dev_ops_url=base_endpoint,
                dev_ops_api_version=DEV_OPS_API_VERSION,
            ),
        ),
    )
    return AstraDBAdmin(api_options=api_options)


def _expect_create_database(
    httpserver: HTTPServer, expected_payload: dict[str, Any]
) -> None:
    httpserver.expect_oneshot_request(
        CREATE_DB_PATH,
        method=HttpMethod.POST,
        json=expected_payload,
    ).respond_with_data("", status=201, headers={"Location": DATABASE_ID})


class TestAdminCreateDatabaseDryMethods:
    @pytest.mark.describe("create_database requests a vector database by default, sync")
    def test_create_database_default_dbtype_sync(
        self, httpserver: HTTPServer, mock_astra_db_admin: AstraDBAdmin
    ) -> None:
        _expect_create_database(httpserver, VECTOR_DEFAULT_PAYLOAD)
        created = mock_astra_db_admin.create_database(
            DATABASE_NAME,
            cloud_provider=CLOUD_PROVIDER,
            region=REGION,
            wait_until_active=False,
        )
        assert isinstance(created, AstraDBDatabaseAdmin)
        assert created.id == DATABASE_ID

    @pytest.mark.describe(
        "create_database requests a vector database by default, async"
    )
    async def test_create_database_default_dbtype_async(
        self, httpserver: HTTPServer, mock_astra_db_admin: AstraDBAdmin
    ) -> None:
        _expect_create_database(httpserver, VECTOR_DEFAULT_PAYLOAD)
        created = await mock_astra_db_admin.async_create_database(
            DATABASE_NAME,
            cloud_provider=CLOUD_PROVIDER,
            region=REGION,
            wait_until_active=False,
        )
        assert isinstance(created, AstraDBDatabaseAdmin)
        assert created.id == DATABASE_ID

    @pytest.mark.describe("create_database with database_type=None omits dbType, sync")
    def test_create_database_non_vector_sync(
        self, httpserver: HTTPServer, mock_astra_db_admin: AstraDBAdmin
    ) -> None:
        _expect_create_database(httpserver, NON_VECTOR_PAYLOAD)
        created = mock_astra_db_admin.create_database(
            DATABASE_NAME,
            cloud_provider=CLOUD_PROVIDER,
            region=REGION,
            database_type=None,
            wait_until_active=False,
        )
        assert created.id == DATABASE_ID

    @pytest.mark.describe("create_database with database_type=None omits dbType, async")
    async def test_create_database_non_vector_async(
        self, httpserver: HTTPServer, mock_astra_db_admin: AstraDBAdmin
    ) -> None:
        _expect_create_database(httpserver, NON_VECTOR_PAYLOAD)
        created = await mock_astra_db_admin.async_create_database(
            DATABASE_NAME,
            cloud_provider=CLOUD_PROVIDER,
            region=REGION,
            database_type=None,
            wait_until_active=False,
        )
        assert created.id == DATABASE_ID

    @pytest.mark.describe(
        "create_database forwards database_type, keyspace and pcu_group_id, sync"
    )
    def test_create_database_full_payload_sync(
        self, httpserver: HTTPServer, mock_astra_db_admin: AstraDBAdmin
    ) -> None:
        _expect_create_database(httpserver, FULL_PAYLOAD)
        created = mock_astra_db_admin.create_database(
            DATABASE_NAME,
            cloud_provider=CLOUD_PROVIDER,
            region=REGION,
            keyspace=KEYSPACE,
            database_type="tabular",
            pcu_group_id=PCU_GROUP_ID,
            wait_until_active=False,
        )
        assert created.id == DATABASE_ID

    @pytest.mark.describe(
        "create_database forwards database_type, keyspace and pcu_group_id, async"
    )
    async def test_create_database_full_payload_async(
        self, httpserver: HTTPServer, mock_astra_db_admin: AstraDBAdmin
    ) -> None:
        _expect_create_database(httpserver, FULL_PAYLOAD)
        created = await mock_astra_db_admin.async_create_database(
            DATABASE_NAME,
            cloud_provider=CLOUD_PROVIDER,
            region=REGION,
            keyspace=KEYSPACE,
            database_type="tabular",
            pcu_group_id=PCU_GROUP_ID,
            wait_until_active=False,
        )
        assert created.id == DATABASE_ID
