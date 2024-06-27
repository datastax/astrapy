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

import itertools
import logging
from typing import Any, Dict, List, cast

import pytest

from astrapy.core.defaults import DEFAULT_KEYSPACE_NAME, DEFAULT_REGION
from astrapy.core.ops import AstraDBOps

from ..conftest import (
    ASTRA_DB_ID,
    ASTRA_DB_KEYSPACE,
    ASTRA_DB_OPS_APPLICATION_TOKEN,
    ASTRA_DB_REGION,
    TEST_ASTRADBOPS,
)

logger = logging.getLogger(__name__)


def find_new_name(existing: List[str], prefix: str) -> str:
    candidate_name = prefix
    for idx in itertools.count():
        candidate_name = f"{prefix}{idx}"
        if candidate_name not in existing:
            break
    return candidate_name


@pytest.fixture
def devops_client() -> AstraDBOps:
    return AstraDBOps(token=ASTRA_DB_OPS_APPLICATION_TOKEN)


# In the regular CI we skip these Ops tests (slow and require manual care).
# To maintainers: please run them now and them while we figure out automation.
@pytest.mark.skipif(
    not TEST_ASTRADBOPS,
    reason="Ops tests not explicitly requested",
)
class TestAstraDBOps:
    @pytest.mark.describe("should initialize an AstraDB Ops Client")
    def test_client_type(self, devops_client: AstraDBOps) -> None:
        assert type(devops_client) is AstraDBOps

    @pytest.mark.describe("should get all databases")
    def test_get_databases(self, devops_client: AstraDBOps) -> None:
        response = devops_client.get_databases()
        assert isinstance(response, list)

    @pytest.mark.describe("should create a database")
    def test_create_database(self, devops_client: AstraDBOps) -> None:
        pre_databases = cast(List[Dict[str, Any]], devops_client.get_databases())
        pre_database_names = [db_item["info"]["name"] for db_item in pre_databases]

        new_database_name = find_new_name(
            existing=pre_database_names,
            prefix="vector_create_test_",
        )

        database_definition = {
            "name": new_database_name,
            "tier": "serverless",
            "cloudProvider": "GCP",
            "keyspace": ASTRA_DB_KEYSPACE or DEFAULT_KEYSPACE_NAME,
            "region": ASTRA_DB_REGION or DEFAULT_REGION,
            "capacityUnits": 1,
            "user": "token",
            "password": ASTRA_DB_OPS_APPLICATION_TOKEN,
            "dbType": "vector",
        }
        response = devops_client.create_database(
            database_definition=database_definition
        )
        assert response is not None
        assert "id" in response
        assert response["id"] is not None
        ASTRA_TEMP_DB = response["id"]

        check_db = devops_client.get_database(database=ASTRA_TEMP_DB)
        # actually, if we get to this (the above didn't error) we're good...
        assert check_db is not None

    @pytest.mark.describe("should create a keyspace")
    def test_create_keyspace(self, devops_client: AstraDBOps) -> None:
        target_db = devops_client.get_database(database=ASTRA_DB_ID)
        pre_keyspaces = target_db["info"]["keyspaces"]

        new_keyspace_name = find_new_name(
            existing=pre_keyspaces,
            prefix="keyspace_create_test_",
        )

        response = devops_client.create_keyspace(
            keyspace=new_keyspace_name, database=ASTRA_DB_ID
        )

        assert response is not None
        assert response["name"] == new_keyspace_name
