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

from astrapy.ops import AstraDBOps
from astrapy.defaults import DEFAULT_KEYSPACE_NAME, DEFAULT_REGION

import pytest
import logging
import os
import uuid

from faker import Faker

logger = logging.getLogger(__name__)
fake = Faker()


from dotenv import load_dotenv

load_dotenv()


# Parameter for the ops testing
ASTRA_DB_APPLICATION_TOKEN = os.environ.get("ASTRA_DB_APPLICATION_TOKEN")
ASTRA_DB_KEYSPACE = os.environ.get("ASTRA_DB_KEYSPACE", DEFAULT_KEYSPACE_NAME)
ASTRA_DB_REGION = os.environ.get("ASTRA_DB_REGION", DEFAULT_REGION)


pytestmark = pytest.mark.skip("Currently skipping all ops tests")


@pytest.fixture
def devops_client():
    return AstraDBOps(token=ASTRA_DB_APPLICATION_TOKEN)


@pytest.mark.describe("should initialize an AstraDB Ops Client")
def test_client_type(devops_client):
    assert type(devops_client) is AstraDBOps


@pytest.mark.describe("should get all databases")
def test_get_databases(devops_client):
    response = devops_client.get_databases()
    assert type(response) is list


@pytest.mark.describe("should create a database")
def test_create_database(devops_client):
    database_definition = {
        "name": "vector_test_create",
        "tier": "serverless",
        "cloudProvider": "GCP",
        "keyspace": ASTRA_DB_KEYSPACE,
        "region": ASTRA_DB_REGION,
        "capacityUnits": 1,
        "user": "token",
        "password": ASTRA_DB_APPLICATION_TOKEN,
        "dbType": "vector",
    }
    response = devops_client.create_database(database_definition=database_definition)
    assert response["id"] is not None
    ASTRA_TEMP_DB = response["id"]

    check_db = devops_client.get_database(database=ASTRA_TEMP_DB)
    assert check_db is not None

    response = devops_client.terminate_database(database=ASTRA_TEMP_DB)
    assert response is None


@pytest.mark.describe("should create a keyspace")
def test_create_keyspace(devops_client):
    response = devops_client.create_keyspace(
        keyspace="test_namespace", database=str(uuid.uuid4())
    )

    assert response is not None
