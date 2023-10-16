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

from astrapy.base import AstraClient
from astrapy.ops import AstraOps
import uuid
import pytest
import logging
import os
import json
from faker import Faker
import http.client as http_client

logger = logging.getLogger(__name__)
fake = Faker()
import dotenv

dotenv.load_dotenv()

http_client.HTTPConnection.debuglevel = 1

ASTRA_DB_ID = os.environ.get("ASTRA_DB_ID")
ASTRA_DB_REGION = os.environ.get("ASTRA_DB_REGION")
ASTRA_DB_APPLICATION_TOKEN = os.environ.get("ASTRA_DB_APPLICATION_TOKEN")
ASTRA_DB_KEYSPACE = os.environ.get("ASTRA_DB_KEYSPACE")
ASTRA_CLIENT_ID = os.environ.get("ASTRA_CLIENT_ID")
ASTRA_CLIENT_SECRET = os.environ.get("ASTRA_CLIENT_SECRET")
ASTRA_TEMP_DB = ""


@pytest.fixture
def devops_client():
    astra_client = AstraClient(
        astra_database_id=ASTRA_DB_ID,
        astra_database_region=ASTRA_DB_REGION,
        astra_application_token=ASTRA_DB_APPLICATION_TOKEN,
    )
    devops_client = AstraOps(astra_client)

    return devops_client


@pytest.mark.it("should initialize an AstraDB Ops Client")
def test_client_type(devops_client):
    assert type(devops_client) is AstraOps


@pytest.mark.it("should get all databases")
def test_list_databases(devops_client):
    response = devops_client.get_databases()
    assert type(response) is list


@pytest.mark.it("should create a database")
def test_create_database(devops_client):
    database_definition = {
        "name": "vector_search",
        "tier": "serverless",
        "cloudProvider": "GCP",
        "keyspace": "vector_search",
        "region": "us-east1",
        "capacityUnits": 1,
        "user": "token",
        "password": os.environ.get("ASTRA_DB_APPLICATION_TOKEN"),
        "dbType": "vector",
    }
    response = devops_client.create_database(database_definition=database_definition)
    assert response["id"] is not None
    ASTRA_TEMP_DB = response["id"]
    print("ASTRA TEMP ID1", ASTRA_TEMP_DB)

    check_db = devops_client.get_database(database=ASTRA_TEMP_DB)
    assert check_db is not None

    response = devops_client.terminate_database(database=ASTRA_TEMP_DB)
    assert response is None


@pytest.mark.it("should create a keyspace")
def test_create_keyspace(devops_client):
    response = devops_client.create_keyspace(
        keyspace="vector_search_2", database=os.environ["ASTRA_DB_ID"]
    )
    print("RESPONSE", response)
