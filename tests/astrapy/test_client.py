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

from astrapy.client import create_astra_client, AstraClient
import pytest
import logging
import os
import uuid

logger = logging.getLogger(__name__)

ASTRA_DB_ID = os.environ.get('ASTRA_DB_ID')
ASTRA_DB_REGION = os.environ.get('ASTRA_DB_REGION')
ASTRA_DB_APPLICATION_TOKEN = os.environ.get('ASTRA_DB_APPLICATION_TOKEN')
ASTRA_DB_KEYSPACE = os.environ.get('ASTRA_DB_KEYSPACE')


@pytest.fixture
def astra_client():
    return create_astra_client(astra_database_id=ASTRA_DB_ID,
                               astra_database_region=ASTRA_DB_REGION,
                               astra_application_token=ASTRA_DB_APPLICATION_TOKEN)


@pytest.mark.it('should initialize an AstraDB REST Client')
def test_connect(astra_client):
    assert type(astra_client) is AstraClient


@pytest.mark.it('should get a list of databases')
def test_get_databases(astra_client):
    databases = astra_client.ops.get_databases()
    assert type(databases) is list


# @pytest.mark.it('should create a database')
# def test_create_database(astra_client):
#     res = astra_client.ops.create_database({
#         "name": "astrapy-test-db",
#         "keyspace": "astrapy_test",
#         "cloudProvider": "AWS",
#         "tier": "serverless",
#         "capacityUnits": 1,
#         "region": "us-west-2"
#     })
#     assert res is None
