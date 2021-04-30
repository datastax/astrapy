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

from astrapy.rest import create_client, AstraClient, http_methods
import pytest
import logging
import os
import uuid

logger = logging.getLogger(__name__)

ASTRA_DB_ID = os.environ.get('ASTRA_DB_ID')
ASTRA_DB_REGION = os.environ.get('ASTRA_DB_REGION')
ASTRA_DB_APPLICATION_TOKEN = os.environ.get('ASTRA_DB_APPLICATION_TOKEN')
ASTRA_DB_KEYSPACE = os.environ.get('ASTRA_DB_KEYSPACE')
ASTRA_DB_COLLECTION = "rest"


@pytest.fixture
def astra_rest_client():
    return create_client(astra_database_id=ASTRA_DB_ID,
                         astra_database_region=ASTRA_DB_REGION,
                         astra_application_token=ASTRA_DB_APPLICATION_TOKEN)


@pytest.mark.it('should initialize an AstraDB REST Client')
def test_connect(astra_rest_client):
    assert type(astra_rest_client) is AstraClient


@pytest.mark.it('should create a document')
def test_creating_document(astra_rest_client):
    doc_uuid = uuid.uuid4()
    r = astra_rest_client.request(
        method=http_methods.PUT,
        path=f"/api/rest/v2/namespaces/{ASTRA_DB_KEYSPACE}/collections/{ASTRA_DB_COLLECTION}/{doc_uuid}",
        json_data={
            "first_name": "Cliff",
            "last_name": "Wicklow",
            "emails": ["cliff.wicklow@example.com"],
        })
    assert r["documentId"] == str(doc_uuid)
