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

# This assumes that you have already created a database and a keyspace
# of "vector"

from typing import List
from astrapy.base import AstraClient, http_methods
from astrapy.serverless import AstraJsonClient
from astrapy.vector import AstraVectorClient
from astrapy.ops import AstraOps
import pytest
import logging
import os
from faker import Faker

from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)
fake = Faker()

ASTRA_DB_ID = os.environ.get("ASTRA_DB_ID")
ASTRA_DB_REGION = os.environ.get("ASTRA_DB_REGION")
ASTRA_DB_APPLICATION_TOKEN = os.environ.get("ASTRA_DB_APPLICATION_TOKEN")
ASTRA_DB_KEYSPACE = os.environ.get("ASTRA_DB_KEYSPACE")
TEST_COLLECTION_NAME = "vector_test"
import http.client as http_client


http_client.HTTPConnection.debuglevel = 1


@pytest.fixture
def astra_client():
    return AstraClient(
        astra_database_id=ASTRA_DB_ID,
        astra_database_region=ASTRA_DB_REGION,
        astra_application_token=ASTRA_DB_APPLICATION_TOKEN,
    )


@pytest.fixture
def devops_client(astra_client):
    return AstraOps(client=astra_client)


@pytest.fixture
def jsonapi_client(astra_client):
    return AstraJsonClient(astra_client=astra_client)


@pytest.fixture
def test_collection():
    astra_client = AstraClient(
        astra_database_id=ASTRA_DB_ID,
        astra_database_region=ASTRA_DB_REGION,
        astra_application_token=ASTRA_DB_APPLICATION_TOKEN,
    )
    vector_client = AstraVectorClient(astra_client=astra_client)
    test_collection = vector_client.namespace(ASTRA_DB_KEYSPACE).collection(
        TEST_COLLECTION_NAME
    )
    return test_collection


@pytest.fixture
def test_namespace():
    astra_client = AstraClient(
        astra_database_id=ASTRA_DB_ID,
        astra_database_region=ASTRA_DB_REGION,
        astra_application_token=ASTRA_DB_APPLICATION_TOKEN,
    )
    vector_client = AstraVectorClient(astra_client=astra_client)

    return vector_client.namespace(ASTRA_DB_KEYSPACE)


@pytest.mark.webtest("should create a vector collection")
def test_create_collection(test_namespace):
    res = test_namespace.create_vector_collection(name=TEST_COLLECTION_NAME, size=5)
    assert res is not None


@pytest.mark.webtest("should get all collections")
def test_get_collections(test_namespace):
    res = test_namespace.get_collections()
    assert res["status"]["collections"] is not None


@pytest.mark.webtest("should create a document")
def test_create_document(test_collection):
    json_query = {
        "_id": "4",
        "name": "Coded Cleats Copy",
        "description": "ChatGPT integrated sneakers that talk to you",
        "$vector": [0.25, 0.25, 0.25, 0.25, 0.25],
    }

    res = test_collection.create(document=json_query)
    assert res is not None


@pytest.mark.webtest("create many documents")
def test_create_documents(test_collection):
    json_query = [
        {
            "_id": "1",
            "name": "Coded Cleats",
            "description": "ChatGPT integrated sneakers that talk to you",
            "$vector": [0.1, 0.15, 0.3, 0.12, 0.05],
        },
        {
            "_id": "2",
            "name": "Logic Layers",
            "description": "An AI quilt to help you sleep forever",
            "$vector": [0.45, 0.09, 0.01, 0.2, 0.11],
        },
        {
            "_id": "3",
            "name": "Vision Vector Frame",
            "description": "Vision Vector Frame - A deep learning display that controls your mood",
            "$vector": [0.1, 0.05, 0.08, 0.3, 0.6],
        },
    ]

    res = test_collection.insert_many(documents=json_query)
    assert res is not None





@pytest.mark.skip("Delete a collection")
def test_delete_collection(test_namespace):
    returnval = test_namespace.delete_collection(name=TEST_COLLECTION_NAME)
    assert returnval is not None
