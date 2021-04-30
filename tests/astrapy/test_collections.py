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

from astrapy.collections import create_client, AstraCollection
import uuid
import pytest
import logging
import os

logger = logging.getLogger(__name__)

ASTRA_DB_ID = os.environ.get('ASTRA_DB_ID')
ASTRA_DB_REGION = os.environ.get('ASTRA_DB_REGION')
ASTRA_DB_APPLICATION_TOKEN = os.environ.get('ASTRA_DB_APPLICATION_TOKEN')
ASTRA_DB_KEYSPACE = os.environ.get('ASTRA_DB_KEYSPACE')

TEST_COLLECTION_NAME = "test"


@pytest.fixture
def test_collection():
    astra_client = create_client(astra_database_id=ASTRA_DB_ID,
                                 astra_database_region=ASTRA_DB_REGION,
                                 astra_application_token=ASTRA_DB_APPLICATION_TOKEN)

    return astra_client.namespace(ASTRA_DB_KEYSPACE).collection(TEST_COLLECTION_NAME)


@pytest.fixture
def cliff_uuid():
    return str(uuid.uuid4())


@pytest.mark.it('should initialize an AstraDB Collections Client')
def test_connect(test_collection):
    assert type(test_collection) is AstraCollection


@pytest.mark.it('should create a document')
def test_create_document(test_collection, cliff_uuid):
    test_collection.create(path=cliff_uuid, document={
        "first_name": "Cliff",
        "last_name": "Wicklow",
    })
    document = test_collection.get(path=cliff_uuid)
    assert document["first_name"] == "Cliff"


@pytest.mark.it('should create a subdocument')
def test_create_subdocument(test_collection, cliff_uuid):
    test_collection.create(path=f"{cliff_uuid}/addresses", document={
        "home": {
            "city": "New York",
            "state": "NY",
        }
    })
    document = test_collection.get(path=f"{cliff_uuid}/addresses")
    assert document["home"]["state"] == "NY"


@pytest.mark.it('should create a document without an ID')
def test_create_document_without_id(test_collection):
    response = test_collection.create(document={
        "first_name": "New",
        "last_name": "Guy",
    })
    document = test_collection.get(path=response["documentId"])
    assert document["first_name"] == "New"


@pytest.mark.it('should udpate a document')
def test_update_document(test_collection, cliff_uuid):
    test_collection.update(path=cliff_uuid, document={
        "first_name": "Dang",
    })
    document = test_collection.get(path=cliff_uuid)
    assert document["first_name"] == "Dang"


@pytest.mark.it('replace a subdocument')
def test_replace_subdocument(test_collection, cliff_uuid):
    test_collection.replace(path=f"{cliff_uuid}/addresses", document={
        "work": {
            "city": "New York",
            "state": "NY",
        }
    })
    document = test_collection.get(path=f"{cliff_uuid}/addresses/work")
    assert document["state"] == "NY"
    document_2 = test_collection.get(path=f"{cliff_uuid}/addresses/home")
    assert document_2 is None


@pytest.mark.it('should delete a subdocument')
def test_delete_subdocument(test_collection, cliff_uuid):
    test_collection.delete(path=f"{cliff_uuid}/addresses")
    document = test_collection.get(path=f"{cliff_uuid}/addresses")
    assert document is None


@pytest.mark.it('should delete a document')
def test_delete_document(test_collection, cliff_uuid):
    test_collection.delete(path=cliff_uuid)
    document = test_collection.get(path=cliff_uuid)
    assert document is None


@pytest.mark.it('should find documents')
def test_find_documents(test_collection):
    user_id = str(uuid.uuid4())
    test_collection.create(path=user_id, document={
        "first_name": f"Cliff-{user_id}",
        "last_name": "Wicklow",
    })
    user_id_2 = str(uuid.uuid4())
    test_collection.create(path=user_id_2, document={
        "first_name": f"Cliff-{user_id}",
        "last_name": "Danger",
    })
    documents = test_collection.find(query={
        "first_name": {"$eq": f"Cliff-{user_id}"},
    })
    assert len(documents.keys()) == 2
    assert documents[user_id]["last_name"] == "Wicklow"
    assert documents[user_id_2]["last_name"] == "Danger"


@pytest.mark.it('should find a single document')
def test_find_one_document(test_collection):
    user_id = str(uuid.uuid4())
    test_collection.create(path=user_id, document={
        "first_name": f"Cliff-{user_id}",
        "last_name": "Wicklow",
    })
    user_id_2 = str(uuid.uuid4())
    test_collection.create(path=user_id_2, document={
        "first_name": f"Cliff-{user_id}",
        "last_name": "Danger",
    })
    document = test_collection.find_one(query={
        "first_name": {"$eq": f"Cliff-{user_id}"},
    })
    assert document["first_name"] == f"Cliff-{user_id}"
