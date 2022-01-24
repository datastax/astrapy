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
from faker import Faker

logger = logging.getLogger(__name__)
fake = Faker()

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


@pytest.fixture
def test_namespace():
    astra_client = create_client(astra_database_id=ASTRA_DB_ID,
                                 astra_database_region=ASTRA_DB_REGION,
                                 astra_application_token=ASTRA_DB_APPLICATION_TOKEN)
    return astra_client.namespace(ASTRA_DB_KEYSPACE)


@pytest.mark.it('should initialize an AstraDB Collections Client')
def test_connect(test_collection):
    assert type(test_collection) is AstraCollection


@pytest.mark.it('should create a collection')
def test_create_collection(test_namespace):
    res = test_namespace.create_collection(name="pytest_collection")
    assert res is None
    res2 = test_namespace.create_collection(name="test_schema")
    assert res2 is None


@pytest.mark.it('should get all collections')
def test_get_collections(test_namespace):
    res = test_namespace.get_collections()
    assert type(res) is list


@pytest.mark.it('should create a collection with schema')
def test_create_schema(test_namespace):
    schema = {
        "$id": "https://example.com/person.schema.json",
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Person",
        "type": "object",
        "properties": {
            "firstName": {
                "type": "string",
                "description": "The persons first name."
            },
            "lastName": {
                "type": "string",
                "description": "The persons last name."
            },
            "age": {
                "description": "Age in years which must be equal to or greater than zero.",
                "type": "integer",
                "minimum": 0
            }
        }
    }
    test_collection = test_namespace.collection("test_schema")
    res = test_collection.create_schema(schema=schema)
    assert res["schema"] is not None


@pytest.mark.it('should update a collection with schema')
def test_update_schema(test_namespace):
    schema = {
        "$id": "https://example.com/person.schema.json",
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Person",
        "type": "object",
        "properties": {
            "firstName": {
                "type": "string",
                "description": "The persons first name."
            }
        }
    }
    test_collection = test_namespace.collection("test_schema")
    res = test_collection.update_schema(schema=schema)
    assert res["schema"] is not None


@pytest.mark.it('should delete a collection')
def test_delete_collection(test_namespace):
    res = test_namespace.delete_collection(name="pytest_collection")
    assert res is None
    res2 = test_namespace.delete_collection(name="test_schema")
    assert res2 is None


@pytest.mark.it('should create a document')
def test_create_document(test_collection, cliff_uuid):
    test_collection.create(path=cliff_uuid, document={
        "first_name": "Cliff",
        "last_name": "Wicklow",
    })
    document = test_collection.get(path=cliff_uuid)
    assert document["first_name"] == "Cliff"


@pytest.mark.it('should create multiple documents')
def test_batch(test_collection):
    id_1 = fake.bothify(text="????????")
    id_2 = fake.bothify(text="????????")
    documents = [{
        "_id": id_1,
        "first_name": "Dang",
        "last_name": "Son",
    }, {
        "_id": id_2,
        "first_name": "Yep",
        "last_name": "Boss",
    }]
    res = test_collection.batch(documents=documents, id_path="_id")
    assert res["documentIds"] is not None

    document_1 = test_collection.get(path=id_1)
    assert document_1["first_name"] == "Dang"

    document_2 = test_collection.get(path=id_2)
    assert document_2["first_name"] == "Yep"


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
    assert len(documents["data"].keys()) == 2
    assert documents["data"][user_id]["last_name"] == "Wicklow"
    assert documents["data"][user_id_2]["last_name"] == "Danger"


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
    document = test_collection.find_one(query={
        "first_name": {"$eq": f"Cliff-Not-There"},
    })
    assert document is None


@pytest.mark.it('should use document functions')
def test_functions(test_collection):
    user_id = str(uuid.uuid4())
    test_collection.create(path=user_id, document={
        "first_name": f"Cliff-{user_id}",
        "last_name": "Wicklow",
        "roles": ["admin", "user"]
    })

    pop_res = test_collection.pop(path=f"{user_id}/roles")
    assert pop_res == "user"

    doc_1 = test_collection.get(path=user_id)
    assert len(doc_1["roles"]) == 1

    test_collection.push(path=f"{user_id}/roles", value="users")
    doc_2 = test_collection.get(path=user_id)
    assert len(doc_2["roles"]) == 2
