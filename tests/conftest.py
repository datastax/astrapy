"""
Test fixtures
"""
import os
import pytest
import uuid
from typing import Dict, Iterable, Optional

from dotenv import load_dotenv

from astrapy.defaults import DEFAULT_KEYSPACE_NAME
from astrapy.types import API_DOC
from astrapy.db import AstraDB, AstraDBCollection


load_dotenv()

ASTRA_DB_APPLICATION_TOKEN = os.environ.get("ASTRA_DB_APPLICATION_TOKEN")
ASTRA_DB_API_ENDPOINT = os.environ.get("ASTRA_DB_API_ENDPOINT")
ASTRA_DB_KEYSPACE = os.environ.get("ASTRA_DB_KEYSPACE", DEFAULT_KEYSPACE_NAME)

# tofix
TEST_COLLECTION_NAME = "test_collection"
TEST_FIXTURE_COLLECTION_NAME = "test_fixture_collection"
TEST_NONVECTOR_COLLECTION_NAME = "test_nonvector_collection"

# fixed
TEST_READONLY_VECTOR_COLLECTION = "readonly_v_col"
TEST_DISPOSABLE_VECTOR_COLLECTION = "disposable_v_col"

VECTOR_DOCUMENTS = [
    {
        "_id": "1",
        "text": "Sample entry number <1>",
        "otherfield": {"subfield": "x1y"},
        "anotherfield": "alpha",
        "$vector": [0.1, 0.9],
    },
    {
        "_id": "2",
        "text": "Sample entry number <2>",
        "otherfield": {"subfield": "x2y"},
        "anotherfield": "alpha",
        "$vector": [0.5, 0.5],
    },
    {
        "_id": "3",
        "text": "Sample entry number <3>",
        "otherfield": {"subfield": "x3y"},
        "anotherfield": "omega",
        "$vector": [0.9, 0.1],
    },
]


@pytest.fixture(scope="session")
def astra_db_credentials_kwargs() -> Dict[str, Optional[str]]:
    return {
        "token": ASTRA_DB_APPLICATION_TOKEN,
        "api_endpoint": ASTRA_DB_API_ENDPOINT,
        "namespace": ASTRA_DB_KEYSPACE,
    }

@pytest.fixture(scope="module")
def cliff_uuid() -> str:
    return str(uuid.uuid4())


@pytest.fixture(scope="module")
def vv_uuid() -> str:
    return str(uuid.uuid4())


@pytest.fixture(scope="module")
def db(astra_db_credentials_kwargs) -> AstraDB:
    return AstraDB(**astra_db_credentials_kwargs)


@pytest.fixture(scope="module")
def cliff_data(cliff_uuid: str) -> API_DOC:
    json_query = {
        "_id": cliff_uuid,
        "first_name": "Cliff",
        "last_name": "Wicklow",
    }

    return json_query


@pytest.fixture(scope="module")
def collection(db: AstraDB, cliff_data: API_DOC) -> Iterable[AstraDBCollection]:
    db.delete_collection(collection_name=TEST_FIXTURE_COLLECTION_NAME)
    collection = db.create_collection(
        collection_name=TEST_FIXTURE_COLLECTION_NAME, dimension=5
    )
    collection.insert_one(document=cliff_data)

    yield collection

    db.delete_collection(collection_name=TEST_FIXTURE_COLLECTION_NAME)


@pytest.fixture(scope="module")
def readonly_vector_collection(db: AstraDB) -> Iterable[AstraDBCollection]:
    collection = db.create_collection(
        collection_name=TEST_READONLY_VECTOR_COLLECTION, dimension=2
    )

    collection.insert_many(VECTOR_DOCUMENTS)

    yield collection

    db.delete_collection(collection_name=TEST_READONLY_VECTOR_COLLECTION)


@pytest.fixture(scope="function")
def disposable_vector_collection(db: AstraDB) -> Iterable[AstraDBCollection]:
    collection = db.create_collection(
        collection_name=TEST_DISPOSABLE_VECTOR_COLLECTION, dimension=2
    )

    collection.insert_many(VECTOR_DOCUMENTS)

    yield collection

    db.delete_collection(collection_name=TEST_DISPOSABLE_VECTOR_COLLECTION)
