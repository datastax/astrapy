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

TEST_COLLECTION_NAME = "test_collection"
TEST_FIXTURE_COLLECTION_NAME = "test_fixture_collection"
TEST_FIXTURE_PROJECTION_COLLECTION_NAME = "test_projection_collection"
TEST_NONVECTOR_COLLECTION_NAME = "test_nonvector_collection"

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
def projection_collection(db: AstraDB) -> Iterable[AstraDBCollection]:
    collection = db.create_collection(
        collection_name=TEST_FIXTURE_PROJECTION_COLLECTION_NAME, dimension=5
    )

    collection.insert_many(
        [
            {
                "_id": "1",
                "text": "Sample entry number <1>",
                "otherfield": {"subfield": "x1y"},
                "anotherfield": "delete_me",
                "$vector": [0.1, 0.15, 0.3, 0.12, 0.05],
            },
            {
                "_id": "2",
                "text": "Sample entry number <2>",
                "otherfield": {"subfield": "x2y"},
                "anotherfield": "delete_me",
                "$vector": [0.45, 0.09, 0.01, 0.2, 0.11],
            },
            {
                "_id": "3",
                "text": "Sample entry number <3>",
                "otherfield": {"subfield": "x3y"},
                "anotherfield": "dont_delete_me",
                "$vector": [0.1, 0.05, 0.08, 0.3, 0.6],
            },
        ],
    )

    yield collection

    db.delete_collection(collection_name=TEST_FIXTURE_PROJECTION_COLLECTION_NAME)
