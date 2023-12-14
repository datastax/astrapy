"""
Test fixtures
"""
import os
import pytest
import uuid
from typing import Dict, Iterable, Optional

from dotenv import load_dotenv

from astrapy.defaults import DEFAULT_KEYSPACE_NAME
from astrapy.db import AstraDB, AstraDBCollection


load_dotenv()

ASTRA_DB_APPLICATION_TOKEN = os.environ.get("ASTRA_DB_APPLICATION_TOKEN")
ASTRA_DB_API_ENDPOINT = os.environ.get("ASTRA_DB_API_ENDPOINT")
ASTRA_DB_KEYSPACE = os.environ.get("ASTRA_DB_KEYSPACE", DEFAULT_KEYSPACE_NAME)

# fixed
TEST_WRITABLE_VECTOR_COLLECTION = "writable_v_col"
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


@pytest.fixture(scope="session")
def astra_invalid_db_credentials_kwargs() -> Dict[str, Optional[str]]:
    return {
        "token": ASTRA_DB_APPLICATION_TOKEN,
        "api_endpoint": "http://localhost:1234",
        "namespace": ASTRA_DB_KEYSPACE,
    }


@pytest.fixture(scope="module")
def cliff_uuid() -> str:
    return str(uuid.uuid4())


@pytest.fixture(scope="module")
def vv_uuid() -> str:
    return str(uuid.uuid4())


@pytest.fixture(scope="module")
def db(astra_db_credentials_kwargs: Dict[str, Optional[str]]) -> AstraDB:
    return AstraDB(**astra_db_credentials_kwargs)


@pytest.fixture(scope="module")
def invalid_db(
    astra_invalid_db_credentials_kwargs: Dict[str, Optional[str]]
) -> AstraDB:
    return AstraDB(**astra_invalid_db_credentials_kwargs)


@pytest.fixture(scope="module")
def writable_vector_collection(db: AstraDB) -> Iterable[AstraDBCollection]:
    """
    This is lasting for the whole test. Functions can write to it,
    no guarantee (i.e. each test should use a different ID...
    """
    collection = db.create_collection(
        TEST_WRITABLE_VECTOR_COLLECTION,
        dimension=2,
    )

    collection.insert_many(VECTOR_DOCUMENTS)

    yield collection

    db.delete_collection(TEST_WRITABLE_VECTOR_COLLECTION)


@pytest.fixture(scope="module")
def invalid_writable_vector_collection(
    invalid_db: AstraDB,
) -> Iterable[AstraDBCollection]:
    """
    This is lasting for the whole test. Functions can write to it,
    no guarantee (i.e. each test should use a different ID...
    """
    collection = invalid_db.collection(
        TEST_WRITABLE_VECTOR_COLLECTION,
    )

    yield collection


@pytest.fixture(scope="module")
def readonly_vector_collection(db: AstraDB) -> Iterable[AstraDBCollection]:
    collection = db.create_collection(
        TEST_READONLY_VECTOR_COLLECTION,
        dimension=2,
    )

    collection.insert_many(VECTOR_DOCUMENTS)

    yield collection

    db.delete_collection(TEST_READONLY_VECTOR_COLLECTION)


@pytest.fixture(scope="function")
def disposable_vector_collection(db: AstraDB) -> Iterable[AstraDBCollection]:
    collection = db.create_collection(
        TEST_DISPOSABLE_VECTOR_COLLECTION,
        dimension=2,
    )

    collection.insert_many(VECTOR_DOCUMENTS)

    yield collection

    db.delete_collection(TEST_DISPOSABLE_VECTOR_COLLECTION)
