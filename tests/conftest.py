"""
Test fixtures
"""

import os
import math

import pytest
from typing import (
    AsyncIterable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    TypeVar,
    TypedDict,
)

import pytest_asyncio

from astrapy.defaults import DEFAULT_KEYSPACE_NAME
from astrapy.db import AstraDB, AstraDBCollection, AsyncAstraDB, AsyncAstraDBCollection

T = TypeVar("T")


ASTRA_DB_APPLICATION_TOKEN = os.environ["ASTRA_DB_APPLICATION_TOKEN"]
ASTRA_DB_API_ENDPOINT = os.environ["ASTRA_DB_API_ENDPOINT"]

ASTRA_DB_KEYSPACE = os.environ.get("ASTRA_DB_KEYSPACE", DEFAULT_KEYSPACE_NAME)

# fixed
TEST_WRITABLE_VECTOR_COLLECTION = "writable_v_col"
TEST_READONLY_VECTOR_COLLECTION = "readonly_v_col"
TEST_WRITABLE_NONVECTOR_COLLECTION = "writable_nonv_col"
TEST_WRITABLE_ALLOWINDEX_NONVECTOR_COLLECTION = "writable_allowindex_nonv_col"
TEST_WRITABLE_DENYINDEX_NONVECTOR_COLLECTION = "writable_denyindex_nonv_col"

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

INDEXING_SAMPLE_DOCUMENT = {
    "_id": "0",
    "A": {
        "a": "A.a",
        "b": "A.b",
    },
    "B": {
        "a": "B.a",
        "b": "B.b",
    },
    "C": {
        "a": "C.a",
        "b": "C.b",
    },
}


class AstraDBCredentials(TypedDict, total=False):
    token: str
    api_endpoint: str
    namespace: Optional[str]


def _batch_iterable(iterable: Iterable[T], batch_size: int) -> Iterable[Iterable[T]]:
    this_batch = []
    for entry in iterable:
        this_batch.append(entry)
        if len(this_batch) == batch_size:
            yield this_batch
            this_batch = []
    if this_batch:
        yield this_batch


@pytest.fixture(scope="session")
def astra_db_credentials_kwargs() -> AstraDBCredentials:
    astra_db_creds: AstraDBCredentials = {
        "token": ASTRA_DB_APPLICATION_TOKEN,
        "api_endpoint": ASTRA_DB_API_ENDPOINT,
        "namespace": ASTRA_DB_KEYSPACE,
    }

    return astra_db_creds


@pytest.fixture(scope="session")
def astra_invalid_db_credentials_kwargs() -> AstraDBCredentials:
    astra_db_creds: AstraDBCredentials = {
        "token": ASTRA_DB_APPLICATION_TOKEN,
        "api_endpoint": "http://localhost:1234",
        "namespace": ASTRA_DB_KEYSPACE,
    }

    return astra_db_creds


@pytest.fixture(scope="session")
def db(astra_db_credentials_kwargs: Dict[str, Optional[str]]) -> AstraDB:
    token = astra_db_credentials_kwargs["token"]
    api_endpoint = astra_db_credentials_kwargs["api_endpoint"]
    namespace = astra_db_credentials_kwargs.get("namespace")

    if token is None or api_endpoint is None:
        raise ValueError("Required ASTRA DB configuration is missing")

    return AstraDB(token=token, api_endpoint=api_endpoint, namespace=namespace)


@pytest_asyncio.fixture(scope="function")
async def async_db(
    astra_db_credentials_kwargs: Dict[str, Optional[str]]
) -> AsyncIterable[AsyncAstraDB]:
    token = astra_db_credentials_kwargs["token"]
    api_endpoint = astra_db_credentials_kwargs["api_endpoint"]
    namespace = astra_db_credentials_kwargs.get("namespace")

    if token is None or api_endpoint is None:
        raise ValueError("Required ASTRA DB configuration is missing")

    async with AsyncAstraDB(
        token=token, api_endpoint=api_endpoint, namespace=namespace
    ) as db:
        yield db


@pytest.fixture(scope="module")
def invalid_db(
    astra_invalid_db_credentials_kwargs: Dict[str, Optional[str]]
) -> AstraDB:
    token = astra_invalid_db_credentials_kwargs["token"]
    api_endpoint = astra_invalid_db_credentials_kwargs["api_endpoint"]
    namespace = astra_invalid_db_credentials_kwargs.get("namespace")

    if token is None or api_endpoint is None:
        raise ValueError("Required ASTRA DB configuration is missing")

    return AstraDB(token=token, api_endpoint=api_endpoint, namespace=namespace)


@pytest.fixture(scope="session")
def readonly_v_collection(db: AstraDB) -> Iterable[AstraDBCollection]:
    collection = db.create_collection(
        TEST_READONLY_VECTOR_COLLECTION,
        dimension=2,
    )

    collection.clear()
    collection.insert_many(VECTOR_DOCUMENTS)

    yield collection

    if int(os.getenv("TEST_SKIP_COLLECTION_DELETE", "0")) == 0:
        db.drop_collection(TEST_READONLY_VECTOR_COLLECTION)


@pytest.fixture(scope="session")
def writable_v_collection(db: AstraDB) -> Iterable[AstraDBCollection]:
    """
    This is lasting for the whole test. Functions can write to it,
    no guarantee (i.e. each test should use a different ID...
    """
    collection = db.create_collection(
        TEST_WRITABLE_VECTOR_COLLECTION,
        dimension=2,
    )

    yield collection

    if int(os.getenv("TEST_SKIP_COLLECTION_DELETE", "0")) == 0:
        db.drop_collection(TEST_WRITABLE_VECTOR_COLLECTION)


@pytest.fixture(scope="function")
def empty_v_collection(
    writable_v_collection: AstraDBCollection,
) -> Iterable[AstraDBCollection]:
    """available empty to each test function."""
    writable_v_collection.clear()
    yield writable_v_collection


@pytest.fixture(scope="function")
def disposable_v_collection(
    writable_v_collection: AstraDBCollection,
) -> Iterable[AstraDBCollection]:
    """available prepopulated to each test function."""
    writable_v_collection.clear()
    writable_v_collection.insert_many(VECTOR_DOCUMENTS)
    yield writable_v_collection


@pytest.fixture(scope="session")
def writable_nonv_collection(db: AstraDB) -> Iterable[AstraDBCollection]:
    """
    This is lasting for the whole test. Functions can write to it,
    no guarantee (i.e. each test should use a different ID...
    """
    collection = db.create_collection(TEST_WRITABLE_NONVECTOR_COLLECTION)

    yield collection

    if int(os.getenv("TEST_SKIP_COLLECTION_DELETE", "0")) == 0:
        db.drop_collection(TEST_WRITABLE_NONVECTOR_COLLECTION)


@pytest.fixture(scope="function")
def allowindex_nonv_collection(db: AstraDB) -> Iterable[AstraDBCollection]:
    """
    This is lasting for the whole test. Functions can write to it,
    no guarantee (i.e. each test should use a different ID...
    """
    collection = db.create_collection(
        TEST_WRITABLE_ALLOWINDEX_NONVECTOR_COLLECTION,
        options={
            "indexing": {
                "allow": [
                    "A",
                    "C.a",
                ],
            },
        },
    )
    collection.upsert(INDEXING_SAMPLE_DOCUMENT)

    yield collection

    if int(os.getenv("TEST_SKIP_COLLECTION_DELETE", "0")) == 0:
        db.drop_collection(TEST_WRITABLE_ALLOWINDEX_NONVECTOR_COLLECTION)


@pytest.fixture(scope="function")
def denyindex_nonv_collection(db: AstraDB) -> Iterable[AstraDBCollection]:
    """
    This is lasting for the whole test. Functions can write to it,
    no guarantee (i.e. each test should use a different ID...

    Note in light of the sample document this almost results in the same
    filtering paths being available ... if one remembers to deny _id here.
    """
    collection = db.create_collection(
        TEST_WRITABLE_DENYINDEX_NONVECTOR_COLLECTION,
        options={
            "indexing": {
                "deny": [
                    "B",
                    "C.b",
                    "_id",
                ],
            },
        },
    )
    collection.upsert(INDEXING_SAMPLE_DOCUMENT)

    yield collection

    if int(os.getenv("TEST_SKIP_COLLECTION_DELETE", "0")) == 0:
        db.drop_collection(TEST_WRITABLE_DENYINDEX_NONVECTOR_COLLECTION)


@pytest.fixture(scope="function")
def empty_nonv_collection(
    writable_nonv_collection: AstraDBCollection,
) -> Iterable[AstraDBCollection]:
    """available empty to each test function."""
    writable_nonv_collection.clear()
    yield writable_nonv_collection


@pytest.fixture(scope="module")
def invalid_writable_v_collection(
    invalid_db: AstraDB,
) -> Iterable[AstraDBCollection]:
    collection = invalid_db.collection(
        TEST_WRITABLE_VECTOR_COLLECTION,
    )

    yield collection


@pytest.fixture(scope="function")
def pagination_v_collection(
    empty_v_collection: AstraDBCollection,
) -> Iterable[AstraDBCollection]:
    INSERT_BATCH_SIZE = 20  # max 20, fixed by API constraints
    N = 200  # must be EVEN

    def _mk_vector(index: int, n_total_steps: int) -> List[float]:
        angle = 2 * math.pi * index / n_total_steps
        return [math.cos(angle), math.sin(angle)]

    inserted_ids: Set[str] = set()
    for i_batch in _batch_iterable(range(N), INSERT_BATCH_SIZE):
        batch_ids = empty_v_collection.insert_many(
            documents=[{"_id": str(i), "$vector": _mk_vector(i, N)} for i in i_batch]
        )["status"]["insertedIds"]
        inserted_ids = inserted_ids | set(batch_ids)
    assert inserted_ids == {str(i) for i in range(N)}

    yield empty_v_collection


@pytest_asyncio.fixture(scope="function")
async def async_readonly_v_collection(
    async_db: AsyncAstraDB,
    readonly_v_collection: AstraDBCollection,
) -> AsyncIterable[AsyncAstraDBCollection]:
    """
    This fixture piggybacks on its sync counterpart (and depends on it):
    it must not actually do anything to the collection
    """
    collection = await async_db.collection(TEST_READONLY_VECTOR_COLLECTION)

    yield collection


@pytest_asyncio.fixture(scope="function")
async def async_writable_v_collection(
    async_db: AsyncAstraDB,
    writable_v_collection: AstraDBCollection,
) -> AsyncIterable[AsyncAstraDBCollection]:
    """
    This fixture piggybacks on its sync counterpart (and depends on it):
    it must not actually do anything to the collection
    """
    collection = await async_db.collection(TEST_WRITABLE_VECTOR_COLLECTION)

    yield collection


@pytest_asyncio.fixture(scope="function")
async def async_empty_v_collection(
    async_writable_v_collection: AsyncAstraDBCollection,
    empty_v_collection: AstraDBCollection,
) -> AsyncIterable[AsyncAstraDBCollection]:
    """
    available empty to each test function.

    This fixture piggybacks on its sync counterpart (and depends on it):
    it must not actually do anything to the collection
    """
    yield async_writable_v_collection


@pytest_asyncio.fixture(scope="function")
async def async_disposable_v_collection(
    async_writable_v_collection: AsyncAstraDBCollection,
    disposable_v_collection: AstraDBCollection,
) -> AsyncIterable[AsyncAstraDBCollection]:
    """
    available prepopulated to each test function.

    This fixture piggybacks on its sync counterpart (and depends on it):
    it must not actually do anything to the collection
    """
    yield async_writable_v_collection


@pytest_asyncio.fixture(scope="function")
async def async_writable_nonv_collection(
    async_db: AsyncAstraDB,
    writable_nonv_collection: AstraDBCollection,
) -> AsyncIterable[AsyncAstraDBCollection]:
    """
    This fixture piggybacks on its sync counterpart (and depends on it):
    it must not actually do anything to the collection
    """
    collection = await async_db.collection(TEST_WRITABLE_NONVECTOR_COLLECTION)

    yield collection


@pytest_asyncio.fixture(scope="function")
async def async_empty_nonv_collection(
    async_writable_nonv_collection: AsyncAstraDBCollection,
    empty_nonv_collection: AstraDBCollection,
) -> AsyncIterable[AsyncAstraDBCollection]:
    """
    available empty to each test function.

    This fixture piggybacks on its sync counterpart (and depends on it):
    it must not actually do anything to the collection
    """
    yield async_writable_nonv_collection


@pytest_asyncio.fixture(scope="function")
async def async_pagination_v_collection(
    async_empty_v_collection: AsyncAstraDBCollection,
    pagination_v_collection: AstraDBCollection,
) -> AsyncIterable[AsyncAstraDBCollection]:
    """
    This fixture piggybacks on its sync counterpart (and depends on it):
    it must not actually do anything to the collection
    """
    yield async_empty_v_collection
