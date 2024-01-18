"""
Test fixtures
"""
import os
import math

import pytest
from typing import AsyncIterable, Dict, Iterable, List, Optional, Set, TypeVar

import pytest_asyncio

from astrapy.defaults import DEFAULT_KEYSPACE_NAME
from astrapy.db import AstraDB, AstraDBCollection, AsyncAstraDB, AsyncAstraDBCollection

T = TypeVar("T")


ASTRA_DB_APPLICATION_TOKEN = os.environ.get("ASTRA_DB_APPLICATION_TOKEN")
ASTRA_DB_API_ENDPOINT = os.environ.get("ASTRA_DB_API_ENDPOINT")
ASTRA_DB_KEYSPACE = os.environ.get("ASTRA_DB_KEYSPACE", DEFAULT_KEYSPACE_NAME)

# fixed
TEST_WRITABLE_VECTOR_COLLECTION = "writable_v_col"
TEST_READONLY_VECTOR_COLLECTION = "readonly_v_col"
TEST_WRITABLE_NONVECTOR_COLLECTION = "writable_nonv_col"

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


@pytest.fixture(scope="session")
def db(astra_db_credentials_kwargs: Dict[str, Optional[str]]) -> AstraDB:
    return AstraDB(**astra_db_credentials_kwargs)


@pytest.fixture(scope="session")
async def async_db(
    astra_db_credentials_kwargs: Dict[str, Optional[str]]
) -> AsyncIterable[AsyncAstraDB]:
    async with AsyncAstraDB(**astra_db_credentials_kwargs) as db:
        yield db


@pytest.fixture(scope="module")
def invalid_db(
    astra_invalid_db_credentials_kwargs: Dict[str, Optional[str]]
) -> AstraDB:
    return AstraDB(**astra_invalid_db_credentials_kwargs)


@pytest.fixture(scope="session")
def readonly_v_collection(db: AstraDB) -> Iterable[AstraDBCollection]:
    collection = db.create_collection(
        TEST_READONLY_VECTOR_COLLECTION,
        dimension=2,
    )

    collection.truncate()
    collection.insert_many(VECTOR_DOCUMENTS)

    yield collection

    if int(os.getenv("TEST_SKIP_COLLECTION_DELETE", "0")) == 0:
        db.delete_collection(TEST_READONLY_VECTOR_COLLECTION)


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
        db.delete_collection(TEST_WRITABLE_VECTOR_COLLECTION)


@pytest.fixture(scope="function")
def empty_v_collection(
    writable_v_collection: AstraDBCollection,
) -> Iterable[AstraDBCollection]:
    """available empty to each test function."""
    writable_v_collection.truncate()
    yield writable_v_collection


@pytest.fixture(scope="function")
def disposable_v_collection(
    writable_v_collection: AstraDBCollection,
) -> Iterable[AstraDBCollection]:
    """available prepopulated to each test function."""
    writable_v_collection.truncate()
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
        db.delete_collection(TEST_WRITABLE_NONVECTOR_COLLECTION)


@pytest.fixture(scope="function")
def empty_nonv_collection(
    writable_nonv_collection: AstraDBCollection,
) -> Iterable[AstraDBCollection]:
    """available empty to each test function."""
    writable_nonv_collection.truncate()
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
) -> AsyncIterable[AsyncAstraDBCollection]:
    """
    This is lasting for the whole test. Functions can write to it,
    no guarantee (i.e. each test should use a different ID...
    """
    collection = await async_db.create_collection(
        TEST_READONLY_VECTOR_COLLECTION,
        dimension=2,
    )

    await collection.truncate()
    await collection.insert_many(VECTOR_DOCUMENTS)

    yield collection

    if int(os.getenv("TEST_SKIP_COLLECTION_DELETE", "0")) == 0:
        await async_db.delete_collection(TEST_READONLY_VECTOR_COLLECTION)


@pytest_asyncio.fixture(scope="session")
async def async_writable_v_collection(
    async_db: AsyncAstraDB,
) -> AsyncIterable[AsyncAstraDBCollection]:
    """
    This is lasting for the whole test. Functions can write to it,
    no guarantee (i.e. each test should use a different ID...
    """
    collection = await async_db.create_collection(
        TEST_WRITABLE_VECTOR_COLLECTION,
        dimension=2,
    )

    yield collection

    if int(os.getenv("TEST_SKIP_COLLECTION_DELETE", "0")) == 0:
        await async_db.delete_collection(TEST_WRITABLE_VECTOR_COLLECTION)


@pytest_asyncio.fixture(scope="function")
async def async_empty_v_collection(
    async_writable_v_collection: AsyncAstraDBCollection,
) -> AsyncIterable[AsyncAstraDBCollection]:
    """available empty to each test function."""
    await async_writable_v_collection.truncate()
    yield async_writable_v_collection


@pytest_asyncio.fixture(scope="function")
async def async_disposable_v_collection(
    async_writable_v_collection: AsyncAstraDBCollection,
) -> AsyncIterable[AsyncAstraDBCollection]:
    """available prepopulated to each test function."""
    await async_writable_v_collection.truncate()
    await async_writable_v_collection.insert_many(VECTOR_DOCUMENTS)
    yield async_writable_v_collection


@pytest_asyncio.fixture(scope="session")
async def async_writable_nonv_collection(
    async_db: AsyncAstraDB,
) -> AsyncIterable[AsyncAstraDBCollection]:
    """
    This is lasting for the whole test. Functions can write to it,
    no guarantee (i.e. each test should use a different ID...
    """
    collection = await async_db.create_collection(
        TEST_WRITABLE_VECTOR_COLLECTION,
        dimension=2,
    )

    yield collection

    if int(os.getenv("TEST_SKIP_COLLECTION_DELETE", "0")) == 0:
        await async_db.delete_collection(TEST_WRITABLE_VECTOR_COLLECTION)


@pytest_asyncio.fixture(scope="function")
async def async_empty_nonv_collection(
    async_writable_nonv_collection: AsyncAstraDBCollection,
) -> AsyncIterable[AsyncAstraDBCollection]:
    """available empty to each test function."""
    await async_writable_nonv_collection.truncate()
    yield async_writable_nonv_collection


@pytest_asyncio.fixture(scope="function")
async def async_pagination_v_collection(
    async_empty_v_collection: AsyncAstraDBCollection,
) -> AsyncIterable[AsyncAstraDBCollection]:
    INSERT_BATCH_SIZE = 20  # max 20, fixed by API constraints
    N = 200  # must be EVEN

    def _mk_vector(index: int, n_total_steps: int) -> List[float]:
        angle = 2 * math.pi * index / n_total_steps
        return [math.cos(angle), math.sin(angle)]

    inserted_ids: Set[str] = set()
    for i_batch in _batch_iterable(range(N), INSERT_BATCH_SIZE):
        insert_response = await async_empty_v_collection.insert_many(
            documents=[{"_id": str(i), "$vector": _mk_vector(i, N)} for i in i_batch]
        )
        batch_ids = insert_response["status"]["insertedIds"]
        inserted_ids = inserted_ids | set(batch_ids)
    assert inserted_ids == {str(i) for i in range(N)}

    yield async_empty_v_collection
