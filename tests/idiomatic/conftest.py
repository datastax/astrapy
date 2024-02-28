"""Fixtures specific to the idiomatic-side testing, if any."""

import os
from typing import AsyncIterable, Iterable
import pytest

from ..conftest import AstraDBCredentials
from astrapy import AsyncCollection, AsyncDatabase, Collection, Database

TEST_COLLECTION_INSTANCE_NAME = "test_coll_instance"
TEST_COLLECTION_NAME = "id_test_collection"
TEST_CREATE_DELETE_VECTOR_COLLECTION_NAME = "ephemeral_v_col"

ASTRA_DB_SECONDARY_KEYSPACE = os.environ.get("ASTRA_DB_SECONDARY_KEYSPACE")


@pytest.fixture(scope="session")
def sync_database(
    astra_db_credentials_kwargs: AstraDBCredentials,
) -> Iterable[Database]:
    yield Database(**astra_db_credentials_kwargs)


@pytest.fixture(scope="session")
def async_database(
    astra_db_credentials_kwargs: AstraDBCredentials,
) -> Iterable[AsyncDatabase]:
    yield AsyncDatabase(**astra_db_credentials_kwargs)


@pytest.fixture(scope="session")
def sync_collection_instance(
    astra_db_credentials_kwargs: AstraDBCredentials,
    sync_database: Database,
) -> Iterable[Collection]:
    """Just an instance of the class, no DB-level stuff."""
    yield Collection(
        sync_database,
        TEST_COLLECTION_INSTANCE_NAME,
        # namespace=astra_db_credentials_kwargs["namespace"],
    )


@pytest.fixture(scope="session")
def async_collection_instance(
    astra_db_credentials_kwargs: AstraDBCredentials,
    async_database: AsyncDatabase,
) -> Iterable[AsyncCollection]:
    """Just an instance of the class, no DB-level stuff."""
    yield AsyncCollection(
        async_database,
        TEST_COLLECTION_INSTANCE_NAME,
        # namespace=astra_db_credentials_kwargs["namespace"],
    )


@pytest.fixture(scope="session")
def sync_collection(
    astra_db_credentials_kwargs: AstraDBCredentials,
    sync_database: Database,
) -> Iterable[Collection]:
    """An actual collection on DB, in the main namespace"""
    collection = sync_database.create_collection(
        TEST_COLLECTION_NAME,
        dimension=2,
        metric="dot_product",
        indexing={"deny": ["not_indexed"]},
    )
    yield collection

    sync_database.drop_collection(TEST_COLLECTION_NAME)


@pytest.fixture(scope="function")
def sync_empty_collection(sync_collection: Collection) -> Iterable[Collection]:
    """Emptied for each test function"""
    sync_collection.delete_many(filter={})
    yield sync_collection


@pytest.fixture(scope="session")
def async_collection(
    sync_collection: Collection,
) -> Iterable[AsyncCollection]:
    """An actual collection on DB, the same as the sync counterpart"""
    yield sync_collection.to_async()


@pytest.fixture(scope="function")
async def async_empty_collection(
    sync_empty_collection: Collection,
) -> AsyncIterable[AsyncCollection]:
    """Emptied for each test function"""
    yield sync_empty_collection.to_async()


__all__ = [
    "AstraDBCredentials",
    "sync_database",
    "async_database",
]
