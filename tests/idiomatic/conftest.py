"""Fixtures specific to the idiomatic-side testing, if any."""

from typing import Iterable
import pytest

from ..conftest import AstraDBCredentials
from astrapy import AsyncCollection, AsyncDatabase, Collection, Database

TEST_COLLECTION_NAME = "test_coll_sync"


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
def sync_collection(
    astra_db_credentials_kwargs: AstraDBCredentials,
    sync_database: Database,
) -> Iterable[Collection]:
    yield Collection(
        sync_database,
        TEST_COLLECTION_NAME,
        namespace=astra_db_credentials_kwargs["namespace"],
    )


@pytest.fixture(scope="session")
def async_collection(
    astra_db_credentials_kwargs: AstraDBCredentials,
    async_database: AsyncDatabase,
) -> Iterable[AsyncCollection]:
    yield AsyncCollection(
        async_database,
        TEST_COLLECTION_NAME,
        namespace=astra_db_credentials_kwargs["namespace"],
    )


__all__ = [
    "AstraDBCredentials",
    "sync_database",
    "async_database",
]
