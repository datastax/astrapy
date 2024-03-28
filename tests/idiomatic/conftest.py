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

"""Fixtures specific to the non-core-side testing."""

import os
from typing import Iterable
import pytest

from ..conftest import AstraDBCredentials
from astrapy import AsyncCollection, AsyncDatabase, Collection, Database
from astrapy.constants import VectorMetric

TEST_COLLECTION_INSTANCE_NAME = "test_coll_instance"
TEST_COLLECTION_NAME = "id_test_collection"
TEST_SERVICE_COLLECTION_NAME = "test_service_collection"

ASTRA_DB_SECONDARY_KEYSPACE = os.environ.get("ASTRA_DB_SECONDARY_KEYSPACE")


def is_vector_service_available() -> bool:
    return all(
        [
            "us-west-2" in os.environ.get("ASTRA_DB_API_ENDPOINT", ""),
            "astra-dev.datastax.com" in os.environ.get("ASTRA_DB_API_ENDPOINT", ""),
        ]
    )


@pytest.fixture(scope="session")
def sync_database(
    astra_db_credentials_kwargs: AstraDBCredentials,
) -> Iterable[Database]:
    yield Database(**astra_db_credentials_kwargs)


@pytest.fixture(scope="function")
def async_database(
    sync_database: Database,
) -> Iterable[AsyncDatabase]:
    yield sync_database.to_async()


@pytest.fixture(scope="session")
def sync_collection_instance(
    astra_db_credentials_kwargs: AstraDBCredentials,
    sync_database: Database,
) -> Iterable[Collection]:
    """Just an instance of the class, no DB-level stuff."""
    yield Collection(
        sync_database,
        TEST_COLLECTION_INSTANCE_NAME,
    )


@pytest.fixture(scope="function")
def async_collection_instance(
    sync_collection_instance: Collection,
) -> Iterable[AsyncCollection]:
    """Just an instance of the class, no DB-level stuff."""
    yield sync_collection_instance.to_async()


@pytest.fixture(scope="session")
def sync_collection(
    astra_db_credentials_kwargs: AstraDBCredentials,
    sync_database: Database,
) -> Iterable[Collection]:
    """An actual collection on DB, in the main namespace"""
    collection = sync_database.create_collection(
        TEST_COLLECTION_NAME,
        dimension=2,
        metric=VectorMetric.COSINE,
        indexing={"deny": ["not_indexed"]},
    )
    yield collection

    sync_database.drop_collection(TEST_COLLECTION_NAME)


@pytest.fixture(scope="session")
def sync_service_collection(
    astra_db_credentials_kwargs: AstraDBCredentials,
    sync_database: Database,
) -> Iterable[Collection]:
    """An actual collection on DB, in the main namespace"""
    collection = sync_database.create_collection(
        TEST_SERVICE_COLLECTION_NAME,
        metric=VectorMetric.DOT_PRODUCT,
        service={"provider": "nvidia", "modelName": "NV-Embed-QA"},
    )
    yield collection

    sync_database.drop_collection(TEST_SERVICE_COLLECTION_NAME)


@pytest.fixture(scope="function")
def sync_empty_collection(sync_collection: Collection) -> Iterable[Collection]:
    """Emptied for each test function"""
    sync_collection.delete_all()
    yield sync_collection


@pytest.fixture(scope="function")
def sync_empty_service_collection(
    sync_service_collection: Collection,
) -> Iterable[Collection]:
    """Emptied for each test function"""
    sync_service_collection.delete_all()
    yield sync_service_collection


@pytest.fixture(scope="function")
def async_collection(
    sync_collection: Collection,
) -> Iterable[AsyncCollection]:
    """An actual collection on DB, the same as the sync counterpart"""
    yield sync_collection.to_async()


@pytest.fixture(scope="function")
def async_empty_collection(
    sync_empty_collection: Collection,
) -> Iterable[AsyncCollection]:
    """Emptied for each test function"""
    yield sync_empty_collection.to_async()


@pytest.fixture(scope="function")
def async_empty_service_collection(
    sync_empty_service_collection: Collection,
) -> Iterable[AsyncCollection]:
    """Emptied for each test function"""
    yield sync_empty_service_collection.to_async()


__all__ = [
    "AstraDBCredentials",
    "sync_database",
    "async_database",
]
