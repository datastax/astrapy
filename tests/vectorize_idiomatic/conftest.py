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

"""
Fixtures specific to testing on vectorize-ready Data API.
"""

from __future__ import annotations

import os
from typing import Any, Iterable

import pytest

from astrapy import AsyncCollection, AsyncDatabase, Collection, DataAPIClient, Database
from astrapy.constants import VectorMetric

from ..conftest import (
    IS_ASTRA_DB,
    DataAPICredentials,
    DataAPICredentialsInfo,
    clean_nulls_from_dict,
)

TEST_SERVICE_COLLECTION_NAME = "test_indepth_vectorize_collection"


@pytest.fixture(scope="session")
def sync_database(
    data_api_credentials_kwargs: DataAPICredentials,
    data_api_credentials_info: DataAPICredentialsInfo,
) -> Iterable[Database]:
    env = data_api_credentials_info["environment"]
    client = DataAPIClient(environment=env)
    database = client.get_database(
        data_api_credentials_kwargs["api_endpoint"],
        token=data_api_credentials_kwargs["token"],
        keyspace=data_api_credentials_kwargs["keyspace"],
    )
    yield database


@pytest.fixture(scope="function")
def async_database(
    sync_database: Database,
) -> Iterable[AsyncDatabase]:
    yield sync_database.to_async()


@pytest.fixture(scope="session")
def service_collection_parameters() -> Iterable[dict[str, Any]]:
    yield {
        "dimension": 1536,
        "provider": "openai",
        "modelName": "text-embedding-ada-002",
        "api_key": os.environ["HEADER_EMBEDDING_API_KEY_OPENAI"],
    }


@pytest.fixture(scope="session")
def sync_service_collection(
    data_api_credentials_kwargs: DataAPICredentials,
    sync_database: Database,
    service_collection_parameters: dict[str, Any],
) -> Iterable[Collection]:
    """
    An actual collection on DB, in the main keyspace.
    """
    params = service_collection_parameters
    collection = sync_database.create_collection(
        TEST_SERVICE_COLLECTION_NAME,
        metric=VectorMetric.DOT_PRODUCT,
        service={"provider": params["provider"], "modelName": params["modelName"]},
        embedding_api_key=params["api_key"],
    )
    yield collection

    sync_database.drop_collection(TEST_SERVICE_COLLECTION_NAME)


@pytest.fixture(scope="function")
def sync_empty_service_collection(
    sync_service_collection: Collection,
) -> Iterable[Collection]:
    """Emptied for each test function"""
    sync_service_collection.delete_many({})
    yield sync_service_collection


@pytest.fixture(scope="function")
def async_empty_service_collection(
    sync_empty_service_collection: Collection,
) -> Iterable[AsyncCollection]:
    """Emptied for each test function"""
    yield sync_empty_service_collection.to_async()


__all__ = [
    "sync_database",
    "async_database",
    "clean_nulls_from_dict",
    "IS_ASTRA_DB",
]
