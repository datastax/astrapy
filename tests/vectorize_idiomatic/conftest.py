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

This whole directory of tests can be run with a docker-compose
local Data API running. In that case, its endpoint and token must be made
available as the env.vars:
    LOCAL_DATA_API_ENDPOINT
    LOCAL_DATA_API_APPLICATION_TOKEN
"""

import os
from typing import Iterable, List, Tuple
import pytest

from ..conftest import AstraDBCredentials
from astrapy import AsyncCollection, Collection
from astrapy import (
    AsyncDatabase,
    DataAPIClient,
    Database,
)
from astrapy.constants import VectorMetric
from astrapy.admin import parse_api_endpoint
from astrapy.constants import Environment

TEST_SERVICE_COLLECTION_NAME = "test_indepth_vectorize_collection"


def is_nvidia_service_available() -> bool:
    return all(
        [
            "us-west-2" in os.environ.get("ASTRA_DB_API_ENDPOINT", ""),
            "astra-dev.datastax.com" in os.environ.get("ASTRA_DB_API_ENDPOINT", ""),
        ]
    )


def _parse_to_testing_environment(api_endpoint: str) -> Tuple[str, str]:
    parsed = parse_api_endpoint(api_endpoint)
    if parsed is not None:
        return (parsed.environment, parsed.region)
    else:
        return (Environment.OTHER, "no-region")


def _env_filter_match1(
    api_endpoint: str, auth_type: str, env_filter: Tuple[str, str, str]
) -> bool:
    env, reg = _parse_to_testing_environment(api_endpoint)

    def _match(s1: str, s2: str) -> bool:
        if s1 == "*" or s2 == "*":
            return True
        else:
            return s1.lower() == s2.lower()

    return all(_match(pc1, pc2) for pc1, pc2 in zip((env, reg, auth_type), env_filter))


def env_filter_match(auth_type: str, env_filters: List[Tuple[str, str, str]]) -> bool:
    api_endpoint = os.environ.get(
        "LOCAL_DATA_API_ENDPOINT", os.environ.get("ASTRA_DB_API_ENDPOINT", "")
    )
    return any(
        _env_filter_match1(api_endpoint, auth_type, env_filter)
        for env_filter in env_filters
    )


@pytest.fixture(scope="session")
def sync_database() -> Iterable[Database]:
    if "LOCAL_DATA_API_ENDPOINT" in os.environ:
        api_endpoint = os.environ["LOCAL_DATA_API_ENDPOINT"]
        token = os.environ["LOCAL_DATA_API_APPLICATION_TOKEN"]
        client = DataAPIClient(token=token, environment=Environment.OTHER)
        database = client.get_database_by_api_endpoint(api_endpoint)
        database.get_database_admin().create_namespace("default_keyspace")
    elif "ASTRA_DB_API_ENDPOINT" in os.environ:
        # regular Astra DB instance
        database = Database(
            api_endpoint=os.environ["ASTRA_DB_API_ENDPOINT"],
            token=os.environ["ASTRA_DB_APPLICATION_TOKEN"],
            namespace=os.environ.get("ASTRA_DB_KEYSPACE"),
        )
    else:
        raise ValueError("No credentials.")
    yield database


@pytest.fixture(scope="function")
def async_database(
    sync_database: Database,
) -> Iterable[AsyncDatabase]:
    yield sync_database.to_async()


@pytest.fixture(scope="session")
def sync_service_collection(
    astra_db_credentials_kwargs: AstraDBCredentials,
    sync_database: Database,
) -> Iterable[Collection]:
    """
    An actual collection on DB, in the main namespace.
    TODO: automate that: if it's nvidia, it has to be some env/regions,
        while if it's openai it can be all (vectorize) regions in prod.
    """
    # collection = sync_database.create_collection(
    #     TEST_SERVICE_COLLECTION_NAME,
    #     metric=VectorMetric.DOT_PRODUCT,
    #     service={"provider": "nvidia", "modelName": "NV-Embed-QA"},
    # )
    collection = sync_database.create_collection(
        TEST_SERVICE_COLLECTION_NAME,
        metric=VectorMetric.DOT_PRODUCT,
        service={"provider": "openai", "modelName": "text-embedding-ada-002"},
        embedding_api_key=os.environ["HEADER_EMBEDDING_API_KEY_OPENAI"],
    )
    yield collection

    sync_database.drop_collection(TEST_SERVICE_COLLECTION_NAME)


@pytest.fixture(scope="session")
def service_vector_dimension() -> Iterable[int]:
    # yield 1024
    yield 1536


@pytest.fixture(scope="function")
def sync_empty_service_collection(
    sync_service_collection: Collection,
) -> Iterable[Collection]:
    """Emptied for each test function"""
    sync_service_collection.delete_all()
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
    "env_filter_match",
    "is_nvidia_service_available",
]
