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

from astrapy import (
    AsyncDatabase,
    DataAPIClient,
    Database,
)
from astrapy.admin import Environment, parse_api_endpoint


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


__all__ = [
    "sync_database",
    "async_database",
    "env_filter_match",
]
