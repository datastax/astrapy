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
Fixtures specific to testing on locally-running Data API.

This whole directory of tests must be run with a docker-compose
local Data API running, its endpoint and token being available as env.vars:
    LOCAL_DATA_API_ENDPOINT
    LOCAL_DATA_API_APPLICATION_TOKEN
"""

import os
from typing import Iterable
import pytest

from astrapy import (
    AsyncDatabase,
    DataAPIClient,
    Database,
)
from astrapy.admin import Environment


@pytest.fixture(scope="session")
def sync_local_database() -> Iterable[Database]:
    api_endpoint = os.environ["LOCAL_DATA_API_ENDPOINT"]
    token = os.environ["LOCAL_DATA_API_APPLICATION_TOKEN"]
    client = DataAPIClient(token=token, environment=Environment.OTHER)
    database = client.get_database_by_api_endpoint(api_endpoint)
    database.get_database_admin().create_namespace("default_keyspace")
    yield database


@pytest.fixture(scope="function")
def async_local_database(
    sync_local_database: Database,
) -> Iterable[AsyncDatabase]:
    yield sync_local_database.to_async()


__all__ = [
    "sync_local_database",
    "async_local_database",
]
