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

from __future__ import annotations

from typing import Dict

from preprocess_env import (
    ASTRA_DB_API_ENDPOINT,
    ASTRA_DB_KEYSPACE,
    ASTRA_DB_TOKEN_PROVIDER,
    IS_ASTRA_DB,
    LOCAL_DATA_API_ENDPOINT,
    LOCAL_DATA_API_KEYSPACE,
    LOCAL_DATA_API_TOKEN_PROVIDER,
)

from astrapy import DataAPIClient, Database
from astrapy.admin import parse_api_endpoint
from astrapy.constants import Environment
from astrapy.info import EmbeddingProvider


def live_provider_info() -> Dict[str, EmbeddingProvider]:
    """
    Query the API endpoint `findEmbeddingProviders` endpoint
    for the latest information.

    This utility function uses the environment variables it can find
    to establish a target database to query.
    """

    database: Database
    if IS_ASTRA_DB:
        parsed = parse_api_endpoint(ASTRA_DB_API_ENDPOINT)
        if parsed is None:
            raise ValueError(
                "Cannot parse the Astra DB API Endpoint '{ASTRA_DB_API_ENDPOINT}'"
            )
        client = DataAPIClient(environment=parsed.environment)
        database = client.get_database(
            ASTRA_DB_API_ENDPOINT,
            token=ASTRA_DB_TOKEN_PROVIDER,
            namespace=ASTRA_DB_KEYSPACE,
        )
    else:
        client = DataAPIClient(environment=Environment.OTHER)
        database = client.get_database(
            LOCAL_DATA_API_ENDPOINT,
            token=LOCAL_DATA_API_TOKEN_PROVIDER,
            namespace=LOCAL_DATA_API_KEYSPACE,
        )

    database_admin = database.get_database_admin()
    response = database_admin.find_embedding_providers()
    return response.embedding_providers
