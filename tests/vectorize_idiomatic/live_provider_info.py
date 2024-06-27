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

from typing import Any, Dict, Optional

from preprocess_env import (
    ASTRA_DB_API_ENDPOINT,
    ASTRA_DB_TOKEN_PROVIDER,
    IS_ASTRA_DB,
    LOCAL_DATA_API_ENDPOINT,
    LOCAL_DATA_API_TOKEN_PROVIDER,
)

from astrapy.api_commander import APICommander


def live_provider_info() -> Dict[str, Any]:
    """
    Query the API endpoint `findEmbeddingProviders` endpoint
    for the latest information.

    This is where the preprocess_env variables are read to figure out whom to ask.
    """
    response: Dict[str, Any]

    if IS_ASTRA_DB:
        if ASTRA_DB_TOKEN_PROVIDER is None:
            raise ValueError("No token provider for Astra DB")
        path = "api/json/v1"
        headers_a: Dict[str, Optional[str]] = {
            "Token": ASTRA_DB_TOKEN_PROVIDER.get_token(),
        }
        cmd = APICommander(
            api_endpoint=ASTRA_DB_API_ENDPOINT or "",
            path=path,
            headers=headers_a,
        )
        response = cmd.request(payload={"findEmbeddingProviders": {}})
    else:
        path = "v1"
        if LOCAL_DATA_API_TOKEN_PROVIDER is None:
            raise ValueError("No token provider for Local Data API")
        headers_l: Dict[str, Optional[str]] = {
            "Token": LOCAL_DATA_API_TOKEN_PROVIDER.get_token(),
        }
        cmd = APICommander(
            api_endpoint=LOCAL_DATA_API_ENDPOINT or "",
            path=path,
            headers=headers_l,
        )
        response = cmd.request(payload={"findEmbeddingProviders": {}})

    return response
