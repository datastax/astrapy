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

import logging
import httpx

from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


REQUESTED_WITH = "AstraPy"
DEFAULT_AUTH_PATH = "/api/rest/v1/auth"
DEFAULT_TIMEOUT = 30000
DEFAULT_AUTH_HEADER = "X-Cassandra-Token"


class http_methods:
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"


class AstraClient:
    def __init__(
        self,
        astra_database_id: str,
        astra_database_region: str,
        astra_application_token: str,
        auth_token: Optional[str] = None,
        base_url: Optional[str] = None,
        auth_base_url: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        auth_header: str = DEFAULT_AUTH_HEADER,
        debug: bool = False,
    ) -> None:
        self.astra_database_id = astra_database_id
        self.astra_database_region = astra_database_region
        self.astra_application_token = astra_application_token
        self.auth_token = auth_token
        self.base_url = base_url
        self.auth_base_url = auth_base_url
        self.username = username
        self.password = password
        self.auth_header = auth_header
        self.debug = debug

    def request(
        self,
        method: str = http_methods.GET,
        path: str = "",
        json_data: Any = {},
        url_params: Optional[Dict[str, Any]] = {},
    ) -> Any:
        if self.auth_token:
            auth_token = self.auth_token
        else:
            auth_token = self.astra_application_token

        r = httpx.request(
            method=method,
            url=f"{self.base_url}{path}",
            params=url_params,
            json=json_data,
            timeout=DEFAULT_TIMEOUT,
            headers={self.auth_header: auth_token},
        )
        try:
            return r.json()
        except Exception as e:
            logger.error(f"Error parsing response: {e}")

            return None


def get_token(
    auth_base_url: Optional[str], username: Optional[str], password: Optional[str]
) -> Any:
    try:
        r = httpx.request(
            method=http_methods.POST,
            url=f"{auth_base_url}",
            json={"username": username, "password": password},
            timeout=DEFAULT_TIMEOUT,
        )

        token_response = r.json()

        return token_response["authToken"]
    except Exception as e:
        logger.error(f"Error getting token: {e}")

        return None


def create_client(
    astra_database_id: str,
    astra_database_region: str,
    astra_application_token: str,
    base_url: Optional[str] = None,
    auth_base_url: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    debug: bool = False,
) -> AstraClient:
    if base_url is None:
        base_url = f"https://{astra_database_id}-{astra_database_region}.apps.astra.datastax.com"

    auth_token = None
    if auth_base_url:
        auth_token = get_token(
            auth_base_url=auth_base_url, username=username, password=password
        )
        if auth_token is None:
            raise Exception("A valid token is required")

    return AstraClient(
        astra_database_id=astra_database_id,
        astra_database_region=astra_database_region,
        astra_application_token=astra_application_token,
        base_url=base_url,
        auth_base_url=auth_base_url,
        username=username,
        password=password,
        auth_token=auth_token,
        auth_header=DEFAULT_AUTH_HEADER,  # Add the missing "auth_header" argument
        debug=debug,
    )
