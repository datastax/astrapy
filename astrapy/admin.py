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

import re
from typing import Optional
from dataclasses import dataclass

import httpx

from astrapy.core.ops import AstraDBOps
from astrapy.info import DatabaseInfo
from astrapy.exceptions import (
    DevOpsAPIException,
    base_timeout_info,
    to_dataapi_timeout_exception,
)


database_id_finder = re.compile(
    "https://"
    "([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
    "-"
    "([a-z0-9\-]+)"
    ".apps.astra[\-]{0,1}"
    "(dev|test)?"
    ".datastax.com"
)


DEV_OPS_URL_MAP = {
    "dev": "https://api.dev.cloud.datastax.com",
    "test": "https://api.test.cloud.datastax.com",
}


@dataclass
class ParsedAPIEndpoint:
    """
    The results of successfully parsing an Astra DB API endpoint, for internal
    by database metadata-related functions.

    Attributes:
        database_id: e. g. "01234567-89ab-cdef-0123-456789abcdef".
        region: a region ID, such as "us-west1".
        environment: a label such as "prod", "dev", "test".
    """

    database_id: str
    region: str
    environment: str  # 'prod', 'dev', 'test'


def parse_api_endpoint(api_endpoint: str) -> Optional[ParsedAPIEndpoint]:
    """
    Parse an API Endpoint into a ParsedAPIEndpoint structure.

    Args:
        api_endpoint: a full API endpoint for the Data Api.

    Returns:
        The parsed ParsedAPIEndpoint. If parsing fails, return None.
    """

    match = database_id_finder.match(api_endpoint)
    if match and match.groups():
        d_id, d_re, d_en_x = match.groups()
        return ParsedAPIEndpoint(
            database_id=d_id,
            region=d_re,
            environment=d_en_x if d_en_x else "prod",
        )
    else:
        return None


def get_database_info(
    api_endpoint: str, token: str, namespace: str, max_time_ms: Optional[int] = None
) -> Optional[DatabaseInfo]:
    """
    Fetch the relevant information through the DevOps API.

    Args:
        api_endpoint: a full API endpoint for the Data Api.
        token: a valid token to access the database.
        namespace: the desired namespace that will be used in the result.
        max_time_ms: a timeout, in milliseconds, for waiting on a response.

    Returns:
        A DatabaseInfo object.
        If the API endpoint fails to be parsed, None is returned.
    """

    parsed_endpoint = parse_api_endpoint(api_endpoint)
    if parsed_endpoint:
        astra_db_ops = AstraDBOps(
            token=token,
            dev_ops_url=DEV_OPS_URL_MAP.get(parsed_endpoint.environment),
        )
        try:
            gd_response = astra_db_ops.get_database(
                database=parsed_endpoint.database_id,
                timeout_info=base_timeout_info(max_time_ms),
            )
        except httpx.TimeoutException as texc:
            raise to_dataapi_timeout_exception(texc)
        raw_info = gd_response["info"]
        if namespace not in raw_info["keyspaces"]:
            raise DevOpsAPIException(f"Namespace {namespace} not found on DB.")
        else:
            return DatabaseInfo(
                id=parsed_endpoint.database_id,
                region=parsed_endpoint.region,
                namespace=namespace,
                name=raw_info["name"],
                environment=parsed_endpoint.environment,
                raw_info=raw_info,
            )
    else:
        return None
