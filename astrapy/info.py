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
from dataclasses import dataclass
from typing import Any, Dict, Optional

import httpx

from astrapy.core.ops import AstraDBOps
from astrapy.exceptions import (
    DevOpsAPIException,
    base_timeout_info,
    to_dataapi_timeout_exception,
)


database_id_finder = re.compile(
    "https://([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
)


def find_database_id(api_endpoint: str) -> Optional[str]:
    """
    Parse an API Endpoint into a database ID.

    Args:
        api_endpoint: a full API endpoint for the Data Api.

    Returns:
        the database ID. If parsing fails, return None.
    """

    match = database_id_finder.match(api_endpoint)
    if match and match.groups():
        return match.groups()[0]
    else:
        return None


def get_database_info(
    api_endpoint: str, token: str, namespace: str, max_time_ms: Optional[int] = None
) -> DatabaseInfo:
    """
    Fetch the relevant information through the DevOps API.

    Args:
        api_endpoint: a full API endpoint for the Data Api.
        token: a valid token to access the database.
        namespace: the desired namespace that will be used in the result.
        max_time_ms: a timeout, in milliseconds, for waiting on a response.

    Returns:
        A DatabaseInfo object.

    Note:
        If the API endpoint does not allow to extract a database_id,
        namespace validation is skipped and the returned info will have
        most fields set to None.
    """

    astra_db_ops = AstraDBOps(token=token)
    database_id = find_database_id(api_endpoint)
    if database_id:
        try:
            gd_response = astra_db_ops.get_database(
                database=database_id, timeout_info=base_timeout_info(max_time_ms)
            )
        except httpx.TimeoutException as texc:
            raise to_dataapi_timeout_exception(texc)
        raw_info = gd_response["info"]
        if namespace not in raw_info["keyspaces"]:
            raise DevOpsAPIException(f"Namespace {namespace} not found on DB.")
        else:
            return DatabaseInfo(
                id=database_id,
                region=raw_info["region"],
                namespace=namespace,
                name=raw_info["name"],
                raw_info=raw_info,
            )
    else:
        return DatabaseInfo(
            id=None,
            region=None,
            namespace=namespace,
            name=None,
            raw_info=None,
        )


@dataclass
class DatabaseInfo:
    """
    Represents the identifying information for a database,
    including the region the connection is established to.

    Attributes:
        id: the database ID.
        region: the ID of the region through which the connection to DB is done.
        namespace: the namespace this DB is set to work with.
        name: the database name. Not necessarily unique: there can be multiple
            databases with the same name.
        raw_info: the full response from the DevOPS API call to get this info.

    Note:
        Most members of this object can be None. This happens when errors occur
        during DevOps API calls and usually signals that the Data API server
        is a deploy where concepts such as "region" or "database ID" do not apply.
    """

    id: Optional[str]
    region: Optional[str]
    namespace: str
    name: Optional[str]
    raw_info: Optional[Dict[str, Any]]


@dataclass
class CollectionInfo:
    """
    Represents the identifying information for a collection,
    including the information about the database the collection belongs to.

    Attributes:
        database_info: a DatabaseInfo instance for the underlying database.
        namespace: the namespace where the collection is located.
        name: collection name. Unique within a namespace.
        full_name: identifier for the collection within the database,
            in the form "namespace.collection_name".
    """

    database_info: DatabaseInfo
    namespace: str
    name: str
    full_name: str
