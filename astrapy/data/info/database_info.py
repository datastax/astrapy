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

from dataclasses import dataclass
from typing import Any


@dataclass
class DatabaseInfo:
    """
    Represents the identifying information for a database,
    including the region the connection is established to.

    Attributes:
        id: the database ID.
        region: the ID of the region through which the connection to DB is done.
        keyspace: the keyspace this DB is set to work with. None if not set.
        name: the database name. Not necessarily unique: there can be multiple
            databases with the same name.
        environment: a label, whose value can be `Environment.PROD`,
            or another value in `Environment.*`.
        raw_info: the full response from the DevOPS API call to get this info.

    Note:
        The `raw_info` dictionary usually has a `region` key describing
        the default region as configured in the database, which does not
        necessarily (for multi-region databases) match the region through
        which the connection is established: the latter is the one specified
        by the "api endpoint" used for connecting. In other words, for multi-region
        databases it is possible that
            database_info.region != database_info.raw_info["region"]
        Conversely, in case of a DatabaseInfo not obtained through a
        connected database, such as when calling `Admin.list_databases()`,
        all fields except `environment` (e.g. keyspace, region, etc)
        are set as found on the DevOps API response directly.
    """

    id: str
    region: str
    keyspace: str | None
    name: str
    environment: str
    raw_info: dict[str, Any] | None


@dataclass
class AdminDatabaseInfo:
    """
    Represents the full response from the DevOps API about a database info.

    Most attributes just contain the corresponding part of the raw response:
    for this reason, please consult the DevOps API documentation for details.

    Attributes:
        info: a DatabaseInfo instance for the underlying database.
            The DatabaseInfo is a subset of the information described by
            AdminDatabaseInfo - in terms of the DevOps API response,
            it corresponds to just its "info" subdictionary.
        available_actions: the "availableActions" value in the full API response.
        cost: the "cost" value in the full API response.
        cqlsh_url: the "cqlshUrl" value in the full API response.
        creation_time: the "creationTime" value in the full API response.
        data_endpoint_url: the "dataEndpointUrl" value in the full API response.
        grafana_url: the "grafanaUrl" value in the full API response.
        graphql_url: the "graphqlUrl" value in the full API response.
        id: the "id" value in the full API response.
        last_usage_time: the "lastUsageTime" value in the full API response.
        metrics: the "metrics" value in the full API response.
        observed_status: the "observedStatus" value in the full API response.
        org_id: the "orgId" value in the full API response.
        owner_id: the "ownerId" value in the full API response.
        status: the "status" value in the full API response.
        storage: the "storage" value in the full API response.
        termination_time: the "terminationTime" value in the full API response.
        raw_info: the full raw response from the DevOps API.
    """

    info: DatabaseInfo
    available_actions: list[str] | None
    cost: dict[str, Any]
    cqlsh_url: str
    creation_time: str
    data_endpoint_url: str
    grafana_url: str
    graphql_url: str
    id: str
    last_usage_time: str
    metrics: dict[str, Any]
    observed_status: str
    org_id: str
    owner_id: str
    status: str
    storage: dict[str, Any]
    termination_time: str
    raw_info: dict[str, Any] | None
