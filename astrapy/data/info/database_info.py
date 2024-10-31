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

import datetime
from dataclasses import dataclass
from typing import Any

from astrapy.admin.endpoints import build_api_endpoint, parse_api_endpoint
from astrapy.data_types import DataAPITimestamp


def _failsafe_parse_date(date_string: str | None) -> datetime.datetime | None:
    try:
        return DataAPITimestamp.from_string(date_string or "").to_datetime()
    except ValueError:
        return None


@dataclass
class AstraDBAdminDatabaseRegionInfo:
    """
    TODO docstring+attrs
    """

    region_name: str
    id: str
    api_endpoint: str
    created_at: datetime.datetime | None

    def __init__(
        self,
        *,
        raw_datacenter_dict: dict[str, Any],
        environment: str,
        database_id: str,
    ) -> None:
        self.region_name = raw_datacenter_dict["region"]
        self.id = raw_datacenter_dict["id"]
        self.api_endpoint = build_api_endpoint(
            environment=environment,
            database_id=database_id,
            region=raw_datacenter_dict["region"],
        )
        self.created_at = _failsafe_parse_date(raw_datacenter_dict.get("dateCreated"))

    def __repr__(self) -> str:
        pieces = [
            f"region_name={self.region_name}",
            f"id={self.id}",
            f"api_endpoint={self.api_endpoint}",
            f"created_at={self.created_at}",
        ]
        return f"{self.__class__.__name__}({', '.join(pieces)})"


@dataclass
class _BaseAstraDBDatabaseInfo:
    """
    TODO docstring+attrs
    """

    id: str
    name: str
    keyspaces: list[str]
    status: str
    environment: str
    cloud_provider: str
    raw: dict[str, Any] | None

    def __init__(
        self,
        *,
        environment: str,
        raw_dict: dict[str, Any],
    ) -> None:
        self.id = raw_dict["id"]
        self.name = raw_dict["info"]["name"]
        self.keyspaces = raw_dict["info"].get("keyspaces", [])
        self.status = raw_dict["status"]
        self.environment = environment
        self.cloud_provider = raw_dict["info"]["cloudProvider"]
        self.raw = raw_dict

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._inner_desc()})"

    def _inner_desc(self) -> str:
        pieces = [
            f"id={self.id}",
            f"name={self.name}",
            f"keyspaces={self.keyspaces}",
            f"status={self.status}",
            f"environment={self.environment}",
            f"cloud_provider={self.cloud_provider}",
        ]
        return ", ".join(pieces)


@dataclass
class AstraDBDatabaseInfo(_BaseAstraDBDatabaseInfo):
    """
    Represents the identifying information for a database,
    including the region the connection is established to.

    TODO revise docstring (also paste the attributes of baseclass)

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
        Conversely, in case of a AstraDBDatabaseInfo not obtained through a
        connected database, such as when calling `Admin.list_databases()`,
        all fields except `environment` (e.g. keyspace, region, etc)
        are set as found on the DevOps API response directly.
    """

    region: str
    api_endpoint: str

    def __init__(
        self,
        *,
        environment: str,
        api_endpoint: str,
        raw_dict: dict[str, Any],
    ) -> None:
        self.api_endpoint = api_endpoint
        parsed_api_endpoint = parse_api_endpoint(self.api_endpoint)
        self.region = "" if parsed_api_endpoint is None else parsed_api_endpoint.region
        _BaseAstraDBDatabaseInfo.__init__(
            self=self,
            environment=environment,
            raw_dict=raw_dict,
        )

    def __repr__(self) -> str:
        pieces = [
            _BaseAstraDBDatabaseInfo._inner_desc(self),
            f"region={self.region}",
            f"api_endpoint={self.api_endpoint}",
            "raw=...",
        ]
        return f"{self.__class__.__name__}({', '.join(pieces)})"


@dataclass
class AstraDBAdminDatabaseInfo(_BaseAstraDBDatabaseInfo):
    """
    Represents the full response from the DevOps API about a database info.

    TODO revise docstring (also paste the attributes of baseclass)

    Additional database details information is kept in the
    "raw DevOps API response" part, the `raw` dictionary attribute.
    For more information, please consult the DevOps API documentation.

    Attributes:
        info: an AstraDBDatabaseInfo instance for the underlying database.
            The AstraDBDatabaseInfo is a subset of the information described by
            AstraDBAdminDatabaseInfo - in terms of the DevOps API response,
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

    created_at: datetime.datetime | None
    last_used: datetime.datetime | None
    org_id: str
    owner_id: str
    regions: list[AstraDBAdminDatabaseRegionInfo]

    def __init__(
        self,
        *,
        environment: str,
        raw_dict: dict[str, Any],
    ) -> None:
        self.created_at = _failsafe_parse_date(raw_dict.get("creationTime"))
        self.last_used = _failsafe_parse_date(raw_dict.get("lastUsageTime"))
        self.org_id = raw_dict["orgId"]
        self.owner_id = raw_dict["ownerId"]
        _BaseAstraDBDatabaseInfo.__init__(
            self=self,
            environment=environment,
            raw_dict=raw_dict,
        )
        self.regions = [
            AstraDBAdminDatabaseRegionInfo(
                raw_datacenter_dict=raw_datacenter_dict,
                environment=environment,
                database_id=self.id,
            )
            for raw_datacenter_dict in raw_dict["info"]["datacenters"]
        ]

    def __repr__(self) -> str:
        pieces = [
            _BaseAstraDBDatabaseInfo._inner_desc(self),
            f"created_at={self.created_at}",
            f"last_used={self.last_used}",
            f"org_id={self.org_id}",
            f"owner_id={self.owner_id}",
            f"regions={self.regions}",
            "raw=...",
        ]
        return f"{self.__class__.__name__}({', '.join(pieces)})"
