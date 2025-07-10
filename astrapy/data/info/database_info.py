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
from astrapy.utils.meta import deprecated_property
from astrapy.utils.parsing import _warn_residual_keys


def _failsafe_parse_date(date_string: str | None) -> datetime.datetime | None:
    try:
        return DataAPITimestamp.from_string(date_string or "").to_datetime(
            tz=datetime.timezone.utc
        )
    except ValueError:
        return None


@dataclass
class AstraDBAdminDatabaseRegionInfo:
    """
    Represents a region where a database is located and reachable.
    A database can be single-region or multi-region: correspondingly, the
    `regions` list attribute of the database's `AstraDBAdminDatabaseInfo` object
    can have one or several entries, each a `AstraDBAdminDatabaseRegionInfo` instance.

    Attributes:
        name: the short, ID-like name of the region. This can be used as a
            unique identifier *for a region*.  In the raw response
            from the DevOps API endpoint, this attribute is called `region`.
        id: This is the datacenter ID, usually composed by the database ID followed
            by a dash and a further integer identifier. It is unique across
            all datacenters of all Astra databases (belonging to any org).
        api_endpoint: the API endpoint one can use to connect to the database through
            a particular region.
        created_at: information on when the region was added to the database.
    """

    name: str
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
        self.name = raw_datacenter_dict["region"]
        self.id = raw_datacenter_dict["id"]
        self.api_endpoint = build_api_endpoint(
            environment=environment,
            database_id=database_id,
            region=raw_datacenter_dict["region"],
        )
        self.created_at = _failsafe_parse_date(raw_datacenter_dict.get("dateCreated"))

    def __repr__(self) -> str:
        pieces = [
            f"name={self.name}",
            f"id={self.id}",
            f"api_endpoint={self.api_endpoint}",
            f"created_at={self.created_at}",
        ]
        return f"{self.__class__.__name__}({', '.join(pieces)})"

    @property
    @deprecated_property(
        new_name="name",
        deprecated_in="2.0.1",
        removed_in="2.3.0",
    )
    def region_name(self) -> str:
        return self.name


@dataclass
class _BaseAstraDBDatabaseInfo:
    """
    An object describing a set of properties of a database, a superclass
    shared by the `AstraDBDatabaseInfo` and `AstraDBAdminDatabaseInfo` classes.

    Attributes:
        id: the Database ID, in the form of a UUID string with dashes. Example:
            "01234567-89ab-cdef-0123-456789abcdef".
        name: the name of the database as set by the user at creation time.
            The database name is not necessarily unique across databases in an org.
        keyspaces: A list of the keyspaces available in the database.
        status: A string describing the current status of the database. Example values
            are: "ACTIVE", "MAINTENANCE", "INITIALIZING", and others (see
            the DevOps API documentation for more on database statuses).
        environment: a string identifying the environment for the database. In the
            typical usage, this equals "prod".
        cloud_provider: a string describing the cloud provider hosting the database.
        raw: a dictionary containing the full response from the DevOps API call
            to obtain the database information.
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
    A class representing the information of an Astra DB database, including
    region details. This is the type of the response from the Database `info`
    method.

    Note:
        a database can in general be replicated across multiple regions, in an
        active/active manner. Yet, when connecting to it, one always explicitly
        specifies a certain region: in other words, the connection (as represented
        by the `Database` class and analogous) is always done to a specific region.
        In this sense, this class represents the notion of "a database reached from
        a certain region". See class `AstraDBAdminDatabaseInfo` for (possibly)
        multi-region database information.

    Attributes:
        id: the Database ID, in the form of a UUID string with dashes. Example:
            "01234567-89ab-cdef-0123-456789abcdef".
        name: the name of the database as set by the user at creation time.
            The database name is not necessarily unique across databases in an org.
        keyspaces: A list of the keyspaces available in the database.
        status: A string describing the current status of the database. Example values
            are: "ACTIVE", "MAINTENANCE", "INITIALIZING", and others (see
            the DevOps API documentation for more on database statuses).
        environment: a string identifying the environment for the database. In the
            typical usage, this equals "prod".
        cloud_provider: a string describing the cloud provider hosting the database.
        raw: a dictionary containing the full response from the DevOps API call
            to obtain the database information.
        region: the region this database is accessed through.
        api_endpoint: the API Endpoint used to connect to this database in this region.

    Note:
        The `raw_info` dictionary usually has a `region` key describing
        the default region as configured in the database, which does not
        necessarily (for multi-region databases) match the region through
        which the connection is established: the latter is the one specified
        by the "api endpoint" used for connecting. In other words, for multi-region
        databases it is possible that
        `database_info.region != database_info.raw_info["region"]`.
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
    A class representing the information of an Astra DB database, including
    region details. This is the type of the response from the AstraDBDatabaseAdmin
    `info` method.

    Note:
        This class, if applicable, describes a multi-region database in all its
        regions, as opposed to the `AstraDBDatabaseInfo`.

    Attributes:
        id: the Database ID, in the form of a UUID string with dashes. Example:
            "01234567-89ab-cdef-0123-456789abcdef".
        name: the name of the database as set by the user at creation time.
            The database name is not necessarily unique across databases in an org.
        keyspaces: A list of the keyspaces available in the database.
        status: A string describing the current status of the database. Example values
            are: "ACTIVE", "MAINTENANCE", "INITIALIZING", and others (see
            the DevOps API documentation for more on database statuses).
        environment: a string identifying the environment for the database. In the
            typical usage, this equals "prod".
        cloud_provider: a string describing the cloud provider hosting the database.
        raw: a dictionary containing the full response from the DevOps API call
            to obtain the database information.
        created_at: information about when the database has been created.
        last_used: information about when the database was accessed last.
        org_id: the ID of the Astra organization the database belongs to,
            in the form of a UUID string with dashes.
        owner_id: the ID of the Astra account owning the database, in the form
            of a UUID string with dashes.
        regions: a list of `AstraDBAdminDatabaseRegionInfo` objects, one for each of
            the regions the database is replicated to.

    Note:
        The `raw_info` dictionary usually has a `region` key describing
        the default region as configured in the database, which does not
        necessarily (for multi-region databases) match the region through
        which the connection is established: the latter is the one specified
        by the "api endpoint" used for connecting. In other words, for multi-region
        databases it is possible that
        `database_info.region != database_info.raw_info["region"]`.
        Conversely, in case of a AstraDBDatabaseInfo not obtained through a
        connected database, such as when calling `Admin.list_databases()`,
        all fields except `environment` (e.g. keyspace, region, etc)
        are set as found on the DevOps API response directly.
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


@dataclass
class AstraDBAvailableRegionInfo:
    """
    Represents a region information as returned by the `find_available_regions`
    method: in other words, it is a descriptor of a certain region available
    for database creation.

    Attributes:
        classification: level of access to the region, one of 'standard', 'premium'
            or 'premium_plus'.
        cloud_provider: one of 'gcp', 'aws' or 'azure'.
        display_name: a region "pretty name" e.g. for printing messages.
        enabled: a boolean flag marking whether the region is enabled.
        name: the short, ID-like name of the region. This can be used as an
            identifier since it determines a region uniquely.
        reserved_for_qualified_users: a boolean flag marking availability settings.
        zone: macro-zone for the region, e.g. "na" or "emea".
    """

    classification: str
    cloud_provider: str
    display_name: str
    enabled: bool
    name: str
    reserved_for_qualified_users: bool
    zone: str

    def __repr__(self) -> str:
        body = f'{self.cloud_provider}/{self.name}: "{self.display_name}", ...'
        return f"{self.__class__.__name__}({body})"

    def as_dict(self) -> dict[str, Any]:
        """
        Recast this object into a dictionary.
        """

        return {
            "classification": self.classification,
            "cloudProvider": self.cloud_provider,
            "displayName": self.display_name,
            "enabled": self.enabled,
            "name": self.name,
            "region_type": "vector",
            "reservedForQualifiedUsers": self.reserved_for_qualified_users,
            "zone": self.zone,
        }

    @property
    @deprecated_property(
        new_name="name",
        deprecated_in="2.0.1",
        removed_in="2.3.0",
    )
    def region_name(self) -> str:
        return self.name

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> AstraDBAvailableRegionInfo:
        """
        Create an instance of AstraDBAvailableRegionInfo from a dictionary
        such as one from the Data API.
        """

        _warn_residual_keys(
            cls,
            raw_dict,
            {
                "classification",
                "cloudProvider",
                "displayName",
                "enabled",
                "name",
                "region_type",
                "reservedForQualifiedUsers",
                "zone",
            },
        )
        return AstraDBAvailableRegionInfo(
            classification=raw_dict["classification"],
            cloud_provider=raw_dict["cloudProvider"],
            display_name=raw_dict["displayName"],
            enabled=raw_dict["enabled"],
            name=raw_dict["name"],
            reserved_for_qualified_users=raw_dict["reservedForQualifiedUsers"],
            zone=raw_dict["zone"],
        )
