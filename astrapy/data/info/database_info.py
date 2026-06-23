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
from astrapy.settings.defaults import (
    DEFAULT_CREATE_DB_CAPACITY_UNITS,
    DEFAULT_CREATE_DB_DB_TYPE,
    DEFAULT_CREATE_DB_TIER,
)
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
    pcu_types: list[PCUGroupTypeDescriptor] | None = None

    def __repr__(self) -> str:
        body = f'{self.cloud_provider}/{self.name}: "{self.display_name}", ...'
        return f"{self.__class__.__name__}({body})"

    def as_dict(self) -> dict[str, Any]:
        """
        Recast this object into a dictionary.
        """

        return {
            k: v
            for k, v in {
                "classification": self.classification,
                "cloudProvider": self.cloud_provider,
                "displayName": self.display_name,
                "enabled": self.enabled,
                "name": self.name,
                "region_type": "vector",
                "reservedForQualifiedUsers": self.reserved_for_qualified_users,
                "zone": self.zone,
                "pcu_types": [pcu_type.as_dict() for pcu_type in self.pcu_types]
                if self.pcu_types is not None
                else None,
            }.items()
            if v is not None
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
                "pcu_types",
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
            pcu_types=[
                PCUGroupTypeDescriptor._from_dict(pcu_type_dict)
                for pcu_type_dict in raw_dict["pcu_types"]
            ]
            if "pcu_types" in raw_dict
            else None,
        )


@dataclass
class DatabaseDefinition:
    """
    Represents a database definition for database creation operations (excluding the DB name).

    Attributes:
        cloud_provider: the cloud provider hosting the database (e.g. 'aws', 'gcp', 'azure').
        region: the region where the database will be created.
        tier: the database tier (e.g. 'serverless'). Optional, defaults to None.
        capacity_units: the number of capacity units for the database. Optional, defaults to None.
        db_type: the type of database (e.g. 'vector'). Optional, defaults to None.
        keyspace: the default keyspace for the database. Optional, defaults to None.
        pcu_group_id: the PCU group ID to use for provisioning the database. Optional, defaults to None.
    """

    cloud_provider: str
    region: str
    tier: str | None = None
    capacity_units: int | None = None
    db_type: str | None = None
    keyspace: str | None = None
    pcu_group_id: str | None = None

    def __repr__(self) -> str:
        pieces = [
            f"cloud_provider={self.cloud_provider}",
            f"region={self.region}",
        ]
        if self.tier is not None:
            pieces.append(f"tier={self.tier}")
        if self.capacity_units is not None:
            pieces.append(f"capacity_units={self.capacity_units}")
        if self.db_type is not None:
            pieces.append(f"db_type={self.db_type}")
        if self.keyspace is not None:
            pieces.append(f"keyspace={self.keyspace}")
        if self.pcu_group_id is not None:
            pieces.append(f"pcu_group_id={self.pcu_group_id}")
        return f"{self.__class__.__name__}({', '.join(pieces)})"

    def as_dict(self, *, name: str | None) -> dict[str, Any]:
        """
        Recast this object into a dictionary.

        Args:
            name: if provided, this is the name of the database and will
                be used to enrich the result making it a complete payload
                suitable for a DevOps API create-database invocation.

        Returns:
            a dictionary expressing the object (plus optionally a DB name).
        """

        return {
            k: v
            for k, v in {
                "name": name,
                "cloudProvider": self.cloud_provider,
                "region": self.region,
                "tier": self.tier,
                "capacityUnits": self.capacity_units,
                "dbType": self.db_type,
                "keyspace": self.keyspace,
                "pcuGroupUUID": self.pcu_group_id,
            }.items()
            if v is not None
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> DatabaseDefinition:
        """
        Create an instance of DatabaseDefinition from a dictionary
        such as one from the Data API.

        This operation, which should never be needed in ordinary client activity,
        exceptionally ignores any 'name' field it would find.
        """

        _warn_residual_keys(
            cls,
            raw_dict,
            {
                "name",
                "cloudProvider",
                "region",
                "tier",
                "capacityUnits",
                "dbType",
                "keyspace",
                "pcuGroupUUID",
            },
        )
        return DatabaseDefinition(
            cloud_provider=raw_dict["cloudProvider"],
            region=raw_dict["region"],
            tier=raw_dict.get("tier"),
            capacity_units=raw_dict.get("capacityUnits"),
            db_type=raw_dict.get("dbType"),
            keyspace=raw_dict.get("keyspace"),
            pcu_group_id=raw_dict.get("pcuGroupUUID"),
        )

    def with_defaults(self) -> DatabaseDefinition:
        """
        Return a new DatabaseDefinition with the default values for all
        fields that are not set, such that the results makes for a valid
        payload for a create-database DevOps API invocation,

        This method assumes that non-optional fields are not None.
        """
        return DatabaseDefinition(
            cloud_provider=self.cloud_provider,
            region=self.region,
            tier=self.tier if self.tier is not None else DEFAULT_CREATE_DB_TIER,
            capacity_units=self.capacity_units
            if self.capacity_units is not None
            else DEFAULT_CREATE_DB_CAPACITY_UNITS,
            db_type=self.db_type
            if self.db_type is not None
            else DEFAULT_CREATE_DB_DB_TYPE,
            keyspace=self.keyspace,
            pcu_group_id=self.pcu_group_id,
        )


@dataclass
class PCUGroupTypeDetailsDescriptor:
    """
    Represents the details of a PCU (Provisioned Capacity Unit) group type,
    describing the hardware specifications for a particular PCU configuration.

    Attributes:
        v_cpu: the number of virtual CPUs for this PCU type.
        memory: the amount of memory for this PCU type.
        disk_cache: the amount of disk cache for this PCU type.
    """

    v_cpu: int
    memory: str
    disk_cache: str

    def __repr__(self) -> str:
        body = f"v_cpu={self.v_cpu}, memory={self.memory}, disk_cache={self.disk_cache}"
        return f"{self.__class__.__name__}({body})"

    def as_dict(self) -> dict[str, Any]:
        """
        Recast this object into a dictionary.
        """

        return {
            "vCPU": self.v_cpu,
            "memory": self.memory,
            "disk_cache": self.disk_cache,
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> PCUGroupTypeDetailsDescriptor:
        """
        Create an instance of PCUGroupTypeDetailsDescriptor from a dictionary
        such as one from the DevOps API.
        """

        _warn_residual_keys(
            cls,
            raw_dict,
            {
                "vCPU",
                "memory",
                "disk_cache",
            },
        )
        return PCUGroupTypeDetailsDescriptor(
            v_cpu=raw_dict["vCPU"],
            memory=raw_dict["memory"],
            disk_cache=raw_dict["disk_cache"],
        )


@dataclass
class PCUGroupTypeDescriptor:
    """
    Represents a PCU (Provisioned Capacity Unit) group type descriptor,
    describing a specific PCU configuration available in a region.

    Attributes:
        type: the type of PCU group (e.g. 'standard').
        region: the region where this PCU type is available.
        cloud_provider: the cloud provider for this PCU type (e.g. 'AWS').
        details: hardware specifications for this PCU type.
    """

    type: str
    region: str | None
    cloud_provider: str | None
    details: PCUGroupTypeDetailsDescriptor

    def __repr__(self) -> str:
        pieces = [
            pc
            for pc in (
                f"type={self.type}",
                f"region={self.region}" if self.region is not None else None,
                f"cloud_provider={self.cloud_provider}"
                if self.cloud_provider is not None
                else None,
                "details=...",
            )
            if pc is not None
        ]
        body = ", ".join(pieces)
        return f"{self.__class__.__name__}({body})"

    def as_dict(self) -> dict[str, Any]:
        """
        Recast this object into a dictionary.
        """

        return {
            k: v
            for k, v in {
                "type": self.type,
                "region": self.region,
                "provider": self.cloud_provider,
                "details": self.details.as_dict(),
            }.items()
            if v is not None
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> PCUGroupTypeDescriptor:
        """
        Create an instance of PCUGroupTypeDescriptor from a dictionary
        such as one from the DevOps API.
        """

        _warn_residual_keys(
            cls,
            raw_dict,
            {
                "type",
                "region",
                "provider",
                "details",
            },
        )
        return PCUGroupTypeDescriptor(
            type=raw_dict["type"],
            region=raw_dict.get("region"),
            cloud_provider=raw_dict["provider"].lower()
            if "provider" in raw_dict
            else None,
            details=PCUGroupTypeDetailsDescriptor._from_dict(raw_dict["details"]),
        )


@dataclass
class PCUGroupDescriptor:
    """
    Represents the descriptor for a PCU (Provisioned Capacity Unit) group,
    such as the ones returned when querying the DevOps API for PCU groups.

    Attributes:
        id: the unique identifier for the PCU group (a UUID as a string).
        org_id: the organization ID this PCU group belongs to.
        title: the title (name) of the PCU group.
        cloud_provider: the cloud provider for this PCU group (e.g. 'AWS').
        region: the region this PCU group is ascribed to.
        instance_type: the instance type for this PCU group.
        pcu_type: the PCU type descriptor.
        provision_type: the provisioning type (e.g. 'shared').
        min: the minimum shared hourly PCUs in the group.
        max: the maximum shared hourly PCUs in the group.
        reserved: the absolute required PCUs in the group.
        description: a description of the PCU group.
        created_at: creation time of the PCU group.
        updated_at: update time of the PCU group.
        created_by: identifier of the user who created the PCU group.
        updated_by: identifier of the user who updated the PCU group.
        status: the current status of the PCU group (e.g. 'INITIALIZING').
    """

    id: str
    org_id: str
    title: str
    cloud_provider: str
    region: str
    instance_type: str
    pcu_type: PCUGroupTypeDescriptor
    provision_type: str
    min: int
    max: int
    reserved: int
    description: str
    created_at: datetime.datetime | None
    updated_at: datetime.datetime | None
    created_by: str
    updated_by: str
    status: str

    def __repr__(self) -> str:
        body = f"id={self.id}, org_id={self.org_id}, title={self.title}, status={self.status}, ..."
        return f"{self.__class__.__name__}({body})"

    def as_dict(self) -> dict[str, Any]:
        """
        Recast this object into a dictionary.
        """

        return {
            k: v
            for k, v in {
                "uuid": self.id,
                "orgId": self.org_id,
                "title": self.title,
                "cloudProvider": self.cloud_provider,
                "region": self.region,
                "instanceType": self.instance_type,
                "pcuType": self.pcu_type.as_dict(),
                "provisionType": self.provision_type,
                "min": self.min,
                "max": self.max,
                "reserved": self.reserved,
                "description": self.description,
                "createdAt": None
                if self.created_at is None
                else DataAPITimestamp.from_datetime(self.created_at).to_string(),
                "updatedAt": None
                if self.updated_at is None
                else DataAPITimestamp.from_datetime(self.updated_at).to_string(),
                "createdBy": self.created_by,
                "updatedBy": self.updated_by,
                "status": self.status,
            }.items()
            if v is not None
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> PCUGroupDescriptor:
        """
        Create an instance of PCUGroupDescriptor from a dictionary
        such as one from the DevOps API.
        """

        _warn_residual_keys(
            cls,
            raw_dict,
            {
                "uuid",
                "orgId",
                "title",
                "cloudProvider",
                "region",
                "instanceType",
                "pcuType",
                "provisionType",
                "min",
                "max",
                "reserved",
                "description",
                "createdAt",
                "updatedAt",
                "createdBy",
                "updatedBy",
                "status",
            },
        )
        return PCUGroupDescriptor(
            id=raw_dict["uuid"],
            org_id=raw_dict["orgId"],
            title=raw_dict["title"],
            cloud_provider=raw_dict["cloudProvider"].lower(),
            region=raw_dict["region"],
            instance_type=raw_dict["instanceType"],
            pcu_type=PCUGroupTypeDescriptor._from_dict(raw_dict["pcuType"]),
            provision_type=raw_dict["provisionType"],
            min=raw_dict["min"],
            max=raw_dict["max"],
            reserved=raw_dict["reserved"],
            description=raw_dict["description"],
            created_at=_failsafe_parse_date(raw_dict.get("createdAt")),
            updated_at=_failsafe_parse_date(raw_dict.get("updatedAt")),
            created_by=raw_dict["createdBy"],
            updated_by=raw_dict["updatedBy"],
            status=raw_dict["status"],
        )
