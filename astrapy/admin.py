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
import time
from typing import Any, Dict, Optional
from dataclasses import dataclass

import httpx

from astrapy.core.ops import AstraDBOps
from astrapy.cursors import CommandCursor
from astrapy.info import AdminDatabaseInfo, DatabaseInfo
from astrapy.exceptions import (
    DevOpsAPIException,
    MultiCallTimeoutManager,
    base_timeout_info,
    to_dataapi_timeout_exception,
    ops_recast_method_sync,
)


DEFAULT_NEW_DATABASE_CLOUD_PROVIDER = "gcp"
DEFAULT_NEW_DATABASE_REGION = "us-east1"
DEFAULT_NEW_DATABASE_NAMESPACE = "default_keyspace"
WAITING_ON_DB_POLL_PERIOD_SECONDS = 2


class Environment:
    """
    Admitted values for `environment` property, such as the one denoting databases.
    """

    def __init__(self) -> None:
        raise NotImplementedError

    PROD = "prod"
    DEV = "dev"
    TEST = "test"


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
    Environment.PROD: "https://api.astra.datastax.com",
    Environment.DEV: "https://api.dev.cloud.datastax.com",
    Environment.TEST: "https://api.test.cloud.datastax.com",
}


@dataclass
class ParsedAPIEndpoint:
    """
    The results of successfully parsing an Astra DB API endpoint, for internal
    by database metadata-related functions.

    Attributes:
        database_id: e. g. "01234567-89ab-cdef-0123-456789abcdef".
        region: a region ID, such as "us-west1".
        environment: a label, whose value is one of Environment.PROD, Environment.DEV
            or Environment.TEST.
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
            dev_ops_url=DEV_OPS_URL_MAP[parsed_endpoint.environment],
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


def _recast_as_admin_database_info(
    admin_database_info_dict: Dict[str, Any],
    *,
    environment: str,
) -> AdminDatabaseInfo:
    return AdminDatabaseInfo(
        info=DatabaseInfo(
            id=admin_database_info_dict["id"],
            region=admin_database_info_dict["info"]["region"],
            namespace=admin_database_info_dict["info"]["keyspace"],
            name=admin_database_info_dict["info"]["name"],
            environment=environment,
            raw_info=admin_database_info_dict["info"],
        ),
        available_actions=admin_database_info_dict.get("availableActions"),
        cost=admin_database_info_dict["cost"],
        cqlsh_url=admin_database_info_dict["cqlshUrl"],
        creation_time=admin_database_info_dict["creationTime"],
        data_endpoint_url=admin_database_info_dict["dataEndpointUrl"],
        grafana_url=admin_database_info_dict["grafanaUrl"],
        graphql_url=admin_database_info_dict["graphqlUrl"],
        id=admin_database_info_dict["id"],
        last_usage_time=admin_database_info_dict["lastUsageTime"],
        metrics=admin_database_info_dict["metrics"],
        observed_status=admin_database_info_dict["observedStatus"],
        org_id=admin_database_info_dict["orgId"],
        owner_id=admin_database_info_dict["ownerId"],
        status=admin_database_info_dict["status"],
        storage=admin_database_info_dict["storage"],
        termination_time=admin_database_info_dict["terminationTime"],
        raw_info=admin_database_info_dict,
    )


class AstraDBAdmin:
    def __init__(
        self,
        token: str,
        *,
        environment: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
        dev_ops_url: Optional[str] = None,
        dev_ops_api_version: Optional[str] = None,
    ) -> None:
        self.token = token
        self.environment = environment or Environment.PROD
        if dev_ops_url is None:
            self.dev_ops_url = DEV_OPS_URL_MAP[self.environment]
        else:
            self.dev_ops_url = dev_ops_url
        self._astra_db_ops = AstraDBOps(
            token=self.token,
            dev_ops_url=dev_ops_url,
            dev_ops_api_version=dev_ops_api_version,
            caller_name=caller_name,
            caller_version=caller_version,
        )

    @ops_recast_method_sync
    def list_databases(
        self,
        *,
        max_time_ms: Optional[int] = None,
    ) -> CommandCursor[AdminDatabaseInfo]:
        gd_list_response = self._astra_db_ops.get_databases(
            timeout_info=base_timeout_info(max_time_ms)
        )
        if not isinstance(gd_list_response, list):
            raise DevOpsAPIException(
                "Faulty response from get-databases DevOps API command.",
            )
        else:
            # we know this is a list of dicts which need a little adjusting
            return CommandCursor(
                address=self._astra_db_ops.base_url,
                items=[
                    _recast_as_admin_database_info(
                        db_dict,
                        environment=self.environment,
                    )
                    for db_dict in gd_list_response
                ],
            )

    def get_database_info(
        self, database_id: str, *, max_time_ms: Optional[int] = None
    ) -> AdminDatabaseInfo:
        gd_response = self._astra_db_ops.get_database(
            database=database_id,
            timeout_info=base_timeout_info(max_time_ms),
        )
        if not isinstance(gd_response, dict):
            raise DevOpsAPIException(
                "Faulty response from get-database DevOps API command.",
            )
        else:
            return _recast_as_admin_database_info(
                gd_response,
                environment=self.environment,
            )

    @ops_recast_method_sync
    def create_database(
        self,
        name: str,
        *,
        wait_until_active: bool = True,
        namespace: str = DEFAULT_NEW_DATABASE_NAMESPACE,
        cloud_provider: str = DEFAULT_NEW_DATABASE_CLOUD_PROVIDER,
        region: str = DEFAULT_NEW_DATABASE_REGION,
        capacity_units: int = 1,
        max_time_ms: Optional[int] = None,
    ) -> AstraDBDatabaseAdmin:
        database_definition = {
            "name": name,
            "tier": "serverless",
            "cloudProvider": cloud_provider,
            "keyspace": namespace,
            "region": region,
            "capacityUnits": capacity_units,
            "user": "token",
            "password": self.token,
            "dbType": "vector",
        }
        timeout_manager = MultiCallTimeoutManager(
            overall_max_time_ms=max_time_ms, exception_type="devops_api"
        )
        cd_response = self._astra_db_ops.create_database(
            database_definition=database_definition,
            timeout_info=base_timeout_info(max_time_ms),
        )
        if cd_response is not None:
            new_database_id = cd_response["id"]
            if wait_until_active:
                last_status_seen = "PENDING"
                while last_status_seen == "PENDING":
                    time.sleep(WAITING_ON_DB_POLL_PERIOD_SECONDS)
                    last_status_seen = self.get_database_info(
                        database_id=new_database_id,
                        max_time_ms=timeout_manager.remaining_timeout_ms(),
                    ).status
                if last_status_seen not in {"ACTIVE", "INITIALIZING"}:
                    raise DevOpsAPIException(
                        f"Database {name} entered unexpected status {last_status_seen} after PENDING"
                    )
            # return the database instance
            return AstraDBDatabaseAdmin(id=new_database_id)  # TODO here
        else:
            raise DevOpsAPIException("Could not create the database.")

    @ops_recast_method_sync
    def drop_database(
        self,
        database_id: str,
        *,
        wait_until_active: bool = True,
        max_time_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        timeout_manager = MultiCallTimeoutManager(
            overall_max_time_ms=max_time_ms, exception_type="devops_api"
        )
        te_response = self._astra_db_ops.terminate_database(
            database=database_id,
            timeout_info=base_timeout_info(max_time_ms),
        )
        if te_response == database_id:
            if wait_until_active:
                last_status_seen: Optional[str] = "TERMINATING"
                _db_name: Optional[str] = None
                while last_status_seen == "TERMINATING":
                    time.sleep(WAITING_ON_DB_POLL_PERIOD_SECONDS)
                    #
                    detected_databases = [
                        a_db_info
                        for a_db_info in self.list_databases(
                            max_time_ms=timeout_manager.remaining_timeout_ms(),
                        )
                        if a_db_info.id == database_id
                    ]
                    if detected_databases:
                        last_status_seen = detected_databases[0].status
                        _db_name = detected_databases[0].status.info.name
                    else:
                        last_status_seen = None
                if last_status_seen is not None:
                    _name_desc = f" ({_db_name})" if _db_name else ""
                    raise DevOpsAPIException(
                        f"Database {database_id}{_name_desc} entered unexpected status {last_status_seen} after PENDING"
                    )
            return {"ok": 1}
        else:
            raise DevOpsAPIException(
                f"Could not issue a successful terminate-database DevOps API request for {database_id}."
            )


class AstraDBDatabaseAdmin:

    def __init__(self, id: str) -> None:
        self.id = id
