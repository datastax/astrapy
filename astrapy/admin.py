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

import asyncio
import logging
import re
import time
import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Sequence

import deprecation

from astrapy import __version__
from astrapy.api_commander import APICommander
from astrapy.authentication import coerce_token_provider, redact_secret
from astrapy.constants import CallerType, Environment
from astrapy.cursors import CommandCursor
from astrapy.defaults import (
    API_ENDPOINT_TEMPLATE_ENV_MAP,
    API_PATH_ENV_MAP,
    API_VERSION_ENV_MAP,
    DEFAULT_DATA_API_AUTH_HEADER,
    DEFAULT_DEV_OPS_AUTH_HEADER,
    DEFAULT_DEV_OPS_AUTH_PREFIX,
    DEV_OPS_DATABASE_POLL_INTERVAL_S,
    DEV_OPS_DATABASE_STATUS_ACTIVE,
    DEV_OPS_DATABASE_STATUS_INITIALIZING,
    DEV_OPS_DATABASE_STATUS_MAINTENANCE,
    DEV_OPS_DATABASE_STATUS_PENDING,
    DEV_OPS_DATABASE_STATUS_TERMINATING,
    DEV_OPS_DEFAULT_DATABASES_PAGE_SIZE,
    DEV_OPS_KEYSPACE_POLL_INTERVAL_S,
    DEV_OPS_RESPONSE_HTTP_ACCEPTED,
    DEV_OPS_RESPONSE_HTTP_CREATED,
    DEV_OPS_URL_ENV_MAP,
    DEV_OPS_VERSION_ENV_MAP,
    NAMESPACE_DEPRECATION_NOTICE_METHOD,
    SET_CALLER_DEPRECATION_NOTICE,
)
from astrapy.exceptions import (
    DataAPIFaultyResponseException,
    DevOpsAPIException,
    MultiCallTimeoutManager,
    base_timeout_info,
)
from astrapy.info import AdminDatabaseInfo, DatabaseInfo, FindEmbeddingProvidersResult
from astrapy.meta import (
    check_caller_parameters,
    check_namespace_keyspace,
    check_update_db_namespace_keyspace,
)
from astrapy.request_tools import HttpMethod

if TYPE_CHECKING:
    from astrapy import AsyncDatabase, Database
    from astrapy.authentication import TokenProvider


logger = logging.getLogger(__name__)


database_id_matcher = re.compile(
    "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

api_endpoint_parser = re.compile(
    r"https://"
    r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
    r"-"
    r"([a-z0-9\-]+)"
    r".apps.astra[\-]{0,1}"
    r"(dev|test)?"
    r".datastax.com"
)
api_endpoint_description = (
    "https://<db uuid, 8-4-4-4-12 hex format>-<db region>.apps.astra.datastax.com"
)

generic_api_url_matcher = re.compile(r"^https?:\/\/[a-zA-Z0-9\-.]+(\:[0-9]{1,6}){0,1}$")
generic_api_url_descriptor = "http[s]://<domain name or IP>[:port]"


@dataclass
class ParsedAPIEndpoint:
    """
    The results of successfully parsing an Astra DB API endpoint, for internal
    by database metadata-related functions.

    Attributes:
        database_id: e. g. "01234567-89ab-cdef-0123-456789abcdef".
        region: a region ID, such as "us-west1".
        environment: a label, whose value is one of Environment.PROD,
            Environment.DEV or Environment.TEST.
    """

    database_id: str
    region: str
    environment: str


def parse_api_endpoint(api_endpoint: str) -> ParsedAPIEndpoint | None:
    """
    Parse an API Endpoint into a ParsedAPIEndpoint structure.

    Args:
        api_endpoint: a full API endpoint for the Data API.

    Returns:
        The parsed ParsedAPIEndpoint. If parsing fails, return None.
    """

    match = api_endpoint_parser.match(api_endpoint)
    if match and match.groups():
        d_id, d_re, d_en_x = match.groups()
        return ParsedAPIEndpoint(
            database_id=d_id,
            region=d_re,
            environment=d_en_x if d_en_x else "prod",
        )
    else:
        return None


def api_endpoint_parsing_error_message(failing_url: str) -> str:
    """
    Format an error message with a suggestion for the expected url format.
    """
    return (
        f"Cannot parse the supplied API endpoint ({failing_url}). The endpoint "
        f'must be in the following form: "{api_endpoint_description}".'
    )


def parse_generic_api_url(api_endpoint: str) -> str | None:
    """
    Validate a generic API Endpoint string,
    such as `http://10.1.1.1:123` or `https://my.domain`.

    Args:
        api_endpoint: a string supposedly expressing a valid API Endpoint
        (not necessarily an Astra DB one).

    Returns:
        a normalized (stripped) version of the endpoint if valid. If invalid,
        return None.
    """
    if api_endpoint and api_endpoint[-1] == "/":
        _api_endpoint = api_endpoint[:-1]
    else:
        _api_endpoint = api_endpoint
    match = generic_api_url_matcher.match(_api_endpoint)
    if match:
        return match[0].rstrip("/")
    else:
        return None


def generic_api_url_parsing_error_message(failing_url: str) -> str:
    """
    Format an error message with a suggestion for the expected url format.
    """
    return (
        f"Cannot parse the supplied API endpoint ({failing_url}). The endpoint "
        f'must be in the following form: "{generic_api_url_descriptor}".'
    )


def build_api_endpoint(environment: str, database_id: str, region: str) -> str:
    """
    Build the API Endpoint full strings from database parameters.

    Args:
        environment: a label, whose value can be Environment.PROD
            or another of Environment.* for which this operation makes sense.
        database_id: e. g. "01234567-89ab-cdef-0123-456789abcdef".
        region: a region ID, such as "us-west1".

    Returns:
        the endpoint string, such as "https://01234567-...-eu-west1.apps.datastax.com"
    """

    return API_ENDPOINT_TEMPLATE_ENV_MAP[environment].format(
        database_id=database_id,
        region=region,
    )


def check_id_endpoint_parg_kwargs(
    p_arg: str | None,
    api_endpoint: str | None,
    id: str | None,
) -> tuple[str | None, str | None]:
    """
    Utility function helping with the transition to endpoint-first constructors,
    with ID being the other possibility.

    It is called with the positional argument, the api_endpoint and id kwargs: it
    then verifies legitimacy and returns a normalized (endpoint, id) "either" value.

    Note: this uses the ID regexp to recognize IDs. Crucially, no endpoint regexp
    here, since even non-Astra endpoints must be properly processed by this validator.
    """
    if p_arg is not None:
        if id is not None:
            raise ValueError(
                "Cannot pass `id` with the id/endpoint positional parameter."
            )
        if api_endpoint is not None:
            raise ValueError(
                "Cannot pass `api_endpoint` with the id/endpoint positional parameter."
            )
        if re.match(database_id_matcher, p_arg):
            return (None, p_arg)
        # p_arg is an endpoint:
        return (p_arg, None)
    # p_arg is None:
    if api_endpoint is None and id is None:
        return (None, None)
    if id is not None:
        if api_endpoint is None:
            return (None, id)
        else:
            raise ValueError("Cannot pass `api_endpoint` and `id` at the same time.")
    # endpoint is not None:
    return (api_endpoint, None)


def fetch_raw_database_info_from_id_token(
    id: str,
    *,
    token: str | None,
    environment: str = Environment.PROD,
    max_time_ms: int | None = None,
) -> dict[str, Any]:
    """
    Fetch database information through the DevOps API and return it in
    full, exactly like the API gives it back.

    Args:
        id: e. g. "01234567-89ab-cdef-0123-456789abcdef".
        token: a valid token to access the database information.
        environment: a string representing the target Data API environment.
            It can be left unspecified for the default value of `Environment.PROD`.
            Only Astra DB environments can be meaningfully supplied.
        max_time_ms: a timeout, in milliseconds, for waiting on a response.

    Returns:
        The full response from the DevOps API about the database.
    """

    ops_headers: dict[str, str | None]
    if token is not None:
        ops_headers = {
            DEFAULT_DEV_OPS_AUTH_HEADER: f"{DEFAULT_DEV_OPS_AUTH_PREFIX}{token}",
        }
    else:
        ops_headers = {}
    full_path = "/".join(
        [
            DEV_OPS_VERSION_ENV_MAP[environment],
            "databases",
            id,
        ]
    )
    ops_commander = APICommander(
        api_endpoint=DEV_OPS_URL_ENV_MAP[environment],
        path=full_path,
        headers=ops_headers,
        dev_ops_api=True,
    )

    gd_response = ops_commander.request(
        http_method=HttpMethod.GET,
        timeout_info=base_timeout_info(max_time_ms),
    )
    return gd_response


async def async_fetch_raw_database_info_from_id_token(
    id: str,
    *,
    token: str | None,
    environment: str = Environment.PROD,
    max_time_ms: int | None = None,
) -> dict[str, Any]:
    """
    Fetch database information through the DevOps API and return it in
    full, exactly like the API gives it back.
    Async version of the function, for use in an asyncio context.

    Args:
        id: e. g. "01234567-89ab-cdef-0123-456789abcdef".
        token: a valid token to access the database information.
        environment: a string representing the target Data API environment.
            It can be left unspecified for the default value of `Environment.PROD`.
            Only Astra DB environments can be meaningfully supplied.
        max_time_ms: a timeout, in milliseconds, for waiting on a response.

    Returns:
        The full response from the DevOps API about the database.
    """

    ops_headers: dict[str, str | None]
    if token is not None:
        ops_headers = {
            DEFAULT_DEV_OPS_AUTH_HEADER: f"{DEFAULT_DEV_OPS_AUTH_PREFIX}{token}",
        }
    else:
        ops_headers = {}
    full_path = "/".join(
        [
            DEV_OPS_VERSION_ENV_MAP[environment],
            "databases",
            id,
        ]
    )
    ops_commander = APICommander(
        api_endpoint=DEV_OPS_URL_ENV_MAP[environment],
        path=full_path,
        headers=ops_headers,
        dev_ops_api=True,
    )

    gd_response = await ops_commander.async_request(
        http_method=HttpMethod.GET,
        timeout_info=base_timeout_info(max_time_ms),
    )
    return gd_response


def fetch_database_info(
    api_endpoint: str,
    token: str | None,
    keyspace: str | None = None,
    namespace: str | None = None,
    max_time_ms: int | None = None,
) -> DatabaseInfo | None:
    """
    Fetch database information through the DevOps API.

    Args:
        api_endpoint: a full API endpoint for the Data API.
        token: a valid token to access the database information.
        keyspace: the desired keyspace that will be used in the result.
            If not specified, the resulting database info will show it as None.
        namespace: an alias for `keyspace`. *DEPRECATED*, removal in 2.0.
        max_time_ms: a timeout, in milliseconds, for waiting on a response.

    Returns:
        A DatabaseInfo object.
        If the API endpoint fails to be parsed, None is returned.
        For valid-looking endpoints, if something goes wrong an exception is raised.
    """

    keyspace_param = check_namespace_keyspace(
        keyspace=keyspace,
        namespace=namespace,
    )

    parsed_endpoint = parse_api_endpoint(api_endpoint)
    if parsed_endpoint:
        gd_response = fetch_raw_database_info_from_id_token(
            id=parsed_endpoint.database_id,
            token=token,
            environment=parsed_endpoint.environment,
            max_time_ms=max_time_ms,
        )
        raw_info = gd_response["info"]
        if keyspace_param is not None and keyspace_param not in (
            raw_info.get("keyspaces") or []
        ):
            raise DevOpsAPIException(f"Keyspace {keyspace_param} not found on DB.")
        else:
            return DatabaseInfo(
                id=parsed_endpoint.database_id,
                region=parsed_endpoint.region,
                keyspace=keyspace_param,
                namespace=keyspace_param,
                name=raw_info["name"],
                environment=parsed_endpoint.environment,
                raw_info=raw_info,
            )
    else:
        return None


async def async_fetch_database_info(
    api_endpoint: str,
    token: str | None,
    keyspace: str | None = None,
    namespace: str | None = None,
    max_time_ms: int | None = None,
) -> DatabaseInfo | None:
    """
    Fetch database information through the DevOps API.
    Async version of the function, for use in an asyncio context.

    Args:
        api_endpoint: a full API endpoint for the Data API.
        token: a valid token to access the database information.
        keyspace: the desired keyspace that will be used in the result.
            If not specified, the resulting database info will show it as None.
        namespace: an alias for `keyspace`. *DEPRECATED*, removal in 2.0.
        max_time_ms: a timeout, in milliseconds, for waiting on a response.

    Returns:
        A DatabaseInfo object.
        If the API endpoint fails to be parsed, None is returned.
        For valid-looking endpoints, if something goes wrong an exception is raised.
    """

    keyspace_param = check_namespace_keyspace(
        keyspace=keyspace,
        namespace=namespace,
    )

    parsed_endpoint = parse_api_endpoint(api_endpoint)
    if parsed_endpoint:
        gd_response = await async_fetch_raw_database_info_from_id_token(
            id=parsed_endpoint.database_id,
            token=token,
            environment=parsed_endpoint.environment,
            max_time_ms=max_time_ms,
        )
        raw_info = gd_response["info"]
        if keyspace_param is not None and keyspace_param not in (
            raw_info.get("keyspaces") or []
        ):
            raise DevOpsAPIException(f"Keyspace {keyspace_param} not found on DB.")
        else:
            return DatabaseInfo(
                id=parsed_endpoint.database_id,
                region=parsed_endpoint.region,
                keyspace=keyspace_param,
                namespace=keyspace_param,
                name=raw_info["name"],
                environment=parsed_endpoint.environment,
                raw_info=raw_info,
            )
    else:
        return None


def _recast_as_admin_database_info(
    admin_database_info_dict: dict[str, Any],
    *,
    environment: str,
) -> AdminDatabaseInfo:
    return AdminDatabaseInfo(
        info=DatabaseInfo(
            id=admin_database_info_dict["id"],
            region=admin_database_info_dict["info"]["region"],
            keyspace=admin_database_info_dict["info"].get("keyspace"),
            namespace=admin_database_info_dict["info"].get("keyspace"),
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


def normalize_region_for_id(
    database_id: str,
    token_str: str | None,
    environment: str,
    region_param: str | None,
    max_time_ms: int | None,
) -> str:
    if region_param:
        return region_param
    else:
        logger.info(f"fetching raw database info for {database_id}")
        this_db_info = fetch_raw_database_info_from_id_token(
            id=database_id,
            token=token_str,
            environment=environment,
            max_time_ms=max_time_ms,
        )
        logger.info(f"finished fetching raw database info for {database_id}")
        found_region = this_db_info.get("info", {}).get("region")
        if not isinstance(found_region, str):
            raise ValueError(
                f"Could not determine 'region' from database info: {str(this_db_info)}"
            )
        return found_region


class AstraDBAdmin:
    """
    An "admin" object, able to perform administrative tasks at the databases
    level, such as creating, listing or dropping databases.

    Args:
        token: an access token with enough permission to perform admin tasks.
            This can be either a literal token string or a subclass of
            `astrapy.authentication.TokenProvider`.
        environment: a label, whose value is one of Environment.PROD (default),
            Environment.DEV or Environment.TEST.
        callers: a list of caller identities, i.e. applications, or frameworks,
            on behalf of which DevOps API calls are performed. These end up in
            the request user-agent.
            Each caller identity is a ("caller_name", "caller_version") pair.
        caller_name: *DEPRECATED*, use `callers`. Removal 2.0. Name of the
            application, or framework, on behalf of which the DevOps API calls
            are performed. This ends up in the request user-agent.
        caller_version: version of the caller. *DEPRECATED*, use `callers`. Removal 2.0.
        dev_ops_url: in case of custom deployments, this can be used to specify
            the URL to the DevOps API, such as "https://api.astra.datastax.com".
            Generally it can be omitted. The environment (prod/dev/...) is
            determined from the API Endpoint.
        dev_ops_api_version: this can specify a custom version of the DevOps API
            (such as "v2"). Generally not needed.

    Example:
        >>> from astrapy import DataAPIClient
        >>> my_client = DataAPIClient("AstraCS:...")
        >>> my_astra_db_admin = my_client.get_admin()
        >>> database_list = my_astra_db_admin.list_databases()
        >>> len(database_list)
        3
        >>> database_list[2].id
        '01234567-...'
        >>> my_db_admin = my_astra_db_admin.get_database_admin("01234567-...")
        >>> my_db_admin.list_keyspaces()
        ['default_keyspace', 'staging_keyspace']
    """

    def __init__(
        self,
        token: str | TokenProvider | None = None,
        *,
        environment: str | None = None,
        callers: Sequence[CallerType] = [],
        caller_name: str | None = None,
        caller_version: str | None = None,
        dev_ops_url: str | None = None,
        dev_ops_api_version: str | None = None,
    ) -> None:
        callers_param = check_caller_parameters(callers, caller_name, caller_version)
        self.token_provider = coerce_token_provider(token)
        self.environment = (environment or Environment.PROD).lower()
        if self.environment not in Environment.astra_db_values:
            raise ValueError("Environments outside of Astra DB are not supported.")
        if dev_ops_url is None:
            self.dev_ops_url = DEV_OPS_URL_ENV_MAP[self.environment]
        else:
            self.dev_ops_url = dev_ops_url
        self._dev_ops_url = dev_ops_url
        self._dev_ops_api_version = dev_ops_api_version

        self._dev_ops_commander_headers: dict[str, str | None]
        if self.token_provider:
            _token = self.token_provider.get_token()
            self._dev_ops_commander_headers = {
                DEFAULT_DEV_OPS_AUTH_HEADER: f"{DEFAULT_DEV_OPS_AUTH_PREFIX}{_token}",
            }
        else:
            self._dev_ops_commander_headers = {}

        self.callers = callers_param
        self._dev_ops_api_commander = self._get_dev_ops_api_commander()

    def __repr__(self) -> str:
        token_desc: str | None
        if self.token_provider:
            token_desc = f'"{redact_secret(str(self.token_provider), 15)}"'
        else:
            token_desc = None
        env_desc: str | None
        if self.environment == Environment.PROD:
            env_desc = None
        else:
            env_desc = f'environment="{self.environment}"'
        parts = [pt for pt in [token_desc, env_desc] if pt is not None]
        return f"{self.__class__.__name__}({', '.join(parts)})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, AstraDBAdmin):
            return all(
                [
                    self.token_provider == other.token_provider,
                    self.environment == other.environment,
                    self.dev_ops_url == other.dev_ops_url,
                    self.dev_ops_url == other.dev_ops_url,
                    self.callers == other.callers,
                    self._dev_ops_url == other._dev_ops_url,
                    self._dev_ops_api_version == other._dev_ops_api_version,
                    self._dev_ops_api_commander == other._dev_ops_api_commander,
                ]
            )
        else:
            return False

    def _get_dev_ops_api_commander(self) -> APICommander:
        """Instantiate a new APICommander based on the properties of this class."""

        dev_ops_base_path = "/".join(
            [DEV_OPS_VERSION_ENV_MAP[self.environment], "databases"]
        )
        dev_ops_commander = APICommander(
            api_endpoint=DEV_OPS_URL_ENV_MAP[self.environment],
            path=dev_ops_base_path,
            headers=self._dev_ops_commander_headers,
            callers=self.callers,
            dev_ops_api=True,
        )
        return dev_ops_commander

    def _copy(
        self,
        *,
        token: str | TokenProvider | None = None,
        environment: str | None = None,
        callers: Sequence[CallerType] = [],
        caller_name: str | None = None,
        caller_version: str | None = None,
        dev_ops_url: str | None = None,
        dev_ops_api_version: str | None = None,
    ) -> AstraDBAdmin:
        callers_param = check_caller_parameters(callers, caller_name, caller_version)
        return AstraDBAdmin(
            token=coerce_token_provider(token) or self.token_provider,
            environment=environment or self.environment,
            callers=callers_param or self.callers,
            dev_ops_url=dev_ops_url or self._dev_ops_url,
            dev_ops_api_version=dev_ops_api_version or self._dev_ops_api_version,
        )

    def with_options(
        self,
        *,
        token: str | TokenProvider | None = None,
        callers: Sequence[CallerType] = [],
        caller_name: str | None = None,
        caller_version: str | None = None,
    ) -> AstraDBAdmin:
        """
        Create a clone of this AstraDBAdmin with some changed attributes.

        Args:
            token: an Access Token to the database. Example: `"AstraCS:xyz..."`.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            callers: a list of caller identities, i.e. applications, or frameworks,
                on behalf of which DevOps API calls are performed. These end up in
                the request user-agent.
                Each caller identity is a ("caller_name", "caller_version") pair.
            caller_name: *DEPRECATED*, use `callers`. Removal 2.0. Name of the
                application, or framework, on behalf of which the DevOps API calls
                are performed. This ends up in the request user-agent.
            caller_version: version of the caller. *DEPRECATED*, use `callers`.
                Removal 2.0.

        Returns:
            a new AstraDBAdmin instance.

        Example:
            >>> another_astra_db_admin = my_astra_db_admin.with_options(
            ...     callers=[("caller_identity", "1.2.0")],
            ... )
        """

        callers_param = check_caller_parameters(callers, caller_name, caller_version)
        return self._copy(
            token=token,
            callers=callers_param,
        )

    @deprecation.deprecated(  # type: ignore[misc]
        deprecated_in="1.5.1",
        removed_in="2.0.0",
        current_version=__version__,
        details=SET_CALLER_DEPRECATION_NOTICE,
    )
    def set_caller(
        self,
        caller_name: str | None = None,
        caller_version: str | None = None,
    ) -> None:
        """
        Set a new identity for the application/framework on behalf of which
        the DevOps API calls will be performed (the "caller").

        New objects spawned from this client afterwards will inherit the new settings.

        Args:
            caller_name: name of the application, or framework, on behalf of which
                the DevOps API calls are performed. This ends up in the request user-agent.
            caller_version: version of the caller.

        Example:
            >>> my_astra_db_admin.set_caller(
            ...     callers=[("the_caller", "0.1.0")],
            ... )
        """

        logger.info(f"setting caller to {caller_name}/{caller_version}")
        callers_param = check_caller_parameters([], caller_name, caller_version)
        self.callers = callers_param
        self._dev_ops_api_commander = self._get_dev_ops_api_commander()

    def list_databases(
        self,
        *,
        include: str | None = None,
        provider: str | None = None,
        page_size: int | None = None,
        max_time_ms: int | None = None,
    ) -> CommandCursor[AdminDatabaseInfo]:
        """
        Get the list of databases, as obtained with a request to the DevOps API.

        Args:
            include: a filter on what databases are to be returned. As per
                DevOps API, defaults to "nonterminated". Pass "all" to include
                the already terminated databases.
            provider: a filter on the cloud provider for the databases.
                As per DevOps API, defaults to "ALL". Pass e.g. "AWS" to
                restrict the results.
            page_size: number of results per page from the DevOps API. Optional.
            max_time_ms: a timeout, in milliseconds, for the API request.

        Returns:
            A CommandCursor to iterate over the detected databases,
            represented as AdminDatabaseInfo objects.

        Example:
            >>> database_cursor = my_astra_db_admin.list_databases()
            >>> database_list = list(database_cursor)
            >>> len(database_list)
            3
            >>> database_list[2].id
            '01234567-...'
            >>> database_list[2].status
            'ACTIVE'
            >>> database_list[2].info.region
            'eu-west-1'
        """

        logger.info("getting databases (DevOps API)")
        request_params_0 = {
            k: v
            for k, v in {
                "include": include,
                "provider": provider,
                "limit": page_size or DEV_OPS_DEFAULT_DATABASES_PAGE_SIZE,
            }.items()
            if v is not None
        }
        responses: list[dict[str, Any]] = []
        logger.info("request 0, getting databases (DevOps API)")
        response_0 = self._dev_ops_api_commander.request(
            http_method=HttpMethod.GET,
            request_params=request_params_0,
            timeout_info=base_timeout_info(max_time_ms),
        )
        if not isinstance(response_0, list):
            raise DevOpsAPIException(
                "Faulty response from get-databases DevOps API command.",
            )
        logger.info("finished request 0, getting databases (DevOps API)")
        responses += [response_0]
        while len(responses[-1]) >= request_params_0["limit"]:
            if "id" not in responses[-1][-1]:
                raise DevOpsAPIException(
                    "Faulty response from get-databases DevOps API command.",
                )
            last_received_db_id = responses[-1][-1]["id"]
            request_params_n = {
                **request_params_0,
                **{"starting_after": last_received_db_id},
            }
            logger.info(
                "request %s, getting databases (DevOps API)",
                len(responses),
            )
            response_n = self._dev_ops_api_commander.request(
                http_method=HttpMethod.GET,
                request_params=request_params_n,
                timeout_info=base_timeout_info(max_time_ms),
            )
            logger.info(
                "finished request %s, getting databases (DevOps API)",
                len(responses),
            )
            if not isinstance(response_n, list):
                raise DevOpsAPIException(
                    "Faulty response from get-databases DevOps API command.",
                )
            responses += [response_n]

        logger.info("finished getting databases (DevOps API)")
        return CommandCursor(
            address=self._dev_ops_api_commander.full_path,
            items=[
                _recast_as_admin_database_info(
                    db_dict,
                    environment=self.environment,
                )
                for response in responses
                for db_dict in response
            ],
        )

    async def async_list_databases(
        self,
        *,
        include: str | None = None,
        provider: str | None = None,
        page_size: int | None = None,
        max_time_ms: int | None = None,
    ) -> CommandCursor[AdminDatabaseInfo]:
        """
        Get the list of databases, as obtained with a request to the DevOps API.
        Async version of the method, for use in an asyncio context.

        Args:
            include: a filter on what databases are to be returned. As per
                DevOps API, defaults to "nonterminated". Pass "all" to include
                the already terminated databases.
            provider: a filter on the cloud provider for the databases.
                As per DevOps API, defaults to "ALL". Pass e.g. "AWS" to
                restrict the results.
            page_size: number of results per page from the DevOps API. Optional.
            max_time_ms: a timeout, in milliseconds, for the API request.

        Returns:
            A CommandCursor to iterate over the detected databases,
            represented as AdminDatabaseInfo objects.
            Note that the return type is not an awaitable, rather
            a regular iterable, e.g. for use in ordinary "for" loops.

        Example:
            >>> async def check_if_db_exists(db_id: str) -> bool:
            ...     db_cursor = await my_astra_db_admin.async_list_databases()
            ...     db_list = list(dd_cursor)
            ...     return db_id in db_list
            ...
            >>> asyncio.run(check_if_db_exists("xyz"))
            True
            >>> asyncio.run(check_if_db_exists("01234567-..."))
            False
        """

        logger.info("getting databases (DevOps API), async")
        request_params_0 = {
            k: v
            for k, v in {
                "include": include,
                "provider": provider,
                "limit": page_size or DEV_OPS_DEFAULT_DATABASES_PAGE_SIZE,
            }.items()
            if v is not None
        }
        responses: list[dict[str, Any]] = []
        logger.info("request 0, getting databases (DevOps API), async")
        response_0 = await self._dev_ops_api_commander.async_request(
            http_method=HttpMethod.GET,
            request_params=request_params_0,
            timeout_info=base_timeout_info(max_time_ms),
        )
        if not isinstance(response_0, list):
            raise DevOpsAPIException(
                "Faulty response from get-databases DevOps API command.",
            )
        logger.info("finished request 0, getting databases (DevOps API), async")
        responses += [response_0]
        while len(responses[-1]) >= request_params_0["limit"]:
            if "id" not in responses[-1][-1]:
                raise DevOpsAPIException(
                    "Faulty response from get-databases DevOps API command.",
                )
            last_received_db_id = responses[-1][-1]["id"]
            request_params_n = {
                **request_params_0,
                **{"starting_after": last_received_db_id},
            }
            logger.info(
                "request %s, getting databases (DevOps API)",
                len(responses),
            )
            response_n = await self._dev_ops_api_commander.async_request(
                http_method=HttpMethod.GET,
                request_params=request_params_n,
                timeout_info=base_timeout_info(max_time_ms),
            )
            logger.info(
                "finished request %s, getting databases (DevOps API), async",
                len(responses),
            )
            if not isinstance(response_n, list):
                raise DevOpsAPIException(
                    "Faulty response from get-databases DevOps API command.",
                )
            responses += [response_n]

        logger.info("finished getting databases (DevOps API), async")
        return CommandCursor(
            address=self._dev_ops_api_commander.full_path,
            items=[
                _recast_as_admin_database_info(
                    db_dict,
                    environment=self.environment,
                )
                for response in responses
                for db_dict in response
            ],
        )

    def database_info(
        self, id: str, *, max_time_ms: int | None = None
    ) -> AdminDatabaseInfo:
        """
        Get the full information on a given database, through a request to the DevOps API.

        Args:
            id: the ID of the target database, e. g.
                "01234567-89ab-cdef-0123-456789abcdef".
            max_time_ms: a timeout, in milliseconds, for the API request.

        Returns:
            An AdminDatabaseInfo object.

        Example:
            >>> details_of_my_db = my_astra_db_admin.database_info("01234567-...")
            >>> details_of_my_db.id
            '01234567-...'
            >>> details_of_my_db.status
            'ACTIVE'
            >>> details_of_my_db.info.region
            'eu-west-1'
        """

        logger.info(f"getting database info for '{id}' (DevOps API)")
        gd_response = self._dev_ops_api_commander.request(
            http_method=HttpMethod.GET,
            additional_path=id,
            timeout_info=base_timeout_info(max_time_ms),
        )
        logger.info(f"finished getting database info for '{id}' (DevOps API)")
        return _recast_as_admin_database_info(
            gd_response,
            environment=self.environment,
        )

    async def async_database_info(
        self, id: str, *, max_time_ms: int | None = None
    ) -> AdminDatabaseInfo:
        """
        Get the full information on a given database, through a request to the DevOps API.
        This is an awaitable method suitable for use within an asyncio event loop.

        Args:
            id: the ID of the target database, e. g.
                "01234567-89ab-cdef-0123-456789abcdef".
            max_time_ms: a timeout, in milliseconds, for the API request.

        Returns:
            An AdminDatabaseInfo object.

        Example:
            >>> async def check_if_db_active(db_id: str) -> bool:
            ...     db_info = await my_astra_db_admin.async_database_info(db_id)
            ...     return db_info.status == "ACTIVE"
            ...
            >>> asyncio.run(check_if_db_active("01234567-..."))
            True
        """

        logger.info(f"getting database info for '{id}' (DevOps API), async")
        gd_response = await self._dev_ops_api_commander.async_request(
            http_method=HttpMethod.GET,
            additional_path=id,
            timeout_info=base_timeout_info(max_time_ms),
        )
        logger.info(f"finished getting database info for '{id}' (DevOps API), async")
        return _recast_as_admin_database_info(
            gd_response,
            environment=self.environment,
        )

    def create_database(
        self,
        name: str,
        *,
        cloud_provider: str,
        region: str,
        keyspace: str | None = None,
        namespace: str | None = None,
        wait_until_active: bool = True,
        max_time_ms: int | None = None,
    ) -> AstraDBDatabaseAdmin:
        """
        Create a database as requested, optionally waiting for it to be ready.

        Args:
            name: the desired name for the database.
            cloud_provider: one of 'aws', 'gcp' or 'azure'.
            region: any of the available cloud regions.
            keyspace: name for the one keyspace the database starts with.
                If omitted, DevOps API will use its default.
            namespace: an alias for `keyspace`. *DEPRECATED*, removal in 2.0.
            wait_until_active: if True (default), the method returns only after
                the newly-created database is in ACTIVE state (a few minutes,
                usually). If False, it will return right after issuing the
                creation request to the DevOps API, and it will be responsibility
                of the caller to check the database status before working with it.
            max_time_ms: a timeout, in milliseconds, for the whole requested
                operation to complete.
                Note that a timeout is no guarantee that the creation request
                has not reached the API server.

        Returns:
            An AstraDBDatabaseAdmin instance.

        Example:
            >>> my_new_db_admin = my_astra_db_admin.create_database(
            ...     "new_database",
            ...     cloud_provider="aws",
            ...     region="ap-south-1",
            ... )
            >>> my_new_db = my_new_db_admin.get_database()
            >>> my_coll = my_new_db.create_collection("movies", dimension=2)
            >>> my_coll.insert_one({"title": "The Title", "$vector": [0.1, 0.2]})
        """

        keyspace_param = check_namespace_keyspace(
            keyspace=keyspace,
            namespace=namespace,
        )

        cd_payload = {
            k: v
            for k, v in {
                "name": name,
                "tier": "serverless",
                "cloudProvider": cloud_provider,
                "region": region,
                "capacityUnits": 1,
                "dbType": "vector",
                "keyspace": keyspace_param,
            }.items()
            if v is not None
        }
        timeout_manager = MultiCallTimeoutManager(
            overall_max_time_ms=max_time_ms, dev_ops_api=True
        )
        logger.info(
            f"creating database {name}/({cloud_provider}, {region}) (DevOps API)"
        )
        cd_raw_response = self._dev_ops_api_commander.raw_request(
            http_method=HttpMethod.POST,
            payload=cd_payload,
            timeout_info=timeout_manager.remaining_timeout_info(),
        )
        if cd_raw_response.status_code != DEV_OPS_RESPONSE_HTTP_CREATED:
            raise DevOpsAPIException(
                f"DB creation ('{name}') failed: API returned HTTP "
                f"{cd_raw_response.status_code} instead of "
                f"{DEV_OPS_RESPONSE_HTTP_CREATED} - Created."
            )
        new_database_id = cd_raw_response.headers["Location"]
        logger.info(
            "DevOps API returned from creating database "
            f"{name}/({cloud_provider}, {region})"
        )
        if wait_until_active:
            last_status_seen = DEV_OPS_DATABASE_STATUS_PENDING
            while last_status_seen in {
                DEV_OPS_DATABASE_STATUS_PENDING,
                DEV_OPS_DATABASE_STATUS_INITIALIZING,
            }:
                logger.info(f"sleeping to poll for status of '{new_database_id}'")
                time.sleep(DEV_OPS_DATABASE_POLL_INTERVAL_S)
                last_db_info = self.database_info(
                    id=new_database_id,
                    max_time_ms=timeout_manager.remaining_timeout_ms(),
                )
                last_status_seen = last_db_info.status
            if last_status_seen != DEV_OPS_DATABASE_STATUS_ACTIVE:
                raise DevOpsAPIException(
                    f"Database {name} entered unexpected status {last_status_seen} after PENDING"
                )
        # return the database instance
        logger.info(
            f"finished creating database '{new_database_id}' = "
            f"{name}/({cloud_provider}, {region}) (DevOps API)"
        )
        return AstraDBDatabaseAdmin.from_astra_db_admin(
            api_endpoint=build_api_endpoint(
                environment=self.environment,
                database_id=new_database_id,
                region=region,
            ),
            astra_db_admin=self,
        )

    async def async_create_database(
        self,
        name: str,
        *,
        cloud_provider: str,
        region: str,
        keyspace: str | None = None,
        namespace: str | None = None,
        wait_until_active: bool = True,
        max_time_ms: int | None = None,
    ) -> AstraDBDatabaseAdmin:
        """
        Create a database as requested, optionally waiting for it to be ready.
        This is an awaitable method suitable for use within an asyncio event loop.

        Args:
            name: the desired name for the database.
            cloud_provider: one of 'aws', 'gcp' or 'azure'.
            region: any of the available cloud regions.
            keyspace: name for the one keyspace the database starts with.
                If omitted, DevOps API will use its default.
            namespace: an alias for `keyspace`. *DEPRECATED*, removal in 2.0.
            wait_until_active: if True (default), the method returns only after
                the newly-created database is in ACTIVE state (a few minutes,
                usually). If False, it will return right after issuing the
                creation request to the DevOps API, and it will be responsibility
                of the caller to check the database status before working with it.
            max_time_ms: a timeout, in milliseconds, for the whole requested
                operation to complete.
                Note that a timeout is no guarantee that the creation request
                has not reached the API server.

        Returns:
            An AstraDBDatabaseAdmin instance.

        Example:
            >>> asyncio.run(
            ...     my_astra_db_admin.async_create_database(
            ...         "new_database",
            ...         cloud_provider="aws",
            ...         region="ap-south-1",
            ....    )
            ... )
            AstraDBDatabaseAdmin(id=...)
        """

        keyspace_param = check_namespace_keyspace(
            keyspace=keyspace,
            namespace=namespace,
        )

        cd_payload = {
            k: v
            for k, v in {
                "name": name,
                "tier": "serverless",
                "cloudProvider": cloud_provider,
                "region": region,
                "capacityUnits": 1,
                "dbType": "vector",
                "keyspace": keyspace_param,
            }.items()
            if v is not None
        }
        timeout_manager = MultiCallTimeoutManager(
            overall_max_time_ms=max_time_ms, dev_ops_api=True
        )
        logger.info(
            f"creating database {name}/({cloud_provider}, {region}) "
            "(DevOps API), async"
        )
        cd_raw_response = await self._dev_ops_api_commander.async_raw_request(
            http_method=HttpMethod.POST,
            payload=cd_payload,
            timeout_info=timeout_manager.remaining_timeout_info(),
        )
        if cd_raw_response.status_code != DEV_OPS_RESPONSE_HTTP_CREATED:
            raise DevOpsAPIException(
                f"DB creation ('{name}') failed: API returned HTTP "
                f"{cd_raw_response.status_code} instead of "
                f"{DEV_OPS_RESPONSE_HTTP_CREATED} - Created."
            )
        new_database_id = cd_raw_response.headers["Location"]
        logger.info(
            "DevOps API returned from creating database "
            f"{name}/({cloud_provider}, {region}), async"
        )
        if wait_until_active:
            last_status_seen = DEV_OPS_DATABASE_STATUS_PENDING
            while last_status_seen in {
                DEV_OPS_DATABASE_STATUS_PENDING,
                DEV_OPS_DATABASE_STATUS_INITIALIZING,
            }:
                logger.info(
                    f"sleeping to poll for status of '{new_database_id}', async"
                )
                await asyncio.sleep(DEV_OPS_DATABASE_POLL_INTERVAL_S)
                last_db_info = await self.async_database_info(
                    id=new_database_id,
                    max_time_ms=timeout_manager.remaining_timeout_ms(),
                )
                last_status_seen = last_db_info.status
            if last_status_seen != DEV_OPS_DATABASE_STATUS_ACTIVE:
                raise DevOpsAPIException(
                    f"Database {name} entered unexpected status "
                    f"{last_status_seen} after PENDING"
                )
        # return the database instance
        logger.info(
            f"finished creating database '{new_database_id}' = "
            f"{name}/({cloud_provider}, {region}) (DevOps API), async"
        )
        return AstraDBDatabaseAdmin.from_astra_db_admin(
            api_endpoint=build_api_endpoint(
                environment=self.environment,
                database_id=new_database_id,
                region=region,
            ),
            astra_db_admin=self,
        )

    def drop_database(
        self,
        id: str,
        *,
        wait_until_active: bool = True,
        max_time_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        Drop a database, i.e. delete it completely and permanently with all its data.

        Args:
            id: The ID of the database to drop, e. g.
                "01234567-89ab-cdef-0123-456789abcdef".
            wait_until_active: if True (default), the method returns only after
                the database has actually been deleted (generally a few minutes).
                If False, it will return right after issuing the
                drop request to the DevOps API, and it will be responsibility
                of the caller to check the database status/availability
                after that, if desired.
            max_time_ms: a timeout, in milliseconds, for the whole requested
                operation to complete.
                Note that a timeout is no guarantee that the deletion request
                has not reached the API server.

        Returns:
            A dictionary of the form {"ok": 1} in case of success.
            Otherwise, an exception is raised.

        Example:
            >>> database_list_pre = my_astra_db_admin.list_databases()
            >>> len(database_list_pre)
            3
            >>> my_astra_db_admin.drop_database("01234567-...")
            {'ok': 1}
            >>> database_list_post = my_astra_db_admin.list_databases()
            >>> len(database_list_post)
            2
        """

        timeout_manager = MultiCallTimeoutManager(
            overall_max_time_ms=max_time_ms, dev_ops_api=True
        )
        logger.info(f"dropping database '{id}' (DevOps API)")
        te_raw_response = self._dev_ops_api_commander.raw_request(
            http_method=HttpMethod.POST,
            additional_path=f"{id}/terminate",
            timeout_info=timeout_manager.remaining_timeout_info(),
        )
        if te_raw_response.status_code != DEV_OPS_RESPONSE_HTTP_ACCEPTED:
            raise DevOpsAPIException(
                f"DB deletion ('{id}') failed: API returned HTTP "
                f"{te_raw_response.status_code} instead of "
                f"{DEV_OPS_RESPONSE_HTTP_ACCEPTED} - Created"
            )
        logger.info(f"DevOps API returned from dropping database '{id}'")
        if wait_until_active:
            last_status_seen: str | None = DEV_OPS_DATABASE_STATUS_TERMINATING
            _db_name: str | None = None
            while last_status_seen == DEV_OPS_DATABASE_STATUS_TERMINATING:
                logger.info(f"sleeping to poll for status of '{id}'")
                time.sleep(DEV_OPS_DATABASE_POLL_INTERVAL_S)
                #
                detected_databases = [
                    a_db_info
                    for a_db_info in self.list_databases(
                        max_time_ms=timeout_manager.remaining_timeout_ms(),
                    )
                    if a_db_info.id == id
                ]
                if detected_databases:
                    last_status_seen = detected_databases[0].status
                    _db_name = detected_databases[0].info.name
                else:
                    last_status_seen = None
            if last_status_seen is not None:
                _name_desc = f" ({_db_name})" if _db_name else ""
                raise DevOpsAPIException(
                    f"Database {id}{_name_desc} entered unexpected status "
                    f"{last_status_seen} after PENDING"
                )
        logger.info(f"finished dropping database '{id}' (DevOps API)")
        return {"ok": 1}

    async def async_drop_database(
        self,
        id: str,
        *,
        wait_until_active: bool = True,
        max_time_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        Drop a database, i.e. delete it completely and permanently with all its data.
        Async version of the method, for use in an asyncio context.

        Args:
            id: The ID of the database to drop, e. g.
                "01234567-89ab-cdef-0123-456789abcdef".
            wait_until_active: if True (default), the method returns only after
                the database has actually been deleted (generally a few minutes).
                If False, it will return right after issuing the
                drop request to the DevOps API, and it will be responsibility
                of the caller to check the database status/availability
                after that, if desired.
            max_time_ms: a timeout, in milliseconds, for the whole requested
                operation to complete.
                Note that a timeout is no guarantee that the deletion request
                has not reached the API server.

        Returns:
            A dictionary of the form {"ok": 1} in case of success.
            Otherwise, an exception is raised.

        Example:
            >>> asyncio.run(
            ...     my_astra_db_admin.async_drop_database("01234567-...")
            ... )
            {'ok': 1}
        """

        timeout_manager = MultiCallTimeoutManager(
            overall_max_time_ms=max_time_ms, dev_ops_api=True
        )
        logger.info(f"dropping database '{id}' (DevOps API), async")
        te_raw_response = await self._dev_ops_api_commander.async_raw_request(
            http_method=HttpMethod.POST,
            additional_path=f"{id}/terminate",
            timeout_info=timeout_manager.remaining_timeout_info(),
        )
        if te_raw_response.status_code != DEV_OPS_RESPONSE_HTTP_ACCEPTED:
            raise DevOpsAPIException(
                f"DB deletion ('{id}') failed: API returned HTTP "
                f"{te_raw_response.status_code} instead of "
                f"{DEV_OPS_RESPONSE_HTTP_ACCEPTED} - Created"
            )
        logger.info(f"DevOps API returned from dropping database '{id}', async")
        if wait_until_active:
            last_status_seen: str | None = DEV_OPS_DATABASE_STATUS_TERMINATING
            _db_name: str | None = None
            while last_status_seen == DEV_OPS_DATABASE_STATUS_TERMINATING:
                logger.info(f"sleeping to poll for status of '{id}', async")
                await asyncio.sleep(DEV_OPS_DATABASE_POLL_INTERVAL_S)
                #
                detected_databases = [
                    a_db_info
                    for a_db_info in await self.async_list_databases(
                        max_time_ms=timeout_manager.remaining_timeout_ms(),
                    )
                    if a_db_info.id == id
                ]
                if detected_databases:
                    last_status_seen = detected_databases[0].status
                    _db_name = detected_databases[0].info.name
                else:
                    last_status_seen = None
            if last_status_seen is not None:
                _name_desc = f" ({_db_name})" if _db_name else ""
                raise DevOpsAPIException(
                    f"Database {id}{_name_desc} entered unexpected status "
                    f"{last_status_seen} after PENDING"
                )
        logger.info(f"finished dropping database '{id}' (DevOps API), async")
        return {"ok": 1}

    def get_database_admin(
        self,
        api_endpoint_or_id: str | None = None,
        *,
        api_endpoint: str | None = None,
        id: str | None = None,
        region: str | None = None,
        max_time_ms: int | None = None,
    ) -> AstraDBDatabaseAdmin:
        """
        Create an AstraDBDatabaseAdmin object for admin work within a certain database.

        Args:
            api_endpoint_or_id: positional parameter that can stand for both
                `api_endpoint` and `id`. Passing them together is an error.
            api_endpoint: the API Endpoint for the target database
                (e.g. `https://<ID>-<REGION>.apps.astra.datastax.com`).
                The database must exist already for the resulting object
                to be effectively used; in other words, this invocation
                does not create the database, just the object instance.
            id: the target database ID. This is alternative to using the API Endpoint.
            region: the region to use for connecting to the database. The
                database must be located in that region. This parameter can be used
                only if the database is specified by its ID (instead of API Endpoint).
                If this parameter is not passed, and cannot be inferred
                from the API endpoint, an additional DevOps API request is made
                to determine the default region and use it subsequently.
            max_time_ms: a timeout, in milliseconds, for the DevOps API
                HTTP request should it be necessary (see the `region` argument).

        Returns:
            An AstraDBDatabaseAdmin instance representing the requested database.

        Example:
            >>> my_db_admin = my_astra_db_admin.get_database_admin("01234567-...")
            >>> my_db_admin.list_keyspaces()
            ['default_keyspace']
            >>> my_db_admin.create_keyspace("that_other_one")
            {'ok': 1}
            >>> my_db_admin.list_keyspaces()
            ['default_keyspace', 'that_other_one']

        Note:
            This method does not perform any admin-level operation through
            the DevOps API. For actual creation of a database, see the
            `create_database` method.
        """

        _api_endpoint_p, _id_p = check_id_endpoint_parg_kwargs(
            p_arg=api_endpoint_or_id, api_endpoint=api_endpoint, id=id
        )
        # handle the "endpoint passed as id" case first:
        if _api_endpoint_p is not None:
            if region is not None:
                raise ValueError(
                    "Parameter `region` not supported with an API endpoint."
                )
            # in this case max_time_ms is ignored (no calls take place)
            return AstraDBDatabaseAdmin.from_astra_db_admin(
                api_endpoint=_api_endpoint_p,
                astra_db_admin=self,
                max_time_ms=max_time_ms,
            )
        else:
            if _id_p is None:
                raise ValueError("Either `api_endpoint` or `id` must be supplied.")

            _region = normalize_region_for_id(
                database_id=_id_p,
                token_str=self.token_provider.get_token(),
                environment=self.environment,
                region_param=region,
                max_time_ms=max_time_ms,
            )
            return AstraDBDatabaseAdmin.from_astra_db_admin(
                api_endpoint=build_api_endpoint(
                    environment=self.environment,
                    database_id=_id_p,
                    region=_region,
                ),
                astra_db_admin=self,
                max_time_ms=max_time_ms,
            )

    def get_database(
        self,
        api_endpoint_or_id: str | None = None,
        *,
        api_endpoint: str | None = None,
        token: str | TokenProvider | None = None,
        keyspace: str | None = None,
        namespace: str | None = None,
        id: str | None = None,
        region: str | None = None,
        api_path: str | None = None,
        api_version: str | None = None,
        max_time_ms: int | None = None,
    ) -> Database:
        """
        Create a Database instance for a specific database, to be used
        when doing data-level work (such as creating/managing collections).

        Args:
            api_endpoint_or_id: positional parameter that can stand for both
                `api_endpoint` and `id`. Passing them together is an error.
            api_endpoint: the API Endpoint for the target database
                (e.g. `https://<ID>-<REGION>.apps.astra.datastax.com`).
                The database must exist already for the resulting object
                to be effectively used; in other words, this invocation
                does not create the database, just the object instance.
            token: if supplied, is passed to the Database instead of
                the one set for this object.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            keyspace: used to specify a certain keyspace the resulting
                Database will primarily work on. If not specified, an additional
                DevOps API call reveals the default keyspace for the target database.
            namespace: an alias for `keyspace`. *DEPRECATED*, removal in 2.0.
            id: the target database ID. This is alternative to using the API Endpoint.
            region: the region to use for connecting to the database. The
                database must be located in that region. This parameter can be used
                only if the database is specified by its ID (instead of API Endpoint).
                If this parameter is not passed, and cannot be inferred
                from the API endpoint, an additional DevOps API request is made
                to determine the default region and use it subsequently.
            api_path: path to append to the API Endpoint. In typical usage, this
                should be left to its default of "/api/json".
            api_version: version specifier to append to the API path. In typical
                usage, this should be left to its default of "v1".
            max_time_ms: a timeout, in milliseconds, for the DevOps API
                HTTP request should it be necessary (see the `region` argument).

        Returns:
            A Database object ready to be used.

        Example:
            >>> my_db = my_astra_db_admin.get_database(
            ...     "https://<ID>-<REGION>.apps.astra.datastax.com",
            ...     keyspace="my_prod_keyspace",
            ... )
            >>> coll = my_db.create_collection("movies", dimension=2)
            >>> my_coll.insert_one({"title": "The Title", "$vector": [0.3, 0.4]})
        """

        _api_endpoint_p, _id_p = check_id_endpoint_parg_kwargs(
            p_arg=api_endpoint_or_id, api_endpoint=api_endpoint, id=id
        )
        keyspace_param = check_namespace_keyspace(
            keyspace=keyspace,
            namespace=namespace,
        )

        # lazy importing here to avoid circular dependency
        from astrapy import Database

        _token = coerce_token_provider(token) or self.token_provider
        _keyspace: str | None
        # handle the "endpoint passed as id" case first:
        if _api_endpoint_p is not None:
            if region is not None:
                raise ValueError(
                    "Parameter `region` not supported with an API endpoint."
                )
            if keyspace_param:
                _keyspace = keyspace_param
            else:
                parsed_api_endpoint = parse_api_endpoint(_api_endpoint_p)
                if parsed_api_endpoint is None:
                    msg = api_endpoint_parsing_error_message(_api_endpoint_p)
                    raise ValueError(msg)
                _keyspace = self.database_info(
                    parsed_api_endpoint.database_id,
                    max_time_ms=max_time_ms,
                ).info.keyspace
            return Database(
                api_endpoint=_api_endpoint_p,
                token=_token,
                keyspace=_keyspace,
                callers=self.callers,
                environment=self.environment,
                api_path=api_path,
                api_version=api_version,
            )
        else:
            # the case where an ID is passed:
            if _id_p is None:
                raise ValueError("Either `api_endpoint` or `id` must be supplied.")
            _region = normalize_region_for_id(
                database_id=_id_p,
                token_str=self.token_provider.get_token(),
                environment=self.environment,
                region_param=region,
                max_time_ms=max_time_ms,
            )
            if keyspace_param:
                _keyspace = keyspace_param
            else:
                _keyspace = self.database_info(
                    _id_p, max_time_ms=max_time_ms
                ).info.keyspace
            return Database(
                api_endpoint=build_api_endpoint(
                    environment=self.environment,
                    database_id=_id_p,
                    region=_region,
                ),
                token=_token,
                keyspace=_keyspace,
                callers=self.callers,
                environment=self.environment,
                api_path=api_path,
                api_version=api_version,
            )

    def get_async_database(
        self,
        api_endpoint_or_id: str | None = None,
        *,
        api_endpoint: str | None = None,
        token: str | TokenProvider | None = None,
        keyspace: str | None = None,
        namespace: str | None = None,
        id: str | None = None,
        region: str | None = None,
        api_path: str | None = None,
        api_version: str | None = None,
    ) -> AsyncDatabase:
        """
        Create an AsyncDatabase instance for a specific database, to be used
        when doing data-level work (such as creating/managing collections).

        Args:
            api_endpoint_or_id: positional parameter that can stand for both
                `api_endpoint` and `id`. Passing them together is an error.
            api_endpoint: the API Endpoint for the target database
                (e.g. `https://<ID>-<REGION>.apps.astra.datastax.com`).
                The database must exist already for the resulting object
                to be effectively used; in other words, this invocation
                does not create the database, just the object instance.
            token: if supplied, is passed to the Database instead of
                the one set for this object.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            keyspace: used to specify a certain keyspace the resulting
                AsyncDatabase will primarily work on. If not specified, an additional
                DevOps API call reveals the default keyspace for the target database.
            namespace: an alias for `keyspace`. *DEPRECATED*, removal in 2.0.
            id: the target database ID. This is alternative to using the API Endpoint.
            region: the region to use for connecting to the database. The
                database must be located in that region. This parameter can be used
                only if the database is specified by its ID (instead of API Endpoint).
                If this parameter is not passed, and cannot be inferred
                from the API endpoint, an additional DevOps API request is made
                to determine the default region and use it subsequently.
            api_path: path to append to the API Endpoint. In typical usage, this
                should be left to its default of "/api/json".
            api_version: version specifier to append to the API path. In typical
                usage, this should be left to its default of "v1".
            max_time_ms: a timeout, in milliseconds, for the DevOps API
                HTTP request should it be necessary (see the `region` argument).

        Returns:
            An AsyncDatabase object ready to be used.

        Example:
            >>> async def create_use_collection(
            ...     admin: AstraDBAdmin,
            ...     api_endpoint: str,
            ...     keyspace: str,
            ... ) -> None:
            ...     my_async_db = admin.get_async_database(
            ...         api_endpoint,
            ...         keyspace=keyspace,
            ...     )
            ...     a_coll = await my_async_db.create_collection("movies", dimension=2)
            ...     await a_coll.insert_one(
            ...         {"title": "The Title", "$vector": [0.3, 0.4]}
            ...     )
            ...
            >>> asyncio.run(create_use_collection(
            ...     my_admin,
            ...     "https://<ID>-<REGION>.apps.astra.datastax.com",
            ...     "default_keyspace",
            ... ))
            >>>
        """

        keyspace_param = check_namespace_keyspace(
            keyspace=keyspace,
            namespace=namespace,
        )

        return self.get_database(
            api_endpoint_or_id=api_endpoint_or_id,
            api_endpoint=api_endpoint,
            token=token,
            keyspace=keyspace_param,
            id=id,
            region=region,
            api_path=api_path,
            api_version=api_version,
        ).to_async()


class DatabaseAdmin(ABC):
    """
    An abstract class defining the interface for a database admin object.
    This supports generic keyspace crud, as well as spawning databases,
    without committing to a specific database architecture (e.g. Astra DB).
    """

    environment: str
    spawner_database: Database | AsyncDatabase

    @abstractmethod
    def list_namespaces(self, *pargs: Any, **kwargs: Any) -> list[str]:
        """Get a list of namespaces for the database."""
        ...

    @abstractmethod
    def list_keyspaces(self, *pargs: Any, **kwargs: Any) -> list[str]:
        """Get a list of keyspaces for the database."""
        ...

    @abstractmethod
    def create_namespace(
        self,
        name: str,
        *,
        update_db_keyspace: bool | None = None,
        update_db_namespace: bool | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Create a namespace in the database, returning {'ok': 1} if successful.
        """
        ...

    @abstractmethod
    def create_keyspace(
        self,
        name: str,
        *,
        update_db_keyspace: bool | None = None,
        update_db_namespace: bool | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Create a keyspace in the database, returning {'ok': 1} if successful.
        """
        ...

    @abstractmethod
    def drop_namespace(self, name: str, *pargs: Any, **kwargs: Any) -> dict[str, Any]:
        """
        Drop (delete) a namespace from the database, returning {'ok': 1} if successful.
        """
        ...

    @abstractmethod
    def drop_keyspace(self, name: str, *pargs: Any, **kwargs: Any) -> dict[str, Any]:
        """
        Drop (delete) a keyspace from the database, returning {'ok': 1} if successful.
        """
        ...

    @abstractmethod
    async def async_list_namespaces(self, *pargs: Any, **kwargs: Any) -> list[str]:
        """
        Get a list of namespaces for the database.
        (Async version of the method.)
        """
        ...

    @abstractmethod
    async def async_list_keyspaces(self, *pargs: Any, **kwargs: Any) -> list[str]:
        """
        Get a list of keyspaces for the database.
        (Async version of the method.)
        """
        ...

    @abstractmethod
    async def async_create_namespace(
        self,
        name: str,
        *,
        update_db_keyspace: bool | None = None,
        update_db_namespace: bool | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Create a namespace in the database, returning {'ok': 1} if successful.
        (Async version of the method.)
        """
        ...

    @abstractmethod
    async def async_create_keyspace(
        self,
        name: str,
        *,
        update_db_keyspace: bool | None = None,
        update_db_namespace: bool | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Create a keyspace in the database, returning {'ok': 1} if successful.
        (Async version of the method.)
        """
        ...

    @abstractmethod
    async def async_drop_namespace(
        self, name: str, *pargs: Any, **kwargs: Any
    ) -> dict[str, Any]:
        """
        Drop (delete) a namespace from the database, returning {'ok': 1} if successful.
        (Async version of the method.)
        """
        ...

    @abstractmethod
    async def async_drop_keyspace(
        self, name: str, *pargs: Any, **kwargs: Any
    ) -> dict[str, Any]:
        """
        Drop (delete) a keyspace from the database, returning {'ok': 1} if successful.
        (Async version of the method.)
        """
        ...

    @abstractmethod
    def get_database(self, *pargs: Any, **kwargs: Any) -> Database:
        """Get a Database object from this database admin."""
        ...

    @abstractmethod
    def get_async_database(self, *pargs: Any, **kwargs: Any) -> AsyncDatabase:
        """Get an AsyncDatabase object from this database admin."""
        ...

    @abstractmethod
    def find_embedding_providers(
        self, *pargs: Any, **kwargs: Any
    ) -> FindEmbeddingProvidersResult:
        """Query the Data API for the available embedding providers."""
        ...

    @abstractmethod
    async def async_find_embedding_providers(
        self, *pargs: Any, **kwargs: Any
    ) -> FindEmbeddingProvidersResult:
        """
        Query the Data API for the available embedding providers.
        (Async version of the method.)
        """
        ...


class AstraDBDatabaseAdmin(DatabaseAdmin):
    """
    An "admin" object, able to perform administrative tasks at the keyspaces level
    (i.e. within a certain database), such as creating/listing/dropping keyspaces.

    This is one layer below the AstraDBAdmin concept, in that it is tied to
    a single database and enables admin work within it. As such, it is generally
    created by a method call on an AstraDBAdmin.

    Args:
        api_endpoint: the API Endpoint for the target database
            (e.g. `https://<ID>-<REGION>.apps.astra.datastax.com`).
            The database must exist already for the resulting object
            to be effectively used; in other words, this invocation
            does not create the database, just the object instance.
        token: an access token with enough permission to perform admin tasks.
            This can be either a literal token string or a subclass of
            `astrapy.authentication.TokenProvider`.
        environment: a label, whose value is one of Environment.PROD (default),
            Environment.DEV or Environment.TEST.
        callers: a list of caller identities, i.e. applications, or frameworks,
            on behalf of which Data API and DevOps API calls are performed.
            These end up in the request user-agent.
            Each caller identity is a ("caller_name", "caller_version") pair.
        caller_name: *DEPRECATED*, use `callers`. Removal 2.0. Name of the
            application, or framework, on behalf of which the Data API and
            DevOps API calls are performed. This ends up in the request user-agent.
        caller_version: version of the caller. *DEPRECATED*, use `callers`. Removal 2.0.
        dev_ops_url: in case of custom deployments, this can be used to specify
            the URL to the DevOps API, such as "https://api.astra.datastax.com".
            Generally it can be omitted. The environment (prod/dev/...) is
            determined from the API Endpoint.
        dev_ops_api_version: this can specify a custom version of the DevOps API
            (such as "v2"). Generally not needed.
        api_path: path to append to the API Endpoint. In typical usage, this
            class is created by a method such as `Database.get_database_admin()`,
            which passes the matching value. Generally to be left to its Astra DB
            default of "/api/json".
        api_version: version specifier to append to the API path. In typical
            usage, this class is created by a method such as
            `Database.get_database_admin()`, which passes the matching value.
            Generally to be left to its Astra DB default of "/v1".
        spawner_database: either a Database or an AsyncDatabase instance. This represents
            the database class which spawns this admin object, so that, if required,
            a keyspace creation can retroactively "use" the new keyspace in the spawner.
            Used to enable the Async/Database.get_admin_database().create_keyspace() pattern.
        max_time_ms: a timeout, in milliseconds, for the DevOps API
            HTTP request should it be necessary (see the `region` argument).

    Example:
        >>> from astrapy import DataAPIClient
        >>> my_client = DataAPIClient("AstraCS:...")
        >>> admin_for_my_db = my_client.get_admin().get_database_admin(
        ...     "https://<ID>-<REGION>.apps.astra.datastax.com"
        ... )
        >>> admin_for_my_db.list_keyspaces()
        ['default_keyspace', 'staging_keyspace']
        >>> admin_for_my_db.info().status
        'ACTIVE'

    Note:
        creating an instance of AstraDBDatabaseAdmin does not trigger actual creation
        of the database itself, which should exist beforehand. To create databases,
        see the AstraDBAdmin class.
    """

    def __init__(
        self,
        api_endpoint: str,
        *,
        token: str | TokenProvider | None = None,
        environment: str | None = None,
        callers: Sequence[CallerType] = [],
        caller_name: str | None = None,
        caller_version: str | None = None,
        dev_ops_url: str | None = None,
        dev_ops_api_version: str | None = None,
        api_path: str | None = None,
        api_version: str | None = None,
        spawner_database: Database | AsyncDatabase | None = None,
        max_time_ms: int | None = None,
    ) -> None:
        # lazy import here to avoid circular dependency
        from astrapy.database import Database

        callers_param = check_caller_parameters(callers, caller_name, caller_version)
        self.token_provider = coerce_token_provider(token)
        self.environment = (environment or Environment.PROD).lower()
        self.api_endpoint = api_endpoint
        parsed_api_endpoint = parse_api_endpoint(self.api_endpoint)
        if parsed_api_endpoint is None:
            msg = api_endpoint_parsing_error_message(self.api_endpoint)
            raise ValueError(msg)

        self._database_id = parsed_api_endpoint.database_id
        self._region = parsed_api_endpoint.region
        if parsed_api_endpoint.environment != self.environment:
            raise ValueError(
                "Environment mismatch between client and provided "
                "API endpoint. You can try adding "
                f'`environment="{parsed_api_endpoint.environment}"` '
                "to the class constructor."
            )
        self.callers = callers_param
        self.api_path = (
            api_path if api_path is not None else API_PATH_ENV_MAP[self.environment]
        )
        self.api_version = (
            api_version
            if api_version is not None
            else API_VERSION_ENV_MAP[self.environment]
        )
        if spawner_database is not None:
            self.spawner_database = spawner_database
        else:
            # leaving the keyspace to its per-environment default
            # (a task for the Database)
            self.spawner_database = Database(
                api_endpoint=self.api_endpoint,
                token=self.token_provider,
                keyspace=None,
                callers=self.callers,
                environment=self.environment,
                api_path=self.api_path,
                api_version=self.api_version,
            )

        # API-commander-specific init (for the vectorizeOps invocations)
        self._commander_headers = {
            DEFAULT_DATA_API_AUTH_HEADER: self.token_provider.get_token(),
        }
        self._api_commander = self._get_api_commander()

        # DevOps-API-commander specific init (keyspace CRUD, etc)
        self.dev_ops_url = (
            dev_ops_url
            if dev_ops_url is not None
            else DEV_OPS_URL_ENV_MAP[self.environment]
        ).rstrip("/")
        self.dev_ops_api_version = (
            dev_ops_api_version
            if dev_ops_api_version is not None
            else DEV_OPS_VERSION_ENV_MAP[self.environment]
        ).strip("/")
        self._dev_ops_commander_headers: dict[str, str | None]
        if self.token_provider:
            _token = self.token_provider.get_token()
            self._dev_ops_commander_headers = {
                DEFAULT_DEV_OPS_AUTH_HEADER: f"{DEFAULT_DEV_OPS_AUTH_PREFIX}{_token}",
            }
        else:
            self._dev_ops_commander_headers = {}
        self._dev_ops_api_commander = self._get_dev_ops_api_commander()

        # this class keeps a reference to the AstraDBAdmin associated to this org:
        self._astra_db_admin = AstraDBAdmin(
            token=self.token_provider,
            environment=self.environment,
            callers=self.callers,
            dev_ops_url=self.dev_ops_url,
            dev_ops_api_version=self.dev_ops_api_version,
        )

    def __repr__(self) -> str:
        ep_desc = f'api_endpoint="{self.api_endpoint}"'
        token_desc: str | None
        if self.token_provider:
            token_desc = f'token="{redact_secret(str(self.token_provider), 15)}"'
        else:
            token_desc = None
        env_desc: str | None
        if self.environment == Environment.PROD:
            env_desc = None
        else:
            env_desc = f'environment="{self.environment}"'
        parts = [pt for pt in [ep_desc, token_desc, env_desc] if pt is not None]
        return f"{self.__class__.__name__}({', '.join(parts)})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, AstraDBDatabaseAdmin):
            return all(
                [
                    self.token_provider == other.token_provider,
                    self.environment == other.environment,
                    self.api_endpoint == other.api_endpoint,
                    self.callers == other.callers,
                    self.api_path == other.api_path,
                    self.api_version == other.api_version,
                    self.spawner_database == other.spawner_database,
                    self.dev_ops_url == other.dev_ops_url,
                    self.dev_ops_api_version == other.dev_ops_api_version,
                ]
            )
        else:
            return False

    def _get_api_commander(self) -> APICommander:
        """Instantiate a new APICommander for Data API calls."""
        base_path = "/".join(comp for comp in [self.api_path, self.api_version] if comp)
        api_commander = APICommander(
            api_endpoint=self.api_endpoint,
            path=base_path,
            headers=self._commander_headers,
            callers=self.callers,
        )
        return api_commander

    def _get_dev_ops_api_commander(self) -> APICommander:
        """Instantiate a new APICommander for DevOps calls."""

        dev_ops_base_path = "/".join(
            [
                self.dev_ops_api_version,
                "databases",
                self._database_id,
            ]
        )
        dev_ops_commander = APICommander(
            api_endpoint=self.dev_ops_url,
            path=dev_ops_base_path,
            headers=self._dev_ops_commander_headers,
            callers=self.callers,
            dev_ops_api=True,
        )
        return dev_ops_commander

    def _copy(
        self,
        api_endpoint: str | None = None,
        *,
        token: str | TokenProvider | None = None,
        environment: str | None = None,
        callers: Sequence[CallerType] = [],
        caller_name: str | None = None,
        caller_version: str | None = None,
        dev_ops_url: str | None = None,
        dev_ops_api_version: str | None = None,
        api_path: str | None = None,
        api_version: str | None = None,
    ) -> AstraDBDatabaseAdmin:
        callers_param = check_caller_parameters(callers, caller_name, caller_version)
        return AstraDBDatabaseAdmin(
            api_endpoint=api_endpoint or self.api_endpoint,
            token=coerce_token_provider(token) or self.token_provider,
            environment=environment or self.environment,
            callers=callers_param or self.callers,
            dev_ops_url=dev_ops_url or self.dev_ops_url,
            dev_ops_api_version=dev_ops_api_version or self.dev_ops_api_version,
            api_path=api_path or self.api_path,
            api_version=api_version or self.api_version,
        )

    def with_options(
        self,
        api_endpoint: str | None = None,
        *,
        token: str | TokenProvider | None = None,
        callers: Sequence[CallerType] = [],
        caller_name: str | None = None,
        caller_version: str | None = None,
    ) -> AstraDBDatabaseAdmin:
        """
        Create a clone of this AstraDBDatabaseAdmin with some changed attributes.

        Args:
            api_endpoint: the API Endpoint for the target database
                (e.g. `https://<ID>-<REGION>.apps.astra.datastax.com`).
                The database must exist already for the resulting object
                to be effectively used; in other words, this invocation
                does not create the database, just the object instance.
            token: an Access Token to the database. Example: `"AstraCS:xyz..."`.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            callers: a list of caller identities, i.e. applications, or frameworks,
                on behalf of which Data API and DevOps API calls are performed.
                These end up in the request user-agent.
                Each caller identity is a ("caller_name", "caller_version") pair.
            caller_name: *DEPRECATED*, use `callers`. Removal 2.0. Name of the
                application, or framework, on behalf of which the Data API and
                DevOps API calls are performed. This ends up in the request user-agent.
            caller_version: version of the caller. *DEPRECATED*, use `callers`.
                Removal 2.0.

        Returns:
            a new AstraDBDatabaseAdmin instance.

        Example:
            >>> admin_for_my_other_db = admin_for_my_db.with_options(
            ...     "https://<ID>-<REGION>.apps.astra.datastax.com",
            ... )
        """

        callers_param = check_caller_parameters(callers, caller_name, caller_version)
        return self._copy(
            api_endpoint=api_endpoint,
            token=token,
            callers=callers_param,
        )

    @deprecation.deprecated(  # type: ignore[misc]
        deprecated_in="1.5.1",
        removed_in="2.0.0",
        current_version=__version__,
        details=SET_CALLER_DEPRECATION_NOTICE,
    )
    def set_caller(
        self,
        caller_name: str | None = None,
        caller_version: str | None = None,
    ) -> None:
        """
        Set a new identity for the application/framework on behalf of which
        the DevOps API calls will be performed (the "caller").

        New objects spawned from this client afterwards will inherit the new settings.

        Args:
            caller_name: name of the application, or framework, on behalf of which
                the DevOps API calls are performed. This ends up in the request user-agent.
            caller_version: version of the caller.

        Example:
            >>> admin_for_my_db.set_caller(
            ...     caller_name="the_caller",
            ...     caller_version="0.1.0",
            ... )
        """

        logger.info(f"setting caller to {caller_name}/{caller_version}")
        callers_param = check_caller_parameters([], caller_name, caller_version)
        self.callers = callers_param or self.callers
        self._api_commander = self._get_api_commander()
        self._dev_ops_api_commander = self._get_dev_ops_api_commander()

    @property
    def id(self) -> str:
        """
        The ID of this database admin.

        Example:
            >>> my_db_admin.id
            '01234567-89ab-cdef-0123-456789abcdef'
        """
        return self._database_id

    @property
    def region(self) -> str:
        """
        The region for this database admin.

        Example:
            >>> my_db_admin.region
            'us-east-1'
        """
        return self._region

    @staticmethod
    def from_astra_db_admin(
        api_endpoint: str,
        *,
        astra_db_admin: AstraDBAdmin,
        max_time_ms: int | None = None,
    ) -> AstraDBDatabaseAdmin:
        """
        Create an AstraDBDatabaseAdmin from an AstraDBAdmin and an API Endpoint.

        Args:
            api_endpoint: the API Endpoint for the target database
                (e.g. `https://<ID>-<REGION>.apps.astra.datastax.com`).
                The database must exist already for the resulting object
                to be effectively used; in other words, this invocation
                does not create the database, just the object instance.
            astra_db_admin: an AstraDBAdmin object that has visibility over
                the target database.
            max_time_ms: a timeout, in milliseconds, for the DevOps API
                HTTP request should it be necessary (see the `region` argument).

        Returns:
            An AstraDBDatabaseAdmin object, for admin work within the database.

        Example:
            >>> from astrapy import DataAPIClient, AstraDBDatabaseAdmin
            >>> admin_for_my_db = AstraDBDatabaseAdmin.from_astra_db_admin(
            ...     "https://<ID>-<REGION>.apps.astra.datastax.com",
            ...     astra_db_admin=DataAPIClient("AstraCS:...").get_admin(),
            ... )
            >>> admin_for_my_db.list_keyspaces()
            ['default_keyspace', 'staging_keyspace']
            >>> admin_for_my_db.info().status
            'ACTIVE'

        Note:
            Creating an instance of AstraDBDatabaseAdmin does not trigger actual creation
            of the database itself, which should exist beforehand. To create databases,
            see the AstraDBAdmin class.
        """

        return AstraDBDatabaseAdmin(
            api_endpoint=api_endpoint,
            token=astra_db_admin.token_provider,
            environment=astra_db_admin.environment,
            callers=astra_db_admin.callers,
            dev_ops_url=astra_db_admin._dev_ops_url,
            dev_ops_api_version=astra_db_admin._dev_ops_api_version,
            max_time_ms=max_time_ms,
        )

    @staticmethod
    def from_api_endpoint(
        api_endpoint: str,
        *,
        token: str | TokenProvider | None = None,
        callers: Sequence[CallerType] = [],
        caller_name: str | None = None,
        caller_version: str | None = None,
        dev_ops_url: str | None = None,
        dev_ops_api_version: str | None = None,
    ) -> AstraDBDatabaseAdmin:
        """
        Create an AstraDBDatabaseAdmin from an API Endpoint and optionally a token.

        Args:
            api_endpoint: the API Endpoint for the target database
                (e.g. `https://<ID>-<REGION>.apps.astra.datastax.com`).
                The database must exist already for the resulting object
                to be effectively used; in other words, this invocation
                does not create the database, just the object instance.
            token: an access token with enough permissions to do admin work.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            callers: a list of caller identities, i.e. applications, or frameworks,
                on behalf of which Data API and DevOps API calls are performed.
                These end up in the request user-agent.
                Each caller identity is a ("caller_name", "caller_version") pair.
            caller_name: *DEPRECATED*, use `callers`. Removal 2.0. Name of the
                application, or framework, on behalf of which the Data API and
                DevOps API calls are performed. This ends up in the request user-agent.
            caller_version: version of the caller. *DEPRECATED*, use `callers`.
                Removal 2.0.
            dev_ops_url: in case of custom deployments, this can be used to specify
                the URL to the DevOps API, such as "https://api.astra.datastax.com".
                Generally it can be omitted. The environment (prod/dev/...) is
                determined from the API Endpoint.
            dev_ops_api_version: this can specify a custom version of the DevOps API
                (such as "v2"). Generally not needed.

        Returns:
            An AstraDBDatabaseAdmin object, for admin work within the database.

        Example:
            >>> from astrapy import AstraDBDatabaseAdmin
            >>> admin_for_my_db = AstraDBDatabaseAdmin.from_api_endpoint(
            ...     api_endpoint="https://01234567-....apps.astra.datastax.com",
            ...     token="AstraCS:...",
            ... )
            >>> admin_for_my_db.list_keyspaces()
            ['default_keyspace', 'another_keyspace']
            >>> admin_for_my_db.info().status
            'ACTIVE'

        Note:
            Creating an instance of AstraDBDatabaseAdmin does not trigger actual creation
            of the database itself, which should exist beforehand. To create databases,
            see the AstraDBAdmin class.
        """

        callers_param = check_caller_parameters(callers, caller_name, caller_version)
        parsed_api_endpoint = parse_api_endpoint(api_endpoint)
        if parsed_api_endpoint:
            return AstraDBDatabaseAdmin(
                api_endpoint=api_endpoint,
                token=token,
                environment=parsed_api_endpoint.environment,
                callers=callers_param,
                dev_ops_url=dev_ops_url,
                dev_ops_api_version=dev_ops_api_version,
            )
        else:
            msg = api_endpoint_parsing_error_message(api_endpoint)
            raise ValueError(msg)

    def info(self, *, max_time_ms: int | None = None) -> AdminDatabaseInfo:
        """
        Query the DevOps API for the full info on this database.

        Args:
            max_time_ms: a timeout, in milliseconds, for the DevOps API request.

        Returns:
            An AdminDatabaseInfo object.

        Example:
            >>> my_db_info = admin_for_my_db.info()
            >>> my_db_info.status
            'ACTIVE'
            >>> my_db_info.info.region
            'us-east1'
        """

        logger.info(f"getting info ('{self._database_id}')")
        req_response = self._astra_db_admin.database_info(
            id=self._database_id,
            max_time_ms=max_time_ms,
        )
        logger.info(f"finished getting info ('{self._database_id}')")
        return req_response

    async def async_info(self, *, max_time_ms: int | None = None) -> AdminDatabaseInfo:
        """
        Query the DevOps API for the full info on this database.
        Async version of the method, for use in an asyncio context.

        Args:
            max_time_ms: a timeout, in milliseconds, for the DevOps API request.

        Returns:
            An AdminDatabaseInfo object.

        Example:
            >>> async def wait_until_active(db_admin: AstraDBDatabaseAdmin) -> None:
            ...     while True:
            ...         info = await db_admin.async_info()
            ...         if info.status == "ACTIVE":
            ...             return
            ...
            >>> asyncio.run(wait_until_active(admin_for_my_db))
        """

        logger.info(f"getting info ('{self._database_id}'), async")
        req_response = await self._astra_db_admin.async_database_info(
            id=self._database_id,
            max_time_ms=max_time_ms,
        )
        logger.info(f"finished getting info ('{self._database_id}'), async")
        return req_response

    @deprecation.deprecated(  # type: ignore[misc]
        deprecated_in="1.5.0",
        removed_in="2.0.0",
        current_version=__version__,
        details=NAMESPACE_DEPRECATION_NOTICE_METHOD,
    )
    def list_namespaces(self, *, max_time_ms: int | None = None) -> list[str]:
        """
        Query the DevOps API for a list of the namespaces in the database.

        *DEPRECATED* (removal in 2.0). Switch to the "list_keyspaces" method.**

        Args:
            max_time_ms: a timeout, in milliseconds, for the DevOps API request.

        Returns:
            A list of the namespaces, each a string, in no particular order.

        Example:
            >>> admin_for_my_db.list_namespaces()
            ['default_keyspace', 'staging_namespace']
        """

        return self.list_keyspaces(max_time_ms=max_time_ms)

    def list_keyspaces(self, *, max_time_ms: int | None = None) -> list[str]:
        """
        Query the DevOps API for a list of the keyspaces in the database.

        Args:
            max_time_ms: a timeout, in milliseconds, for the DevOps API request.

        Returns:
            A list of the keyspaces, each a string, in no particular order.

        Example:
            >>> admin_for_my_db.list_keyspaces()
            ['default_keyspace', 'staging_keyspace']
        """

        logger.info(f"getting keyspaces ('{self._database_id}')")
        info = self.info(max_time_ms=max_time_ms)
        logger.info(f"finished getting keyspaces ('{self._database_id}')")
        if info.raw_info is None:
            raise DevOpsAPIException("Could not get the keyspace list.")
        else:
            return info.raw_info.get("info", {}).get("keyspaces") or []

    @deprecation.deprecated(  # type: ignore[misc]
        deprecated_in="1.5.0",
        removed_in="2.0.0",
        current_version=__version__,
        details=NAMESPACE_DEPRECATION_NOTICE_METHOD,
    )
    async def async_list_namespaces(
        self, *, max_time_ms: int | None = None
    ) -> list[str]:
        """
        Query the DevOps API for a list of the namespaces in the database.
        Async version of the method, for use in an asyncio context.

        *DEPRECATED* (removal in 2.0). Switch to the "keyspace" property.**

        Args:
            max_time_ms: a timeout, in milliseconds, for the DevOps API request.

        Returns:
            A list of the namespaces, each a string, in no particular order.

        Example:
            >>> async def check_if_ns_exists(
            ...     db_admin: AstraDBDatabaseAdmin, namespace: str
            ... ) -> bool:
            ...     ns_list = await db_admin.async_list_namespaces()
            ...     return namespace in ns_list
            ...
            >>> asyncio.run(check_if_ns_exists(admin_for_my_db, "dragons"))
            False
            >>> asyncio.run(check_if_db_exists(admin_for_my_db, "app_namespace"))
            True
        """

        return await self.async_list_keyspaces(max_time_ms=max_time_ms)

    async def async_list_keyspaces(
        self, *, max_time_ms: int | None = None
    ) -> list[str]:
        """
        Query the DevOps API for a list of the keyspaces in the database.
        Async version of the method, for use in an asyncio context.

        Args:
            max_time_ms: a timeout, in milliseconds, for the DevOps API request.

        Returns:
            A list of the keyspaces, each a string, in no particular order.

        Example:
            >>> async def check_if_ks_exists(
            ...     db_admin: AstraDBDatabaseAdmin, keyspace: str
            ... ) -> bool:
            ...     ks_list = await db_admin.async_list_keyspaces()
            ...     return keyspace in ks_list
            ...
            >>> asyncio.run(check_if_ks_exists(admin_for_my_db, "dragons"))
            False
            >>> asyncio.run(check_if_db_exists(admin_for_my_db, "app_keyspace"))
            True
        """

        logger.info(f"getting keyspaces ('{self._database_id}'), async")
        info = await self.async_info(max_time_ms=max_time_ms)
        logger.info(f"finished getting keyspaces ('{self._database_id}'), async")
        if info.raw_info is None:
            raise DevOpsAPIException("Could not get the keyspace list.")
        else:
            return info.raw_info.get("info", {}).get("keyspaces") or []

    @deprecation.deprecated(  # type: ignore[misc]
        deprecated_in="1.5.0",
        removed_in="2.0.0",
        current_version=__version__,
        details=NAMESPACE_DEPRECATION_NOTICE_METHOD,
    )
    def create_namespace(
        self,
        name: str,
        *,
        wait_until_active: bool = True,
        update_db_keyspace: bool | None = None,
        update_db_namespace: bool | None = None,
        max_time_ms: int | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Create a namespace in this database as requested,
        optionally waiting for it to be ready.

        *DEPRECATED* (removal in 2.0). Switch to the "keyspace" property.**

        Args:
            name: the namespace name. If supplying a namespace that exists
                already, the method call proceeds as usual, no errors are
                raised, and the whole invocation is a no-op.
            wait_until_active: if True (default), the method returns only after
                the target database is in ACTIVE state again (a few
                seconds, usually). If False, it will return right after issuing the
                creation request to the DevOps API, and it will be responsibility
                of the caller to check the database status/namespace availability
                before working with it.
            update_db_keyspace: if True, the `Database` or `AsyncDatabase` class
                that spawned this DatabaseAdmin, if any, gets updated to work on
                the newly-created keyspace starting when this method returns.
            update_db_namespace: an alias for update_db_keyspace.
                *DEPRECATED* as of v1.5.0, removal in v2.0.0.
            max_time_ms: a timeout, in milliseconds, for the whole requested
                operation to complete.
                Note that a timeout is no guarantee that the creation request
                has not reached the API server.

        Returns:
            A dictionary of the form {"ok": 1} in case of success.
            Otherwise, an exception is raised.

        Example:
            >>> my_db_admin.list_namespaces()
            ['default_keyspace']
            >>> my_db_admin.create_namespace("that_other_one")
            {'ok': 1}
            >>> my_db_admin.list_namespaces()
            ['default_keyspace', 'that_other_one']
        """

        return self.create_keyspace(
            name=name,
            wait_until_active=wait_until_active,
            update_db_keyspace=update_db_keyspace,
            update_db_namespace=update_db_namespace,
            max_time_ms=max_time_ms,
            **kwargs,
        )

    def create_keyspace(
        self,
        name: str,
        *,
        wait_until_active: bool = True,
        update_db_keyspace: bool | None = None,
        update_db_namespace: bool | None = None,
        max_time_ms: int | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Create a keyspace in this database as requested,
        optionally waiting for it to be ready.

        Args:
            name: the keyspace name. If supplying a keyspace that exists
                already, the method call proceeds as usual, no errors are
                raised, and the whole invocation is a no-op.
            wait_until_active: if True (default), the method returns only after
                the target database is in ACTIVE state again (a few
                seconds, usually). If False, it will return right after issuing the
                creation request to the DevOps API, and it will be responsibility
                of the caller to check the database status/keyspace availability
                before working with it.
            update_db_keyspace: if True, the `Database` or `AsyncDatabase` class
                that spawned this DatabaseAdmin, if any, gets updated to work on
                the newly-created keyspace starting when this method returns.
            update_db_namespace: an alias for update_db_keyspace.
                *DEPRECATED* as of v1.5.0, removal in v2.0.0.
            max_time_ms: a timeout, in milliseconds, for the whole requested
                operation to complete.
                Note that a timeout is no guarantee that the creation request
                has not reached the API server.

        Returns:
            A dictionary of the form {"ok": 1} in case of success.
            Otherwise, an exception is raised.

        Example:
            >>> my_db_admin.keyspaces()
            ['default_keyspace']
            >>> my_db_admin.create_keyspace("that_other_one")
            {'ok': 1}
            >>> my_db_admin.list_keyspaces()
            ['default_keyspace', 'that_other_one']
        """

        _update_db_keyspace = check_update_db_namespace_keyspace(
            update_db_keyspace=update_db_keyspace,
            update_db_namespace=update_db_namespace,
        )

        timeout_manager = MultiCallTimeoutManager(
            overall_max_time_ms=max_time_ms, dev_ops_api=True
        )
        logger.info(
            f"creating keyspace '{name}' on " f"'{self._database_id}' (DevOps API)"
        )
        cn_raw_response = self._dev_ops_api_commander.raw_request(
            http_method=HttpMethod.POST,
            additional_path=f"keyspaces/{name}",
            timeout_info=timeout_manager.remaining_timeout_info(),
        )
        if cn_raw_response.status_code != DEV_OPS_RESPONSE_HTTP_CREATED:
            raise DevOpsAPIException(
                f"keyspace creation ('{name}') failed: API returned HTTP "
                f"{cn_raw_response.status_code} instead of "
                f"{DEV_OPS_RESPONSE_HTTP_CREATED} - Created."
            )
        logger.info(
            "DevOps API returned from creating keyspace "
            f"'{name}' on '{self._database_id}'"
        )
        if wait_until_active:
            last_status_seen = DEV_OPS_DATABASE_STATUS_MAINTENANCE
            while last_status_seen == DEV_OPS_DATABASE_STATUS_MAINTENANCE:
                logger.info(f"sleeping to poll for status of '{self._database_id}'")
                time.sleep(DEV_OPS_KEYSPACE_POLL_INTERVAL_S)
                last_status_seen = self.info(
                    max_time_ms=timeout_manager.remaining_timeout_ms(),
                ).status
            if last_status_seen != DEV_OPS_DATABASE_STATUS_ACTIVE:
                raise DevOpsAPIException(
                    f"Database entered unexpected status {last_status_seen} after MAINTENANCE."
                )
            # is the keyspace found?
            if name not in self.list_keyspaces():
                raise DevOpsAPIException("Could not create the keyspace.")
        logger.info(
            f"finished creating keyspace '{name}' on "
            f"'{self._database_id}' (DevOps API)"
        )
        if _update_db_keyspace:
            self.spawner_database.use_keyspace(name)
        return {"ok": 1}

    @deprecation.deprecated(  # type: ignore[misc]
        deprecated_in="1.5.0",
        removed_in="2.0.0",
        current_version=__version__,
        details=NAMESPACE_DEPRECATION_NOTICE_METHOD,
    )
    async def async_create_namespace(
        self,
        name: str,
        *,
        wait_until_active: bool = True,
        update_db_keyspace: bool | None = None,
        update_db_namespace: bool | None = None,
        max_time_ms: int | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Create a namespace in this database as requested,
        optionally waiting for it to be ready.
        Async version of the method, for use in an asyncio context.

        *DEPRECATED* (removal in 2.0). Switch to the "async_create_keyspace" method.**

        Args:
            name: the namespace name. If supplying a namespace that exists
                already, the method call proceeds as usual, no errors are
                raised, and the whole invocation is a no-op.
            wait_until_active: if True (default), the method returns only after
                the target database is in ACTIVE state again (a few
                seconds, usually). If False, it will return right after issuing the
                creation request to the DevOps API, and it will be responsibility
                of the caller to check the database status/namespace availability
                before working with it.
            update_db_keyspace: if True, the `Database` or `AsyncDatabase` class
                that spawned this DatabaseAdmin, if any, gets updated to work on
                the newly-created keyspace starting when this method returns.
            update_db_namespace: an alias for update_db_keyspace.
                *DEPRECATED* as of v1.5.0, removal in v2.0.0.
            max_time_ms: a timeout, in milliseconds, for the whole requested
                operation to complete.
                Note that a timeout is no guarantee that the creation request
                has not reached the API server.

        Returns:
            A dictionary of the form {"ok": 1} in case of success.
            Otherwise, an exception is raised.

        Example:
            >>> asyncio.run(
            ...     my_db_admin.async_create_namespace("app_namespace")
            ... )
            {'ok': 1}
        """

        return await self.async_create_keyspace(
            name=name,
            wait_until_active=wait_until_active,
            update_db_keyspace=update_db_keyspace,
            update_db_namespace=update_db_namespace,
            max_time_ms=max_time_ms,
            **kwargs,
        )

    async def async_create_keyspace(
        self,
        name: str,
        *,
        wait_until_active: bool = True,
        update_db_keyspace: bool | None = None,
        update_db_namespace: bool | None = None,
        max_time_ms: int | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Create a keyspace in this database as requested,
        optionally waiting for it to be ready.
        Async version of the method, for use in an asyncio context.

        Args:
            name: the keyspace name. If supplying a keyspace that exists
                already, the method call proceeds as usual, no errors are
                raised, and the whole invocation is a no-op.
            wait_until_active: if True (default), the method returns only after
                the target database is in ACTIVE state again (a few
                seconds, usually). If False, it will return right after issuing the
                creation request to the DevOps API, and it will be responsibility
                of the caller to check the database status/keyspace availability
                before working with it.
            update_db_keyspace: if True, the `Database` or `AsyncDatabase` class
                that spawned this DatabaseAdmin, if any, gets updated to work on
                the newly-created keyspace starting when this method returns.
            update_db_namespace: an alias for update_db_keyspace.
                *DEPRECATED* as of v1.5.0, removal in v2.0.0.
            max_time_ms: a timeout, in milliseconds, for the whole requested
                operation to complete.
                Note that a timeout is no guarantee that the creation request
                has not reached the API server.

        Returns:
            A dictionary of the form {"ok": 1} in case of success.
            Otherwise, an exception is raised.

        Example:
            >>> asyncio.run(
            ...     my_db_admin.async_create_keyspace("app_keyspace")
            ... )
            {'ok': 1}
        """

        _update_db_keyspace = check_update_db_namespace_keyspace(
            update_db_keyspace=update_db_keyspace,
            update_db_namespace=update_db_namespace,
        )

        timeout_manager = MultiCallTimeoutManager(
            overall_max_time_ms=max_time_ms, dev_ops_api=True
        )
        logger.info(
            f"creating keyspace '{name}' on "
            f"'{self._database_id}' (DevOps API), async"
        )
        cn_raw_response = await self._dev_ops_api_commander.async_raw_request(
            http_method=HttpMethod.POST,
            additional_path=f"keyspaces/{name}",
            timeout_info=timeout_manager.remaining_timeout_info(),
        )
        if cn_raw_response.status_code != DEV_OPS_RESPONSE_HTTP_CREATED:
            raise DevOpsAPIException(
                f"keyspace creation ('{name}') failed: API returned HTTP "
                f"{cn_raw_response.status_code} instead of "
                f"{DEV_OPS_RESPONSE_HTTP_CREATED} - Created."
            )
        logger.info(
            f"DevOps API returned from creating keyspace "
            f"'{name}' on '{self._database_id}', async"
        )
        if wait_until_active:
            last_status_seen = DEV_OPS_DATABASE_STATUS_MAINTENANCE
            while last_status_seen == DEV_OPS_DATABASE_STATUS_MAINTENANCE:
                logger.info(
                    f"sleeping to poll for status of '{self._database_id}', async"
                )
                await asyncio.sleep(DEV_OPS_KEYSPACE_POLL_INTERVAL_S)
                last_db_info = await self.async_info(
                    max_time_ms=timeout_manager.remaining_timeout_ms(),
                )
                last_status_seen = last_db_info.status
            if last_status_seen != DEV_OPS_DATABASE_STATUS_ACTIVE:
                raise DevOpsAPIException(
                    f"Database entered unexpected status {last_status_seen} after MAINTENANCE."
                )
            # is the keyspace found?
            if name not in await self.async_list_keyspaces():
                raise DevOpsAPIException("Could not create the keyspace.")
        logger.info(
            f"finished creating keyspace '{name}' on "
            f"'{self._database_id}' (DevOps API), async"
        )
        if _update_db_keyspace:
            self.spawner_database.use_keyspace(name)
        return {"ok": 1}

    @deprecation.deprecated(  # type: ignore[misc]
        deprecated_in="1.5.0",
        removed_in="2.0.0",
        current_version=__version__,
        details=NAMESPACE_DEPRECATION_NOTICE_METHOD,
    )
    def drop_namespace(
        self,
        name: str,
        *,
        wait_until_active: bool = True,
        max_time_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        Delete a namespace from the database, optionally waiting for the database
        to become active again.

        *DEPRECATED* (removal in 2.0). Switch to the "drop_keyspace" method.**

        Args:
            name: the namespace to delete. If it does not exist in this database,
                an error is raised.
            wait_until_active: if True (default), the method returns only after
                the target database is in ACTIVE state again (a few
                seconds, usually). If False, it will return right after issuing the
                deletion request to the DevOps API, and it will be responsibility
                of the caller to check the database status/namespace availability
                before working with it.
            max_time_ms: a timeout, in milliseconds, for the whole requested
                operation to complete.
                Note that a timeout is no guarantee that the deletion request
                has not reached the API server.

        Returns:
            A dictionary of the form {"ok": 1} in case of success.
            Otherwise, an exception is raised.

        Example:
            >>> my_db_admin.list_namespaces()
            ['default_keyspace', 'that_other_one']
            >>> my_db_admin.drop_namespace("that_other_one")
            {'ok': 1}
            >>> my_db_admin.list_namespaces()
            ['default_keyspace']
        """

        return self.drop_keyspace(
            name=name,
            wait_until_active=wait_until_active,
            max_time_ms=max_time_ms,
        )

    def drop_keyspace(
        self,
        name: str,
        *,
        wait_until_active: bool = True,
        max_time_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        Delete a keyspace from the database, optionally waiting for the database
        to become active again.

        Args:
            name: the keyspace to delete. If it does not exist in this database,
                an error is raised.
            wait_until_active: if True (default), the method returns only after
                the target database is in ACTIVE state again (a few
                seconds, usually). If False, it will return right after issuing the
                deletion request to the DevOps API, and it will be responsibility
                of the caller to check the database status/keyspace availability
                before working with it.
            max_time_ms: a timeout, in milliseconds, for the whole requested
                operation to complete.
                Note that a timeout is no guarantee that the deletion request
                has not reached the API server.

        Returns:
            A dictionary of the form {"ok": 1} in case of success.
            Otherwise, an exception is raised.

        Example:
            >>> my_db_admin.list_keyspaces()
            ['default_keyspace', 'that_other_one']
            >>> my_db_admin.drop_keyspace("that_other_one")
            {'ok': 1}
            >>> my_db_admin.list_keyspaces()
            ['default_keyspace']
        """

        timeout_manager = MultiCallTimeoutManager(
            overall_max_time_ms=max_time_ms, dev_ops_api=True
        )
        logger.info(
            f"dropping keyspace '{name}' on " f"'{self._database_id}' (DevOps API)"
        )
        dk_raw_response = self._dev_ops_api_commander.raw_request(
            http_method=HttpMethod.DELETE,
            additional_path=f"keyspaces/{name}",
            timeout_info=timeout_manager.remaining_timeout_info(),
        )
        if dk_raw_response.status_code != DEV_OPS_RESPONSE_HTTP_ACCEPTED:
            raise DevOpsAPIException(
                f"keyspace deletion ('{id}') failed: API returned HTTP "
                f"{dk_raw_response.status_code} instead of "
                f"{DEV_OPS_RESPONSE_HTTP_ACCEPTED} - Created"
            )
        logger.info(
            "DevOps API returned from dropping keyspace "
            f"'{name}' on '{self._database_id}'"
        )
        if wait_until_active:
            last_status_seen = DEV_OPS_DATABASE_STATUS_MAINTENANCE
            while last_status_seen == DEV_OPS_DATABASE_STATUS_MAINTENANCE:
                logger.info(f"sleeping to poll for status of '{self._database_id}'")
                time.sleep(DEV_OPS_KEYSPACE_POLL_INTERVAL_S)
                last_status_seen = self.info(
                    max_time_ms=timeout_manager.remaining_timeout_ms(),
                ).status
            if last_status_seen != DEV_OPS_DATABASE_STATUS_ACTIVE:
                raise DevOpsAPIException(
                    f"Database entered unexpected status {last_status_seen} after MAINTENANCE."
                )
            # is the keyspace found?
            if name in self.list_keyspaces():
                raise DevOpsAPIException("Could not drop the keyspace.")
        logger.info(
            f"finished dropping keyspace '{name}' on "
            f"'{self._database_id}' (DevOps API)"
        )
        return {"ok": 1}

    @deprecation.deprecated(  # type: ignore[misc]
        deprecated_in="1.5.0",
        removed_in="2.0.0",
        current_version=__version__,
        details=NAMESPACE_DEPRECATION_NOTICE_METHOD,
    )
    async def async_drop_namespace(
        self,
        name: str,
        *,
        wait_until_active: bool = True,
        max_time_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        Delete a namespace from the database, optionally waiting for the database
        to become active again.
        Async version of the method, for use in an asyncio context.

        *DEPRECATED* (removal in 2.0). Switch to the "async_drop_namespace" method.**

        Args:
            name: the namespace to delete. If it does not exist in this database,
                an error is raised.
            wait_until_active: if True (default), the method returns only after
                the target database is in ACTIVE state again (a few
                seconds, usually). If False, it will return right after issuing the
                deletion request to the DevOps API, and it will be responsibility
                of the caller to check the database status/namespace availability
                before working with it.
            max_time_ms: a timeout, in milliseconds, for the whole requested
                operation to complete.
                Note that a timeout is no guarantee that the deletion request
                has not reached the API server.

        Returns:
            A dictionary of the form {"ok": 1} in case of success.
            Otherwise, an exception is raised.

        Example:
            >>> asyncio.run(
            ...     my_db_admin.async_drop_namespace("app_namespace")
            ... )
            {'ok': 1}
        """

        return await self.async_drop_keyspace(
            name=name,
            wait_until_active=wait_until_active,
            max_time_ms=max_time_ms,
        )

    async def async_drop_keyspace(
        self,
        name: str,
        *,
        wait_until_active: bool = True,
        max_time_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        Delete a keyspace from the database, optionally waiting for the database
        to become active again.
        Async version of the method, for use in an asyncio context.

        Args:
            name: the keyspace to delete. If it does not exist in this database,
                an error is raised.
            wait_until_active: if True (default), the method returns only after
                the target database is in ACTIVE state again (a few
                seconds, usually). If False, it will return right after issuing the
                deletion request to the DevOps API, and it will be responsibility
                of the caller to check the database status/keyspace availability
                before working with it.
            max_time_ms: a timeout, in milliseconds, for the whole requested
                operation to complete.
                Note that a timeout is no guarantee that the deletion request
                has not reached the API server.

        Returns:
            A dictionary of the form {"ok": 1} in case of success.
            Otherwise, an exception is raised.

        Example:
            >>> asyncio.run(
            ...     my_db_admin.async_drop_keyspace("app_keyspace")
            ... )
            {'ok': 1}
        """

        timeout_manager = MultiCallTimeoutManager(
            overall_max_time_ms=max_time_ms, dev_ops_api=True
        )
        logger.info(
            f"dropping keyspace '{name}' on "
            f"'{self._database_id}' (DevOps API), async"
        )
        dk_raw_response = await self._dev_ops_api_commander.async_raw_request(
            http_method=HttpMethod.DELETE,
            additional_path=f"keyspaces/{name}",
            timeout_info=timeout_manager.remaining_timeout_info(),
        )
        if dk_raw_response.status_code != DEV_OPS_RESPONSE_HTTP_ACCEPTED:
            raise DevOpsAPIException(
                f"keyspace deletion ('{id}') failed: API returned HTTP "
                f"{dk_raw_response.status_code} instead of "
                f"{DEV_OPS_RESPONSE_HTTP_ACCEPTED} - Created"
            )
        logger.info(
            f"DevOps API returned from dropping keyspace "
            f"'{name}' on '{self._database_id}', async"
        )
        if wait_until_active:
            last_status_seen = DEV_OPS_DATABASE_STATUS_MAINTENANCE
            while last_status_seen == DEV_OPS_DATABASE_STATUS_MAINTENANCE:
                logger.info(
                    f"sleeping to poll for status of '{self._database_id}', async"
                )
                await asyncio.sleep(DEV_OPS_KEYSPACE_POLL_INTERVAL_S)
                last_db_info = await self.async_info(
                    max_time_ms=timeout_manager.remaining_timeout_ms(),
                )
                last_status_seen = last_db_info.status
            if last_status_seen != DEV_OPS_DATABASE_STATUS_ACTIVE:
                raise DevOpsAPIException(
                    f"Database entered unexpected status {last_status_seen} after MAINTENANCE."
                )
            # is the keyspace found?
            if name in await self.async_list_keyspaces():
                raise DevOpsAPIException("Could not drop the keyspace.")
        logger.info(
            f"finished dropping keyspace '{name}' on "
            f"'{self._database_id}' (DevOps API), async"
        )
        return {"ok": 1}

    def drop(
        self,
        *,
        wait_until_active: bool = True,
        max_time_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        Drop this database, i.e. delete it completely and permanently with all its data.

        This method wraps the `drop_database` method of the AstraDBAdmin class,
        where more information may be found.

        Args:
            wait_until_active: if True (default), the method returns only after
                the database has actually been deleted (generally a few minutes).
                If False, it will return right after issuing the
                drop request to the DevOps API, and it will be responsibility
                of the caller to check the database status/availability
                after that, if desired.
            max_time_ms: a timeout, in milliseconds, for the whole requested
                operation to complete.
                Note that a timeout is no guarantee that the deletion request
                has not reached the API server.

        Returns:
            A dictionary of the form {"ok": 1} in case of success.
            Otherwise, an exception is raised.

        Example:
            >>> my_db_admin.list_keyspaces()
            ['default_keyspace', 'that_other_one']
            >>> my_db_admin.drop()
            {'ok': 1}
            >>> my_db_admin.list_keyspaces()  # raises a 404 Not Found http error

        Note:
            Once the method succeeds, methods on this object -- such as `info()`,
            or `list_keyspaces()` -- can still be invoked: however, this hardly
            makes sense as the underlying actual database is no more.
            It is responsibility of the developer to design a correct flow
            which avoids using a deceased database any further.
        """

        logger.info(f"dropping this database ('{self._database_id}')")
        return self._astra_db_admin.drop_database(
            id=self._database_id,
            wait_until_active=wait_until_active,
            max_time_ms=max_time_ms,
        )
        logger.info(f"finished dropping this database ('{self._database_id}')")

    async def async_drop(
        self,
        *,
        wait_until_active: bool = True,
        max_time_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        Drop this database, i.e. delete it completely and permanently with all its data.
        Async version of the method, for use in an asyncio context.

        This method wraps the `drop_database` method of the AstraDBAdmin class,
        where more information may be found.

        Args:
            wait_until_active: if True (default), the method returns only after
                the database has actually been deleted (generally a few minutes).
                If False, it will return right after issuing the
                drop request to the DevOps API, and it will be responsibility
                of the caller to check the database status/availability
                after that, if desired.
            max_time_ms: a timeout, in milliseconds, for the whole requested
                operation to complete.
                Note that a timeout is no guarantee that the deletion request
                has not reached the API server.

        Returns:
            A dictionary of the form {"ok": 1} in case of success.
            Otherwise, an exception is raised.

        Example:
            >>> asyncio.run(my_db_admin.async_drop())
            {'ok': 1}

        Note:
            Once the method succeeds, methods on this object -- such as `info()`,
            or `list_keyspaces()` -- can still be invoked: however, this hardly
            makes sense as the underlying actual database is no more.
            It is responsibility of the developer to design a correct flow
            which avoids using a deceased database any further.
        """

        logger.info(f"dropping this database ('{self._database_id}'), async")
        return await self._astra_db_admin.async_drop_database(
            id=self._database_id,
            wait_until_active=wait_until_active,
            max_time_ms=max_time_ms,
        )
        logger.info(f"finished dropping this database ('{self._database_id}'), async")

    def get_database(
        self,
        *,
        token: str | TokenProvider | None = None,
        keyspace: str | None = None,
        namespace: str | None = None,
        region: str | None = None,
        api_path: str | None = None,
        api_version: str | None = None,
        max_time_ms: int | None = None,
    ) -> Database:
        """
        Create a Database instance from this database admin, for data-related tasks.

        Args:
            token: if supplied, is passed to the Database instead of
                the one set for this object. Useful if one wants to work in
                a least-privilege manner, limiting the permissions for non-admin work.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            keyspace: an optional keyspace to set in the resulting Database.
                The same default logic as for `AstraDBAdmin.get_database` applies.
            namespace: an alias for `keyspace`. *DEPRECATED*, removal in 2.0.
            region: *This parameter is deprecated and should not be used.*
                Ignored in the method.
            api_path: path to append to the API Endpoint. In typical usage, this
                should be left to its default of "/api/json".
            api_version: version specifier to append to the API path. In typical
                usage, this should be left to its default of "v1".

        Returns:
            A Database object, ready to be used for working with data and collections.

        Example:
            >>> my_db = my_db_admin.get_database()
            >>> my_db.list_collection_names()
            ['movies', 'another_collection']

        Note:
            creating an instance of Database does not trigger actual creation
            of the database itself, which should exist beforehand. To create databases,
            see the AstraDBAdmin class.
        """

        keyspace_param = check_namespace_keyspace(
            keyspace=keyspace,
            namespace=namespace,
        )

        if region is not None:
            the_warning = deprecation.DeprecatedWarning(
                "The 'region' parameter is deprecated in this method and will be ignored.",
                deprecated_in="1.3.2",
                removed_in="2.0.0",
                details="The database class whose method is invoked already has a region set.",
            )
            warnings.warn(
                the_warning,
                stacklevel=2,
            )

        return self._astra_db_admin.get_database(
            api_endpoint=self.api_endpoint,
            token=token,
            keyspace=keyspace_param,
            api_path=api_path,
            api_version=api_version,
            max_time_ms=max_time_ms,
        )

    def get_async_database(
        self,
        *,
        token: str | TokenProvider | None = None,
        keyspace: str | None = None,
        namespace: str | None = None,
        region: str | None = None,
        api_path: str | None = None,
        api_version: str | None = None,
        max_time_ms: int | None = None,
    ) -> AsyncDatabase:
        """
        Create an AsyncDatabase instance out of this class for working
        with the data in it.

        This method has identical behavior and signature as the sync
        counterpart `get_database`: please see that one for more details.
        """

        keyspace_param = check_namespace_keyspace(
            keyspace=keyspace,
            namespace=namespace,
        )

        return self.get_database(
            token=token,
            keyspace=keyspace_param,
            region=region,
            api_path=api_path,
            api_version=api_version,
            max_time_ms=max_time_ms,
        ).to_async()

    def find_embedding_providers(
        self, *, max_time_ms: int | None = None
    ) -> FindEmbeddingProvidersResult:
        """
        Query the API for the full information on available embedding providers.

        Args:
            max_time_ms: a timeout, in milliseconds, for the DevOps API request.

        Returns:
            A `FindEmbeddingProvidersResult` object with the complete information
            returned by the API about available embedding providers

        Example (output abridged and indented for clarity):
            >>> admin_for_my_db.find_embedding_providers()
            FindEmbeddingProvidersResult(embedding_providers=..., openai, ...)
            >>> admin_for_my_db.find_embedding_providers().embedding_providers
            {
                'openai': EmbeddingProvider(
                    display_name='OpenAI',
                    models=[
                        EmbeddingProviderModel(name='text-embedding-3-small'),
                        ...
                    ]
                ),
                ...
            }
        """

        logger.info("findEmbeddingProviders")
        fe_response = self._api_commander.request(
            payload={"findEmbeddingProviders": {}},
            timeout_info=base_timeout_info(max_time_ms),
        )
        if "embeddingProviders" not in fe_response.get("status", {}):
            raise DataAPIFaultyResponseException(
                text="Faulty response from findEmbeddingProviders API command.",
                raw_response=fe_response,
            )
        else:
            logger.info("finished findEmbeddingProviders")
            return FindEmbeddingProvidersResult.from_dict(fe_response["status"])

    async def async_find_embedding_providers(
        self, *, max_time_ms: int | None = None
    ) -> FindEmbeddingProvidersResult:
        """
        Query the API for the full information on available embedding providers.
        Async version of the method, for use in an asyncio context.

        Args:
            max_time_ms: a timeout, in milliseconds, for the DevOps API request.

        Returns:
            A `FindEmbeddingProvidersResult` object with the complete information
            returned by the API about available embedding providers

        Example (output abridged and indented for clarity):
            >>> admin_for_my_db.find_embedding_providers()
            FindEmbeddingProvidersResult(embedding_providers=..., openai, ...)
            >>> admin_for_my_db.find_embedding_providers().embedding_providers
            {
                'openai': EmbeddingProvider(
                    display_name='OpenAI',
                    models=[
                        EmbeddingProviderModel(name='text-embedding-3-small'),
                        ...
                    ]
                ),
                ...
            }
        """

        logger.info("findEmbeddingProviders, async")
        fe_response = await self._api_commander.async_request(
            payload={"findEmbeddingProviders": {}},
            timeout_info=base_timeout_info(max_time_ms),
        )
        if "embeddingProviders" not in fe_response.get("status", {}):
            raise DataAPIFaultyResponseException(
                text="Faulty response from findEmbeddingProviders API command.",
                raw_response=fe_response,
            )
        else:
            logger.info("finished findEmbeddingProviders, async")
            return FindEmbeddingProvidersResult.from_dict(fe_response["status"])


class DataAPIDatabaseAdmin(DatabaseAdmin):
    """
    An "admin" object for non-Astra Data API environments, to perform administrative
    tasks at the keyspaces level such as creating/listing/dropping keyspaces.

    Conforming to the architecture of non-Astra deployments of the Data API,
    this object works within the one existing database. It is within that database
    that the keyspace CRUD operations (and possibly other admin operations)
    are performed. Since non-Astra environment lack the concept of an overall
    admin (such as the all-databases AstraDBAdmin class), a `DataAPIDatabaseAdmin`
    is generally created by invoking the `get_database_admin` method of the
    corresponding `Database` object (which in turn is spawned by a DataAPIClient).

    Args:
        api_endpoint: the full URI to access the Data API,
            e.g. "http://localhost:8181".
        token: an access token with enough permission to perform admin tasks.
            This can be either a literal token string or a subclass of
            `astrapy.authentication.TokenProvider`.
        environment: a label, whose value is one of Environment.OTHER (default)
            or other non-Astra environment values in the `Environment` enum.
        api_path: path to append to the API Endpoint. In typical usage, this
            class is created by a method such as `Database.get_database_admin()`,
            which passes the matching value. Defaults to this portion of the path
            being absent.
        api_version: version specifier to append to the API path. In typical
            usage, this class is created by a method such as
            `Database.get_database_admin()`, which passes the matching value.
            Defaults to this portion of the path being absent.
        callers: a list of caller identities, i.e. applications, or frameworks,
            on behalf of which Data API calls are performed. These end up in the
            request user-agent.
            Each caller identity is a ("caller_name", "caller_version") pair.
        caller_name: *DEPRECATED*, use `callers`. Removal 2.0. Name of the
            application, or framework, on behalf of which the Data API calls
            are performed. This ends up in the request user-agent.
        caller_version: version of the caller. *DEPRECATED*, use `callers`. Removal 2.0.
        spawner_database: either a Database or an AsyncDatabase instance.
            This represents the database class which spawns this admin object, so that,
            if required, a keyspace creation can retroactively "use" the new keyspace
            in the spawner. Used to enable the
            Async/Database.get_admin_database().create_keyspace() pattern.

    Example:
        >>> from astrapy import DataAPIClient
        >>> from astrapy.constants import Environment
        >>> from astrapy.authentication import UsernamePasswordTokenProvider
        >>>
        >>> token_provider = UsernamePasswordTokenProvider("username", "password")
        >>> endpoint = "http://localhost:8181"
        >>>
        >>> client = DataAPIClient(
        >>>     token=token_provider,
        >>>     environment=Environment.OTHER,
        >>> )
        >>> database = client.get_database(endpoint)
        >>> admin_for_my_db = database.get_database_admin()
        >>>
        >>> admin_for_my_db.list_keyspaces()
        ['keyspace1', 'keyspace2']
    """

    def __init__(
        self,
        api_endpoint: str,
        *,
        token: str | TokenProvider | None = None,
        environment: str | None = None,
        api_path: str | None = None,
        api_version: str | None = None,
        callers: Sequence[CallerType] = [],
        caller_name: str | None = None,
        caller_version: str | None = None,
        spawner_database: Database | AsyncDatabase | None = None,
    ) -> None:
        # lazy import here to avoid circular dependency
        from astrapy.database import Database

        callers_param = check_caller_parameters(callers, caller_name, caller_version)
        self.environment = (environment or Environment.OTHER).lower()
        self.token_provider = coerce_token_provider(token)
        self.api_endpoint = api_endpoint
        self.callers = callers_param
        self.api_path = api_path if api_path is not None else ""
        self.api_version = api_version if api_version is not None else ""
        self._commander_headers = {
            DEFAULT_DATA_API_AUTH_HEADER: self.token_provider.get_token(),
        }
        self._api_commander = self._get_api_commander()

        if spawner_database is not None:
            self.spawner_database = spawner_database
        else:
            # leaving the keyspace to its per-environment default
            # (a task for the Database)
            self.spawner_database = Database(
                api_endpoint=self.api_endpoint,
                token=self.token_provider,
                keyspace=None,
                callers=self.callers,
                environment=self.environment,
                api_path=self.api_path,
                api_version=self.api_version,
            )

    def __repr__(self) -> str:
        ep_desc = f'api_endpoint="{self.api_endpoint}"'
        token_desc: str | None
        if self.token_provider:
            token_desc = f'token="{redact_secret(str(self.token_provider), 15)}"'
        else:
            token_desc = None
        env_desc = f'environment="{self.environment}"'
        parts = [pt for pt in [ep_desc, token_desc, env_desc] if pt is not None]
        return f"{self.__class__.__name__}({', '.join(parts)})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DataAPIDatabaseAdmin):
            return all(
                [
                    self.environment == other.environment,
                    self._api_commander == other._api_commander,
                ]
            )
        else:
            return False

    def _get_api_commander(self) -> APICommander:
        base_path = "/".join(comp for comp in [self.api_path, self.api_version] if comp)
        api_commander = APICommander(
            api_endpoint=self.api_endpoint,
            path=base_path,
            headers=self._commander_headers,
            callers=self.callers,
        )
        return api_commander

    def _copy(
        self,
        api_endpoint: str | None = None,
        token: str | TokenProvider | None = None,
        environment: str | None = None,
        api_path: str | None = None,
        api_version: str | None = None,
        callers: Sequence[CallerType] = [],
        caller_name: str | None = None,
        caller_version: str | None = None,
    ) -> DataAPIDatabaseAdmin:
        callers_param = check_caller_parameters(callers, caller_name, caller_version)
        return DataAPIDatabaseAdmin(
            api_endpoint=api_endpoint or self.api_endpoint,
            token=coerce_token_provider(token) or self.token_provider,
            environment=environment or self.environment,
            api_path=api_path or self.api_path,
            api_version=api_version or self.api_version,
            callers=callers_param or self.callers,
        )

    def with_options(
        self,
        *,
        api_endpoint: str | None = None,
        token: str | TokenProvider | None = None,
        callers: Sequence[CallerType] = [],
        caller_name: str | None = None,
        caller_version: str | None = None,
    ) -> DataAPIDatabaseAdmin:
        """
        Create a clone of this DataAPIDatabaseAdmin with some changed attributes.

        Args:
            api_endpoint: the full URI to access the Data API,
                e.g. "http://localhost:8181".
            token: an access token with enough permission to perform admin tasks.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            callers: a list of caller identities, i.e. applications, or frameworks,
                on behalf of which Data API calls are performed. These end up in the
                request user-agent.
                Each caller identity is a ("caller_name", "caller_version") pair.
            caller_name: *DEPRECATED*, use `callers`. Removal 2.0. Name of the
                application, or framework, on behalf of which the Data API calls
                are performed. This ends up in the request user-agent.
            caller_version: version of the caller. *DEPRECATED*, use `callers`.
                Removal 2.0.

        Returns:
            a new DataAPIDatabaseAdmin instance.

        Example:
            >>> admin_for_my_other_db = admin_for_my_db.with_options(
            ...     api_endpoint="http://10.1.1.5:8181",
            ... )
        """

        callers_param = check_caller_parameters(callers, caller_name, caller_version)
        return self._copy(
            api_endpoint=api_endpoint,
            token=token,
            callers=callers_param,
        )

    @deprecation.deprecated(  # type: ignore[misc]
        deprecated_in="1.5.1",
        removed_in="2.0.0",
        current_version=__version__,
        details=SET_CALLER_DEPRECATION_NOTICE,
    )
    def set_caller(
        self,
        caller_name: str | None = None,
        caller_version: str | None = None,
    ) -> None:
        """
        Set a new identity for the application/framework on behalf of which
        the DevOps API calls will be performed (the "caller").

        New objects spawned from this client afterwards will inherit the new settings.

        Args:
            caller_name: name of the application, or framework, on behalf of which
                the DevOps API calls are performed. This ends up in the request user-agent.
            caller_version: version of the caller.

        Example:
            >>> admin_for_my_db.set_caller(
            ...     caller_name="the_caller",
            ...     caller_version="0.1.0",
            ... )
        """

        logger.info(f"setting caller to {caller_name}/{caller_version}")
        callers_param = check_caller_parameters([], caller_name, caller_version)
        self.callers = callers_param
        self._api_commander = self._get_api_commander()

    @deprecation.deprecated(  # type: ignore[misc]
        deprecated_in="1.5.0",
        removed_in="2.0.0",
        current_version=__version__,
        details=NAMESPACE_DEPRECATION_NOTICE_METHOD,
    )
    def list_namespaces(self, *, max_time_ms: int | None = None) -> list[str]:
        """
        Query the API for a list of the namespaces in the database.

        *DEPRECATED* (removal in 2.0). Switch to the "list_keyspaces" method.**

        Args:
            max_time_ms: a timeout, in milliseconds, for the DevOps API request.

        Returns:
            A list of the namespaces, each a string, in no particular order.

        Example:
            >>> admin_for_my_db.list_namespaces()
            ['default_keyspace', 'staging_namespace']
        """
        logger.info("getting list of namespaces")
        fn_response = self._api_commander.request(
            payload={"findNamespaces": {}},
            timeout_info=base_timeout_info(max_time_ms),
        )
        if "namespaces" not in fn_response.get("status", {}):
            raise DataAPIFaultyResponseException(
                text="Faulty response from findNamespaces API command.",
                raw_response=fn_response,
            )
        else:
            logger.info("finished getting list of namespaces")
            return fn_response["status"]["namespaces"]  # type: ignore[no-any-return]

    def list_keyspaces(self, *, max_time_ms: int | None = None) -> list[str]:
        """
        Query the API for a list of the keyspaces in the database.

        Args:
            max_time_ms: a timeout, in milliseconds, for the DevOps API request.

        Returns:
            A list of the keyspaces, each a string, in no particular order.

        Example:
            >>> admin_for_my_db.list_keyspaces()
            ['default_keyspace', 'staging_keyspace']
        """
        logger.info("getting list of keyspaces")
        fn_response = self._api_commander.request(
            payload={"findKeyspaces": {}},
            timeout_info=base_timeout_info(max_time_ms),
        )
        if "keyspaces" not in fn_response.get("status", {}):
            raise DataAPIFaultyResponseException(
                text="Faulty response from findKeyspaces API command.",
                raw_response=fn_response,
            )
        else:
            logger.info("finished getting list of keyspaces")
            return fn_response["status"]["keyspaces"]  # type: ignore[no-any-return]

    @deprecation.deprecated(  # type: ignore[misc]
        deprecated_in="1.5.0",
        removed_in="2.0.0",
        current_version=__version__,
        details=NAMESPACE_DEPRECATION_NOTICE_METHOD,
    )
    def create_namespace(
        self,
        name: str,
        *,
        replication_options: dict[str, Any] | None = None,
        update_db_keyspace: bool | None = None,
        update_db_namespace: bool | None = None,
        max_time_ms: int | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Create a namespace in the database, returning {'ok': 1} if successful.

        *DEPRECATED* (removal in 2.0). Switch to the "create_keyspace" method.**

        Args:
            name: the namespace name. If supplying a namespace that exists
                already, the method call proceeds as usual, no errors are
                raised, and the whole invocation is a no-op.
            replication_options: this dictionary can specify the options about
                replication of the namespace (across database nodes). If provided,
                it must have a structure similar to:
                `{"class": "SimpleStrategy", "replication_factor": 1}`.
            update_db_keyspace: if True, the `Database` or `AsyncDatabase` class
                that spawned this DatabaseAdmin, if any, gets updated to work on
                the newly-created keyspace starting when this method returns.
            update_db_namespace: an alias for update_db_keyspace.
                *DEPRECATED* as of v1.5.0, removal in v2.0.0.
            max_time_ms: a timeout, in milliseconds, for the whole requested
                operation to complete.
                Note that a timeout is no guarantee that the creation request
                has not reached the API server.

        Returns:
            A dictionary of the form {"ok": 1} in case of success.
            Otherwise, an exception is raised.

        Example:
            >>> admin_for_my_db.list_namespaces()
            ['default_keyspace']
            >>> admin_for_my_db.create_namespace("that_other_one")
            {'ok': 1}
            >>> admin_for_my_db.list_namespaces()
            ['default_keyspace', 'that_other_one']
        """

        _update_db_keyspace = check_update_db_namespace_keyspace(
            update_db_keyspace=update_db_keyspace,
            update_db_namespace=update_db_namespace,
        )

        options = {
            k: v
            for k, v in {
                "replication": replication_options,
            }.items()
            if v
        }
        payload = {
            "createNamespace": {
                **{"name": name},
                **({"options": options} if options else {}),
            }
        }
        logger.info("creating namespace")
        cn_response = self._api_commander.request(
            payload=payload,
            timeout_info=base_timeout_info(max_time_ms),
        )
        if (cn_response.get("status") or {}).get("ok") != 1:
            raise DataAPIFaultyResponseException(
                text="Faulty response from createNamespace API command.",
                raw_response=cn_response,
            )
        else:
            logger.info("finished creating namespace")
            if _update_db_keyspace:
                self.spawner_database.use_keyspace(name)
            return {k: v for k, v in cn_response["status"].items() if k == "ok"}

    def create_keyspace(
        self,
        name: str,
        *,
        replication_options: dict[str, Any] | None = None,
        update_db_keyspace: bool | None = None,
        update_db_namespace: bool | None = None,
        max_time_ms: int | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Create a keyspace in the database, returning {'ok': 1} if successful.

        Args:
            name: the keyspace name. If supplying a keyspace that exists
                already, the method call proceeds as usual, no errors are
                raised, and the whole invocation is a no-op.
            replication_options: this dictionary can specify the options about
                replication of the keyspace (across database nodes). If provided,
                it must have a structure similar to:
                `{"class": "SimpleStrategy", "replication_factor": 1}`.
            update_db_keyspace: if True, the `Database` or `AsyncDatabase` class
                that spawned this DatabaseAdmin, if any, gets updated to work on
                the newly-created keyspace starting when this method returns.
            update_db_namespace: an alias for update_db_keyspace.
                *DEPRECATED* as of v1.5.0, removal in v2.0.0.
            max_time_ms: a timeout, in milliseconds, for the whole requested
                operation to complete.
                Note that a timeout is no guarantee that the creation request
                has not reached the API server.

        Returns:
            A dictionary of the form {"ok": 1} in case of success.
            Otherwise, an exception is raised.

        Example:
            >>> admin_for_my_db.list_keyspaces()
            ['default_keyspace']
            >>> admin_for_my_db.create_keyspace("that_other_one")
            {'ok': 1}
            >>> admin_for_my_db.list_keyspaces()
            ['default_keyspace', 'that_other_one']
        """

        _update_db_keyspace = check_update_db_namespace_keyspace(
            update_db_keyspace=update_db_keyspace,
            update_db_namespace=update_db_namespace,
        )

        options = {
            k: v
            for k, v in {
                "replication": replication_options,
            }.items()
            if v
        }
        payload = {
            "createKeyspace": {
                **{"name": name},
                **({"options": options} if options else {}),
            }
        }
        logger.info("creating keyspace")
        cn_response = self._api_commander.request(
            payload=payload,
            timeout_info=base_timeout_info(max_time_ms),
        )
        if (cn_response.get("status") or {}).get("ok") != 1:
            raise DataAPIFaultyResponseException(
                text="Faulty response from createKeyspace API command.",
                raw_response=cn_response,
            )
        else:
            logger.info("finished creating keyspace")
            if _update_db_keyspace:
                self.spawner_database.use_keyspace(name)
            return {k: v for k, v in cn_response["status"].items() if k == "ok"}

    @deprecation.deprecated(  # type: ignore[misc]
        deprecated_in="1.5.0",
        removed_in="2.0.0",
        current_version=__version__,
        details=NAMESPACE_DEPRECATION_NOTICE_METHOD,
    )
    def drop_namespace(
        self,
        name: str,
        *,
        max_time_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        Drop (delete) a namespace from the database.

        *DEPRECATED* (removal in 2.0). Switch to the "drop_namespace" method.**

        Args:
            name: the namespace to delete. If it does not exist in this database,
                an error is raised.
            max_time_ms: a timeout, in milliseconds, for the whole requested
                operation to complete.
                Note that a timeout is no guarantee that the deletion request
                has not reached the API server.

        Returns:
            A dictionary of the form {"ok": 1} in case of success.
            Otherwise, an exception is raised.

        Example:
            >>> admin_for_my_db.list_namespaces()
            ['default_keyspace', 'that_other_one']
            >>> admin_for_my_db.drop_namespace("that_other_one")
            {'ok': 1}
            >>> admin_for_my_db.list_namespaces()
            ['default_keyspace']
        """
        logger.info("dropping namespace")
        dn_response = self._api_commander.request(
            payload={"dropNamespace": {"name": name}},
            timeout_info=base_timeout_info(max_time_ms),
        )
        if (dn_response.get("status") or {}).get("ok") != 1:
            raise DataAPIFaultyResponseException(
                text="Faulty response from dropNamespace API command.",
                raw_response=dn_response,
            )
        else:
            logger.info("finished dropping namespace")
            return {k: v for k, v in dn_response["status"].items() if k == "ok"}

    def drop_keyspace(
        self,
        name: str,
        *,
        max_time_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        Drop (delete) a keyspace from the database.

        Args:
            name: the keyspace to delete. If it does not exist in this database,
                an error is raised.
            max_time_ms: a timeout, in milliseconds, for the whole requested
                operation to complete.
                Note that a timeout is no guarantee that the deletion request
                has not reached the API server.

        Returns:
            A dictionary of the form {"ok": 1} in case of success.
            Otherwise, an exception is raised.

        Example:
            >>> admin_for_my_db.list_keyspaces()
            ['default_keyspace', 'that_other_one']
            >>> admin_for_my_db.drop_keyspace("that_other_one")
            {'ok': 1}
            >>> admin_for_my_db.list_keyspaces()
            ['default_keyspace']
        """
        logger.info("dropping keyspace")
        dn_response = self._api_commander.request(
            payload={"dropKeyspace": {"name": name}},
            timeout_info=base_timeout_info(max_time_ms),
        )
        if (dn_response.get("status") or {}).get("ok") != 1:
            raise DataAPIFaultyResponseException(
                text="Faulty response from dropKeyspace API command.",
                raw_response=dn_response,
            )
        else:
            logger.info("finished dropping keyspace")
            return {k: v for k, v in dn_response["status"].items() if k == "ok"}

    @deprecation.deprecated(  # type: ignore[misc]
        deprecated_in="1.5.0",
        removed_in="2.0.0",
        current_version=__version__,
        details=NAMESPACE_DEPRECATION_NOTICE_METHOD,
    )
    async def async_list_namespaces(
        self, *, max_time_ms: int | None = None
    ) -> list[str]:
        """
        Query the API for a list of the namespaces in the database.
        Async version of the method, for use in an asyncio context.

        *DEPRECATED* (removal in 2.0). Switch to the "async_list_keyspaces" method.**

        Args:
            max_time_ms: a timeout, in milliseconds, for the DevOps API request.

        Returns:
            A list of the namespaces, each a string, in no particular order.

        Example:
            >>> asyncio.run(admin_for_my_db.async_list_namespaces())
            ['default_keyspace', 'staging_namespace']
        """
        logger.info("getting list of namespaces, async")
        fn_response = await self._api_commander.async_request(
            payload={"findNamespaces": {}},
            timeout_info=base_timeout_info(max_time_ms),
        )
        if "namespaces" not in fn_response.get("status", {}):
            raise DataAPIFaultyResponseException(
                text="Faulty response from findNamespaces API command.",
                raw_response=fn_response,
            )
        else:
            logger.info("finished getting list of namespaces, async")
            return fn_response["status"]["namespaces"]  # type: ignore[no-any-return]

    async def async_list_keyspaces(
        self, *, max_time_ms: int | None = None
    ) -> list[str]:
        """
        Query the API for a list of the keyspaces in the database.
        Async version of the method, for use in an asyncio context.

        Args:
            max_time_ms: a timeout, in milliseconds, for the DevOps API request.

        Returns:
            A list of the keyspaces, each a string, in no particular order.

        Example:
            >>> asyncio.run(admin_for_my_db.async_list_keyspaces())
            ['default_keyspace', 'staging_keyspace']
        """
        logger.info("getting list of keyspaces, async")
        fn_response = await self._api_commander.async_request(
            payload={"findKeyspaces": {}},
            timeout_info=base_timeout_info(max_time_ms),
        )
        if "keyspaces" not in fn_response.get("status", {}):
            raise DataAPIFaultyResponseException(
                text="Faulty response from findKeyspaces API command.",
                raw_response=fn_response,
            )
        else:
            logger.info("finished getting list of keyspaces, async")
            return fn_response["status"]["keyspaces"]  # type: ignore[no-any-return]

    @deprecation.deprecated(  # type: ignore[misc]
        deprecated_in="1.5.0",
        removed_in="2.0.0",
        current_version=__version__,
        details=NAMESPACE_DEPRECATION_NOTICE_METHOD,
    )
    async def async_create_namespace(
        self,
        name: str,
        *,
        replication_options: dict[str, Any] | None = None,
        update_db_keyspace: bool | None = None,
        update_db_namespace: bool | None = None,
        max_time_ms: int | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Create a namespace in the database, returning {'ok': 1} if successful.
        Async version of the method, for use in an asyncio context.

        *DEPRECATED* (removal in 2.0). Switch to the "async_create_keyspace" method.**

        Args:
            name: the namespace name. If supplying a namespace that exists
                already, the method call proceeds as usual, no errors are
                raised, and the whole invocation is a no-op.
            replication_options: this dictionary can specify the options about
                replication of the namespace (across database nodes). If provided,
                it must have a structure similar to:
                `{"class": "SimpleStrategy", "replication_factor": 1}`.
            update_db_keyspace: if True, the `Database` or `AsyncDatabase` class
                that spawned this DatabaseAdmin, if any, gets updated to work on
                the newly-created keyspace starting when this method returns.
            update_db_namespace: an alias for update_db_keyspace.
                *DEPRECATED* as of v1.5.0, removal in v2.0.0.
            max_time_ms: a timeout, in milliseconds, for the whole requested
                operation to complete.
                Note that a timeout is no guarantee that the creation request
                has not reached the API server.

        Returns:
            A dictionary of the form {"ok": 1} in case of success.
            Otherwise, an exception is raised.

        Example:
            >>> admin_for_my_db.list_namespaces()
            ['default_keyspace']
            >>> asyncio.run(admin_for_my_db.async_create_namespace(
            ...     "that_other_one"
            ... ))
            {'ok': 1}
            >>> admin_for_my_db.list_namespaces()
            ['default_keyspace', 'that_other_one']
        """

        _update_db_keyspace = check_update_db_namespace_keyspace(
            update_db_keyspace=update_db_keyspace,
            update_db_namespace=update_db_namespace,
        )

        options = {
            k: v
            for k, v in {
                "replication": replication_options,
            }.items()
            if v
        }
        payload = {
            "createNamespace": {
                **{"name": name},
                **({"options": options} if options else {}),
            }
        }
        logger.info("creating namespace, async")
        cn_response = await self._api_commander.async_request(
            payload=payload,
            timeout_info=base_timeout_info(max_time_ms),
        )
        if (cn_response.get("status") or {}).get("ok") != 1:
            raise DataAPIFaultyResponseException(
                text="Faulty response from createNamespace API command.",
                raw_response=cn_response,
            )
        else:
            logger.info("finished creating namespace, async")
            if _update_db_keyspace:
                self.spawner_database.use_keyspace(name)
            return {k: v for k, v in cn_response["status"].items() if k == "ok"}

    async def async_create_keyspace(
        self,
        name: str,
        *,
        replication_options: dict[str, Any] | None = None,
        update_db_keyspace: bool | None = None,
        update_db_namespace: bool | None = None,
        max_time_ms: int | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Create a keyspace in the database, returning {'ok': 1} if successful.
        Async version of the method, for use in an asyncio context.

        Args:
            name: the keyspace name. If supplying a keyspace that exists
                already, the method call proceeds as usual, no errors are
                raised, and the whole invocation is a no-op.
            replication_options: this dictionary can specify the options about
                replication of the keyspace (across database nodes). If provided,
                it must have a structure similar to:
                `{"class": "SimpleStrategy", "replication_factor": 1}`.
            update_db_keyspace: if True, the `Database` or `AsyncDatabase` class
                that spawned this DatabaseAdmin, if any, gets updated to work on
                the newly-created keyspace starting when this method returns.
            update_db_namespace: an alias for update_db_keyspace.
                *DEPRECATED* as of v1.5.0, removal in v2.0.0.
            max_time_ms: a timeout, in milliseconds, for the whole requested
                operation to complete.
                Note that a timeout is no guarantee that the creation request
                has not reached the API server.

        Returns:
            A dictionary of the form {"ok": 1} in case of success.
            Otherwise, an exception is raised.

        Example:
            >>> admin_for_my_db.list_keyspaces()
            ['default_keyspace']
            >>> asyncio.run(admin_for_my_db.async_create_keyspace(
            ...     "that_other_one"
            ... ))
            {'ok': 1}
            >>> admin_for_my_db.list_leyspaces()
            ['default_keyspace', 'that_other_one']
        """

        _update_db_keyspace = check_update_db_namespace_keyspace(
            update_db_keyspace=update_db_keyspace,
            update_db_namespace=update_db_namespace,
        )

        options = {
            k: v
            for k, v in {
                "replication": replication_options,
            }.items()
            if v
        }
        payload = {
            "createKeyspace": {
                **{"name": name},
                **({"options": options} if options else {}),
            }
        }
        logger.info("creating keyspace, async")
        cn_response = await self._api_commander.async_request(
            payload=payload,
            timeout_info=base_timeout_info(max_time_ms),
        )
        if (cn_response.get("status") or {}).get("ok") != 1:
            raise DataAPIFaultyResponseException(
                text="Faulty response from createKeyspace API command.",
                raw_response=cn_response,
            )
        else:
            logger.info("finished creating keyspace, async")
            if _update_db_keyspace:
                self.spawner_database.use_keyspace(name)
            return {k: v for k, v in cn_response["status"].items() if k == "ok"}

    @deprecation.deprecated(  # type: ignore[misc]
        deprecated_in="1.5.0",
        removed_in="2.0.0",
        current_version=__version__,
        details=NAMESPACE_DEPRECATION_NOTICE_METHOD,
    )
    async def async_drop_namespace(
        self,
        name: str,
        *,
        max_time_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        Drop (delete) a namespace from the database.
        Async version of the method, for use in an asyncio context.

        *DEPRECATED* (removal in 2.0). Switch to the "async_drop_keyspace" method.**

        Args:
            name: the namespace to delete. If it does not exist in this database,
                an error is raised.
            max_time_ms: a timeout, in milliseconds, for the whole requested
                operation to complete.
                Note that a timeout is no guarantee that the deletion request
                has not reached the API server.

        Returns:
            A dictionary of the form {"ok": 1} in case of success.
            Otherwise, an exception is raised.

        Example:
            >>> admin_for_my_db.list_namespaces()
            ['that_other_one', 'default_keyspace']
            >>> asyncio.run(admin_for_my_db.async_drop_namespace(
            ...     "that_other_one"
            ... ))
            {'ok': 1}
            >>> admin_for_my_db.list_namespaces()
            ['default_keyspace']
        """
        logger.info("dropping namespace, async")
        dn_response = await self._api_commander.async_request(
            payload={"dropNamespace": {"name": name}},
            timeout_info=base_timeout_info(max_time_ms),
        )
        if (dn_response.get("status") or {}).get("ok") != 1:
            raise DataAPIFaultyResponseException(
                text="Faulty response from dropNamespace API command.",
                raw_response=dn_response,
            )
        else:
            logger.info("finished dropping namespace, async")
            return {k: v for k, v in dn_response["status"].items() if k == "ok"}

    async def async_drop_keyspace(
        self,
        name: str,
        *,
        max_time_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        Drop (delete) a keyspace from the database.
        Async version of the method, for use in an asyncio context.

        Args:
            name: the keyspace to delete. If it does not exist in this database,
                an error is raised.
            max_time_ms: a timeout, in milliseconds, for the whole requested
                operation to complete.
                Note that a timeout is no guarantee that the deletion request
                has not reached the API server.

        Returns:
            A dictionary of the form {"ok": 1} in case of success.
            Otherwise, an exception is raised.

        Example:
            >>> admin_for_my_db.list_keyspaces()
            ['that_other_one', 'default_keyspace']
            >>> asyncio.run(admin_for_my_db.async_drop_keyspace(
            ...     "that_other_one"
            ... ))
            {'ok': 1}
            >>> admin_for_my_db.list_keyspaces()
            ['default_keyspace']
        """
        logger.info("dropping keyspace, async")
        dn_response = await self._api_commander.async_request(
            payload={"dropKeyspace": {"name": name}},
            timeout_info=base_timeout_info(max_time_ms),
        )
        if (dn_response.get("status") or {}).get("ok") != 1:
            raise DataAPIFaultyResponseException(
                text="Faulty response from dropKeyspace API command.",
                raw_response=dn_response,
            )
        else:
            logger.info("finished dropping keyspace, async")
            return {k: v for k, v in dn_response["status"].items() if k == "ok"}

    def get_database(
        self,
        *,
        token: str | TokenProvider | None = None,
        keyspace: str | None = None,
        namespace: str | None = None,
        api_path: str | None = None,
        api_version: str | None = None,
    ) -> Database:
        """
        Create a Database instance out of this class for working with the data in it.

        Args:
            token: if supplied, is passed to the Database instead of
                the one set for this object. Useful if one wants to work in
                a least-privilege manner, limiting the permissions for non-admin work.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            keyspace: an optional keyspace to set in the resulting Database.
                If not provided, no keyspace is set, limiting what the Database
                can do until setting it with e.g. a `use_keyspace` method call.
            namespace: an alias for `keyspace`. *DEPRECATED*, removal in 2.0.
            api_path: path to append to the API Endpoint. In typical usage, this
                should be left to its default of "".
            api_version: version specifier to append to the API path. In typical
                usage, this should be left to its default of "v1".

        Returns:
            A Database object, ready to be used for working with data and collections.

        Example:
            >>> my_db = admin_for_my_db.get_database()
            >>> my_db.list_collection_names()
            ['movies', 'another_collection']

        Note:
            creating an instance of Database does not trigger actual creation
            of the database itself, which should exist beforehand.
        """

        keyspace_param = check_namespace_keyspace(
            keyspace=keyspace,
            namespace=namespace,
        )

        # lazy importing here to avoid circular dependency
        from astrapy import Database

        return Database(
            api_endpoint=self.api_endpoint,
            token=coerce_token_provider(token) or self.token_provider,
            keyspace=keyspace_param,
            callers=self.callers,
            environment=self.environment,
            api_path=api_path,
            api_version=api_version,
        )

    def get_async_database(
        self,
        *,
        token: str | TokenProvider | None = None,
        keyspace: str | None = None,
        namespace: str | None = None,
        api_path: str | None = None,
        api_version: str | None = None,
    ) -> AsyncDatabase:
        """
        Create an AsyncDatabase instance for the database, to be used
        when doing data-level work (such as creating/managing collections).

        This method has identical behavior and signature as the sync
        counterpart `get_database`: please see that one for more details.
        """

        keyspace_param = check_namespace_keyspace(
            keyspace=keyspace,
            namespace=namespace,
        )

        return self.get_database(
            token=token,
            keyspace=keyspace_param,
            api_path=api_path,
            api_version=api_version,
        ).to_async()

    def find_embedding_providers(
        self, *, max_time_ms: int | None = None
    ) -> FindEmbeddingProvidersResult:
        """
        Query the API for the full information on available embedding providers.

        Args:
            max_time_ms: a timeout, in milliseconds, for the DevOps API request.

        Returns:
            A `FindEmbeddingProvidersResult` object with the complete information
            returned by the API about available embedding providers

        Example (output abridged and indented for clarity):
            >>> admin_for_my_db.find_embedding_providers()
            FindEmbeddingProvidersResult(embedding_providers=..., openai, ...)
            >>> admin_for_my_db.find_embedding_providers().embedding_providers
            {
                'openai': EmbeddingProvider(
                    display_name='OpenAI',
                    models=[
                        EmbeddingProviderModel(name='text-embedding-3-small'),
                        ...
                    ]
                ),
                ...
            }
        """

        logger.info("findEmbeddingProviders")
        fe_response = self._api_commander.request(
            payload={"findEmbeddingProviders": {}},
            timeout_info=base_timeout_info(max_time_ms),
        )
        if "embeddingProviders" not in fe_response.get("status", {}):
            raise DataAPIFaultyResponseException(
                text="Faulty response from findEmbeddingProviders API command.",
                raw_response=fe_response,
            )
        else:
            logger.info("finished findEmbeddingProviders")
            return FindEmbeddingProvidersResult.from_dict(fe_response["status"])

    async def async_find_embedding_providers(
        self, *, max_time_ms: int | None = None
    ) -> FindEmbeddingProvidersResult:
        """
        Query the API for the full information on available embedding providers.
        Async version of the method, for use in an asyncio context.

        Args:
            max_time_ms: a timeout, in milliseconds, for the DevOps API request.

        Returns:
            A `FindEmbeddingProvidersResult` object with the complete information
            returned by the API about available embedding providers

        Example (output abridged and indented for clarity):
            >>> admin_for_my_db.find_embedding_providers()
            FindEmbeddingProvidersResult(embedding_providers=..., openai, ...)
            >>> admin_for_my_db.find_embedding_providers().embedding_providers
            {
                'openai': EmbeddingProvider(
                    display_name='OpenAI',
                    models=[
                        EmbeddingProviderModel(name='text-embedding-3-small'),
                        ...
                    ]
                ),
                ...
            }
        """

        logger.info("findEmbeddingProviders, async")
        fe_response = await self._api_commander.async_request(
            payload={"findEmbeddingProviders": {}},
            timeout_info=base_timeout_info(max_time_ms),
        )
        if "embeddingProviders" not in fe_response.get("status", {}):
            raise DataAPIFaultyResponseException(
                text="Faulty response from findEmbeddingProviders API command.",
                raw_response=fe_response,
            )
        else:
            logger.info("finished findEmbeddingProviders, async")
            return FindEmbeddingProvidersResult.from_dict(fe_response["status"])
