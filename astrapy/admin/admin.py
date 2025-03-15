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
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from astrapy.admin.endpoints import (
    ParsedAPIEndpoint,
    api_endpoint_parsing_cdinfo_message,
    api_endpoint_parsing_error_message,
    build_api_endpoint,
    database_id_matcher,
    parse_api_endpoint,
)
from astrapy.constants import Environment
from astrapy.exceptions import (
    DevOpsAPIException,
    InvalidEnvironmentException,
    MultiCallTimeoutManager,
    UnexpectedDataAPIResponseException,
    _first_valid_timeout,
    _select_singlereq_timeout_da,
    _select_singlereq_timeout_ka,
    _TimeoutContext,
)
from astrapy.info import (
    AstraDBAdminDatabaseInfo,
    AstraDBDatabaseInfo,
    FindEmbeddingProvidersResult,
    FindRerankingProvidersResult,
)
from astrapy.settings.defaults import (
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
)
from astrapy.utils.api_commander import APICommander
from astrapy.utils.api_options import (
    APIOptions,
    FullAPIOptions,
    TimeoutOptions,
    defaultAPIOptions,
)
from astrapy.utils.request_tools import HttpMethod
from astrapy.utils.unset import _UNSET, UnsetType

if TYPE_CHECKING:
    from astrapy import AsyncDatabase, Database
    from astrapy.authentication import TokenProvider


logger = logging.getLogger(__name__)


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
    token: str | TokenProvider | UnsetType = _UNSET,
    environment: str = Environment.PROD,
    request_timeout_ms: int | None = None,
    timeout_ms: int | None = None,
    api_options: APIOptions | None = None,
) -> dict[str, Any]:
    """
    Fetch database information through the DevOps API and return it in
    full, exactly like the API gives it back.

    Args:
        id: e. g. "01234567-89ab-cdef-0123-456789abcdef".
        token: a valid token to access the database information.
            If provided, overrides any token information in api_options.
        environment: a string representing the target Data API environment.
            It can be left unspecified for the default value of `Environment.PROD`.
            Only Astra DB environments can be meaningfully supplied.
        request_timeout_ms: a timeout, in milliseconds, for waiting on a response.
        timeout_ms: an alias for `request_timeout_ms`.
        api_options: a (possibly partial) specification of the API Options to use.

    Returns:
        The full response from the DevOps API about the database.
    """

    _request_timeout_ms: int | None | UnsetType
    _timeout_context_label = "request_timeout_ms"
    if request_timeout_ms is not None:
        _request_timeout_ms = request_timeout_ms
    else:
        if timeout_ms is not None:
            _request_timeout_ms = timeout_ms
            _timeout_context_label = "timeout_ms"
        else:
            _request_timeout_ms = _UNSET
    _api_options = (
        defaultAPIOptions(environment=environment)
        .with_override(api_options)
        .with_override(
            APIOptions(
                token=token,
                timeout_options=TimeoutOptions(
                    request_timeout_ms=_request_timeout_ms,
                ),
            ),
        )
    )
    base_path_components = [
        comp
        for comp in (
            ncomp.strip("/")
            for ncomp in (
                _api_options.dev_ops_api_url_options.dev_ops_api_version,
                "databases",
                id,
            )
            if ncomp is not None
        )
        if comp != ""
    ]
    dev_ops_base_path = "/".join(base_path_components)
    _dev_ops_commander_headers: dict[str, str | None]
    if _api_options.token:
        _token_str = _api_options.token.get_token()
        _dev_ops_commander_headers = {
            DEFAULT_DEV_OPS_AUTH_HEADER: f"{DEFAULT_DEV_OPS_AUTH_PREFIX}{_token_str}",
            **_api_options.admin_additional_headers,
        }
    else:
        _dev_ops_commander_headers = {
            **_api_options.admin_additional_headers,
        }
    dev_ops_commander = APICommander(
        api_endpoint=_api_options.dev_ops_api_url_options.dev_ops_url,
        path=dev_ops_base_path,
        headers=_dev_ops_commander_headers,
        callers=_api_options.callers,
        dev_ops_api=True,
        redacted_header_names=_api_options.redacted_header_names,
    )

    gd_response = dev_ops_commander.request(
        http_method=HttpMethod.GET,
        timeout_context=_TimeoutContext(
            request_ms=_api_options.timeout_options.request_timeout_ms,
            label=_timeout_context_label,
        ),
    )
    return gd_response


async def async_fetch_raw_database_info_from_id_token(
    id: str,
    *,
    token: str | TokenProvider | UnsetType = _UNSET,
    environment: str = Environment.PROD,
    request_timeout_ms: int | None = None,
    timeout_ms: int | None = None,
    api_options: APIOptions | None = None,
) -> dict[str, Any]:
    """
    Fetch database information through the DevOps API and return it in
    full, exactly like the API gives it back.
    Async version of the function, for use in an asyncio context.

    Args:
        id: e. g. "01234567-89ab-cdef-0123-456789abcdef".
        token: a valid token to access the database information.
            If provided, overrides any token information in api_options.
        environment: a string representing the target Data API environment.
            It can be left unspecified for the default value of `Environment.PROD`.
            Only Astra DB environments can be meaningfully supplied.
        request_timeout_ms: a timeout, in milliseconds, for waiting on a response.
        timeout_ms: an alias for `request_timeout_ms`.
        api_options: a (possibly partial) specification of the API Options to use.

    Returns:
        The full response from the DevOps API about the database.
    """

    _request_timeout_ms: int | None | UnsetType
    _timeout_context_label = "request_timeout_ms"
    if request_timeout_ms is not None:
        _request_timeout_ms = request_timeout_ms
    else:
        if timeout_ms is not None:
            _request_timeout_ms = timeout_ms
            _timeout_context_label = "timeout_ms"
        else:
            _request_timeout_ms = _UNSET
    _api_options = (
        defaultAPIOptions(environment=environment)
        .with_override(api_options)
        .with_override(
            APIOptions(
                token=token,
                timeout_options=TimeoutOptions(
                    request_timeout_ms=_request_timeout_ms,
                ),
            ),
        )
    )
    base_path_components = [
        comp
        for comp in (
            ncomp.strip("/")
            for ncomp in (
                _api_options.dev_ops_api_url_options.dev_ops_api_version,
                "databases",
                id,
            )
            if ncomp is not None
        )
        if comp != ""
    ]
    dev_ops_base_path = "/".join(base_path_components)
    _dev_ops_commander_headers: dict[str, str | None]
    if _api_options.token:
        _token_str = _api_options.token.get_token()
        _dev_ops_commander_headers = {
            DEFAULT_DEV_OPS_AUTH_HEADER: f"{DEFAULT_DEV_OPS_AUTH_PREFIX}{_token_str}",
            **_api_options.admin_additional_headers,
        }
    else:
        _dev_ops_commander_headers = {
            **_api_options.admin_additional_headers,
        }
    dev_ops_commander = APICommander(
        api_endpoint=_api_options.dev_ops_api_url_options.dev_ops_url,
        path=dev_ops_base_path,
        headers=_dev_ops_commander_headers,
        callers=_api_options.callers,
        dev_ops_api=True,
        redacted_header_names=_api_options.redacted_header_names,
    )

    gd_response = await dev_ops_commander.async_request(
        http_method=HttpMethod.GET,
        timeout_context=_TimeoutContext(
            request_ms=_api_options.timeout_options.request_timeout_ms,
            label=_timeout_context_label,
        ),
    )
    return gd_response


def fetch_database_info(
    api_endpoint: str,
    token: str | TokenProvider | UnsetType = _UNSET,
    keyspace: str | None = None,
    request_timeout_ms: int | None = None,
    timeout_ms: int | None = None,
    api_options: APIOptions | None = None,
) -> AstraDBDatabaseInfo | None:
    """
    Fetch database information through the DevOps API.

    Args:
        api_endpoint: a full API endpoint for the Data API.
        token: a valid token to access the database information.
            If provided, overrides any token info found in api_options.
        keyspace: the desired keyspace that will be used in the result.
            If not specified, the resulting database info will show it as None.
        request_timeout_ms: a timeout, in milliseconds, for waiting on a response.
        timeout_ms: an alias for `request_timeout_ms`.
        api_options: a (possibly partial) specification of the API Options to use.

    Returns:
        An AstraDBDatabaseInfo object.
        If the API endpoint fails to be parsed, None is returned.
        For valid-looking endpoints, if something goes wrong an exception is raised.
    """

    parsed_endpoint = parse_api_endpoint(api_endpoint)
    if parsed_endpoint:
        gd_response = fetch_raw_database_info_from_id_token(
            id=parsed_endpoint.database_id,
            token=token,
            environment=parsed_endpoint.environment,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
            api_options=api_options,
        )
        raw_info = gd_response
        return AstraDBDatabaseInfo(
            environment=parsed_endpoint.environment,
            api_endpoint=api_endpoint,
            raw_dict=raw_info,
        )
    else:
        return None


async def async_fetch_database_info(
    api_endpoint: str,
    token: str | TokenProvider | UnsetType = _UNSET,
    keyspace: str | None = None,
    request_timeout_ms: int | None = None,
    timeout_ms: int | None = None,
    api_options: APIOptions | None = None,
) -> AstraDBDatabaseInfo | None:
    """
    Fetch database information through the DevOps API.
    Async version of the function, for use in an asyncio context.

    Args:
        api_endpoint: a full API endpoint for the Data API.
        token: a valid token to access the database information.
            If provided, overrides any token info found in api_options.
        keyspace: the desired keyspace that will be used in the result.
            If not specified, the resulting database info will show it as None.
        request_timeout_ms: a timeout, in milliseconds, for waiting on a response.
        timeout_ms: an alias for `request_timeout_ms`.
        api_options: a (possibly partial) specification of the API Options to use.

    Returns:
        An AstraDBDatabaseInfo object.
        If the API endpoint fails to be parsed, None is returned.
        For valid-looking endpoints, if something goes wrong an exception is raised.
    """

    parsed_endpoint = parse_api_endpoint(api_endpoint)
    if parsed_endpoint:
        gd_response = await async_fetch_raw_database_info_from_id_token(
            id=parsed_endpoint.database_id,
            token=token,
            environment=parsed_endpoint.environment,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
            api_options=api_options,
        )
        raw_info = gd_response
        return AstraDBDatabaseInfo(
            environment=parsed_endpoint.environment,
            api_endpoint=api_endpoint,
            raw_dict=raw_info,
        )
    else:
        return None


def _recast_as_admin_database_info(
    admin_database_info_dict: dict[str, Any],
    *,
    environment: str,
) -> AstraDBAdminDatabaseInfo:
    return AstraDBAdminDatabaseInfo(
        environment=environment,
        raw_dict=admin_database_info_dict,
    )


class AstraDBAdmin:
    """
    An "admin" object, able to perform administrative tasks at the databases
    level, such as creating, listing or dropping databases.

    This class is not meant for direct instantiation by the user, rather
    it is obtained by invoking methods such as `get_admin`
    of AstraDBClient.

    Args:
        api_options: a complete specification of the API Options for this instance.

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

    Note:
        a more powerful token may be required than the one sufficient for working
        in the Database, Collection and Table classes. Check the provided token
        if "Unauthorized" errors are encountered.
    """

    def __init__(
        self,
        *,
        api_options: FullAPIOptions,
    ) -> None:
        if api_options.environment not in Environment.astra_db_values:
            raise InvalidEnvironmentException(
                "Environments outside of Astra DB are not supported."
            )

        self.api_options = api_options
        self._dev_ops_commander_headers: dict[str, str | None]
        if self.api_options.token:
            _token_str = self.api_options.token.get_token()
            self._dev_ops_commander_headers = {
                DEFAULT_DEV_OPS_AUTH_HEADER: f"{DEFAULT_DEV_OPS_AUTH_PREFIX}{_token_str}",
                **self.api_options.admin_additional_headers,
            }
        else:
            self._dev_ops_commander_headers = {
                **self.api_options.admin_additional_headers,
            }
        self._dev_ops_api_commander = self._get_dev_ops_api_commander()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.api_options})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, AstraDBAdmin):
            return all([self.api_options == other.api_options])
        else:
            return False

    def _get_dev_ops_api_commander(self) -> APICommander:
        """Instantiate a new APICommander based on the properties of this class."""
        base_path_components = [
            comp
            for comp in (
                ncomp.strip("/")
                for ncomp in (
                    self.api_options.dev_ops_api_url_options.dev_ops_api_version,
                    "databases",
                )
                if ncomp is not None
            )
            if comp != ""
        ]
        dev_ops_base_path = "/".join(base_path_components)
        dev_ops_commander = APICommander(
            api_endpoint=self.api_options.dev_ops_api_url_options.dev_ops_url,
            path=dev_ops_base_path,
            headers=self._dev_ops_commander_headers,
            callers=self.api_options.callers,
            dev_ops_api=True,
            redacted_header_names=self.api_options.redacted_header_names,
        )
        return dev_ops_commander

    def _copy(
        self,
        *,
        token: str | TokenProvider | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> AstraDBAdmin:
        arg_api_options = APIOptions(
            token=token,
        )
        final_api_options = self.api_options.with_override(api_options).with_override(
            arg_api_options
        )
        return AstraDBAdmin(api_options=final_api_options)

    def with_options(
        self,
        *,
        token: str | TokenProvider | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> AstraDBAdmin:
        """
        Create a clone of this AstraDBAdmin with some changed attributes.

        Args:
            token: an Access Token to the database. Example: `"AstraCS:xyz..."`.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            api_options: any additional options to set for the clone, in the form of
                an APIOptions instance (where one can set just the needed attributes).
                In case the same setting is also provided as named parameter,
                the latter takes precedence.

        Returns:
            a new AstraDBAdmin instance.

        Example:
            >>> different_auth_astra_db_admin = my_astra_db_admin.with_options(
            ...     token="AstraCS:xyz...",
            ... )
        """

        return self._copy(
            token=token,
            api_options=api_options,
        )

    def list_databases(
        self,
        *,
        include: str | None = None,
        provider: str | None = None,
        page_size: int | None = None,
        database_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[AstraDBAdminDatabaseInfo]:
        """
        Get the list of databases, as obtained with a request to the DevOps API.

        Args:
            include: a filter on what databases are to be returned. As per
                DevOps API, defaults to "nonterminated". Pass "all" to include
                the already terminated databases.
            provider: a filter on the cloud provider for the databases.
                As per DevOps API, defaults to "ALL". Pass e.g. "AWS" to
                restrict the results.
            page_size: number of results per page from the DevOps API.
            database_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (While in the case of very many databases this method may entail
                multiple DevOps API requests, it is assumed here that this method
                amounts almost always to one single request: the only timeout
                imposed on this method execution is one acting on each individual
                request, with no checks on its overall completion time.)
            request_timeout_ms: an alias for `database_admin_timeout_ms`.
            timeout_ms: an alias for `database_admin_timeout_ms`.

        Returns:
            A list of AstraDBAdminDatabaseInfo objects.

        Example:
            >>> database_list = my_astra_db_admin.list_databases()
            >>> len(database_list)
            3
            >>> database_list[2].id
            '01234567-...'
            >>> database_list[2].status
            'ACTIVE'
            >>> database_list[2].info.region
            'eu-west-1'
        """

        _database_admin_timeout_ms, _da_label = _select_singlereq_timeout_da(
            timeout_options=self.api_options.timeout_options,
            database_admin_timeout_ms=database_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        return self._list_databases_ctx(
            include=include,
            provider=provider,
            page_size=page_size,
            timeout_context=_TimeoutContext(
                request_ms=_database_admin_timeout_ms, label=_da_label
            ),
        )

    def _list_databases_ctx(
        self,
        *,
        include: str | None,
        provider: str | None,
        page_size: int | None,
        timeout_context: _TimeoutContext,
    ) -> list[AstraDBAdminDatabaseInfo]:
        # version of the method, but with timeouts made into a _TimeoutContext
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
            timeout_context=timeout_context,
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
                timeout_context=timeout_context,
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
        return [
            _recast_as_admin_database_info(
                db_dict,
                environment=self.api_options.environment,
            )
            for response in responses
            for db_dict in response
        ]

    async def async_list_databases(
        self,
        *,
        include: str | None = None,
        provider: str | None = None,
        page_size: int | None = None,
        database_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[AstraDBAdminDatabaseInfo]:
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
            page_size: number of results per page from the DevOps API.
            database_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (While in the case of very many databases this method may entail
                multiple DevOps API requests, it is assumed here that this method
                amounts almost always to one single request: the only timeout
                imposed on this method execution is one acting on each individual
                request, with no checks on its overall completion time.)
            request_timeout_ms: an alias for `database_admin_timeout_ms`.
            timeout_ms: an alias for `database_admin_timeout_ms`.

        Returns:
            A list of AstraDBAdminDatabaseInfo objects.

        Example:
            >>> async def check_if_db_exists(db_id: str) -> bool:
            ...     db_list = await my_astra_db_admin.async_list_databases()
            ...     return db_id in db_list
            ...
            >>> asyncio.run(check_if_db_exists("xyz"))
            True
            >>> asyncio.run(check_if_db_exists("01234567-..."))
            False
        """

        _database_admin_timeout_ms, _da_label = _select_singlereq_timeout_da(
            timeout_options=self.api_options.timeout_options,
            database_admin_timeout_ms=database_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        return await self._async_list_databases_ctx(
            include=include,
            provider=provider,
            page_size=page_size,
            timeout_context=_TimeoutContext(
                request_ms=_database_admin_timeout_ms, label=_da_label
            ),
        )

    async def _async_list_databases_ctx(
        self,
        *,
        include: str | None,
        provider: str | None,
        page_size: int | None,
        timeout_context: _TimeoutContext,
    ) -> list[AstraDBAdminDatabaseInfo]:
        # version of the method, but with timeouts made into a _TimeoutContext
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
            timeout_context=timeout_context,
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
                timeout_context=timeout_context,
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
        return [
            _recast_as_admin_database_info(
                db_dict,
                environment=self.api_options.environment,
            )
            for response in responses
            for db_dict in response
        ]

    def database_info(
        self,
        id: str,
        *,
        database_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> AstraDBAdminDatabaseInfo:
        """
        Get the full information on a given database, through a request to the DevOps API.

        Args:
            id: the ID of the target database, e. g.
                "01234567-89ab-cdef-0123-456789abcdef".
            database_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying DevOps API request.
                If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `database_admin_timeout_ms`.
            timeout_ms: an alias for `database_admin_timeout_ms`.

        Returns:
            An AstraDBAdminDatabaseInfo object.

        Example:
            >>> details_of_my_db = my_astra_db_admin.database_info("01234567-...")
            >>> details_of_my_db.id
            '01234567-...'
            >>> details_of_my_db.status
            'ACTIVE'
            >>> details_of_my_db.info.region
            'eu-west-1'
        """

        _database_admin_timeout_ms, _da_label = _select_singlereq_timeout_da(
            timeout_options=self.api_options.timeout_options,
            database_admin_timeout_ms=database_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        return self._database_info_ctx(
            id=id,
            timeout_context=_TimeoutContext(
                request_ms=_database_admin_timeout_ms, label=_da_label
            ),
        )

    def _database_info_ctx(
        self,
        id: str,
        *,
        timeout_context: _TimeoutContext,
    ) -> AstraDBAdminDatabaseInfo:
        # version of the method, but with timeouts made into a _TimeoutContext
        logger.info(f"getting database info for '{id}' (DevOps API)")
        gd_response = self._dev_ops_api_commander.request(
            http_method=HttpMethod.GET,
            additional_path=id,
            timeout_context=timeout_context,
        )
        logger.info(f"finished getting database info for '{id}' (DevOps API)")
        return _recast_as_admin_database_info(
            gd_response,
            environment=self.api_options.environment,
        )

    async def async_database_info(
        self,
        id: str,
        *,
        database_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> AstraDBAdminDatabaseInfo:
        """
        Get the full information on a given database, through a request to the DevOps API.
        This is an awaitable method suitable for use within an asyncio event loop.

        Args:
            id: the ID of the target database, e. g.
                "01234567-89ab-cdef-0123-456789abcdef".
            database_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying DevOps API request.
                If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `database_admin_timeout_ms`.
            timeout_ms: an alias for `database_admin_timeout_ms`.

        Returns:
            An AstraDBAdminDatabaseInfo object.

        Example:
            >>> async def check_if_db_active(db_id: str) -> bool:
            ...     db_info = await my_astra_db_admin.async_database_info(db_id)
            ...     return db_info.status == "ACTIVE"
            ...
            >>> asyncio.run(check_if_db_active("01234567-..."))
            True
        """

        _database_admin_timeout_ms, _da_label = _select_singlereq_timeout_da(
            timeout_options=self.api_options.timeout_options,
            database_admin_timeout_ms=database_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        return await self._async_database_info_ctx(
            id=id,
            timeout_context=_TimeoutContext(
                request_ms=_database_admin_timeout_ms, label=_da_label
            ),
        )

    async def _async_database_info_ctx(
        self,
        id: str,
        *,
        timeout_context: _TimeoutContext,
    ) -> AstraDBAdminDatabaseInfo:
        # version of the method, but with timeouts made into a _TimeoutContext
        logger.info(f"getting database info for '{id}' (DevOps API), async")
        gd_response = await self._dev_ops_api_commander.async_request(
            http_method=HttpMethod.GET,
            additional_path=id,
            timeout_context=timeout_context,
        )
        logger.info(f"finished getting database info for '{id}' (DevOps API), async")
        return _recast_as_admin_database_info(
            gd_response,
            environment=self.api_options.environment,
        )

    def create_database(
        self,
        name: str,
        *,
        cloud_provider: str,
        region: str,
        keyspace: str | None = None,
        wait_until_active: bool = True,
        database_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
        token: str | TokenProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> AstraDBDatabaseAdmin:
        """
        Create a database as requested, optionally waiting for it to be ready.

        Args:
            name: the desired name for the database.
            cloud_provider: one of 'aws', 'gcp' or 'azure'.
            region: any of the available cloud regions.
            keyspace: name for the one keyspace the database starts with.
                If omitted, DevOps API will use its default.
            wait_until_active: if True (default), the method returns only after
                the newly-created database is in ACTIVE state (a few minutes,
                usually). If False, it will return right after issuing the
                creation request to the DevOps API, and it will be responsibility
                of the caller to check the database status before working with it.
            database_admin_timeout_ms: a timeout, in milliseconds, for the whole
                requested operation to complete. This is used only
                if `wait_until_active` is true, i.e. if the method call must
                wait and keep querying the DevOps API for the status of the
                newly-created database.
            request_timeout_ms: a timeout, in milliseconds, for
                each underlying DevOps API HTTP request.
            timeout_ms: an alias for *both* the `request_timeout_ms` and
                `database_admin_timeout_ms` timeout parameters. In practice,
                regardless of `wait_until_active`, this parameter dictates an
                overall timeout on this method call.
            token: if supplied, is passed to the returned Database instead of
                the one set for this object.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            spawn_api_options: a specification - complete or partial - of the
                API Options to override the defaults inherited from the AstraDBAdmin.
                This allows for a deeper configuration of the database admin, e.g.
                concerning timeouts; if this is passed together with
                the named timeout parameters, the latter will take precedence
                in their respective settings.

        Returns:
            An AstraDBDatabaseAdmin instance.

        Note: a timeout event is no guarantee at all that the
        creation request has not reached the API server and is not going
        to be, in fact, honored.

        Example:
            >>> my_new_db_admin = my_astra_db_admin.create_database(
            ...     "new_database",
            ...     cloud_provider="aws",
            ...     region="ap-south-1",
            ... )
            >>> my_new_db = my_new_db_admin.get_database()
            >>> my_coll = my_new_db.create_collection(
            ...     "movies",
            ...     definition=(
            ...         CollectionDefinition.builder()
            ...         .set_vector_dimension(2)
            ...         .build()
            ...     )
            ... )
            >>> my_coll.insert_one({"title": "The Title", "$vector": [0.1, 0.2]})
        """

        _database_admin_timeout_ms, _da_label = _first_valid_timeout(
            (database_admin_timeout_ms, "database_admin_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (
                self.api_options.timeout_options.database_admin_timeout_ms,
                "database_admin_timeout_ms",
            ),
        )
        _request_timeout_ms, _rt_label = _first_valid_timeout(
            (request_timeout_ms, "request_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (self.api_options.timeout_options.request_timeout_ms, "request_timeout_ms"),
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
                "keyspace": keyspace,
            }.items()
            if v is not None
        }
        timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=_database_admin_timeout_ms,
            dev_ops_api=True,
            timeout_label=_da_label,
        )
        logger.info(
            f"creating database {name}/({cloud_provider}, {region}) (DevOps API)"
        )
        cd_raw_response = self._dev_ops_api_commander.raw_request(
            http_method=HttpMethod.POST,
            payload=cd_payload,
            timeout_context=timeout_manager.remaining_timeout(
                cap_time_ms=_request_timeout_ms,
                cap_timeout_label=_rt_label,
            ),
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
                last_db_info = self._database_info_ctx(
                    id=new_database_id,
                    timeout_context=timeout_manager.remaining_timeout(
                        cap_time_ms=_request_timeout_ms,
                        cap_timeout_label=_rt_label,
                    ),
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
        _final_api_options = self.api_options.with_override(
            spawn_api_options
        ).with_override(APIOptions(token=token))
        return AstraDBDatabaseAdmin.from_astra_db_admin(
            api_endpoint=build_api_endpoint(
                environment=self.api_options.environment,
                database_id=new_database_id,
                region=region,
            ),
            astra_db_admin=self,
            spawn_api_options=_final_api_options,
        )

    async def async_create_database(
        self,
        name: str,
        *,
        cloud_provider: str,
        region: str,
        keyspace: str | None = None,
        wait_until_active: bool = True,
        database_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
        token: str | TokenProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
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
            wait_until_active: if True (default), the method returns only after
                the newly-created database is in ACTIVE state (a few minutes,
                usually). If False, it will return right after issuing the
                creation request to the DevOps API, and it will be responsibility
                of the caller to check the database status before working with it.
            database_admin_timeout_ms: a timeout, in milliseconds, for the whole
                requested operation to complete. This is used only
                if `wait_until_active` is true, i.e. if the method call must
                wait and keep querying the DevOps API for the status of the
                newly-created database.
            request_timeout_ms: a timeout, in milliseconds, for
                each underlying DevOps API HTTP request.
            timeout_ms: an alias for *both* the `request_timeout_ms` and
                `database_admin_timeout_ms` timeout parameters. In practice,
                regardless of `wait_until_active`, this parameter dictates an
                overall timeout on this method call.
            token: if supplied, is passed to the returned Database instead of
                the one set for this object.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            spawn_api_options: a specification - complete or partial - of the
                API Options to override the defaults inherited from the AstraDBAdmin.
                This allows for a deeper configuration of the database admin, e.g.
                concerning timeouts; if this is passed together with
                the named timeout parameters, the latter will take precedence
                in their respective settings.

        Returns:
            An AstraDBDatabaseAdmin instance.

        Note: a timeout event is no guarantee at all that the
        creation request has not reached the API server and is not going
        to be, in fact, honored.

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

        _database_admin_timeout_ms, _da_label = _first_valid_timeout(
            (database_admin_timeout_ms, "database_admin_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (
                self.api_options.timeout_options.database_admin_timeout_ms,
                "database_admin_timeout_ms",
            ),
        )
        _request_timeout_ms, _rt_label = _first_valid_timeout(
            (request_timeout_ms, "request_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (self.api_options.timeout_options.request_timeout_ms, "request_timeout_ms"),
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
                "keyspace": keyspace,
            }.items()
            if v is not None
        }
        timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=_database_admin_timeout_ms,
            dev_ops_api=True,
            timeout_label=_da_label,
        )
        logger.info(
            f"creating database {name}/({cloud_provider}, {region}) "
            "(DevOps API), async"
        )
        cd_raw_response = await self._dev_ops_api_commander.async_raw_request(
            http_method=HttpMethod.POST,
            payload=cd_payload,
            timeout_context=timeout_manager.remaining_timeout(
                cap_time_ms=_request_timeout_ms,
                cap_timeout_label=_rt_label,
            ),
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
                last_db_info = await self._async_database_info_ctx(
                    id=new_database_id,
                    timeout_context=timeout_manager.remaining_timeout(
                        cap_time_ms=_request_timeout_ms,
                        cap_timeout_label=_rt_label,
                    ),
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
        _final_api_options = self.api_options.with_override(
            spawn_api_options
        ).with_override(APIOptions(token=token))
        return AstraDBDatabaseAdmin.from_astra_db_admin(
            api_endpoint=build_api_endpoint(
                environment=self.api_options.environment,
                database_id=new_database_id,
                region=region,
            ),
            astra_db_admin=self,
            spawn_api_options=_final_api_options,
        )

    def drop_database(
        self,
        id: str,
        *,
        wait_until_active: bool = True,
        database_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
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
            database_admin_timeout_ms: a timeout, in milliseconds, for the whole
                requested operation to complete. This is used only
                if `wait_until_active` is true, i.e. if the method call must
                wait and keep querying the DevOps API for the status of the
                newly-deleted database.
            request_timeout_ms: a timeout, in milliseconds, for
                each underlying DevOps API HTTP request.
            timeout_ms: an alias for *both* the `request_timeout_ms` and
                `database_admin_timeout_ms` timeout parameters. In practice,
                regardless of `wait_until_active`, this parameter dictates an
                overall timeout on this method call.

        Note: a timeout event is no guarantee at all that the
        deletion request has not reached the API server and is not going
        to be, in fact, honored.

        Example:
            >>> database_list_pre = my_astra_db_admin.list_databases()
            >>> len(database_list_pre)
            3
            >>> my_astra_db_admin.drop_database("01234567-...")
            >>> database_list_post = my_astra_db_admin.list_databases()
            >>> len(database_list_post)
            2
        """

        _database_admin_timeout_ms, _da_label = _first_valid_timeout(
            (database_admin_timeout_ms, "database_admin_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (
                self.api_options.timeout_options.database_admin_timeout_ms,
                "database_admin_timeout_ms",
            ),
        )
        _request_timeout_ms, _rt_label = _first_valid_timeout(
            (request_timeout_ms, "request_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (self.api_options.timeout_options.request_timeout_ms, "request_timeout_ms"),
        )
        timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=_database_admin_timeout_ms,
            dev_ops_api=True,
            timeout_label=_da_label,
        )
        logger.info(f"dropping database '{id}' (DevOps API)")
        te_raw_response = self._dev_ops_api_commander.raw_request(
            http_method=HttpMethod.POST,
            additional_path=f"{id}/terminate",
            timeout_context=timeout_manager.remaining_timeout(
                cap_time_ms=_request_timeout_ms,
                cap_timeout_label=_rt_label,
            ),
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
                    for a_db_info in self._list_databases_ctx(
                        include=None,
                        provider=None,
                        page_size=None,
                        timeout_context=timeout_manager.remaining_timeout(
                            cap_time_ms=_request_timeout_ms,
                            cap_timeout_label=_rt_label,
                        ),
                    )
                    if a_db_info.id == id
                ]
                if detected_databases:
                    last_status_seen = detected_databases[0].status
                    _db_name = detected_databases[0].name
                else:
                    last_status_seen = None
            if last_status_seen is not None:
                _name_desc = f" ({_db_name})" if _db_name else ""
                raise DevOpsAPIException(
                    f"Database {id}{_name_desc} entered unexpected status "
                    f"{last_status_seen} after PENDING"
                )
        logger.info(f"finished dropping database '{id}' (DevOps API)")

    async def async_drop_database(
        self,
        id: str,
        *,
        wait_until_active: bool = True,
        database_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
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
            database_admin_timeout_ms: a timeout, in milliseconds, for the whole
                requested operation to complete. This is used only
                if `wait_until_active` is true, i.e. if the method call must
                wait and keep querying the DevOps API for the status of the
                newly-deleted database.
            request_timeout_ms: a timeout, in milliseconds, for
                each underlying DevOps API HTTP request.
            timeout_ms: an alias for *both* the `request_timeout_ms` and
                `database_admin_timeout_ms` timeout parameters. In practice,
                regardless of `wait_until_active`, this parameter dictates an
                overall timeout on this method call.

        Note: a timeout event is no guarantee at all that the
        deletion request has not reached the API server and is not going
        to be, in fact, honored.

        Example:
            >>> asyncio.run(
            ...     my_astra_db_admin.async_drop_database("01234567-...")
            ... )
        """

        _database_admin_timeout_ms, _da_label = _first_valid_timeout(
            (database_admin_timeout_ms, "database_admin_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (
                self.api_options.timeout_options.database_admin_timeout_ms,
                "database_admin_timeout_ms",
            ),
        )
        _request_timeout_ms, _rt_label = _first_valid_timeout(
            (request_timeout_ms, "request_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (self.api_options.timeout_options.request_timeout_ms, "request_timeout_ms"),
        )
        timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=_database_admin_timeout_ms,
            dev_ops_api=True,
            timeout_label=_da_label,
        )
        logger.info(f"dropping database '{id}' (DevOps API), async")
        te_raw_response = await self._dev_ops_api_commander.async_raw_request(
            http_method=HttpMethod.POST,
            additional_path=f"{id}/terminate",
            timeout_context=timeout_manager.remaining_timeout(
                cap_time_ms=_request_timeout_ms,
                cap_timeout_label=_rt_label,
            ),
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
                    for a_db_info in await self._async_list_databases_ctx(
                        include=None,
                        provider=None,
                        page_size=None,
                        timeout_context=timeout_manager.remaining_timeout(
                            cap_time_ms=_request_timeout_ms,
                            cap_timeout_label=_rt_label,
                        ),
                    )
                    if a_db_info.id == id
                ]
                if detected_databases:
                    last_status_seen = detected_databases[0].status
                    _db_name = detected_databases[0].name
                else:
                    last_status_seen = None
            if last_status_seen is not None:
                _name_desc = f" ({_db_name})" if _db_name else ""
                raise DevOpsAPIException(
                    f"Database {id}{_name_desc} entered unexpected status "
                    f"{last_status_seen} after PENDING"
                )
        logger.info(f"finished dropping database '{id}' (DevOps API), async")

    def get_database_admin(
        self,
        api_endpoint_or_id: str | None = None,
        *,
        api_endpoint: str | None = None,
        id: str | None = None,
        region: str | None = None,
        token: str | TokenProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> AstraDBDatabaseAdmin:
        """
        Create an AstraDBDatabaseAdmin object for admin work within a certain database.

        The database can be specified by its API endpoint or, alternatively,
        by its (id, region) parameters: these two call patterns exclude each other.

        Args:
            api_endpoint_or_id: positional parameter that can stand for both
                `api_endpoint` and `id`. Passing them together is an error.
            api_endpoint: the API Endpoint for the target database
                (i.e. `https://<ID>-<REGION>.apps.astra.datastax.com`.
                Note that no 'Custom Domain' endpoints are accepted).
                The database must exist already for the resulting object
                to be effectively used; in other words, this invocation
                does not create the database, just the object instance.
            id: the target database ID. This is alternative to using the API Endpoint.
            region: the region to use for connecting to the database.
                This parameter must be supplied if (and only if) the `id` is
                given for the database instead of the full API endpoint.
            token: if supplied, is passed to the Database instead of
                the one set for this object.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            spawn_api_options: a specification - complete or partial - of the
                API Options to override the defaults inherited from the AstraDBAdmin.
                This allows for a deeper configuration of the database admin, i.e.
                beyond just specifying a token; if this is passed together with
                the named token parameter, the latter will take precedence.

        Returns:
            An AstraDBDatabaseAdmin instance representing the requested database.

        Example:
            >>> my_db_admin = my_astra_db_admin.get_database_admin("01234567-...")
            >>> my_db_admin.list_keyspaces()
            ['default_keyspace']
            >>> my_db_admin.create_keyspace("that_other_one")
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
        resulting_api_options = self.api_options.with_override(
            spawn_api_options
        ).with_override(
            APIOptions(token=token),
        )

        # handle the "endpoint passed as id" case first:
        if _api_endpoint_p is not None:
            if region is not None:
                raise ValueError(
                    "Parameter `region` not supported with an API endpoint."
                )
            return AstraDBDatabaseAdmin.from_astra_db_admin(
                api_endpoint=_api_endpoint_p,
                astra_db_admin=self,
                spawn_api_options=resulting_api_options,
            )
        else:
            if _id_p is None:
                raise ValueError("Either `api_endpoint` or `id` must be supplied.")
            if region is None:
                raise ValueError("Parameter `region` must be supplied with `id`.")

            return AstraDBDatabaseAdmin.from_astra_db_admin(
                api_endpoint=build_api_endpoint(
                    environment=self.api_options.environment,
                    database_id=_id_p,
                    region=region,
                ),
                astra_db_admin=self,
                spawn_api_options=resulting_api_options,
            )

    def get_database(
        self,
        api_endpoint_or_id: str | None = None,
        *,
        api_endpoint: str | None = None,
        keyspace: str | None = None,
        id: str | None = None,
        region: str | None = None,
        token: str | TokenProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> Database:
        """
        Create a Database instance for a specific database, to be used
        when doing data-level work (such as creating/managing collections).

        The database can be specified by its API endpoint or, alternatively,
        by its (id, region) parameters: these two call patterns exclude each other.

        Args:
            api_endpoint_or_id: positional parameter that can stand for both
                `api_endpoint` and `id`. Passing them together is an error.
            api_endpoint: the API Endpoint for the target database
                (e.g. `https://<ID>-<REGION>.apps.astra.datastax.com`,
                or a custom domain if one is configured for the database).
                The database must exist already for the resulting object
                to be effectively used; in other words, this invocation
                does not create the database, just the object instance.
            keyspace: if provided, it is passed to the Database; otherwise
                the Database class will apply an environment-specific default.
            id: the target database ID. This is alternative to using the API Endpoint.
            region: the region to use for connecting to the database.
                This parameter must be supplied if (and only if) the `id` is
                given for the database instead of the full API endpoint.
            token: if supplied, is passed to the Database instead of
                the one set for this object.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            spawn_api_options: a specification - complete or partial - of the
                API Options to override the defaults inherited from the AstraDBAdmin.
                This allows for a deeper configuration of the database, i.e. beyond
                just specifying a token; if this is passed together with
                the named token parameter, the latter will take precedence.

        Returns:
            A Database object ready to be used.

        Example:
            >>> my_db = my_astra_db_admin.get_database(
            ...     "https://<ID>-<REGION>.apps.astra.datastax.com",
            ...     keyspace="my_prod_keyspace",
            ... )
            >>> coll = my_db.create_collection(
            ...     "movies",
            ...     definition=(
            ...         CollectionDefinition.builder()
            ...         .set_vector_dimension(2)
            ...         .build()
            ...     )
            ... )
            >>> my_coll.insert_one({"title": "The Title", "$vector": [0.3, 0.4]})
        """

        _api_endpoint_p, _id_p = check_id_endpoint_parg_kwargs(
            p_arg=api_endpoint_or_id, api_endpoint=api_endpoint, id=id
        )
        # lazy importing here to avoid circular dependency
        from astrapy import Database

        resulting_api_options = self.api_options.with_override(
            spawn_api_options
        ).with_override(
            APIOptions(token=token),
        )

        # handle the "endpoint passed as id" case first:
        if _api_endpoint_p is not None:
            if region is not None:
                raise ValueError(
                    "Parameter `region` not supported with an API endpoint."
                )

            parsed_api_endpoint = parse_api_endpoint(_api_endpoint_p)
            if parsed_api_endpoint is not None:
                if parsed_api_endpoint.environment != resulting_api_options.environment:
                    raise InvalidEnvironmentException(
                        "Environment mismatch between client and provided "
                        "API endpoint. You can try adding "
                        f'`environment="{parsed_api_endpoint.environment}"` '
                        "to the DataAPIClient creation statement."
                    )
                return Database(
                    api_endpoint=_api_endpoint_p,
                    keyspace=keyspace,
                    api_options=resulting_api_options,
                )
            else:
                msg = api_endpoint_parsing_cdinfo_message(_api_endpoint_p)
                logger.info(msg)
                return Database(
                    api_endpoint=_api_endpoint_p,
                    keyspace=keyspace,
                    api_options=resulting_api_options,
                )
        else:
            # the case where an ID is passed:
            if _id_p is None:
                raise ValueError("Either `api_endpoint` or `id` must be supplied.")
            if region is None:
                raise ValueError("Parameter `region` must be supplied with `id`.")

            _api_endpoint = build_api_endpoint(
                environment=self.api_options.environment,
                database_id=_id_p,
                region=region,
            )
            return Database(
                api_endpoint=_api_endpoint,
                keyspace=keyspace,
                api_options=resulting_api_options,
            )

    def get_async_database(
        self,
        api_endpoint_or_id: str | None = None,
        *,
        api_endpoint: str | None = None,
        keyspace: str | None = None,
        id: str | None = None,
        region: str | None = None,
        token: str | TokenProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncDatabase:
        """
        Create an AsyncDatabase instance for a specific database, to be used
        when doing data-level work (such as creating/managing collections).

        The database can be specified by its API endpoint or, alternatively,
        by its (id, region) parameters: these two call patterns exclude each other.

        Args:
            api_endpoint_or_id: positional parameter that can stand for both
                `api_endpoint` and `id`. Passing them together is an error.
            api_endpoint: the API Endpoint for the target database
                (e.g. `https://<ID>-<REGION>.apps.astra.datastax.com`,
                or a custom domain if one is configured for the database).
                The database must exist already for the resulting object
                to be effectively used; in other words, this invocation
                does not create the database, just the object instance.
            keyspace: if provided, it is passed to the AsyncDatabase; otherwise
                the AsyncDatabase class will apply an environment-specific default.
            id: the target database ID. This is alternative to using the API Endpoint.
            region: the region to use for connecting to the database.
                This parameter must be supplied if (and only if) the `id` is
                given for the database instead of the full API endpoint.
            token: if supplied, is passed to the AsyncDatabase instead of
                the one set for this object.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            spawn_api_options: a specification - complete or partial - of the
                API Options to override the defaults inherited from the AstraDBAdmin.
                This allows for a deeper configuration of the database, i.e. beyond
                just specifying a token; if this is passed together with
                the named token parameter, the latter will take precedence.

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
            ...     a_coll = await my_async_db.create_collection(
            ...         "movies",
            ...         definition=(
            ...             CollectionDefinition.builder()
            ...             .set_vector_dimension(2)
            ...             .build()
            ...         )
            ...     )
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

        return self.get_database(
            api_endpoint_or_id=api_endpoint_or_id,
            api_endpoint=api_endpoint,
            keyspace=keyspace,
            id=id,
            region=region,
            token=token,
            spawn_api_options=spawn_api_options,
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
    def list_keyspaces(self, *pargs: Any, **kwargs: Any) -> list[str]:
        """Get a list of keyspaces for the database."""
        ...

    @abstractmethod
    def create_keyspace(
        self,
        name: str,
        *,
        update_db_keyspace: bool | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Create a keyspace in the database.
        """
        ...

    @abstractmethod
    def drop_keyspace(self, name: str, *pargs: Any, **kwargs: Any) -> None:
        """
        Drop (delete) a keyspace from the database.
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
    async def async_create_keyspace(
        self,
        name: str,
        *,
        update_db_keyspace: bool | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Create a keyspace in the database.
        (Async version of the method.)
        """
        ...

    @abstractmethod
    async def async_drop_keyspace(self, name: str, *pargs: Any, **kwargs: Any) -> None:
        """
        Drop (delete) a keyspace from the database.
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
    def find_reranking_providers(
        self, *pargs: Any, **kwargs: Any
    ) -> FindRerankingProvidersResult:
        """Query the Data API for the available reranking providers."""
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

    @abstractmethod
    async def async_find_reranking_providers(
        self, *pargs: Any, **kwargs: Any
    ) -> FindRerankingProvidersResult:
        """
        Query the Data API for the available reranking providers.
        (Async version of the method.)
        """
        ...


class AstraDBDatabaseAdmin(DatabaseAdmin):
    """
    An "admin" object, able to perform administrative tasks at the keyspaces level
    (i.e. within a certain database), such as creating/listing/dropping keyspaces.

    This is one layer below the AstraDBAdmin concept, in that it is tied to
    a single database and enables admin work within it.

    This class is not meant for direct instantiation by the user, rather
    it is obtained by invoking methods such as `get_database_admin`
    of AstraDBAdmin.

    Args:
        api_endpoint: the API Endpoint for the target database
            (i.e. `https://<ID>-<REGION>.apps.astra.datastax.com`.
            Note that no 'Custom Domain' endpoints are accepted).
            The database must exist already for the resulting object
            to be effectively used; in other words, this invocation
            does not create the database, just the object instance.
            Database admin objects cannot work with 'Custom Domain' endpoints.
        api_options: a complete specification of the API Options for this instance.
        spawner_database: either a Database or an AsyncDatabase instance. This represents
            the database class which spawns this admin object, so that, if required,
            a keyspace creation can retroactively "use" the new keyspace in the spawner.
            Used to enable the Async/Database.get_admin_database().create_keyspace()
            pattern.
        spawner_astra_db_admin: an AstraDBAdmin instance. This, if provided, is
            the instance that spawned this Database Admin and is used to delegate
            operations such as drop, get_database and so on. If not passed, a new
            one is created automatically.

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

    Note:
        a more powerful token may be required than the one sufficient for working
        in the Database, Collection and Table classes. Check the provided token
        if "Unauthorized" errors are encountered.
    """

    def __init__(
        self,
        *,
        api_endpoint: str,
        api_options: FullAPIOptions,
        spawner_database: Database | AsyncDatabase | None = None,
        spawner_astra_db_admin: AstraDBAdmin | None = None,
    ) -> None:
        # lazy import here to avoid circular dependency
        from astrapy.database import Database

        if api_options.environment not in Environment.astra_db_values:
            raise InvalidEnvironmentException(
                "Environments outside of Astra DB are not supported."
            )

        self.api_options = api_options
        self.api_endpoint = api_endpoint
        parsed_api_endpoint = parse_api_endpoint(self.api_endpoint)
        if parsed_api_endpoint is None:
            msg = api_endpoint_parsing_error_message(self.api_endpoint)
            raise ValueError(msg)
        self._database_id = parsed_api_endpoint.database_id
        self._region = parsed_api_endpoint.region
        if parsed_api_endpoint.environment != self.api_options.environment:
            raise InvalidEnvironmentException(
                "Environment mismatch between client and provided "
                "API endpoint. You can try adding "
                f'`environment="{parsed_api_endpoint.environment}"` '
                "to the class constructor."
            )
        if spawner_database is not None:
            self.spawner_database = spawner_database
        else:
            # leaving the keyspace to its per-environment default
            # (a task for the Database)
            self.spawner_database = Database(
                api_endpoint=self.api_endpoint,
                keyspace=None,
                api_options=self.api_options,
            )

        # API-commander-specific init (for the vectorizeOps invocations)
        # even if Data API, this is admin and must use the Admin additional headers:
        self._commander_headers = {
            DEFAULT_DATA_API_AUTH_HEADER: self.api_options.token.get_token(),
            **self.api_options.admin_additional_headers,
        }
        self._api_commander = self._get_api_commander()

        # DevOps-API-commander specific init (keyspace CRUD, etc)
        self._dev_ops_commander_headers: dict[str, str | None]
        if self.api_options.token:
            _token = self.api_options.token.get_token()
            self._dev_ops_commander_headers = {
                DEFAULT_DEV_OPS_AUTH_HEADER: f"{DEFAULT_DEV_OPS_AUTH_PREFIX}{_token}",
                **self.api_options.admin_additional_headers,
            }
        else:
            self._dev_ops_commander_headers = {
                **self.api_options.admin_additional_headers,
            }
        self._dev_ops_api_commander = self._get_dev_ops_api_commander()

        # this class keeps a reference to the AstraDBAdmin associated to this org:
        if spawner_astra_db_admin is None:
            self._astra_db_admin = AstraDBAdmin(api_options=self.api_options)
        else:
            self._astra_db_admin = spawner_astra_db_admin

    def __repr__(self) -> str:
        parts = [
            f'api_endpoint="{self.api_endpoint}"',
            f"api_options={self.api_options}",
        ]
        return f"{self.__class__.__name__}({', '.join(parts)})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, AstraDBDatabaseAdmin):
            return all(
                [
                    self.api_endpoint == other.api_endpoint,
                    self.api_options == other.api_options,
                ]
            )
        else:
            return False

    def _get_api_commander(self) -> APICommander:
        """Instantiate a new APICommander for Data API calls."""
        base_path_components = [
            comp
            for comp in (
                ncomp.strip("/")
                for ncomp in (
                    self.api_options.data_api_url_options.api_path,
                    self.api_options.data_api_url_options.api_version,
                )
                if ncomp is not None
            )
            if comp != ""
        ]
        base_path = f"/{'/'.join(base_path_components)}"
        api_commander = APICommander(
            api_endpoint=self.api_endpoint,
            path=base_path,
            headers=self._commander_headers,
            callers=self.api_options.callers,
            redacted_header_names=self.api_options.redacted_header_names,
        )
        return api_commander

    def _get_dev_ops_api_commander(self) -> APICommander:
        """Instantiate a new APICommander for DevOps calls."""
        base_path_components = [
            comp
            for comp in (
                ncomp.strip("/")
                for ncomp in (
                    self.api_options.dev_ops_api_url_options.dev_ops_api_version,
                    "databases",
                    self._database_id,
                )
                if ncomp is not None
            )
            if comp != ""
        ]
        dev_ops_base_path = "/".join(base_path_components)
        dev_ops_commander = APICommander(
            api_endpoint=self.api_options.dev_ops_api_url_options.dev_ops_url,
            path=dev_ops_base_path,
            headers=self._dev_ops_commander_headers,
            callers=self.api_options.callers,
            dev_ops_api=True,
            redacted_header_names=self.api_options.redacted_header_names,
        )
        return dev_ops_commander

    def _copy(
        self,
        *,
        token: str | TokenProvider | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> AstraDBDatabaseAdmin:
        arg_api_options = APIOptions(
            token=token,
        )
        final_api_options = self.api_options.with_override(api_options).with_override(
            arg_api_options
        )
        return AstraDBDatabaseAdmin(
            api_endpoint=self.api_endpoint,
            api_options=final_api_options,
            spawner_database=self.spawner_database,
            spawner_astra_db_admin=self._astra_db_admin,
        )

    def with_options(
        self,
        *,
        token: str | TokenProvider | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> AstraDBDatabaseAdmin:
        """
        Create a clone of this AstraDBDatabaseAdmin with some changed attributes.

        Args:
            token: an Access Token to the database. Example: `"AstraCS:xyz..."`.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            api_options: any additional options to set for the clone, in the form of
                an APIOptions instance (where one can set just the needed attributes).
                In case the same setting is also provided as named parameter,
                the latter takes precedence.

        Returns:
            a new AstraDBDatabaseAdmin instance.

        Example:
            >>> admin_for_my_other_db = admin_for_my_db.with_options(
            ...     "https://<ID>-<REGION>.apps.astra.datastax.com",
            ... )
        """

        return self._copy(
            token=token,
            api_options=api_options,
        )

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
        spawn_api_options: APIOptions | UnsetType = _UNSET,
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
            spawn_api_options: a specification - complete or partial - of the
                API Options to override the defaults inherited from the AstraDBAdmin.
                This allows for a deeper configuration of the database, e.g.
                concerning timeouts.

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
            api_options=astra_db_admin.api_options.with_override(spawn_api_options),
            spawner_astra_db_admin=astra_db_admin,
        )

    def info(
        self,
        *,
        database_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> AstraDBAdminDatabaseInfo:
        """
        Query the DevOps API for the full info on this database.

        Args:
            database_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `database_admin_timeout_ms`.
            timeout_ms: an alias for `database_admin_timeout_ms`.

        Returns:
            An AstraDBAdminDatabaseInfo object.

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
            database_admin_timeout_ms=database_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info(f"finished getting info ('{self._database_id}')")
        return req_response

    async def async_info(
        self,
        *,
        database_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> AstraDBAdminDatabaseInfo:
        """
        Query the DevOps API for the full info on this database.
        Async version of the method, for use in an asyncio context.

        Args:
            database_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `database_admin_timeout_ms`.
            timeout_ms: an alias for `database_admin_timeout_ms`.

        Returns:
            An AstraDBAdminDatabaseInfo object.

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
            database_admin_timeout_ms=database_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info(f"finished getting info ('{self._database_id}'), async")
        return req_response

    def list_keyspaces(
        self,
        *,
        keyspace_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[str]:
        """
        Query the DevOps API for a list of the keyspaces in the database.

        Args:
            keyspace_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `keyspace_admin_timeout_ms`.
            timeout_ms: an alias for `keyspace_admin_timeout_ms`.

        Returns:
            A list of the keyspaces, each a string, in no particular order.

        Example:
            >>> admin_for_my_db.list_keyspaces()
            ['default_keyspace', 'staging_keyspace']
        """

        logger.info(f"getting keyspaces ('{self._database_id}')")
        info = self.info(
            database_admin_timeout_ms=keyspace_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info(f"finished getting keyspaces ('{self._database_id}')")
        if info.raw is None:
            raise DevOpsAPIException("Could not get the keyspace list.")
        else:
            return info.raw.get("info", {}).get("keyspaces") or []

    async def async_list_keyspaces(
        self,
        *,
        keyspace_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[str]:
        """
        Query the DevOps API for a list of the keyspaces in the database.
        Async version of the method, for use in an asyncio context.

        Args:
            keyspace_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `keyspace_admin_timeout_ms`.
            timeout_ms: an alias for `keyspace_admin_timeout_ms`.

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
        info = await self.async_info(
            database_admin_timeout_ms=keyspace_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info(f"finished getting keyspaces ('{self._database_id}'), async")
        if info.raw is None:
            raise DevOpsAPIException("Could not get the keyspace list.")
        else:
            return info.raw.get("info", {}).get("keyspaces") or []

    def create_keyspace(
        self,
        name: str,
        *,
        wait_until_active: bool = True,
        update_db_keyspace: bool | None = None,
        keyspace_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
        **kwargs: Any,
    ) -> None:
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
            keyspace_admin_timeout_ms: a timeout, in milliseconds, for the whole
                requested operation to complete. This is used only
                if `wait_until_active` is true, i.e. if the method call must
                wait and keep querying the DevOps API for the status of the
                database during keyspace creation.
            request_timeout_ms: a timeout, in milliseconds, for
                each underlying DevOps API HTTP request.
            timeout_ms: an alias for *both* the `request_timeout_ms` and
                `keyspace_admin_timeout_ms` timeout parameters. In practice,
                regardless of `wait_until_active`, this parameter dictates an
                overall timeout on this method call.

        Note: a timeout event is no guarantee at all that the
        creation request has not reached the API server and is not going
        to be, in fact, honored.

        Example:
            >>> my_db_admin.keyspaces()
            ['default_keyspace']
            >>> my_db_admin.create_keyspace("that_other_one")
            >>> my_db_admin.list_keyspaces()
            ['default_keyspace', 'that_other_one']
        """

        _keyspace_admin_timeout_ms, _ka_label = _first_valid_timeout(
            (keyspace_admin_timeout_ms, "keyspace_admin_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (
                self.api_options.timeout_options.keyspace_admin_timeout_ms,
                "keyspace_admin_timeout_ms",
            ),
        )
        _request_timeout_ms, _rt_label = _first_valid_timeout(
            (request_timeout_ms, "request_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (self.api_options.timeout_options.request_timeout_ms, "request_timeout_ms"),
        )
        timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=_keyspace_admin_timeout_ms,
            dev_ops_api=True,
            timeout_label=_ka_label,
        )
        logger.info(
            f"creating keyspace '{name}' on " f"'{self._database_id}' (DevOps API)"
        )
        cn_raw_response = self._dev_ops_api_commander.raw_request(
            http_method=HttpMethod.POST,
            additional_path=f"keyspaces/{name}",
            timeout_context=timeout_manager.remaining_timeout(
                cap_time_ms=_request_timeout_ms,
                cap_timeout_label=_rt_label,
            ),
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
                last_status_seen = self._astra_db_admin._database_info_ctx(
                    id=self._database_id,
                    timeout_context=timeout_manager.remaining_timeout(
                        cap_time_ms=_request_timeout_ms,
                        cap_timeout_label=_rt_label,
                    ),
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
        if update_db_keyspace:
            self.spawner_database.use_keyspace(name)

    async def async_create_keyspace(
        self,
        name: str,
        *,
        wait_until_active: bool = True,
        update_db_keyspace: bool | None = None,
        keyspace_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
        **kwargs: Any,
    ) -> None:
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
            keyspace_admin_timeout_ms: a timeout, in milliseconds, for the whole
                requested operation to complete. This is used only
                if `wait_until_active` is true, i.e. if the method call must
                wait and keep querying the DevOps API for the status of the
                database during keyspace creation.
            request_timeout_ms: a timeout, in milliseconds, for
                each underlying DevOps API HTTP request.
            timeout_ms: an alias for *both* the `request_timeout_ms` and
                `keyspace_admin_timeout_ms` timeout parameters. In practice,
                regardless of `wait_until_active`, this parameter dictates an
                overall timeout on this method call.

        Note: a timeout event is no guarantee at all that the
        creation request has not reached the API server and is not going
        to be, in fact, honored.

        Example:
            >>> asyncio.run(
            ...     my_db_admin.async_create_keyspace("app_keyspace")
            ... )
        """

        _keyspace_admin_timeout_ms, _ka_label = _first_valid_timeout(
            (keyspace_admin_timeout_ms, "keyspace_admin_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (
                self.api_options.timeout_options.keyspace_admin_timeout_ms,
                "keyspace_admin_timeout_ms",
            ),
        )
        _request_timeout_ms, _rt_label = _first_valid_timeout(
            (request_timeout_ms, "request_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (self.api_options.timeout_options.request_timeout_ms, "request_timeout_ms"),
        )
        timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=_keyspace_admin_timeout_ms,
            dev_ops_api=True,
            timeout_label=_ka_label,
        )
        logger.info(
            f"creating keyspace '{name}' on "
            f"'{self._database_id}' (DevOps API), async"
        )
        cn_raw_response = await self._dev_ops_api_commander.async_raw_request(
            http_method=HttpMethod.POST,
            additional_path=f"keyspaces/{name}",
            timeout_context=timeout_manager.remaining_timeout(
                cap_time_ms=_request_timeout_ms,
                cap_timeout_label=_rt_label,
            ),
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
                last_db_info = await self._astra_db_admin._async_database_info_ctx(
                    id=self._database_id,
                    timeout_context=timeout_manager.remaining_timeout(
                        cap_time_ms=_request_timeout_ms,
                        cap_timeout_label=_rt_label,
                    ),
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
        if update_db_keyspace:
            self.spawner_database.use_keyspace(name)

    def drop_keyspace(
        self,
        name: str,
        *,
        wait_until_active: bool = True,
        keyspace_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
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
            keyspace_admin_timeout_ms: a timeout, in milliseconds, for the whole
                requested operation to complete. This is used only
                if `wait_until_active` is true, i.e. if the method call must
                wait and keep querying the DevOps API for the status of the
                database during keyspace deletion.
            request_timeout_ms: a timeout, in milliseconds, for
                each underlying DevOps API HTTP request.
            timeout_ms: an alias for *both* the `request_timeout_ms` and
                `keyspace_admin_timeout_ms` timeout parameters. In practice,
                regardless of `wait_until_active`, this parameter dictates an
                overall timeout on this method call.

        Note: a timeout event is no guarantee at all that the
        deletion request has not reached the API server and is not going
        to be, in fact, honored.

        Example:
            >>> my_db_admin.list_keyspaces()
            ['default_keyspace', 'that_other_one']
            >>> my_db_admin.drop_keyspace("that_other_one")
            >>> my_db_admin.list_keyspaces()
            ['default_keyspace']
        """

        _keyspace_admin_timeout_ms, _ka_label = _first_valid_timeout(
            (keyspace_admin_timeout_ms, "keyspace_admin_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (
                self.api_options.timeout_options.keyspace_admin_timeout_ms,
                "keyspace_admin_timeout_ms",
            ),
        )
        _request_timeout_ms, _rt_label = _first_valid_timeout(
            (request_timeout_ms, "request_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (self.api_options.timeout_options.request_timeout_ms, "request_timeout_ms"),
        )
        timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=_keyspace_admin_timeout_ms,
            dev_ops_api=True,
            timeout_label=_ka_label,
        )
        logger.info(
            f"dropping keyspace '{name}' on " f"'{self._database_id}' (DevOps API)"
        )
        dk_raw_response = self._dev_ops_api_commander.raw_request(
            http_method=HttpMethod.DELETE,
            additional_path=f"keyspaces/{name}",
            timeout_context=timeout_manager.remaining_timeout(
                cap_time_ms=_request_timeout_ms,
                cap_timeout_label=_rt_label,
            ),
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
                last_status_seen = self._astra_db_admin._database_info_ctx(
                    id=self._database_id,
                    timeout_context=timeout_manager.remaining_timeout(
                        cap_time_ms=_request_timeout_ms,
                        cap_timeout_label=_rt_label,
                    ),
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

    async def async_drop_keyspace(
        self,
        name: str,
        *,
        wait_until_active: bool = True,
        keyspace_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
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
            keyspace_admin_timeout_ms: a timeout, in milliseconds, for the whole
                requested operation to complete. This is used only
                if `wait_until_active` is true, i.e. if the method call must
                wait and keep querying the DevOps API for the status of the
                database during keyspace deletion.
            request_timeout_ms: a timeout, in milliseconds, for
                each underlying DevOps API HTTP request.
            timeout_ms: an alias for *both* the `request_timeout_ms` and
                `keyspace_admin_timeout_ms` timeout parameters. In practice,
                regardless of `wait_until_active`, this parameter dictates an
                overall timeout on this method call.

        Note: a timeout event is no guarantee at all that the
        deletion request has not reached the API server and is not going
        to be, in fact, honored.

        Example:
            >>> asyncio.run(
            ...     my_db_admin.async_drop_keyspace("app_keyspace")
            ... )
        """

        _keyspace_admin_timeout_ms, _ka_label = _first_valid_timeout(
            (keyspace_admin_timeout_ms, "keyspace_admin_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (
                self.api_options.timeout_options.keyspace_admin_timeout_ms,
                "keyspace_admin_timeout_ms",
            ),
        )
        _request_timeout_ms, _rt_label = _first_valid_timeout(
            (request_timeout_ms, "request_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (self.api_options.timeout_options.request_timeout_ms, "request_timeout_ms"),
        )
        timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=_keyspace_admin_timeout_ms,
            dev_ops_api=True,
            timeout_label=_ka_label,
        )
        logger.info(
            f"dropping keyspace '{name}' on "
            f"'{self._database_id}' (DevOps API), async"
        )
        dk_raw_response = await self._dev_ops_api_commander.async_raw_request(
            http_method=HttpMethod.DELETE,
            additional_path=f"keyspaces/{name}",
            timeout_context=timeout_manager.remaining_timeout(
                cap_time_ms=_request_timeout_ms,
                cap_timeout_label=_rt_label,
            ),
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
                last_db_info = await self._astra_db_admin._async_database_info_ctx(
                    id=self._database_id,
                    timeout_context=timeout_manager.remaining_timeout(
                        cap_time_ms=_request_timeout_ms,
                        cap_timeout_label=_rt_label,
                    ),
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

    def drop(
        self,
        *,
        wait_until_active: bool = True,
        database_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
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
            database_admin_timeout_ms: a timeout, in milliseconds, for the whole
                requested operation to complete. This is used only
                if `wait_until_active` is true, i.e. if the method call must
                wait and keep querying the DevOps API for the status of the
                newly-deleted database.
            request_timeout_ms: a timeout, in milliseconds, for
                each underlying DevOps API HTTP request.
            timeout_ms: an alias for *both* the `request_timeout_ms` and
                `database_admin_timeout_ms` timeout parameters. In practice,
                regardless of `wait_until_active`, this parameter dictates an
                overall timeout on this method call.

        Note: a timeout event is no guarantee at all that the
        deletion request has not reached the API server and is not going
        to be, in fact, honored.

        Example:
            >>> my_db_admin.list_keyspaces()
            ['default_keyspace', 'that_other_one']
            >>> my_db_admin.drop()
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
            database_admin_timeout_ms=database_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info(f"finished dropping this database ('{self._database_id}')")

    async def async_drop(
        self,
        *,
        wait_until_active: bool = True,
        database_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
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
            database_admin_timeout_ms: a timeout, in milliseconds, for the whole
                requested operation to complete. This is used only
                if `wait_until_active` is true, i.e. if the method call must
                wait and keep querying the DevOps API for the status of the
                newly-deleted database.
            request_timeout_ms: a timeout, in milliseconds, for
                each underlying DevOps API HTTP request.
            timeout_ms: an alias for *both* the `request_timeout_ms` and
                `database_admin_timeout_ms` timeout parameters. In practice,
                regardless of `wait_until_active`, this parameter dictates an
                overall timeout on this method call.

        Note: a timeout event is no guarantee at all that the
        deletion request has not reached the API server and is not going
        to be, in fact, honored.

        Example:
            >>> asyncio.run(my_db_admin.async_drop())

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
            database_admin_timeout_ms=database_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info(f"finished dropping this database ('{self._database_id}'), async")

    def get_database(
        self,
        *,
        keyspace: str | None = None,
        token: str | TokenProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> Database:
        """
        Create a Database instance from this database admin, for data-related tasks.

        Args:
            keyspace: if provided, it is passed to the Database; otherwise
                the Database class will apply an environment-specific default.
            token: if supplied, is passed to the Database instead of
                the one set for this object. Useful if one wants to work in
                a least-privilege manner, limiting the permissions for non-admin work.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            spawn_api_options: a specification - complete or partial - of the
                API Options to override the defaults inherited from the database admin.
                This allows for a deeper configuration of the database admin, e.g.
                concerning timeouts; if this is passed together with
                the named timeout parameters, the latter will take precedence
                in their respective settings.

        Returns:
            A Database object, ready to work with data, collections and tables.

        Example:
            >>> my_db = my_db_admin.get_database()
            >>> my_db.list_collection_names()
            ['movies', 'another_collection']

        Note:
            creating an instance of Database does not trigger actual creation
            of the database itself, which should exist beforehand. To create databases,
            see the AstraDBAdmin class.
        """

        return self._astra_db_admin.get_database(
            api_endpoint=self.api_endpoint,
            keyspace=keyspace,
            token=token,
            spawn_api_options=spawn_api_options,
        )

    def get_async_database(
        self,
        *,
        keyspace: str | None = None,
        token: str | TokenProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncDatabase:
        """
        Create an AsyncDatabase instance from this database admin,
        for data-related tasks.

        Args:
            keyspace: if provided, it is passed to the AsyncDatabase; otherwise
                the AsyncDatabase class will apply an environment-specific default.
            token: if supplied, is passed to the AsyncDatabase instead of
                the one set for this object. Useful if one wants to work in
                a least-privilege manner, limiting the permissions for non-admin work.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            spawn_api_options: a specification - complete or partial - of the
                API Options to override the defaults inherited from the database admin.
                This allows for a deeper configuration of the database admin, e.g.
                concerning timeouts; if this is passed together with
                the named timeout parameters, the latter will take precedence
                in their respective settings.

        Returns:
            An AsyncDatabase object, ready to work with data, collections and tables.
        """

        return self.get_database(
            keyspace=keyspace,
            token=token,
            spawn_api_options=spawn_api_options,
        ).to_async()

    def find_embedding_providers(
        self,
        *,
        database_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> FindEmbeddingProvidersResult:
        """
        Query the API for the full information on available embedding providers.

        Args:
            database_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `database_admin_timeout_ms`.
            timeout_ms: an alias for `database_admin_timeout_ms`.

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

        _database_admin_timeout_ms, _da_label = _select_singlereq_timeout_da(
            timeout_options=self.api_options.timeout_options,
            database_admin_timeout_ms=database_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info("findEmbeddingProviders")
        fe_response = self._api_commander.request(
            payload={"findEmbeddingProviders": {}},
            timeout_context=_TimeoutContext(
                request_ms=_database_admin_timeout_ms, label=_da_label
            ),
        )
        if "embeddingProviders" not in fe_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from findEmbeddingProviders API command.",
                raw_response=fe_response,
            )
        else:
            logger.info("finished findEmbeddingProviders")
            return FindEmbeddingProvidersResult._from_dict(fe_response["status"])

    async def async_find_embedding_providers(
        self,
        *,
        database_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> FindEmbeddingProvidersResult:
        """
        Query the API for the full information on available embedding providers.
        Async version of the method, for use in an asyncio context.

        Args:
            database_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `database_admin_timeout_ms`.
            timeout_ms: an alias for `database_admin_timeout_ms`.

        Returns:
            A `FindEmbeddingProvidersResult` object with the complete information
            returned by the API about available embedding providers

        Example (output abridged and indented for clarity):
            >>> asyncio.run(admin_for_my_db.find_embedding_providers())
            FindEmbeddingProvidersResult(embedding_providers=..., openai, ...)
            >>> asyncio.run(
            ...     admin_for_my_db.find_embedding_providers()
            ... ).embedding_providers
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

        _database_admin_timeout_ms, _da_label = _select_singlereq_timeout_da(
            timeout_options=self.api_options.timeout_options,
            database_admin_timeout_ms=database_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info("findEmbeddingProviders, async")
        fe_response = await self._api_commander.async_request(
            payload={"findEmbeddingProviders": {}},
            timeout_context=_TimeoutContext(
                request_ms=_database_admin_timeout_ms, label=_da_label
            ),
        )
        if "embeddingProviders" not in fe_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from findEmbeddingProviders API command.",
                raw_response=fe_response,
            )
        else:
            logger.info("finished findEmbeddingProviders, async")
            return FindEmbeddingProvidersResult._from_dict(fe_response["status"])

    def find_reranking_providers(
        self,
        *,
        database_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> FindRerankingProvidersResult:
        """
        Query the API for the full information on available reranking providers.

        Args:
            database_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `database_admin_timeout_ms`.
            timeout_ms: an alias for `database_admin_timeout_ms`.

        Returns:
            A `FindRerankingProvidersResult` object with the complete information
            returned by the API about available reranking providers

        Example (output abridged and indented for clarity):
            >>> admin_for_my_db.find_reranking_providers()
            FindRerankingProvidersResult(reranking_providers=nvidia)
            >>> admin_for_my_db.find_reranking_providers().reranking_providers
            {
                'nvidia': RerankingProvider(
                    <Default>
                    display_name='Nvidia',
                    models=[
                        RerankingProviderModel(
                            <Default>
                            name='nvidia/llama-3.2-nv-rerankqa-1b-v2'
                        ),
                        ...
                    ]
                ),
                ...
            }
        """

        _database_admin_timeout_ms, _da_label = _select_singlereq_timeout_da(
            timeout_options=self.api_options.timeout_options,
            database_admin_timeout_ms=database_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info("findRerankingProviders")
        fr_response = self._api_commander.request(
            payload={"findRerankingProviders": {}},
            timeout_context=_TimeoutContext(
                request_ms=_database_admin_timeout_ms, label=_da_label
            ),
        )
        if "rerankingProviders" not in fr_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from findRerankingProviders API command.",
                raw_response=fr_response,
            )
        else:
            logger.info("finished findRerankingProviders")
            return FindRerankingProvidersResult._from_dict(fr_response["status"])

    async def async_find_reranking_providers(
        self,
        *,
        database_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> FindRerankingProvidersResult:
        """
        Query the API for the full information on available reranking providers.
        Async version of the method, for use in an asyncio context.

        Args:
            database_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `database_admin_timeout_ms`.
            timeout_ms: an alias for `database_admin_timeout_ms`.

        Returns:
            A `FindRerankingProvidersResult` object with the complete information
            returned by the API about available reranking providers

        Example (output abridged and indented for clarity):
            >>> asyncio.run(admin_for_my_db.find_reranking_providers())
            FindRerankingProvidersResult(reranking_providers=nvidia)
            >>> asyncio.run(
            ...     admin_for_my_db.find_reranking_providers()
            ... ).reranking_providers
            {
                'nvidia': RerankingProvider(
                    <Default>
                    display_name='Nvidia',
                    models=[
                        RerankingProviderModel(
                            <Default>
                            name='nvidia/llama-3.2-nv-rerankqa-1b-v2'
                        ),
                        ...
                    ]
                ),
                ...
            }
        """

        _database_admin_timeout_ms, _da_label = _select_singlereq_timeout_da(
            timeout_options=self.api_options.timeout_options,
            database_admin_timeout_ms=database_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info("findRerankingProviders, async")
        fr_response = await self._api_commander.async_request(
            payload={"findRerankingProviders": {}},
            timeout_context=_TimeoutContext(
                request_ms=_database_admin_timeout_ms, label=_da_label
            ),
        )
        if "rerankingProviders" not in fr_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from findRerankingProviders API command.",
                raw_response=fr_response,
            )
        else:
            logger.info("finished findRerankingProviders, async")
            return FindRerankingProvidersResult._from_dict(fr_response["status"])


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
        api_options: a complete specification of the API Options for this instance.
        spawner_database: either a Database or an AsyncDatabase instance. This represents
            the database class which spawns this admin object, so that, if required,
            a keyspace creation can retroactively "use" the new keyspace in the spawner.
            Used to enable the Async/Database.get_admin_database().create_keyspace()
            pattern.

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

    Note:
        a more powerful token may be required than the one sufficient for working
        in the Database, Collection and Table classes. Check the provided token
        if "Unauthorized" errors are encountered.
    """

    def __init__(
        self,
        *,
        api_endpoint: str,
        api_options: FullAPIOptions,
        spawner_database: Database | AsyncDatabase | None = None,
    ) -> None:
        # lazy import here to avoid circular dependency
        from astrapy.database import Database

        self.api_options = api_options
        self.api_endpoint = api_endpoint

        if spawner_database is not None:
            self.spawner_database = spawner_database
        else:
            # leaving the keyspace to its per-environment default
            # (a task for the Database)
            self.spawner_database = Database(
                api_endpoint=self.api_endpoint,
                keyspace=None,
                api_options=self.api_options,
            )

        # even if Data API, this is admin and must use the Admin additional headers:
        self._commander_headers = {
            DEFAULT_DATA_API_AUTH_HEADER: self.api_options.token.get_token(),
            **self.api_options.admin_additional_headers,
        }
        self._api_commander = self._get_api_commander()

    def __repr__(self) -> str:
        parts = [
            f'api_endpoint="{self.api_endpoint}"',
            f"api_options={self.api_options}",
        ]
        return f"{self.__class__.__name__}({', '.join(parts)})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DataAPIDatabaseAdmin):
            return all(
                [
                    self.api_endpoint == other.api_endpoint,
                    self.api_options == other.api_options,
                ]
            )
        else:
            return False

    def _get_api_commander(self) -> APICommander:
        base_path_components = [
            comp
            for comp in (
                ncomp.strip("/")
                for ncomp in (
                    self.api_options.data_api_url_options.api_path,
                    self.api_options.data_api_url_options.api_version,
                )
                if ncomp is not None
            )
            if comp != ""
        ]
        base_path = f"/{'/'.join(base_path_components)}"
        api_commander = APICommander(
            api_endpoint=self.api_endpoint,
            path=base_path,
            headers=self._commander_headers,
            callers=self.api_options.callers,
            redacted_header_names=self.api_options.redacted_header_names,
        )
        return api_commander

    def _copy(
        self,
        *,
        token: str | TokenProvider | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> DataAPIDatabaseAdmin:
        arg_api_options = APIOptions(
            token=token,
        )
        final_api_options = self.api_options.with_override(api_options).with_override(
            arg_api_options
        )
        return DataAPIDatabaseAdmin(
            api_endpoint=self.api_endpoint,
            api_options=final_api_options,
            spawner_database=self.spawner_database,
        )

    def with_options(
        self,
        *,
        token: str | TokenProvider | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> DataAPIDatabaseAdmin:
        """
        Create a clone of this DataAPIDatabaseAdmin with some changed attributes.

        Args:
            token: an access token with enough permission to perform admin tasks.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            api_options: any additional options to set for the clone, in the form of
                an APIOptions instance (where one can set just the needed attributes).
                In case the same setting is also provided as named parameter,
                the latter takes precedence.

        Returns:
            a new DataAPIDatabaseAdmin instance.

        Example:
            >>> admin_for_my_other_db = admin_for_my_db.with_options(
            ...     api_endpoint="http://10.1.1.5:8181",
            ... )
        """

        return self._copy(
            token=token,
            api_options=api_options,
        )

    def list_keyspaces(
        self,
        *,
        keyspace_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[str]:
        """
        Query the API for a list of the keyspaces in the database.

        Args:
            keyspace_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `keyspace_admin_timeout_ms`.
            timeout_ms: an alias for `keyspace_admin_timeout_ms`.

        Returns:
            A list of the keyspaces, each a string, in no particular order.

        Example:
            >>> admin_for_my_db.list_keyspaces()
            ['default_keyspace', 'staging_keyspace']
        """

        _keyspace_admin_timeout_ms, _ka_label = _select_singlereq_timeout_ka(
            timeout_options=self.api_options.timeout_options,
            keyspace_admin_timeout_ms=keyspace_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info("getting list of keyspaces")
        fn_response = self._api_commander.request(
            payload={"findKeyspaces": {}},
            timeout_context=_TimeoutContext(
                request_ms=_keyspace_admin_timeout_ms, label=_ka_label
            ),
        )
        if "keyspaces" not in fn_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from findKeyspaces API command.",
                raw_response=fn_response,
            )
        else:
            logger.info("finished getting list of keyspaces")
            return fn_response["status"]["keyspaces"]  # type: ignore[no-any-return]

    def create_keyspace(
        self,
        name: str,
        *,
        replication_options: dict[str, Any] | None = None,
        update_db_keyspace: bool | None = None,
        keyspace_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Create a keyspace in the database.

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
            keyspace_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `keyspace_admin_timeout_ms`.
            timeout_ms: an alias for `keyspace_admin_timeout_ms`.

        Note: a timeout event is no guarantee at all that the
        creation request has not reached the API server and is not going
        to be, in fact, honored.

        Example:
            >>> admin_for_my_db.list_keyspaces()
            ['default_keyspace']
            >>> admin_for_my_db.create_keyspace("that_other_one")
            >>> admin_for_my_db.list_keyspaces()
            ['default_keyspace', 'that_other_one']
        """

        _keyspace_admin_timeout_ms, _ka_label = _select_singlereq_timeout_ka(
            timeout_options=self.api_options.timeout_options,
            keyspace_admin_timeout_ms=keyspace_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
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
            timeout_context=_TimeoutContext(
                request_ms=_keyspace_admin_timeout_ms, label=_ka_label
            ),
        )
        if (cn_response.get("status") or {}).get("ok") != 1:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from createKeyspace API command.",
                raw_response=cn_response,
            )
        else:
            logger.info("finished creating keyspace")
            if update_db_keyspace:
                self.spawner_database.use_keyspace(name)

    def drop_keyspace(
        self,
        name: str,
        *,
        keyspace_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        """
        Drop (delete) a keyspace from the database.

        Args:
            name: the keyspace to delete. If it does not exist in this database,
                an error is raised.
            keyspace_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `keyspace_admin_timeout_ms`.
            timeout_ms: an alias for `keyspace_admin_timeout_ms`.

        Note: a timeout event is no guarantee at all that the
        deletion request has not reached the API server and is not going
        to be, in fact, honored.

        Example:
            >>> admin_for_my_db.list_keyspaces()
            ['default_keyspace', 'that_other_one']
            >>> admin_for_my_db.drop_keyspace("that_other_one")
            >>> admin_for_my_db.list_keyspaces()
            ['default_keyspace']
        """

        _keyspace_admin_timeout_ms, _ka_label = _select_singlereq_timeout_ka(
            timeout_options=self.api_options.timeout_options,
            keyspace_admin_timeout_ms=keyspace_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info("dropping keyspace")
        dn_response = self._api_commander.request(
            payload={"dropKeyspace": {"name": name}},
            timeout_context=_TimeoutContext(
                request_ms=_keyspace_admin_timeout_ms, label=_ka_label
            ),
        )
        if (dn_response.get("status") or {}).get("ok") != 1:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from dropKeyspace API command.",
                raw_response=dn_response,
            )
        else:
            logger.info("finished dropping keyspace")

    async def async_list_keyspaces(
        self,
        *,
        keyspace_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[str]:
        """
        Query the API for a list of the keyspaces in the database.
        Async version of the method, for use in an asyncio context.

        Args:
            keyspace_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `keyspace_admin_timeout_ms`.
            timeout_ms: an alias for `keyspace_admin_timeout_ms`.

        Returns:
            A list of the keyspaces, each a string, in no particular order.

        Example:
            >>> asyncio.run(admin_for_my_db.async_list_keyspaces())
            ['default_keyspace', 'staging_keyspace']
        """

        _keyspace_admin_timeout_ms, _ka_label = _select_singlereq_timeout_ka(
            timeout_options=self.api_options.timeout_options,
            keyspace_admin_timeout_ms=keyspace_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info("getting list of keyspaces, async")
        fn_response = await self._api_commander.async_request(
            payload={"findKeyspaces": {}},
            timeout_context=_TimeoutContext(
                request_ms=_keyspace_admin_timeout_ms, label=_ka_label
            ),
        )
        if "keyspaces" not in fn_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from findKeyspaces API command.",
                raw_response=fn_response,
            )
        else:
            logger.info("finished getting list of keyspaces, async")
            return fn_response["status"]["keyspaces"]  # type: ignore[no-any-return]

    async def async_create_keyspace(
        self,
        name: str,
        *,
        replication_options: dict[str, Any] | None = None,
        update_db_keyspace: bool | None = None,
        keyspace_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Create a keyspace in the database.
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
            keyspace_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `keyspace_admin_timeout_ms`.
            timeout_ms: an alias for `keyspace_admin_timeout_ms`.

        Note: a timeout event is no guarantee at all that the
        creation request has not reached the API server and is not going
        to be, in fact, honored.

        Example:
            >>> admin_for_my_db.list_keyspaces()
            ['default_keyspace']
            >>> asyncio.run(admin_for_my_db.async_create_keyspace(
            ...     "that_other_one"
            ... ))
            >>> admin_for_my_db.list_leyspaces()
            ['default_keyspace', 'that_other_one']
        """

        _keyspace_admin_timeout_ms, _ka_label = _select_singlereq_timeout_ka(
            timeout_options=self.api_options.timeout_options,
            keyspace_admin_timeout_ms=keyspace_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
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
            timeout_context=_TimeoutContext(
                request_ms=_keyspace_admin_timeout_ms, label=_ka_label
            ),
        )
        if (cn_response.get("status") or {}).get("ok") != 1:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from createKeyspace API command.",
                raw_response=cn_response,
            )
        else:
            logger.info("finished creating keyspace, async")
            if update_db_keyspace:
                self.spawner_database.use_keyspace(name)

    async def async_drop_keyspace(
        self,
        name: str,
        *,
        keyspace_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        """
        Drop (delete) a keyspace from the database.
        Async version of the method, for use in an asyncio context.

        Args:
            name: the keyspace to delete. If it does not exist in this database,
                an error is raised.
            keyspace_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `keyspace_admin_timeout_ms`.
            timeout_ms: an alias for `keyspace_admin_timeout_ms`.

        Note: a timeout event is no guarantee at all that the
        deletion request has not reached the API server and is not going
        to be, in fact, honored.

        Example:
            >>> admin_for_my_db.list_keyspaces()
            ['that_other_one', 'default_keyspace']
            >>> asyncio.run(admin_for_my_db.async_drop_keyspace(
            ...     "that_other_one"
            ... ))
            >>> admin_for_my_db.list_keyspaces()
            ['default_keyspace']
        """

        _keyspace_admin_timeout_ms, _ka_label = _select_singlereq_timeout_ka(
            timeout_options=self.api_options.timeout_options,
            keyspace_admin_timeout_ms=keyspace_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info("dropping keyspace, async")
        dn_response = await self._api_commander.async_request(
            payload={"dropKeyspace": {"name": name}},
            timeout_context=_TimeoutContext(
                request_ms=_keyspace_admin_timeout_ms, label=_ka_label
            ),
        )
        if (dn_response.get("status") or {}).get("ok") != 1:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from dropKeyspace API command.",
                raw_response=dn_response,
            )
        else:
            logger.info("finished dropping keyspace, async")

    def get_database(
        self,
        *,
        keyspace: str | None = None,
        token: str | TokenProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> Database:
        """
        Create a Database instance for a specific database, to be used
        when doing data-level work (such as creating/managing collections).

        Args:
            keyspace: an optional keyspace to set in the resulting Database.
                If not set, the keyspace remains unspecified and must be set later
                with the `use_keyspace` method.
            token: if supplied, is passed to the Database instead of
                the one set for this object. Useful if one wants to work in
                a least-privilege manner, limiting the permissions for non-admin work.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            spawn_api_options: a specification - complete or partial - of the
                API Options to override the defaults inherited from the database admin.
                This allows for a deeper configuration of the database admin, e.g.
                concerning timeouts; if this is passed together with
                the named timeout parameters, the latter will take precedence
                in their respective settings.

        Returns:
            A Database object, ready to work with data, collections and tables.

        Example:
            >>> my_db = admin_for_my_db.get_database()
            >>> my_db.list_collection_names()
            ['movies', 'another_collection']

        Note:
            creating an instance of Database does not trigger actual creation
            of the database itself, which should exist beforehand.
        """

        # lazy importing here to avoid circular dependency
        from astrapy import Database

        # this multiple-override implements the alias on timeout params
        resulting_api_options = self.api_options.with_override(
            spawn_api_options,
        ).with_override(
            APIOptions(
                token=token,
            ),
        )

        return Database(
            api_endpoint=self.api_endpoint,
            keyspace=keyspace,
            api_options=resulting_api_options,
        )

    def get_async_database(
        self,
        *,
        keyspace: str | None = None,
        token: str | TokenProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncDatabase:
        """
        Create an AsyncDatabase instance for a specific database, to be used
        when doing data-level work (such as creating/managing collections).

        Args:
            keyspace: an optional keyspace to set in the resulting AsyncDatabase.
                If not set, the keyspace remains unspecified and must be set later
                with the `use_keyspace` method.
            token: if supplied, is passed to the AsyncDatabase instead of
                the one set for this object. Useful if one wants to work in
                a least-privilege manner, limiting the permissions for non-admin work.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            spawn_api_options: a specification - complete or partial - of the
                API Options to override the defaults inherited from the database admin.
                This allows for a deeper configuration of the database admin, e.g.
                concerning timeouts; if this is passed together with
                the named timeout parameters, the latter will take precedence
                in their respective settings.

        Returns:
            An AsyncDatabase object, ready to work with data, collections and tables.
        Note:
            creating an instance of AsyncDatabase does not trigger actual creation
            of the database itself, which should exist beforehand.
        """

        return self.get_database(
            token=token,
            keyspace=keyspace,
            spawn_api_options=spawn_api_options,
        ).to_async()

    def find_embedding_providers(
        self,
        *,
        database_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> FindEmbeddingProvidersResult:
        """
        Query the API for the full information on available embedding providers.

        Args:
            database_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `database_admin_timeout_ms`.
            timeout_ms: an alias for `database_admin_timeout_ms`.

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

        _database_admin_timeout_ms, _da_label = _select_singlereq_timeout_da(
            timeout_options=self.api_options.timeout_options,
            database_admin_timeout_ms=database_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info("findEmbeddingProviders")
        fe_response = self._api_commander.request(
            payload={"findEmbeddingProviders": {}},
            timeout_context=_TimeoutContext(
                request_ms=_database_admin_timeout_ms, label=_da_label
            ),
        )
        if "embeddingProviders" not in fe_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from findEmbeddingProviders API command.",
                raw_response=fe_response,
            )
        else:
            logger.info("finished findEmbeddingProviders")
            return FindEmbeddingProvidersResult._from_dict(fe_response["status"])

    async def async_find_embedding_providers(
        self,
        *,
        database_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> FindEmbeddingProvidersResult:
        """
        Query the API for the full information on available embedding providers.
        Async version of the method, for use in an asyncio context.

        Args:
            database_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `database_admin_timeout_ms`.
            timeout_ms: an alias for `database_admin_timeout_ms`.

        Returns:
            A `FindEmbeddingProvidersResult` object with the complete information
            returned by the API about available embedding providers

        Example (output abridged and indented for clarity):
            >>> asyncio.run(admin_for_my_db.find_embedding_providers())
            FindEmbeddingProvidersResult(embedding_providers=..., openai, ...)
            >>> asyncio.run(
            ...     admin_for_my_db.find_embedding_providers()
            ... ).embedding_providers
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

        _database_admin_timeout_ms, _da_label = _select_singlereq_timeout_da(
            timeout_options=self.api_options.timeout_options,
            database_admin_timeout_ms=database_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info("findEmbeddingProviders, async")
        fe_response = await self._api_commander.async_request(
            payload={"findEmbeddingProviders": {}},
            timeout_context=_TimeoutContext(
                request_ms=_database_admin_timeout_ms, label=_da_label
            ),
        )
        if "embeddingProviders" not in fe_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from findEmbeddingProviders API command.",
                raw_response=fe_response,
            )
        else:
            logger.info("finished findEmbeddingProviders, async")
            return FindEmbeddingProvidersResult._from_dict(fe_response["status"])

    def find_reranking_providers(
        self,
        *,
        database_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> FindRerankingProvidersResult:
        """
        Query the API for the full information on available reranking providers.

        Args:
            database_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `database_admin_timeout_ms`.
            timeout_ms: an alias for `database_admin_timeout_ms`.

        Returns:
            A `FindRerankingProvidersResult` object with the complete information
            returned by the API about available reranking providers

        Example (output abridged and indented for clarity):
            >>> admin_for_my_db.find_reranking_providers()
            FindRerankingProvidersResult(reranking_providers=nvidia)
            >>> admin_for_my_db.find_reranking_providers().reranking_providers
            {
                'nvidia': RerankingProvider(
                    <Default>
                    display_name='Nvidia',
                    models=[
                        RerankingProviderModel(
                            <Default>
                            name='nvidia/llama-3.2-nv-rerankqa-1b-v2'
                        ),
                        ...
                    ]
                ),
                ...
            }
        """

        _database_admin_timeout_ms, _da_label = _select_singlereq_timeout_da(
            timeout_options=self.api_options.timeout_options,
            database_admin_timeout_ms=database_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info("findRerankingProviders")
        fr_response = self._api_commander.request(
            payload={"findRerankingProviders": {}},
            timeout_context=_TimeoutContext(
                request_ms=_database_admin_timeout_ms, label=_da_label
            ),
        )
        if "rerankingProviders" not in fr_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from findRerankingProviders API command.",
                raw_response=fr_response,
            )
        else:
            logger.info("finished findRerankingProviders")
            return FindRerankingProvidersResult._from_dict(fr_response["status"])

    async def async_find_reranking_providers(
        self,
        *,
        database_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> FindRerankingProvidersResult:
        """
        Query the API for the full information on available reranking providers.
        Async version of the method, for use in an asyncio context.

        Args:
            database_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `database_admin_timeout_ms`.
            timeout_ms: an alias for `database_admin_timeout_ms`.

        Returns:
            A `FindRerankingProvidersResult` object with the complete information
            returned by the API about available reranking providers

        Example (output abridged and indented for clarity):
            >>> asyncio.run(admin_for_my_db.find_reranking_providers())
            FindRerankingProvidersResult(reranking_providers=nvidia)
            >>> asyncio.run(
            ...     admin_for_my_db.find_reranking_providers()
            ... ).reranking_providers
            {
                'nvidia': RerankingProvider(
                    <Default>
                    display_name='Nvidia',
                    models=[
                        RerankingProviderModel(
                            <Default>
                            name='nvidia/llama-3.2-nv-rerankqa-1b-v2'
                        ),
                        ...
                    ]
                ),
                ...
            }
        """

        _database_admin_timeout_ms, _da_label = _select_singlereq_timeout_da(
            timeout_options=self.api_options.timeout_options,
            database_admin_timeout_ms=database_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info("findRerankingProviders, async")
        fr_response = await self._api_commander.async_request(
            payload={"findRerankingProviders": {}},
            timeout_context=_TimeoutContext(
                request_ms=_database_admin_timeout_ms, label=_da_label
            ),
        )
        if "rerankingProviders" not in fr_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from findRerankingProviders API command.",
                raw_response=fr_response,
            )
        else:
            logger.info("finished findRerankingProviders, async")
            return FindRerankingProvidersResult._from_dict(fr_response["status"])


__all__ = [
    "AstraDBAdmin",
    "DatabaseAdmin",
    "AstraDBDatabaseAdmin",
    "DataAPIDatabaseAdmin",
    "ParsedAPIEndpoint",
    "parse_api_endpoint",
]
