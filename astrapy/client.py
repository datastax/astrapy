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

import logging
from typing import TYPE_CHECKING, Any, Sequence

import deprecation

from astrapy import __version__
from astrapy.admin import (
    api_endpoint_parsing_error_message,
    build_api_endpoint,
    check_id_endpoint_parg_kwargs,
    generic_api_url_parsing_error_message,
    normalize_region_for_id,
    parse_api_endpoint,
    parse_generic_api_url,
)
from astrapy.authentication import coerce_token_provider, redact_secret
from astrapy.constants import CallerType, Environment
from astrapy.defaults import SET_CALLER_DEPRECATION_NOTICE
from astrapy.meta import (
    check_caller_parameters,
    check_deprecated_id_region,
    check_namespace_keyspace,
)

if TYPE_CHECKING:
    from astrapy import AsyncDatabase, Database
    from astrapy.admin import AstraDBAdmin
    from astrapy.authentication import TokenProvider


logger = logging.getLogger(__name__)


class DataAPIClient:
    """
    A client for using the Data API. This is the main entry point and sits
    at the top of the conceptual "client -> database -> collection" hierarchy.

    A client is created first, optionally passing it a suitable Access Token.
    Starting from the client, then:
        - databases (Database and AsyncDatabase) are created for working with data
        - AstraDBAdmin objects can be created for admin-level work

    Args:
        token: an Access Token to the database. Example: `"AstraCS:xyz..."`.
            This can be either a literal token string or a subclass of
            `astrapy.authentication.TokenProvider`.
        environment: a string representing the target Data API environment.
            It can be left unspecified for the default value of `Environment.PROD`;
            other values include `Environment.OTHER`, `Environment.DSE`.
        callers: a list of caller identities, i.e. applications, or frameworks,
            on behalf of which Data API and DevOps API calls are performed.
            These end up in the request user-agent.
            Each caller identity is a ("caller_name", "caller_version") pair.
        caller_name: *DEPRECATED*, use `callers`. Removal 2.0. Name of the
            application, or framework, on behalf of which the Data API and
            DevOps API calls are performed. This ends up in the request user-agent.
        caller_version: version of the caller. *DEPRECATED*, use `callers`.
            Removal 2.0.

    Example:
        >>> from astrapy import DataAPIClient
        >>> my_client = DataAPIClient("AstraCS:...")
        >>> my_db0 = my_client.get_database(
        ...     "https://01234567-....apps.astra.datastax.com"
        ... )
        >>> my_coll = my_db0.create_collection("movies", dimension=2)
        >>> my_coll.insert_one({"title": "The Title", "$vector": [0.1, 0.3]})
        >>> my_db1 = my_client.get_database("01234567-...")
        >>> my_db2 = my_client.get_database("01234567-...", region="us-east1")
        >>> my_adm0 = my_client.get_admin()
        >>> my_adm1 = my_client.get_admin(token=more_powerful_token_override)
        >>> database_list = my_adm0.list_databases()
    """

    def __init__(
        self,
        token: str | TokenProvider | None = None,
        *,
        environment: str | None = None,
        callers: Sequence[CallerType] = [],
        caller_name: str | None = None,
        caller_version: str | None = None,
    ) -> None:
        callers_param = check_caller_parameters(callers, caller_name, caller_version)
        self.token_provider = coerce_token_provider(token)
        self.environment = (environment or Environment.PROD).lower()

        if self.environment not in Environment.values:
            raise ValueError(f"Unsupported `environment` value: '{self.environment}'.")

        self.callers = callers_param

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
        if isinstance(other, DataAPIClient):
            return all(
                [
                    self.token_provider == other.token_provider,
                    self.environment == other.environment,
                    self.callers == other.callers,
                ]
            )
        else:
            return False

    def __getitem__(self, database_id_or_api_endpoint: str) -> Database:
        return self.get_database(api_endpoint_or_id=database_id_or_api_endpoint)

    def _copy(
        self,
        *,
        token: str | TokenProvider | None = None,
        environment: str | None = None,
        callers: Sequence[CallerType] = [],
        caller_name: str | None = None,
        caller_version: str | None = None,
    ) -> DataAPIClient:
        callers_param = check_caller_parameters(callers, caller_name, caller_version)
        return DataAPIClient(
            token=coerce_token_provider(token) or self.token_provider,
            environment=environment or self.environment,
            callers=callers_param or self.callers,
        )

    def with_options(
        self,
        *,
        token: str | TokenProvider | None = None,
        callers: Sequence[CallerType] = [],
        caller_name: str | None = None,
        caller_version: str | None = None,
    ) -> DataAPIClient:
        """
        Create a clone of this DataAPIClient with some changed attributes.

        Args:
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
            a new DataAPIClient instance.

        Example:
            >>> another_client = my_client.with_options(
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
        the API calls will be performed (the "caller").

        New objects spawned from this client afterwards will inherit the new settings.

        Args:
            caller_name: name of the application, or framework, on behalf of which
                the API API calls are performed. This ends up in the request user-agent.
            caller_version: version of the caller.

        Example:
            >>> my_client.set_caller(caller_name="the_caller", caller_version="0.1.0")
        """

        logger.info(f"setting caller to {caller_name}/{caller_version}")
        callers_param = check_caller_parameters([], caller_name, caller_version)
        self.callers = callers_param

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
        Get a Database object from this client, for doing data-related work.

        Args:
            api_endpoint_or_id: positional parameter that can stand for both
                `api_endpoint` and `id`. Passing them together is an error.
            api_endpoint: the API Endpoint for the target database
                (e.g. `https://<ID>-<REGION>.apps.astra.datastax.com`).
                The database must exist already for the resulting object
                to be effectively used; in other words, this invocation
                does not create the database, just the object instance.
                Actual admin work can be achieved by using the AstraDBAdmin object.
            token: if supplied, is passed to the Database instead of the client token.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            keyspace: if provided, it is passed to the Database; otherwise
                the Database class will apply an environment-specific default.
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
            a Database object with which to work on Data API collections.

        Example:
            >>> my_db0 = my_client.get_database("01234567-...")
            >>> my_db1 = my_client.get_database(
            ...     "https://01234567-...us-west1.apps.astra.datastax.com",
            ... )
            >>> my_db2 = my_client.get_database("01234567-...", token="AstraCS:...")
            >>> my_db3 = my_client.get_database("01234567-...", region="us-west1")
            >>> my_coll = my_db0.create_collection("movies", dimension=2)
            >>> my_coll.insert_one({"title": "The Title", "$vector": [0.3, 0.4]})

        Note:
            This method does not perform any admin-level operation through
            the DevOps API. For actual creation of a database, see the
            `create_database` method of class AstraDBAdmin.
        """

        _api_endpoint_p, _id_p = check_id_endpoint_parg_kwargs(
            p_arg=api_endpoint_or_id, api_endpoint=api_endpoint, id=id
        )
        check_deprecated_id_region(_id_p, region)
        keyspace_param = check_namespace_keyspace(
            keyspace=keyspace,
            namespace=namespace,
        )

        # lazy importing here to avoid circular dependency
        from astrapy import Database

        if self.environment in Environment.astra_db_values:
            # handle the "endpoint passed as id" case first:
            if _api_endpoint_p is not None:
                if region is not None:
                    raise ValueError(
                        "Parameter `region` not supported with an API endpoint."
                    )
                # in this case max_time_ms is ignored (no calls take place)
                return self.get_database_by_api_endpoint(
                    api_endpoint=_api_endpoint_p,
                    token=token,
                    keyspace=keyspace_param,
                    api_path=api_path,
                    api_version=api_version,
                )
            else:
                if _id_p is None:
                    raise ValueError("Either `api_endpoint` or `id` must be supplied.")
                _token = coerce_token_provider(token) or self.token_provider
                _region = normalize_region_for_id(
                    database_id=_id_p,
                    token_str=_token.get_token(),
                    environment=self.environment,
                    region_param=region,
                    max_time_ms=max_time_ms,
                )
                _api_endpoint = build_api_endpoint(
                    environment=self.environment,
                    database_id=_id_p,
                    region=_region,
                )
                return Database(
                    api_endpoint=_api_endpoint,
                    token=_token,
                    keyspace=keyspace_param,
                    callers=self.callers,
                    environment=self.environment,
                    api_path=api_path,
                    api_version=api_version,
                )
        else:
            # in this case, this call is an alias for get_database_by_api_endpoint
            #   - max_time_ms ignored
            #   - require the endpoint to be passed
            if _id_p is not None:
                raise ValueError("Cannot use a Database ID outside of Astra DB.")
            if region is not None:
                raise ValueError(
                    "Parameter `region` not supported outside of Astra DB."
                )
            if _api_endpoint_p is None:
                raise ValueError("Parameter `api_endpoint` is required.")
            # _api_endpoint_p guaranteed not null at this point
            return self.get_database_by_api_endpoint(
                api_endpoint=_api_endpoint_p,
                token=token,
                keyspace=keyspace_param,
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
        max_time_ms: int | None = None,
    ) -> AsyncDatabase:
        """
        Get an AsyncDatabase object from this client, for doing data-related work.

        Args:
            api_endpoint_or_id: positional parameter that can stand for both
                `api_endpoint` and `id`. Passing them together is an error.
            api_endpoint: the API Endpoint for the target database
                (e.g. `https://<ID>-<REGION>.apps.astra.datastax.com`).
                The database must exist already for the resulting object
                to be effectively used; in other words, this invocation
                does not create the database, just the object instance.
                Actual admin work can be achieved by using the AstraDBAdmin object.
            token: if supplied, is passed to the Database instead of the client token.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            keyspace: if provided, it is passed to the Database; otherwise
                the Database class will apply an environment-specific default.
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
            a Database object with which to work on Data API collections.

        Example:
            >>> async def create_use_db(cl: DataAPIClient, api_ep: str) -> None:
            ...     async_db = cl.get_async_database(api_ep)
            ...     my_a_coll = await async_db.create_collection("movies", dimension=2)
            ...     await my_a_coll.insert_one({"title": "The Title", "$vector": [0.3, 0.4]})
            ...
            >>> asyncio.run(
            ...   create_use_db(
            ...       my_client,
            ...       "https://01234567-...us-west1.apps.astra.datastax.com",
            ...   )
            ... )

        Note:
            This method does not perform any admin-level operation through
            the DevOps API. For actual creation of a database, see the
            `create_database` method of class AstraDBAdmin.
        """

        _api_endpoint_p, _id_p = check_id_endpoint_parg_kwargs(
            p_arg=api_endpoint_or_id, api_endpoint=api_endpoint, id=id
        )
        check_deprecated_id_region(_id_p, region)
        keyspace_param = check_namespace_keyspace(
            keyspace=keyspace,
            namespace=namespace,
        )
        return self.get_database(
            api_endpoint=_api_endpoint_p,
            token=token,
            keyspace=keyspace_param,
            id=_id_p,
            region=region,
            api_path=api_path,
            api_version=api_version,
            max_time_ms=max_time_ms,
        ).to_async()

    def get_database_by_api_endpoint(
        self,
        api_endpoint: str,
        *,
        token: str | TokenProvider | None = None,
        keyspace: str | None = None,
        namespace: str | None = None,
        api_path: str | None = None,
        api_version: str | None = None,
    ) -> Database:
        """
        Get a Database object from this client, for doing data-related work.
        The Database is specified by an API Endpoint instead of the ID and a region.

        Note that using this method is generally equivalent to passing
        an API Endpoint as parameter to the `get_database` method (see).

        Args:
            api_endpoint: the full "API Endpoint" string used to reach the Data API.
                Example: "https://DATABASE_ID-REGION.apps.astra.datastax.com"
            token: if supplied, is passed to the Database instead of the client token.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            keyspace: if provided, it is passed to the Database; otherwise
                the Database class will apply an environment-specific default.
            namespace: an alias for `keyspace`. *DEPRECATED*, removal in 2.0.
            api_path: path to append to the API Endpoint. In typical usage, this
                should be left to its default of "/api/json".
            api_version: version specifier to append to the API path. In typical
                usage, this should be left to its default of "v1".

        Returns:
            a Database object with which to work on Data API collections.

        Example:
            >>> my_db0 = my_client.get_database_by_api_endpoint("01234567-...")
            >>> my_db1 = my_client.get_database_by_api_endpoint(
            ...     "https://01234567-....apps.astra.datastax.com",
            ...     token="AstraCS:...",
            ... )
            >>> my_db2 = my_client.get_database_by_api_endpoint(
            ...     "https://01234567-....apps.astra.datastax.com",
            ...     keyspace="the_other_keyspace",
            ... )
            >>> my_coll = my_db0.create_collection("movies", dimension=2)
            >>> my_coll.insert_one({"title": "The Title", "$vector": [0.5, 0.6]})

        Note:
            This method does not perform any admin-level operation through
            the DevOps API. For actual creation of a database, see the
            `create_database` method of class AstraDBAdmin.
        """

        keyspace_param = check_namespace_keyspace(
            keyspace=keyspace,
            namespace=namespace,
        )

        # lazy importing here to avoid circular dependency
        from astrapy import Database

        if self.environment in Environment.astra_db_values:
            parsed_api_endpoint = parse_api_endpoint(api_endpoint)
            if parsed_api_endpoint is not None:
                if parsed_api_endpoint.environment != self.environment:
                    raise ValueError(
                        "Environment mismatch between client and provided "
                        "API endpoint. You can try adding "
                        f'`environment="{parsed_api_endpoint.environment}"` '
                        "to the DataAPIClient creation statement."
                    )
                _token = coerce_token_provider(token) or self.token_provider
                return Database(
                    api_endpoint=api_endpoint,
                    token=_token,
                    keyspace=keyspace_param,
                    callers=self.callers,
                    environment=self.environment,
                    api_path=api_path,
                    api_version=api_version,
                )
            else:
                msg = api_endpoint_parsing_error_message(api_endpoint)
                raise ValueError(msg)
        else:
            parsed_generic_api_endpoint = parse_generic_api_url(api_endpoint)
            if parsed_generic_api_endpoint:
                _token = coerce_token_provider(token) or self.token_provider
                return Database(
                    api_endpoint=parsed_generic_api_endpoint,
                    token=_token,
                    keyspace=keyspace_param,
                    callers=self.callers,
                    environment=self.environment,
                    api_path=api_path,
                    api_version=api_version,
                )
            else:
                msg = generic_api_url_parsing_error_message(api_endpoint)
                raise ValueError(msg)

    def get_async_database_by_api_endpoint(
        self,
        api_endpoint: str,
        *,
        token: str | TokenProvider | None = None,
        keyspace: str | None = None,
        namespace: str | None = None,
        api_path: str | None = None,
        api_version: str | None = None,
    ) -> AsyncDatabase:
        """
        Get an AsyncDatabase object from this client, for doing data-related work.
        The Database is specified by an API Endpoint instead of the ID and a region.

        Note that using this method is generally equivalent to passing
        an API Endpoint as parameter to the `get_async_database` method (see).

        This method has identical behavior and signature as the sync
        counterpart `get_database_by_api_endpoint`: please see that one
        for more details.
        """

        keyspace_param = check_namespace_keyspace(
            keyspace=keyspace,
            namespace=namespace,
        )
        return self.get_database_by_api_endpoint(
            api_endpoint=api_endpoint,
            token=token,
            keyspace=keyspace_param,
            api_path=api_path,
            api_version=api_version,
        ).to_async()

    def get_admin(
        self,
        *,
        token: str | TokenProvider | None = None,
        dev_ops_url: str | None = None,
        dev_ops_api_version: str | None = None,
    ) -> AstraDBAdmin:
        """
        Get an AstraDBAdmin instance corresponding to this client, for
        admin work such as managing databases.

        Args:
            token: if supplied, is passed to the Astra DB Admin instead of the
                client token. This may be useful when switching to a more powerful,
                admin-capable permission set.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            dev_ops_url: in case of custom deployments, this can be used to specify
                the URL to the DevOps API, such as "https://api.astra.datastax.com".
                Generally it can be omitted. The environment (prod/dev/...) is
                determined from the API Endpoint.
            dev_ops_api_version: this can specify a custom version of the DevOps API
                (such as "v2"). Generally not needed.

        Returns:
            An AstraDBAdmin instance, wich which to perform management at the
            database level.

        Example:
            >>> my_adm0 = my_client.get_admin()
            >>> my_adm1 = my_client.get_admin(token=more_powerful_token_override)
            >>> database_list = my_adm0.list_databases()
            >>> my_db_admin = my_adm0.create_database(
            ...     "the_other_database",
            ...     cloud_provider="AWS",
            ...     region="eu-west-1",
            ... )
            >>> my_db_admin.list_keyspaces()
            ['default_keyspace', 'that_other_one']
        """

        # lazy importing here to avoid circular dependency
        from astrapy.admin import AstraDBAdmin

        if self.environment not in Environment.astra_db_values:
            raise ValueError("Method not supported outside of Astra DB.")

        return AstraDBAdmin(
            token=coerce_token_provider(token) or self.token_provider,
            environment=self.environment,
            callers=self.callers,
            dev_ops_url=dev_ops_url,
            dev_ops_api_version=dev_ops_api_version,
        )
