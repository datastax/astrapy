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
import re
from typing import TYPE_CHECKING, Any, Optional, Union

from astrapy.admin import (
    api_endpoint_parser,
    build_api_endpoint,
    database_id_matcher,
    fetch_raw_database_info_from_id_token,
    normalize_id_endpoint_parameters,
    parse_api_endpoint,
    parse_generic_api_url,
)
from astrapy.authentication import coerce_token_provider
from astrapy.constants import Environment

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
        caller_name: name of the application, or framework, on behalf of which
            the Data API and DevOps API calls are performed. This ends up in
            the request user-agent.
        caller_version: version of the caller.

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
        token: Optional[Union[str, TokenProvider]] = None,
        *,
        environment: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> None:
        self.token_provider = coerce_token_provider(token)
        self.environment = (environment or Environment.PROD).lower()

        if self.environment not in Environment.values:
            raise ValueError(f"Unsupported `environment` value: '{self.environment}'.")

        self._caller_name = caller_name
        self._caller_version = caller_version

    def __repr__(self) -> str:
        env_desc: str
        if self.environment == Environment.PROD:
            env_desc = ""
        else:
            env_desc = f', environment="{self.environment}"'
        return (
            f'{self.__class__.__name__}("{str(self.token_provider)[:12]}..."{env_desc})'
        )

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DataAPIClient):
            return all(
                [
                    self.token_provider == other.token_provider,
                    self.environment == other.environment,
                    self._caller_name == other._caller_name,
                    self._caller_version == other._caller_version,
                ]
            )
        else:
            return False

    def __getitem__(self, database_id_or_api_endpoint: str) -> Database:
        if self.environment in Environment.astra_db_values:
            if re.match(database_id_matcher, database_id_or_api_endpoint):
                return self.get_database(database_id_or_api_endpoint)
            elif re.match(api_endpoint_parser, database_id_or_api_endpoint):
                return self.get_database_by_api_endpoint(database_id_or_api_endpoint)
            else:
                raise ValueError(
                    "The provided input does not look like either a database ID "
                    f"or an API endpoint ('{database_id_or_api_endpoint}')."
                )
        else:
            return self.get_database_by_api_endpoint(database_id_or_api_endpoint)

    def _copy(
        self,
        *,
        token: Optional[Union[str, TokenProvider]] = None,
        environment: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> DataAPIClient:
        return DataAPIClient(
            token=coerce_token_provider(token) or self.token_provider,
            environment=environment or self.environment,
            caller_name=caller_name or self._caller_name,
            caller_version=caller_version or self._caller_version,
        )

    def with_options(
        self,
        *,
        token: Optional[Union[str, TokenProvider]] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> DataAPIClient:
        """
        Create a clone of this DataAPIClient with some changed attributes.

        Args:
            token: an Access Token to the database. Example: `"AstraCS:xyz..."`.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            caller_name: name of the application, or framework, on behalf of which
                the Data API and DevOps API calls are performed. This ends up in
                the request user-agent.
            caller_version: version of the caller.

        Returns:
            a new DataAPIClient instance.

        Example:
            >>> another_client = my_client.with_options(
            ...     caller_name="caller_identity",
            ...     caller_version="1.2.0",
            ... )
        """

        return self._copy(
            token=token,
            caller_name=caller_name,
            caller_version=caller_version,
        )

    def set_caller(
        self,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
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
        self._caller_name = caller_name
        self._caller_version = caller_version

    def get_database(
        self,
        id: Optional[str] = None,
        *,
        api_endpoint: Optional[str] = None,
        token: Optional[Union[str, TokenProvider]] = None,
        namespace: Optional[str] = None,
        region: Optional[str] = None,
        api_path: Optional[str] = None,
        api_version: Optional[str] = None,
        max_time_ms: Optional[int] = None,
    ) -> Database:
        """
        Get a Database object from this client, for doing data-related work.

        Args:
            id: the target database ID or the corresponding API Endpoint.
                The database must exist already for the resulting object
                to be effectively used; in other words, this invocation
                does not create the database, just the object instance.
                Actual admin work can be achieved by using the AstraDBAdmin object.
            api_endpoint: a named alias for the `id` first (positional) parameter,
                with the same meaning. It cannot be passed together with `id`.
            token: if supplied, is passed to the Database instead of the client token.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            namespace: if provided, it is passed to the Database; otherwise
                the Database class will apply an environment-specific default.
            region: the region to use for connecting to the database. The
                database must be located in that region.
                The region cannot be specified when the API endoint is used as `id`.
                Note that if this parameter is not passed, and cannot be inferred
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

        # lazy importing here to avoid circular dependency
        from astrapy import Database

        # id/endpoint parameter normalization
        _id_or_endpoint = normalize_id_endpoint_parameters(id, api_endpoint)
        if self.environment in Environment.astra_db_values:
            # handle the "endpoint passed as id" case first:
            if re.match(api_endpoint_parser, _id_or_endpoint):
                if region is not None:
                    raise ValueError(
                        "Parameter `region` not supported when supplying an API endpoint."
                    )
                # in this case max_time_ms is ignored (no calls take place)
                return self.get_database_by_api_endpoint(
                    api_endpoint=_id_or_endpoint,
                    token=token,
                    namespace=namespace,
                    api_path=api_path,
                    api_version=api_version,
                )
            else:
                # handle overrides. Only region is needed (namespace can stay empty)
                if region:
                    _region = region
                else:
                    logger.info(f"fetching raw database info for {_id_or_endpoint}")
                    this_db_info = fetch_raw_database_info_from_id_token(
                        id=_id_or_endpoint,
                        token=self.token_provider.get_token(),
                        environment=self.environment,
                        max_time_ms=max_time_ms,
                    )
                    logger.info(
                        f"finished fetching raw database info for {_id_or_endpoint}"
                    )
                    _region = this_db_info["info"]["region"]

                _token = coerce_token_provider(token) or self.token_provider
                _api_endpoint = build_api_endpoint(
                    environment=self.environment,
                    database_id=_id_or_endpoint,
                    region=_region,
                )
                return Database(
                    api_endpoint=_api_endpoint,
                    token=_token,
                    namespace=namespace,
                    caller_name=self._caller_name,
                    caller_version=self._caller_version,
                    environment=self.environment,
                    api_path=api_path,
                    api_version=api_version,
                )
        else:
            # in this case, this call is an alias for get_database_by_api_endpoint
            #   - max_time_ms ignored
            #   - assume `_id_or_endpoint` is actually the endpoint
            if region is not None:
                raise ValueError(
                    "Parameter `region` not supported outside of Astra DB."
                )
            return self.get_database_by_api_endpoint(
                api_endpoint=_id_or_endpoint,
                token=token,
                namespace=namespace,
                api_path=api_path,
                api_version=api_version,
            )

    def get_async_database(
        self,
        id: Optional[str] = None,
        *,
        api_endpoint: Optional[str] = None,
        token: Optional[Union[str, TokenProvider]] = None,
        namespace: Optional[str] = None,
        region: Optional[str] = None,
        api_path: Optional[str] = None,
        api_version: Optional[str] = None,
        max_time_ms: Optional[int] = None,
    ) -> AsyncDatabase:
        """
        Get an AsyncDatabase object from this client.

        This method has identical behavior and signature as the sync
        counterpart `get_database`: please see that one for more details.
        """

        return self.get_database(
            id=id,
            api_endpoint=api_endpoint,
            token=token,
            namespace=namespace,
            region=region,
            api_path=api_path,
            api_version=api_version,
            max_time_ms=max_time_ms,
        ).to_async()

    def get_database_by_api_endpoint(
        self,
        api_endpoint: str,
        *,
        token: Optional[Union[str, TokenProvider]] = None,
        namespace: Optional[str] = None,
        api_path: Optional[str] = None,
        api_version: Optional[str] = None,
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
            namespace: if provided, it is passed to the Database; otherwise
                the Database class will apply an environment-specific default.
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
            ...     namespace="the_other_namespace",
            ... )
            >>> my_coll = my_db0.create_collection("movies", dimension=2)
            >>> my_coll.insert_one({"title": "The Title", "$vector": [0.5, 0.6]})

        Note:
            This method does not perform any admin-level operation through
            the DevOps API. For actual creation of a database, see the
            `create_database` method of class AstraDBAdmin.
        """

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
                    namespace=namespace,
                    caller_name=self._caller_name,
                    caller_version=self._caller_version,
                    environment=self.environment,
                    api_path=api_path,
                    api_version=api_version,
                )
            else:
                raise ValueError(
                    f"Cannot parse the provided API endpoint ({api_endpoint})."
                )
        else:
            parsed_generic_api_endpoint = parse_generic_api_url(api_endpoint)
            if parsed_generic_api_endpoint:
                _token = coerce_token_provider(token) or self.token_provider
                return Database(
                    api_endpoint=parsed_generic_api_endpoint,
                    token=_token,
                    namespace=namespace,
                    caller_name=self._caller_name,
                    caller_version=self._caller_version,
                    environment=self.environment,
                    api_path=api_path,
                    api_version=api_version,
                )
            else:
                raise ValueError(
                    f"Cannot parse the provided API endpoint ({api_endpoint})."
                )

    def get_async_database_by_api_endpoint(
        self,
        api_endpoint: str,
        *,
        token: Optional[Union[str, TokenProvider]] = None,
        namespace: Optional[str] = None,
        api_path: Optional[str] = None,
        api_version: Optional[str] = None,
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

        return self.get_database_by_api_endpoint(
            api_endpoint=api_endpoint,
            token=token,
            namespace=namespace,
            api_path=api_path,
            api_version=api_version,
        ).to_async()

    def get_admin(
        self,
        *,
        token: Optional[Union[str, TokenProvider]] = None,
        dev_ops_url: Optional[str] = None,
        dev_ops_api_version: Optional[str] = None,
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
            >>> my_db_admin.list_namespaces()
            ['default_keyspace', 'that_other_one']
        """

        # lazy importing here to avoid circular dependency
        from astrapy.admin import AstraDBAdmin

        if self.environment not in Environment.astra_db_values:
            raise ValueError("Method not supported outside of Astra DB.")

        return AstraDBAdmin(
            token=coerce_token_provider(token) or self.token_provider,
            environment=self.environment,
            caller_name=self._caller_name,
            caller_version=self._caller_version,
            dev_ops_url=dev_ops_url,
            dev_ops_api_version=dev_ops_api_version,
        )
