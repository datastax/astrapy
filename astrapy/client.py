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

from astrapy.admin.endpoints import (
    api_endpoint_parsing_cdinfo_message,
    generic_api_url_parsing_error_message,
    parse_api_endpoint,
    parse_generic_api_url,
)
from astrapy.constants import CallerType, Environment
from astrapy.exceptions import InvalidEnvironmentException
from astrapy.utils.api_options import (
    APIOptions,
    defaultAPIOptions,
)
from astrapy.utils.unset import _UNSET, UnsetType

if TYPE_CHECKING:
    from astrapy import AsyncDatabase, Database
    from astrapy.admin import AstraDBAdmin
    from astrapy.authentication import TokenProvider


logger = logging.getLogger(__name__)


class DataAPIClient:
    """
    A client for using the Data API. This is the entry point, sitting
    at the top of the conceptual "client -> database -> collection" hierarchy
    and of the "client -> admin -> database admin" chain as well.

    A client is created first, optionally passing it a suitable Access Token.
    Starting from the client, then:
        - databases (Database and AsyncDatabase) are created for working with data
        - AstraDBAdmin objects can be created for admin-level work

    Args:
        token: an Access Token to the database. Example: `"AstraCS:xyz..."`.
            This can be either a literal token string or a subclass of
            `astrapy.authentication.TokenProvider`.
            Note that generally one should pass the token later, when spawning
            Database instances from the client (with the `get_database`) method
            of DataAPIClient; the reason is that the typical tokens are scoped
            to a single database. However, when performing administrative tasks
            at the AstraDBAdmin level (such as creating databases), an org-wide
            token is required -- then it makes sense to provide it when creating
            the DataAPIClient instance.
        environment: a string representing the target Data API environment.
            It can be left unspecified for the default value of `Environment.PROD`;
            other values include `Environment.OTHER`, `Environment.DSE`.
        callers: a list of caller identities, i.e. applications, or frameworks,
            on behalf of which Data API and DevOps API calls are performed.
            These end up in the request user-agent.
            Each caller identity is a ("caller_name", "caller_version") pair.
        api_options: a specification - complete or partial - of the API Options
            to override the system defaults. This allows for a deeper configuration
            than what the named parameters (token, environment, callers) offer.
            If this is passed alongside these named parameters, those will take
            precedence.

    Example:
        >>> from astrapy import DataAPIClient
        >>> from astrapy.info import CollectionDefinition
        >>> my_client = DataAPIClient()
        >>> my_db0 = my_client.get_database(
        ...     "https://01234567-....apps.astra.datastax.com",
        ...     token="AstraCS:...",
        ... )
        >>> my_coll = my_db0.create_collection(
        ...     "movies",
        ...     definition=(
        ...         CollectionDefinition.builder()
        ...         .set_vector_dimension(2)
        ...         .build()
        ...     ),
        ... )
        >>> my_coll.insert_one({"title": "The Title", "$vector": [0.1, 0.3]})
        >>> my_db1 = my_client.get_database("01234567-...")
        >>> my_db2 = my_client.get_database("01234567-...", region="us-east1")
        >>> my_adm0 = my_client.get_admin()
        >>> my_adm1 = my_client.get_admin(token=more_powerful_token_override)
        >>> database_list = my_adm0.list_databases()
    """

    def __init__(
        self,
        token: str | TokenProvider | UnsetType = _UNSET,
        *,
        environment: str | UnsetType = _UNSET,
        callers: Sequence[CallerType] | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> None:
        # this parameter bootstraps the defaults, has a special treatment:
        _environment: str
        if isinstance(environment, UnsetType):
            _environment = Environment.PROD.lower()
        else:
            _environment = environment.lower()
        if _environment not in Environment.values:
            raise InvalidEnvironmentException(
                f"Unsupported `environment` value: '{_environment}'."
            )
        arg_api_options = APIOptions(
            callers=callers,
            token=token,
        )
        self.api_options = (
            defaultAPIOptions(_environment)
            .with_override(api_options)
            .with_override(arg_api_options)
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.api_options})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DataAPIClient):
            return all(
                [
                    self.api_options.token == other.api_options.token,
                    self.api_options.environment == other.api_options.environment,
                    self.api_options.callers == other.api_options.callers,
                ]
            )
        else:
            return False

    def __getitem__(self, api_endpoint: str) -> Database:
        return self.get_database(api_endpoint=api_endpoint)

    def _copy(
        self,
        *,
        token: str | TokenProvider | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> DataAPIClient:
        arg_api_options = APIOptions(token=token)
        final_api_options = self.api_options.with_override(api_options).with_override(
            arg_api_options
        )
        return DataAPIClient(
            token=token,
            environment=final_api_options.environment,
            api_options=final_api_options,
        )

    def with_options(
        self,
        *,
        token: str | TokenProvider | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> DataAPIClient:
        """
        Create a clone of this DataAPIClient with some changed attributes.

        Args:
            token: an Access Token to the database. Example: `"AstraCS:xyz..."`.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            api_options: any additional options to set for the clone, in the form of
                an APIOptions instance (where one can set just the needed attributes).
                In case the same setting is also provided as named parameter,
                the latter takes precedence.

        Returns:
            a new DataAPIClient instance.

        Example:
            >>> other_auth_client = my_client.with_options(
            ...     token="AstraCS:xyz...",
            ... )
        """

        return self._copy(
            token=token,
            api_options=api_options,
        )

    def get_database(
        self,
        api_endpoint: str,
        *,
        token: str | TokenProvider | UnsetType = _UNSET,
        keyspace: str | None = None,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> Database:
        """
        Get a Database object from this client, for doing data-related work.

        Args:
            api_endpoint: the API Endpoint for the target database
                (e.g. `https://<ID>-<REGION>.apps.astra.datastax.com`,
                or a custom domain if one is configured for the database).
                The database must exist already for the resulting object
                to be effectively used; in other words, this invocation
                does not create the database, just the object instance.
                Actual admin work can be achieved by using the AstraDBAdmin object.
            token: if supplied, is passed to the Database instead of the client token.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            keyspace: if provided, it is passed to the Database; otherwise
                the Database class will apply an environment-specific default.
            spawn_api_options: a specification - complete or partial - of the
                API Options to override the defaults.
                This allows for a deeper configuration of the database, e.g.
                concerning timeouts; if this is passed together with
                the equivalent named parameters, the latter will take precedence
                in their respective settings.

        Returns:
            a Database object with which to work on Data API collections.

        Example:
            >>> my_db1 = my_client.get_database(
            ...     "https://01234567-...us-west1.apps.astra.datastax.com",
            ... )
            >>> my_db2 = my_client.get_database(
            ...     "https://01234567-...us-west1.apps.astra.datastax.com",
            ...     token="AstraCS:...",
            ...     keyspace="prod_keyspace",
            ... )
            >>> my_coll = my_db0.create_collection(
            ...     "movies",
            ...     definition=(
            ...         CollectionDefinition.builder()
            ...         .set_vector_dimension(2)
            ...         .build()
            ...     ),
            ... )
            >>> my_coll.insert_one({"title": "The Title", "$vector": [0.3, 0.4]})

        Note:
            This method does not perform any admin-level operation through
            the DevOps API. For actual creation of a database, see the
            `create_database` method of class AstraDBAdmin.
        """

        # lazy importing here to avoid circular dependency
        from astrapy import Database

        arg_api_options = APIOptions(token=token)
        resulting_api_options = self.api_options.with_override(
            spawn_api_options
        ).with_override(arg_api_options)

        if resulting_api_options.environment in Environment.astra_db_values:
            parsed_api_endpoint = parse_api_endpoint(api_endpoint)
            if parsed_api_endpoint is not None:
                if parsed_api_endpoint.environment != resulting_api_options.environment:
                    raise InvalidEnvironmentException(
                        "Environment mismatch between client and provided "
                        "API endpoint. You can try adding "
                        f'`environment="{parsed_api_endpoint.environment}"` '
                        "to the DataAPIClient creation statement."
                    )
                return Database(
                    api_endpoint=api_endpoint,
                    keyspace=keyspace,
                    api_options=resulting_api_options,
                )
            else:
                msg = api_endpoint_parsing_cdinfo_message(api_endpoint)
                logger.info(msg)
                return Database(
                    api_endpoint=api_endpoint,
                    keyspace=keyspace,
                    api_options=resulting_api_options,
                )
        else:
            parsed_generic_api_endpoint = parse_generic_api_url(api_endpoint)
            if parsed_generic_api_endpoint:
                return Database(
                    api_endpoint=parsed_generic_api_endpoint,
                    keyspace=keyspace,
                    api_options=resulting_api_options,
                )
            else:
                msg = generic_api_url_parsing_error_message(api_endpoint)
                raise ValueError(msg)

    def get_async_database(
        self,
        api_endpoint: str,
        *,
        token: str | TokenProvider | UnsetType = _UNSET,
        keyspace: str | None = None,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncDatabase:
        """
        Get an AsyncDatabase object from this client, for doing data-related work.

        Args:
            api_endpoint: the API Endpoint for the target database
                (e.g. `https://<ID>-<REGION>.apps.astra.datastax.com`,
                or a custom domain if one is configured for the database).
                The database must exist already for the resulting object
                to be effectively used; in other words, this invocation
                does not create the database, just the object instance.
                Actual admin work can be achieved by using the AstraDBAdmin object.
            token: if supplied, is passed to the Database instead of the client token.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            keyspace: if provided, it is passed to the Database; otherwise
                the Database class will apply an environment-specific default.
            spawn_api_options: a specification - complete or partial - of the
                API Options to override the defaults.
                This allows for a deeper configuration of the database, e.g.
                concerning timeouts; if this is passed together with
                the equivalent named parameters, the latter will take precedence
                in their respective settings.

        Returns:
            an AsyncDatabase object with which to work on Data API collections.

        Example:
            >>> async def create_use_db(cl: DataAPIClient, api_ep: str) -> None:
            ...     async_db = cl.get_async_database(api_ep)
            ...     my_a_coll = await async_db.create_collection(
            ...         "movies",
            ...         definition=(
            ...             CollectionDefinition.builder()
            ...             .set_vector_dimension(2)
            ...         .build()
            ...         )
            ...     )
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

        return self.get_database(
            api_endpoint=api_endpoint,
            token=token,
            keyspace=keyspace,
            spawn_api_options=spawn_api_options,
        ).to_async()

    def get_database_by_api_endpoint(
        self,
        api_endpoint: str,
        *,
        token: str | TokenProvider | UnsetType = _UNSET,
        keyspace: str | None = None,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> Database:
        """
        Get a Database object from this client, for doing data-related work.

        Note: this is an alias for `get_database` (see).

        Args:
            api_endpoint: the API Endpoint for the target database
                (e.g. `https://<ID>-<REGION>.apps.astra.datastax.com`,
                or a custom domain if one is configured for the database).
                The database must exist already for the resulting object
                to be effectively used; in other words, this invocation
                does not create the database, just the object instance.
                Actual admin work can be achieved by using the AstraDBAdmin object.
            token: if supplied, is passed to the Database instead of the client token.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            keyspace: if provided, it is passed to the Database; otherwise
                the Database class will apply an environment-specific default.
            spawn_api_options: a specification - complete or partial - of the
                API Options to override the defaults.
                This allows for a deeper configuration of the database, e.g.
                concerning timeouts; if this is passed together with
                the equivalent named parameters, the latter will take precedence
                in their respective settings.

        Returns:
            a Database object with which to work on Data API collections.
        """

        return self.get_database(
            api_endpoint=api_endpoint,
            token=token,
            keyspace=keyspace,
            spawn_api_options=spawn_api_options,
        )

    def get_async_database_by_api_endpoint(
        self,
        api_endpoint: str,
        *,
        token: str | TokenProvider | UnsetType = _UNSET,
        keyspace: str | None = None,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncDatabase:
        """
        Get an AsyncDatabase object from this client, for doing data-related work.

        Note: this is an alias for `get_async_database` (see).

        Args:
            api_endpoint: the API Endpoint for the target database
                (e.g. `https://<ID>-<REGION>.apps.astra.datastax.com`,
                or a custom domain if one is configured for the database).
                The database must exist already for the resulting object
                to be effectively used; in other words, this invocation
                does not create the database, just the object instance.
                Actual admin work can be achieved by using the AstraDBAdmin object.
            token: if supplied, is passed to the Database instead of the client token.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            keyspace: if provided, it is passed to the Database; otherwise
                the Database class will apply an environment-specific default.
            spawn_api_options: a specification - complete or partial - of the
                API Options to override the defaults.
                This allows for a deeper configuration of the database, e.g.
                concerning timeouts; if this is passed together with
                the equivalent named parameters, the latter will take precedence
                in their respective settings.

        Returns:
            an AsyncDatabase object with which to work on Data API collections.
        """

        return self.get_async_database(
            api_endpoint=api_endpoint,
            token=token,
            keyspace=keyspace,
            spawn_api_options=spawn_api_options,
        )

    def get_admin(
        self,
        *,
        token: str | TokenProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
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
            spawn_api_options: a specification - complete or partial - of the
                API Options to override the defaults.
                This allows for a deeper configuration of the admin, e.g.
                concerning timeouts; if this is passed together with
                the equivalent named parameters, the latter will take precedence
                in their respective settings.

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

        arg_api_options = APIOptions(token=token)
        resulting_api_options = self.api_options.with_override(
            spawn_api_options
        ).with_override(arg_api_options)

        if resulting_api_options.environment not in Environment.astra_db_values:
            raise InvalidEnvironmentException(
                "Method not supported outside of Astra DB."
            )

        return AstraDBAdmin(api_options=resulting_api_options)
