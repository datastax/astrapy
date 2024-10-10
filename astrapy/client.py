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

from astrapy.admin import (
    api_endpoint_parsing_error_message,
    generic_api_url_parsing_error_message,
    parse_api_endpoint,
    parse_generic_api_url,
)
from astrapy.authentication import coerce_token_provider, redact_secret
from astrapy.constants import CallerType, Environment
from astrapy.meta import check_namespace_keyspace

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
    ) -> None:
        self.token_provider = coerce_token_provider(token)
        self.environment = (environment or Environment.PROD).lower()

        if self.environment not in Environment.values:
            raise ValueError(f"Unsupported `environment` value: '{self.environment}'.")

        self.callers = callers

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

    def __getitem__(self, api_endpoint: str) -> Database:
        return self.get_database(api_endpoint=api_endpoint)

    def _copy(
        self,
        *,
        token: str | TokenProvider | None = None,
        environment: str | None = None,
        callers: Sequence[CallerType] = [],
    ) -> DataAPIClient:
        return DataAPIClient(
            token=coerce_token_provider(token) or self.token_provider,
            environment=environment or self.environment,
            callers=callers or self.callers,
        )

    def with_options(
        self,
        *,
        token: str | TokenProvider | None = None,
        callers: Sequence[CallerType] = [],
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

        Returns:
            a new DataAPIClient instance.

        Example:
            >>> another_client = my_client.with_options(
            ...     callers=[("caller_identity", "1.2.0")],
            ... )
        """

        return self._copy(
            token=token,
            callers=callers,
        )

    def get_database(
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

        Args:
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
            api_path: path to append to the API Endpoint. In typical usage, this
                should be left to its default of "/api/json".
            api_version: version specifier to append to the API path. In typical
                usage, this should be left to its default of "v1".

        Returns:
            a Database object with which to work on Data API collections.

        Example:
            >>> my_db1 = my_client.get_database(
            ...     "https://01234567-...us-west1.apps.astra.datastax.com",
            ... )
            >>> my_db2 = my_client.get_database(
            ...     "https://01234567-...us-west1.apps.astra.datastax.com",
            ...     tolen="AstraCS:...",
            ... )
            >>> my_coll = my_db1.create_collection("movies", dimension=2)
            >>> my_coll.insert_one({"title": "The Title", "$vector": [0.3, 0.4]})

        Note:
            This method does not perform any admin-level operation through
            the DevOps API. For actual creation of a database, see the
            `create_database` method of class AstraDBAdmin.
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
        )

    def get_async_database(
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

        Args:
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
            api_path: path to append to the API Endpoint. In typical usage, this
                should be left to its default of "/api/json".
            api_version: version specifier to append to the API path. In typical
                usage, this should be left to its default of "v1".

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

        keyspace_param = check_namespace_keyspace(
            keyspace=keyspace,
            namespace=namespace,
        )
        return self.get_database(
            api_endpoint=api_endpoint,
            token=token,
            keyspace=keyspace_param,
            api_path=api_path,
            api_version=api_version,
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
