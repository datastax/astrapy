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
from types import TracebackType
from typing import TYPE_CHECKING, Any, overload

from astrapy.admin import (
    async_fetch_database_info,
    fetch_database_info,
    parse_api_endpoint,
)
from astrapy.constants import (
    DOC,
    ROW,
    DefaultDocumentType,
    DefaultRowType,
    Environment,
)
from astrapy.exceptions import (
    DevOpsAPIException,
    InvalidEnvironmentException,
    UnexpectedDataAPIResponseException,
    _select_singlereq_timeout_ca,
    _select_singlereq_timeout_da,
    _select_singlereq_timeout_gm,
    _select_singlereq_timeout_ta,
    _TimeoutContext,
)
from astrapy.info import (
    AstraDBDatabaseInfo,
    CollectionDefinition,
    CollectionDescriptor,
    CreateTableDefinition,
    ListTableDescriptor,
)
from astrapy.settings.defaults import (
    DEFAULT_ASTRA_DB_KEYSPACE,
    DEFAULT_DATA_API_AUTH_HEADER,
)
from astrapy.utils.api_commander import APICommander
from astrapy.utils.api_options import (
    APIOptions,
    FullAPIOptions,
)
from astrapy.utils.unset import _UNSET, UnsetType

if TYPE_CHECKING:
    from astrapy.admin import DatabaseAdmin
    from astrapy.authentication import (
        EmbeddingHeadersProvider,
        RerankingHeadersProvider,
        TokenProvider,
    )
    from astrapy.collection import AsyncCollection, Collection
    from astrapy.table import AsyncTable, Table


logger = logging.getLogger(__name__)


class Database:
    """
    A Data API database. This is the object for doing database-level
    DML, such as creating/deleting collections, and for obtaining Collection
    objects themselves. This class has a synchronous interface.

    This class is not meant for direct instantiation by the user, rather
    it is obtained by invoking methods such as `get_database`
    of AstraDBClient.

    On Astra DB, a Database comes with an "API Endpoint", which implies
    a Database object instance reaches a specific region (relevant point in
    case of multi-region databases).

    A Database is also always set with a "working keyspace" on which all
    data operations are done (unless otherwise specified).

    Args:
        api_endpoint: the full "API Endpoint" string used to reach the Data API.
            Example: "https://<database_id>-<region>.apps.astra.datastax.com"
        keyspace: this is the keyspace all method calls will target, unless
            one is explicitly specified in the call. If no keyspace is supplied
            when creating a Database, on Astra DB the name "default_keyspace" is set,
            while on other environments the keyspace is left unspecified: in this case,
            most operations are unavailable until a keyspace is set (through an explicit
            `use_keyspace` invocation or equivalent).
        api_options: a complete specification of the API Options for this instance.

    Example:
        >>> from astrapy import DataAPIClient
        >>> my_client = astrapy.DataAPIClient()
        >>> my_db = my_client.get_database(
        ...     "https://01234567-....apps.astra.datastax.com",
        ...     token="AstraCS:...",
        ... )

    Note:
        creating an instance of Database does not trigger actual creation
        of the database itself, which should exist beforehand. To create databases,
        see the AstraDBAdmin class.
    """

    def __init__(
        self,
        *,
        api_endpoint: str,
        keyspace: str | None,
        api_options: FullAPIOptions,
    ) -> None:
        self.api_options = api_options
        self.api_endpoint = api_endpoint.strip("/")
        # enforce defaults if on Astra DB:
        self._using_keyspace: str | None
        if (
            keyspace is None
            and self.api_options.environment in Environment.astra_db_values
        ):
            self._using_keyspace = DEFAULT_ASTRA_DB_KEYSPACE
        else:
            self._using_keyspace = keyspace

        self._commander_headers = {
            DEFAULT_DATA_API_AUTH_HEADER: self.api_options.token.get_token(),
            **self.api_options.database_additional_headers,
        }
        self._name: str | None = None
        self._api_commander = self._get_api_commander(keyspace=self.keyspace)

    def __getattr__(self, collection_name: str) -> Collection[DefaultDocumentType]:
        return self.get_collection(name=collection_name)

    def __getitem__(self, collection_name: str) -> Collection[DefaultDocumentType]:
        return self.get_collection(name=collection_name)

    def __repr__(self) -> str:
        ep_desc = f'api_endpoint="{self.api_endpoint}"'
        keyspace_desc: str | None
        if self._using_keyspace is None:
            keyspace_desc = "keyspace not set"
        else:
            keyspace_desc = f'keyspace="{self._using_keyspace}"'
        api_options_desc = f"api_options={self.api_options}"
        parts = [
            pt for pt in [ep_desc, keyspace_desc, api_options_desc] if pt is not None
        ]
        return f"{self.__class__.__name__}({', '.join(parts)})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Database):
            return all(
                [
                    self.api_endpoint == other.api_endpoint,
                    self.keyspace == other.keyspace,
                    self.api_options == other.api_options,
                ]
            )
        else:
            return False

    def _get_api_commander(self, keyspace: str | None) -> APICommander | None:
        """
        Instantiate a new APICommander based on the properties of this class
        and a provided keyspace.

        If keyspace is None, return None (signaling a "keyspace not set").
        """

        if keyspace is None:
            return None
        else:
            base_path_components = [
                comp
                for comp in (
                    ncomp.strip("/")
                    for ncomp in (
                        self.api_options.data_api_url_options.api_path,
                        self.api_options.data_api_url_options.api_version,
                        keyspace,
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

    def _get_driver_commander(self, keyspace: str | None) -> APICommander:
        """
        Building on _get_api_commander, fall back to class keyspace in
        creating/returning a commander, and in any case raise an error if not set.
        """
        driver_commander: APICommander | None
        if keyspace:
            driver_commander = self._get_api_commander(keyspace=keyspace)
        else:
            driver_commander = self._api_commander
        if driver_commander is None:
            raise ValueError(
                "No keyspace specified. This operation requires a keyspace to "
                "be set, e.g. through the `use_keyspace` method."
            )
        return driver_commander

    def _copy(
        self,
        *,
        keyspace: str | None = None,
        token: str | TokenProvider | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> Database:
        arg_api_options = APIOptions(
            token=token,
        )
        final_api_options = self.api_options.with_override(api_options).with_override(
            arg_api_options
        )
        return Database(
            api_endpoint=self.api_endpoint,
            keyspace=keyspace or self.keyspace,
            api_options=final_api_options,
        )

    def with_options(
        self,
        *,
        keyspace: str | None = None,
        token: str | TokenProvider | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> Database:
        """
        Create a clone of this database with some changed attributes.

        Args:
            keyspace: this is the keyspace all method calls will target, unless
                one is explicitly specified in the call. If no keyspace is supplied
                when creating a Database, the name "default_keyspace" is set.
            token: an Access Token to the database. Example: `"AstraCS:xyz..."`.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            api_options: any additional options to set for the clone, in the form of
                an APIOptions instance (where one can set just the needed attributes).
                In case the same setting is also provided as named parameter,
                the latter takes precedence.

        Returns:
            a new `Database` instance.

        Example:
            >>> my_db_2 = my_db.with_options(
            ...     keyspace="the_other_keyspace",
            ...     token="AstraCS:xyz...",
            ... )
        """

        return self._copy(
            keyspace=keyspace,
            token=token,
            api_options=api_options,
        )

    def to_async(
        self,
        *,
        keyspace: str | None = None,
        token: str | TokenProvider | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncDatabase:
        """
        Create an AsyncDatabase from this one. Save for the arguments
        explicitly provided as overrides, everything else is kept identical
        to this database in the copy.

        Args:
            keyspace: this is the keyspace all method calls will target, unless
                one is explicitly specified in the call. If no keyspace is supplied
                when creating a Database, the name "default_keyspace" is set.
            token: an Access Token to the database. Example: "AstraCS:xyz..."
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            api_options: any additional options to set for the result, in the form of
                an APIOptions instance (where one can set just the needed attributes).
                In case the same setting is also provided as named parameter,
                the latter takes precedence.

        Returns:
            the new copy, an `AsyncDatabase` instance.

        Example:
            >>> async_database = my_db.to_async()
            >>> asyncio.run(async_database.list_collection_names())
        """

        arg_api_options = APIOptions(
            token=token,
        )
        final_api_options = self.api_options.with_override(api_options).with_override(
            arg_api_options
        )
        return AsyncDatabase(
            api_endpoint=self.api_endpoint,
            keyspace=keyspace or self.keyspace,
            api_options=final_api_options,
        )

    def use_keyspace(self, keyspace: str) -> None:
        """
        Switch to a new working keyspace for this database.
        This method changes (mutates) the Database instance.

        Note that this method does not create the keyspace, which should exist
        already (created for instance with a `DatabaseAdmin.create_keyspace` call).

        Args:
            keyspace: the new keyspace to use as the database working keyspace.

        Returns:
            None.

        Example:
            >>> my_db.list_collection_names()
            ['coll_1', 'coll_2']
            >>> my_db.use_keyspace("an_empty_keyspace")
            >>> my_db.list_collection_names()
            []
        """
        logger.info(f"switching to keyspace '{keyspace}'")
        self._using_keyspace = keyspace
        self._api_commander = self._get_api_commander(keyspace=self.keyspace)

    def info(
        self,
        *,
        database_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> AstraDBDatabaseInfo:
        """
        Additional information on the database as an AstraDBDatabaseInfo instance.

        Some of the returned properties are dynamic throughout the lifetime
        of the database (such as raw_info["keyspaces"]). For this reason,
        each invocation of this method triggers a new request to the DevOps API.

        Not available outside of Astra DB and when using custom domains.

        Args:
            database_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `database_admin_timeout_ms`.
            timeout_ms: an alias for `database_admin_timeout_ms`.

        Example:
            >>> my_db.info().region
            'eu-west-1'

            >>> my_db.info().raw_info['datacenters'][0]['dateCreated']
            '2023-01-30T12:34:56Z'

        Note:
            see the AstraDBDatabaseInfo documentation for a caveat about the difference
            between the `region` and the `raw["region"]` attributes.
        """

        if self.api_options.environment not in Environment.astra_db_values:
            raise InvalidEnvironmentException(
                "Environments outside of Astra DB are not supported."
            )
        elif parse_api_endpoint(self.api_endpoint) is None:
            raise InvalidEnvironmentException(
                "Cannot inspect a nonstandard API endpoint for properties."
            )

        _database_admin_timeout_ms, _da_label = _select_singlereq_timeout_da(
            timeout_options=self.api_options.timeout_options,
            database_admin_timeout_ms=database_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info("getting database info")
        database_info = fetch_database_info(
            self.api_endpoint,
            keyspace=self.keyspace,
            request_timeout_ms=_database_admin_timeout_ms,
            api_options=self.api_options,
        )
        if database_info is not None:
            logger.info("finished getting database info")
            return database_info
        else:
            raise DevOpsAPIException("Failure while fetching database info.")

    @property
    def id(self) -> str:
        """
        The ID of this database.
        Not available outside of Astra DB and when using custom domains.

        Example:
            >>> my_db.id
            '01234567-89ab-cdef-0123-456789abcdef'
        """

        if self.api_options.environment in Environment.astra_db_values:
            parsed_api_endpoint = parse_api_endpoint(self.api_endpoint)
            if parsed_api_endpoint is not None:
                return parsed_api_endpoint.database_id
            else:
                raise InvalidEnvironmentException(
                    "Cannot inspect a nonstandard API endpoint for properties."
                )
        else:
            raise InvalidEnvironmentException(
                "Database is not in a supported environment for this operation."
            )

    @property
    def region(self) -> str:
        """
        The region where this database is located.

        The region is still well defined in case of multi-region databases,
        since a Database instance connects to exactly one of the regions
        (as specified by the API Endpoint).

        Not available outside of Astra DB and when using custom domains.

        Example:
            >>> my_db.region
            'us-west-2'
        """

        if self.api_options.environment in Environment.astra_db_values:
            parsed_api_endpoint = parse_api_endpoint(self.api_endpoint)
            if parsed_api_endpoint is not None:
                return parsed_api_endpoint.region
            else:
                raise InvalidEnvironmentException(
                    "Cannot inspect a nonstandard API endpoint for properties."
                )
        else:
            raise InvalidEnvironmentException(
                "Database is not in a supported environment for this operation."
            )

    def name(self) -> str:
        """
        The name of this database. Note that this bears no unicity guarantees.

        Calling this method the first time involves a request
        to the DevOps API (the resulting database name is then cached).
        See the `info()` method for more details.

        Example:
            >>> my_db.name()
            'the_application_database'
        """

        if self._name is None:
            self._name = self.info().name
        return self._name

    @property
    def keyspace(self) -> str | None:
        """
        The keyspace this database uses as target for all commands when
        no method-call-specific keyspace is specified.

        Returns:
            the working keyspace (a string), or None if not set.

        Example:
            >>> my_db.keyspace
            'the_keyspace'
        """

        return self._using_keyspace

    @overload
    def get_collection(
        self,
        name: str,
        *,
        keyspace: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> Collection[DefaultDocumentType]: ...

    @overload
    def get_collection(
        self,
        name: str,
        *,
        document_type: type[DOC],
        keyspace: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> Collection[DOC]: ...

    def get_collection(
        self,
        name: str,
        *,
        document_type: type[Any] = DefaultDocumentType,
        keyspace: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> Collection[DOC]:
        """
        Spawn a `Collection` object instance representing a collection
        on this database.

        Creating a `Collection` instance does not have any effect on the
        actual state of the database: in other words, for the created
        `Collection` instance to be used meaningfully, the collection
        must exist already (for instance, it should have been created
        previously by calling the `create_collection` method).

        Args:
            name: the name of the collection.
            document_type: this parameter acts a formal specifier for the type checker.
                If omitted, the resulting Collection is implicitly
                a `Collection[dict[str, Any]]`. If provided, it must match the
                type hint specified in the assignment.
                See the examples below.
            keyspace: the keyspace containing the collection. If no keyspace
                is specified, the general setting for this database is used.
            embedding_api_key: optional API key(s) for interacting with the collection.
                If an embedding service is configured, and this parameter is not None,
                each Data API call will include the necessary embedding-related headers
                as specified by this parameter. If a string is passed, it translates
                into the one "embedding api key" header
                (i.e. `astrapy.authentication.EmbeddingAPIKeyHeaderProvider`).
                For some vectorize providers/models, if using header-based
                authentication, specialized subclasses of
                `astrapy.authentication.EmbeddingHeadersProvider` should be supplied.
            reranking_api_key: optional API key(s) for interacting with the collection.
                If a reranker is configured for the collection, and this parameter
                is not None, Data API calls will include the appropriate
                reranker-related headers according to this parameter. Reranker services
                may not necessarily require this setting (e.g. if the service needs no
                authentication, or one is configured as part of the collection
                definition relying on a "shared secret").
                If a string is passed, it is translated into an instance of
                `astrapy.authentication.RerankingAPIKeyHeaderProvider`.
            spawn_api_options: a specification - complete or partial - of the
                API Options to override the defaults inherited from the Database.
                This allows for a deeper configuration of the collection, e.g.
                concerning timeouts; if this is passed together with
                the named timeout parameters, the latter will take precedence
                in their respective settings.

        Returns:
            a `Collection` instance, representing the desired collection
                (but without any form of validation).

        Example:
            >>> my_col = my_db.get_collection("my_collection")
            >>> my_col.count_documents({}, upper_bound=100)
            41

        Note:
            The attribute and indexing syntax forms achieve the same effect
            as this method. In other words, the following are equivalent:
                my_db.get_collection("coll_name")
                my_db.coll_name
                my_db["coll_name"]
        """

        # lazy importing here against circular-import error
        from astrapy.collection import Collection

        resulting_api_options = self.api_options.with_override(
            spawn_api_options,
        ).with_override(
            APIOptions(
                embedding_api_key=embedding_api_key,
                reranking_api_key=reranking_api_key,
            ),
        )

        _keyspace = keyspace or self.keyspace
        if _keyspace is None:
            raise ValueError(
                "No keyspace specified. This operation requires a keyspace to "
                "be set, e.g. through the `use_keyspace` method."
            )
        return Collection(
            database=self,
            name=name,
            keyspace=_keyspace,
            api_options=resulting_api_options,
        )

    @overload
    def create_collection(
        self,
        name: str,
        *,
        definition: CollectionDefinition | dict[str, Any] | None = None,
        keyspace: str | None = None,
        collection_admin_timeout_ms: int | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> Collection[DefaultDocumentType]: ...

    @overload
    def create_collection(
        self,
        name: str,
        *,
        definition: CollectionDefinition | dict[str, Any] | None = None,
        document_type: type[DOC],
        keyspace: str | None = None,
        collection_admin_timeout_ms: int | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> Collection[DOC]: ...

    def create_collection(
        self,
        name: str,
        *,
        definition: CollectionDefinition | dict[str, Any] | None = None,
        document_type: type[Any] = DefaultDocumentType,
        keyspace: str | None = None,
        collection_admin_timeout_ms: int | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> Collection[DOC]:
        """
        Creates a collection on the database and return the Collection
        instance that represents it.

        This is a blocking operation: the method returns when the collection
        is ready to be used. As opposed to the `get_collection` instance,
        this method triggers causes the collection to be actually created on DB.

        Args:
            name: the name of the collection.
            definition: a complete collection definition for the table. This can be an
                instance of `CollectionDefinition` or an equivalent (nested) dictionary,
                in which case it will be parsed into a `CollectionDefinition`.
                See the `astrapy.info.CollectionDefinition` class and the
                `Collection` class for more details and ways to construct this object.
            document_type: this parameter acts a formal specifier for the type checker.
                If omitted, the resulting Collection is implicitly
                a `Collection[dict[str, Any]]`. If provided, it must match the
                type hint specified in the assignment.
                See the examples below.
            keyspace: the keyspace where the collection is to be created.
                If not specified, the general setting for this database is used.
            collection_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
            embedding_api_key: optional API key(s) for interacting with the collection.
                If an embedding service is configured, and this parameter is not None,
                each Data API call will include the necessary embedding-related headers
                as specified by this parameter. If a string is passed, it translates
                into the one "embedding api key" header
                (i.e. `astrapy.authentication.EmbeddingAPIKeyHeaderProvider`).
                For some vectorize providers/models, if using header-based authentication,
                specialized subclasses of `astrapy.authentication.EmbeddingHeadersProvider`
                should be supplied.
            reranking_api_key: optional API key(s) for interacting with the collection.
                If a reranker is configured for the collection, and this parameter
                is not None, Data API calls will include the appropriate
                reranker-related headers according to this parameter. Reranker services
                may not necessarily require this setting (e.g. if the service needs no
                authentication, or one is configured as part of the collection
                definition relying on a "shared secret").
                If a string is passed, it is translated into an instance of
                `astrapy.authentication.RerankingAPIKeyHeaderProvider`.
            spawn_api_options: a specification - complete or partial - of the
                API Options to override the defaults inherited from the Database.
                This allows for a deeper configuration of the collection, e.g.
                concerning timeouts; if this is passed together with
                the named timeout parameters, the latter will take precedence
                in their respective settings.

        Returns:
            a (synchronous) `Collection` instance, representing the
            newly-created collection.

        Example:
            >>> # Create a collection using the fluent syntax for its definition
            >>> from astrapy.constants import VectorMetric
            >>> from astrapy.info import CollectionDefinition
            >>>
            >>> collection_definition = (
            ...     CollectionDefinition.builder()
            ...     .set_vector_dimension(3)
            ...     .set_vector_metric(VectorMetric.DOT_PRODUCT)
            ...     .set_indexing("deny", ["annotations", "logs"])
            ...     .build()
            ... )
            >>> my_collection = database.create_collection(
            ...     "my_events",
            ...     definition=collection_definition,
            ... )

            >>>
            >>> # Create a collection with the definition as object
            >>> from astrapy.info import CollectionVectorOptions
            >>>
            >>> collection_definition_1 = CollectionDefinition(
            ...     vector=CollectionVectorOptions(
            ...         dimension=3,
            ...         metric=VectorMetric.DOT_PRODUCT,
            ...     ),
            ...     indexing={"deny": ["annotations", "logs"]},
            ... )
            >>> my_collection_1 = database.create_collection(
            ...     "my_events",
            ...     definition=collection_definition_1,
            ... )
            >>>

            >>> # Create a collection with the definition as plain dictionary
            >>> collection_definition_2 = {
            ...     "indexing": {"deny": ["annotations", "logs"]},
            ...     "vector": {
            ...         "dimension": 3,
            ...         "metric": VectorMetric.DOT_PRODUCT,
            ...     },
            ... }
            >>> my_collection_2 = database.create_collection(
            ...     "my_events",
            ...     definition=collection_definition_2,
            ... )
        """

        cc_definition: dict[str, Any] = CollectionDefinition.coerce(
            definition or {}
        ).as_dict()
        # this method has custom code to pick its timeout
        _collection_admin_timeout_ms: int
        _ca_label: str
        if collection_admin_timeout_ms is not None:
            _collection_admin_timeout_ms = collection_admin_timeout_ms
            _ca_label = "collection_admin_timeout_ms"
        else:
            _collection_admin_timeout_ms = (
                self.api_options.timeout_options.collection_admin_timeout_ms
            )
            _ca_label = "collection_admin_timeout_ms"
        driver_commander = self._get_driver_commander(keyspace=keyspace)
        cc_payload = {
            "createCollection": {
                k: v
                for k, v in {
                    "name": name,
                    "options": cc_definition,
                }.items()
                if v is not None
                if v != {}
            }
        }
        logger.info(f"createCollection('{name}')")
        cc_response = driver_commander.request(
            payload=cc_payload,
            timeout_context=_TimeoutContext(
                request_ms=_collection_admin_timeout_ms, label=_ca_label
            ),
        )
        if cc_response.get("status") != {"ok": 1}:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from createCollection API command.",
                raw_response=cc_response,
            )
        logger.info(f"finished createCollection('{name}')")
        return self.get_collection(
            name,
            document_type=document_type,
            keyspace=keyspace,
            embedding_api_key=embedding_api_key,
            reranking_api_key=reranking_api_key,
            spawn_api_options=spawn_api_options,
        )

    def drop_collection(
        self,
        name: str,
        *,
        keyspace: str | None = None,
        collection_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        """
        Drop a collection from the database, along with all documents therein.

        Args:
            name: the name of the collection to drop.
            keyspace: the keyspace where the collection resides. If not specified,
                the database working keyspace is assumed.
            collection_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `collection_admin_timeout_ms`.
            timeout_ms: an alias for `collection_admin_timeout_ms`.

        Example:
            >>> my_db.list_collection_names()
            ['a_collection', 'my_v_col', 'another_col']
            >>> my_db.drop_collection("my_v_col")
            >>> my_db.list_collection_names()
            ['a_collection', 'another_col']
        """

        _collection_admin_timeout_ms, _ca_label = _select_singlereq_timeout_ca(
            timeout_options=self.api_options.timeout_options,
            collection_admin_timeout_ms=collection_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        _keyspace = keyspace or self.keyspace
        driver_commander = self._get_driver_commander(keyspace=_keyspace)
        dc_payload = {"deleteCollection": {"name": name}}
        logger.info(f"deleteCollection('{name}')")
        dc_response = driver_commander.request(
            payload=dc_payload,
            timeout_context=_TimeoutContext(
                request_ms=_collection_admin_timeout_ms, label=_ca_label
            ),
        )
        if dc_response.get("status") != {"ok": 1}:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from deleteCollection API command.",
                raw_response=dc_response,
            )
        logger.info(f"finished deleteCollection('{name}')")
        return dc_response.get("status", {})  # type: ignore[no-any-return]

    def list_collections(
        self,
        *,
        keyspace: str | None = None,
        collection_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[CollectionDescriptor]:
        """
        List all collections in a given keyspace for this database.

        Args:
            keyspace: the keyspace to be inspected. If not specified,
                the general setting for this database is assumed.
            collection_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `collection_admin_timeout_ms`.
            timeout_ms: an alias for `collection_admin_timeout_ms`.

        Returns:
            a list of CollectionDescriptor instances one for each collection.

        Example:
            >>> coll_list = my_db.list_collections()
            >>> coll_list
            [CollectionDescriptor(name='my_v_col', options=CollectionDefinition())]
            >>> for coll_dict in my_db.list_collections():
            ...     print(coll_dict)
            ...
            CollectionDescriptor(name='my_v_col', options=CollectionDefinition())
        """

        _collection_admin_timeout_ms, _ca_label = _select_singlereq_timeout_ca(
            timeout_options=self.api_options.timeout_options,
            collection_admin_timeout_ms=collection_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        return self._list_collections_ctx(
            keyspace=keyspace,
            timeout_context=_TimeoutContext(
                request_ms=_collection_admin_timeout_ms, label=_ca_label
            ),
        )

    def _list_collections_ctx(
        self,
        *,
        keyspace: str | None,
        timeout_context: _TimeoutContext,
    ) -> list[CollectionDescriptor]:
        driver_commander = self._get_driver_commander(keyspace=keyspace)
        gc_payload = {"findCollections": {"options": {"explain": True}}}
        logger.info("findCollections")
        gc_response = driver_commander.request(
            payload=gc_payload,
            timeout_context=timeout_context,
        )
        if "collections" not in gc_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from findCollections API command.",
                raw_response=gc_response,
            )
        else:
            # we know this is a list of dicts, to marshal into "descriptors"
            logger.info("finished findCollections")
            return [
                CollectionDescriptor._from_dict(col_dict)
                for col_dict in gc_response["status"]["collections"]
            ]

    def list_collection_names(
        self,
        *,
        keyspace: str | None = None,
        collection_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[str]:
        """
        List the names of all collections in a given keyspace of this database.

        Args:
            keyspace: the keyspace to be inspected. If not specified,
                the general setting for this database is assumed.
            collection_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `collection_admin_timeout_ms`.
            timeout_ms: an alias for `collection_admin_timeout_ms`.

        Returns:
            a list of the collection names as strings, in no particular order.

        Example:
            >>> my_db.list_collection_names()
            ['a_collection', 'another_col']
        """

        _collection_admin_timeout_ms, _ca_label = _select_singlereq_timeout_ca(
            timeout_options=self.api_options.timeout_options,
            collection_admin_timeout_ms=collection_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        driver_commander = self._get_driver_commander(keyspace=keyspace)
        gc_payload: dict[str, Any] = {"findCollections": {}}
        logger.info("findCollections")
        gc_response = driver_commander.request(
            payload=gc_payload,
            timeout_context=_TimeoutContext(
                request_ms=_collection_admin_timeout_ms, label=_ca_label
            ),
        )
        if "collections" not in gc_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from findCollections API command.",
                raw_response=gc_response,
            )
        else:
            logger.info("finished findCollections")
            return gc_response["status"]["collections"]  # type: ignore[no-any-return]

    @overload
    def get_table(
        self,
        name: str,
        *,
        keyspace: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> Table[DefaultRowType]: ...

    @overload
    def get_table(
        self,
        name: str,
        *,
        row_type: type[ROW],
        keyspace: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> Table[ROW]: ...

    def get_table(
        self,
        name: str,
        *,
        row_type: type[Any] = DefaultRowType,
        keyspace: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> Table[ROW]:
        """
        Spawn a `Table` object instance representing a table
        on this database.

        Creating a `Table` instance does not have any effect on the
        actual state of the database: in other words, for the created
        `Table` instance to be used meaningfully, the table
        must exist already (for instance, it should have been created
        previously by calling the `create_table` method).

        Args:
            name: the name of the table.
            row_type: this parameter acts a formal specifier for the type checker.
                If omitted, the resulting Table is implicitly a `Table[dict[str, Any]]`.
                If provided, it must match the type hint specified in the assignment.
                See the examples below.
            keyspace: the keyspace containing the table. If no keyspace
                is specified, the general setting for this database is used.
            embedding_api_key: optional API key(s) for interacting with the table.
                If an embedding service is configured, and this parameter is not None,
                each Data API call will include the necessary embedding-related headers
                as specified by this parameter. If a string is passed, it translates
                into the one "embedding api key" header
                (i.e. `astrapy.authentication.EmbeddingAPIKeyHeaderProvider`).
                For some vectorize providers/models, if using header-based
                authentication, specialized subclasses of
                `astrapy.authentication.EmbeddingHeadersProvider` should be supplied.
            reranking_api_key: optional API key(s) for interacting with the table.
                If a reranker is configured for the table, and this parameter
                is not None, Data API calls will include the appropriate
                reranker-related headers according to this parameter. Reranker services
                may not necessarily require this setting (e.g. if the service needs no
                authentication, or one is configured as part of the table
                definition relying on a "shared secret").
                If a string is passed, it is translated into an instance of
                `astrapy.authentication.RerankingAPIKeyHeaderProvider`.
            spawn_api_options: a specification - complete or partial - of the
                API Options to override the defaults inherited from the Database.
                This allows for a deeper configuration of the table, e.g.
                concerning timeouts; if this is passed together with
                the named timeout parameters, the latter will take precedence
                in their respective settings.

        Returns:
            a `Table` instance, representing the desired table
                (but without any form of validation).

        Example:
            >>> # Get a Table object (and read a property of it as an example):
            >>> my_table = database.get_table("games")
            >>> my_table.full_name
            'default_keyspace.games'
            >>>
            >>> # Get a Table object in a specific keyspace,
            >>> # and set an embedding API key to it:
            >>> my_other_table = database.get_table(
            ...     "tournaments",
            ...     keyspace="the_other_keyspace",
            ...     embedding_api_key="secret-012abc...",
            ... )
            >>>
            >>> from astrapy import Table
            >>> MyCustomDictType = dict[str, int]
            >>>
            >>> # Get a Table object typed with a specific type for its rows:
            >>> my_typed_table: Table[MyCustomDictType] = database.get_table(
            ...     "games",
            ...     row_type=MyCustomDictType,
            ... )
        """

        # lazy importing here against circular-import error
        from astrapy.table import Table

        resulting_api_options = self.api_options.with_override(
            spawn_api_options,
        ).with_override(
            APIOptions(
                embedding_api_key=embedding_api_key,
                reranking_api_key=reranking_api_key,
            ),
        )

        _keyspace = keyspace or self.keyspace
        if _keyspace is None:
            raise ValueError(
                "No keyspace specified. This operation requires a keyspace to "
                "be set, e.g. through the `use_keyspace` method."
            )
        return Table[ROW](
            database=self,
            name=name,
            keyspace=_keyspace,
            api_options=resulting_api_options,
        )

    @overload
    def create_table(
        self,
        name: str,
        *,
        definition: CreateTableDefinition | dict[str, Any],
        keyspace: str | None = None,
        if_not_exists: bool | None = None,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> Table[DefaultRowType]: ...

    @overload
    def create_table(
        self,
        name: str,
        *,
        definition: CreateTableDefinition | dict[str, Any],
        row_type: type[ROW],
        keyspace: str | None = None,
        if_not_exists: bool | None = None,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> Table[ROW]: ...

    def create_table(
        self,
        name: str,
        *,
        definition: CreateTableDefinition | dict[str, Any],
        row_type: type[Any] = DefaultRowType,
        keyspace: str | None = None,
        if_not_exists: bool | None = None,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> Table[ROW]:
        """
        Creates a table on the database and return the Table
        instance that represents it.

        This is a blocking operation: the method returns when the table
        is ready to be used. As opposed to the `get_table` method call,
        this method causes the table to be actually created on DB.

        Args:
            name: the name of the table.
            definition: a complete table definition for the table. This can be an
                instance of `CreateTableDefinition` or an equivalent (nested) dictionary,
                in which case it will be parsed into a `CreateTableDefinition`.
                See the `astrapy.info.CreateTableDefinition` class and the
                `Table` class for more details and ways to construct this object.
            row_type: this parameter acts a formal specifier for the type checker.
                If omitted, the resulting Table is implicitly a `Table[dict[str, Any]]`.
                If provided, it must match the type hint specified in the assignment.
                See the examples below.
            keyspace: the keyspace where the table is to be created.
                If not specified, the general setting for this database is used.
            if_not_exists: if set to True, the command will succeed even if a table
                with the specified name already exists (in which case no actual
                table creation takes place on the database). Defaults to False,
                i.e. an error is raised by the API in case of table-name collision.
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.
            embedding_api_key: optional API key(s) for interacting with the table.
                If an embedding service is configured, and this parameter is not None,
                each Data API call will include the necessary embedding-related headers
                as specified by this parameter. If a string is passed, it translates
                into the one "embedding api key" header
                (i.e. `astrapy.authentication.EmbeddingAPIKeyHeaderProvider`).
                For some vectorize providers/models, if using header-based authentication,
                specialized subclasses of `astrapy.authentication.EmbeddingHeadersProvider`
                should be supplied.
            reranking_api_key: optional API key(s) for interacting with the table.
                If a reranker is configured for the table, and this parameter
                is not None, Data API calls will include the appropriate
                reranker-related headers according to this parameter. Reranker services
                may not necessarily require this setting (e.g. if the service needs no
                authentication, or one is configured as part of the table
                definition relying on a "shared secret").
                If a string is passed, it is translated into an instance of
                `astrapy.authentication.RerankingAPIKeyHeaderProvider`.
            spawn_api_options: a specification - complete or partial - of the
                API Options to override the defaults inherited from the Database.
                This allows for a deeper configuration of the table, e.g.
                concerning timeouts; if this is passed together with
                the named timeout parameters, the latter will take precedence
                in their respective settings.

        Returns:
            a (synchronous) `Table` instance, representing the
            newly-created table.

        Example:
            >>> # Create a table using the fluent syntax for definition
            >>> from astrapy.constants import SortMode
            >>> from astrapy.info import (
            ...     CreateTableDefinition,
            ...     ColumnType,
            ... )
            >>> table_definition = (
            ...     CreateTableDefinition.builder()
            ...     .add_column("match_id", ColumnType.TEXT)
            ...     .add_column("round", ColumnType.INT)
            ...     .add_vector_column("m_vector", dimension=3)
            ...     .add_column("score", ColumnType.INT)
            ...     .add_column("when", ColumnType.TIMESTAMP)
            ...     .add_column("winner", ColumnType.TEXT)
            ...     .add_set_column("fighters", ColumnType.UUID)
            ...     .add_partition_by(["match_id"])
            ...     .add_partition_sort({"round": SortMode.ASCENDING})
            ...     .build()
            ... )
            >>> my_table = database.create_table(
            ...     "games",
            ...     definition=table_definition,
            ... )
            >>>
            >>> # Create a table with the definition as object
            >>> # (and do not raise an error if the table exists already)
            >>> from astrapy.info import (
            ...     CreateTableDefinition,
            ...     TablePrimaryKeyDescriptor,
            ...     TableScalarColumnTypeDescriptor,
            ...     TableValuedColumnType,
            ...     TableValuedColumnTypeDescriptor,
            ...     TableVectorColumnTypeDescriptor,
            ... )
            >>> table_definition_1 = CreateTableDefinition(
            ...     columns={
            ...         "match_id": TableScalarColumnTypeDescriptor(
            ...             ColumnType.TEXT,
            ...         ),
            ...         "round": TableScalarColumnTypeDescriptor(
            ...             ColumnType.INT,
            ...         ),
            ...         "m_vector": TableVectorColumnTypeDescriptor(
            ...             column_type="vector", dimension=3
            ...         ),
            ...         "score": TableScalarColumnTypeDescriptor(
            ...             ColumnType.INT,
            ...         ),
            ...         "when": TableScalarColumnTypeDescriptor(
            ...             ColumnType.TIMESTAMP,
            ...         ),
            ...         "winner": TableScalarColumnTypeDescriptor(
            ...             ColumnType.TEXT,
            ...         ),
            ...         "fighters": TableValuedColumnTypeDescriptor(
            ...             column_type=TableValuedColumnType.SET,
            ...             value_type=ColumnType.UUID,
            ...         ),
            ...     },
            ...     primary_key=TablePrimaryKeyDescriptor(
            ...         partition_by=["match_id"],
            ...         partition_sort={"round": SortMode.ASCENDING},
            ...     ),
            ... )
            >>> my_table_1 = database.create_table(
            ...     "games",
            ...     definition=table_definition_1,
            ...     if_not_exists=True,
            ... )
            >>>
            >>> # Create a table with the definition as plain dictionary
            >>> # (and do not raise an error if the table exists already)
            >>> table_definition_2 = {
            ...     "columns": {
            ...         "match_id": {"type": "text"},
            ...         "round": {"type": "int"},
            ...         "m_vector": {"type": "vector", "dimension": 3},
            ...         "score": {"type": "int"},
            ...         "when": {"type": "timestamp"},
            ...         "winner": {"type": "text"},
            ...         "fighters": {"type": "set", "valueType": "uuid"},
            ...     },
            ...     "primaryKey": {
            ...         "partitionBy": ["match_id"],
            ...         "partitionSort": {"round": 1},
            ...     },
            ... }
            >>> my_table_2 = database.create_table(
            ...     "games",
            ...     definition=table_definition_2,
            ...     if_not_exists=True,
            ... )
        """

        ct_options: dict[str, bool]
        if if_not_exists is not None:
            ct_options = {"ifNotExists": if_not_exists}
        else:
            ct_options = {}
        ct_definition: dict[str, Any] = CreateTableDefinition.coerce(
            definition
        ).as_dict()
        _table_admin_timeout_ms, _ta_label = _select_singlereq_timeout_ta(
            timeout_options=self.api_options.timeout_options,
            table_admin_timeout_ms=table_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        driver_commander = self._get_driver_commander(keyspace=keyspace)
        ct_payload = {
            "createTable": {
                k: v
                for k, v in {
                    "name": name,
                    "definition": ct_definition,
                    "options": ct_options,
                }.items()
                if v is not None
                if v != {}
            }
        }
        logger.info(f"createTable('{name}')")
        ct_response = driver_commander.request(
            payload=ct_payload,
            timeout_context=_TimeoutContext(
                request_ms=_table_admin_timeout_ms, label=_ta_label
            ),
        )
        if ct_response.get("status") != {"ok": 1}:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from createTable API command.",
                raw_response=ct_response,
            )
        logger.info(f"finished createTable('{name}')")
        return self.get_table(
            name,
            row_type=row_type,
            keyspace=keyspace,
            embedding_api_key=embedding_api_key,
            reranking_api_key=reranking_api_key,
            spawn_api_options=spawn_api_options,
        )

    def drop_table_index(
        self,
        name: str,
        *,
        keyspace: str | None = None,
        if_exists: bool | None = None,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        """
        Drops (deletes) an index (of any kind) from the table it is associated to.

        This is a blocking operation: the method returns once the index
        is deleted.

        Note:
            Although associated to a table, index names are unique across a keyspace.
            For this reason, no table name is required in this call.

        Args:
            name: the name of the index.
            keyspace: the keyspace to which the index belongs.
                If not specified, the general setting for this database is used.
            if_exists: if passed as True, trying to drop a non-existing index
                will not error, just silently do nothing instead. If not provided,
                the API default behaviour will hold.
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Example:
            >>> # Drop an index from the keyspace:
            >>> database.drop_table_index("score_index")
            >>> # Drop an index, unless it does not exist already:
            >>> database.drop_table_index("score_index", if_exists=True)
        """

        _table_admin_timeout_ms, _ta_label = _select_singlereq_timeout_ta(
            timeout_options=self.api_options.timeout_options,
            table_admin_timeout_ms=table_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        di_options: dict[str, bool]
        if if_exists is not None:
            di_options = {"ifExists": if_exists}
        else:
            di_options = {}
        di_payload = {
            "dropIndex": {
                k: v
                for k, v in {
                    "name": name,
                    "options": di_options,
                }.items()
                if v is not None
                if v != {}
            }
        }
        driver_commander = self._get_driver_commander(keyspace=keyspace)
        logger.info(f"dropIndex('{name}')")
        di_response = driver_commander.request(
            payload=di_payload,
            timeout_context=_TimeoutContext(
                request_ms=_table_admin_timeout_ms, label=_ta_label
            ),
        )
        if di_response.get("status") != {"ok": 1}:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from dropIndex API command.",
                raw_response=di_response,
            )
        logger.info(f"finished dropIndex('{name}')")

    def drop_table(
        self,
        name: str,
        *,
        keyspace: str | None = None,
        if_exists: bool | None = None,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        """
        Drop a table from the database, along with all rows therein and related indexes.

        Args:
            name: the name of the table to drop.
            keyspace: the keyspace where the table resides. If not specified,
                the database working keyspace is assumed.
            if_exists: if passed as True, trying to drop a non-existing table
                will not error, just silently do nothing instead. If not provided,
                the API default behaviour will hold.
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Example:
            >>> database.list_table_names()
            ['fighters', 'games']
            >>> database.drop_table("fighters")
            >>> database.list_table_names()
            ['games']
            >>> # not erroring because of if_not_exists:
            >>> database.drop_table("fighters", if_not_exists=True)
        """

        _table_admin_timeout_ms, _ta_label = _select_singlereq_timeout_ta(
            timeout_options=self.api_options.timeout_options,
            table_admin_timeout_ms=table_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        _keyspace = keyspace or self.keyspace
        dt_options: dict[str, bool]
        if if_exists is not None:
            dt_options = {"ifExists": if_exists}
        else:
            dt_options = {}
        driver_commander = self._get_driver_commander(keyspace=_keyspace)
        dt_payload = {
            "dropTable": {
                k: v
                for k, v in {
                    "name": name,
                    "options": dt_options,
                }.items()
                if v is not None
                if v != {}
            }
        }
        logger.info(f"dropTable('{name}')")
        dt_response = driver_commander.request(
            payload=dt_payload,
            timeout_context=_TimeoutContext(
                request_ms=_table_admin_timeout_ms, label=_ta_label
            ),
        )
        if dt_response.get("status") != {"ok": 1}:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from dropTable API command.",
                raw_response=dt_response,
            )
        logger.info(f"finished dropTable('{name}')")
        return dt_response.get("status", {})  # type: ignore[no-any-return]

    def list_tables(
        self,
        *,
        keyspace: str | None = None,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[ListTableDescriptor]:
        """
        List all tables in a given keyspace for this database.

        Args:
            keyspace: the keyspace to be inspected. If not specified,
                the general setting for this database is assumed.
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Returns:
            a list of ListTableDescriptor instances, one for each table.

        Example:
            >>> tables = my_database.list_tables()
            >>> tables
            [ListTableDescriptor(name='fighters', definition=ListTableDefinition(...
            >>> tables[1].name
            'games'
            >>> tables[1].definition.columns
            {'match_id': TableScalarColumnTypeDescriptor(ColumnType.TEXT),...
            >>> tables[1].definition.columns['score']
            TableScalarColumnTypeDescriptor(ColumnType.INT)
            >>> tables[1].definition.primary_key.partition_by
            ['match_id']
            >>> tables[1].definition.primary_key.partition_sort
            {'round': 1}
        """

        _table_admin_timeout_ms, _ta_label = _select_singlereq_timeout_ta(
            timeout_options=self.api_options.timeout_options,
            table_admin_timeout_ms=table_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        return self._list_tables_ctx(
            keyspace=keyspace,
            timeout_context=_TimeoutContext(
                request_ms=_table_admin_timeout_ms, label=_ta_label
            ),
        )

    def _list_tables_ctx(
        self,
        *,
        keyspace: str | None,
        timeout_context: _TimeoutContext,
    ) -> list[ListTableDescriptor]:
        driver_commander = self._get_driver_commander(keyspace=keyspace)
        lt_payload = {"listTables": {"options": {"explain": True}}}
        logger.info("listTables")
        lt_response = driver_commander.request(
            payload=lt_payload,
            timeout_context=timeout_context,
        )
        if "tables" not in lt_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from listTables API command.",
                raw_response=lt_response,
            )
        else:
            # we know this is a list of dicts, to marshal into "descriptors"
            logger.info("finished listTables")
            return [
                ListTableDescriptor.coerce(tab_dict)
                for tab_dict in lt_response["status"]["tables"]
            ]

    def list_table_names(
        self,
        *,
        keyspace: str | None = None,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[str]:
        """
        List the names of all tables in a given keyspace of this database.

        Args:
            keyspace: the keyspace to be inspected. If not specified,
                the general setting for this database is assumed.
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Returns:
            a list of the table names as strings, in no particular order.

        Example:
            >>> database.list_table_names()
            ['fighters', 'games']
        """

        _table_admin_timeout_ms, _ta_label = _select_singlereq_timeout_ta(
            timeout_options=self.api_options.timeout_options,
            table_admin_timeout_ms=table_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        driver_commander = self._get_driver_commander(keyspace=keyspace)
        lt_payload: dict[str, Any] = {"listTables": {}}
        logger.info("listTables")
        lt_response = driver_commander.request(
            payload=lt_payload,
            timeout_context=_TimeoutContext(
                request_ms=_table_admin_timeout_ms, label=_ta_label
            ),
        )
        if "tables" not in lt_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from listTables API command.",
                raw_response=lt_response,
            )
        else:
            logger.info("finished listTables")
            return lt_response["status"]["tables"]  # type: ignore[no-any-return]

    def command(
        self,
        body: dict[str, Any],
        *,
        keyspace: str | None | UnsetType = _UNSET,
        collection_or_table_name: str | None = None,
        raise_api_errors: bool = True,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        Send a POST request to the Data API for this database with
        an arbitrary, caller-provided payload.

        Args:
            body: a JSON-serializable dictionary, the payload of the request.
            keyspace: the keyspace to use, if any. If a keyspace is employed,
                it is used to construct the full request URL. To run a command
                targeting no specific keyspace (rather, the database as a whole),
                pass an explicit `None`: the request URL will lack the suffix
                "/<keyspace>" component. If unspecified, the working keyspace of
                this database is used. If another keyspace is passed, it will be
                used instead of the database's working one.
            collection_or_table_name: if provided, the name is appended at the end
                of the endpoint. In this way, this method allows collection-
                and table-level arbitrary POST requests as well.
                This parameter cannot be used if `keyspace=None` is explicitly provided.
            raise_api_errors: if True, responses with a nonempty 'errors' field
                result in an astrapy exception being raised.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a dictionary with the response of the HTTP request.

        Example:
            >>> my_db.command({"findCollections": {}})
            {'status': {'collections': ['my_coll']}}
            >>> my_db.command({"countDocuments": {}}, collection_or_table_name="my_coll")
            {'status': {'count': 123}}
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        _keyspace: str | None
        if keyspace is None:
            if collection_or_table_name is not None:
                raise ValueError(
                    "Cannot pass collection_or_table_name to database "
                    "`command` on a no-keyspace command"
                )
            _keyspace = None
        else:
            if isinstance(keyspace, UnsetType):
                _keyspace = self.keyspace
            else:
                _keyspace = keyspace
        # build the ad-hoc-commander path with _keyspace and the coll.or.table
        base_path_components = [
            comp
            for comp in (
                ncomp.strip("/")
                for ncomp in (
                    self.api_options.data_api_url_options.api_path,
                    self.api_options.data_api_url_options.api_version,
                    _keyspace,
                    collection_or_table_name,
                )
                if ncomp is not None
            )
            if comp != ""
        ]
        base_path = f"/{'/'.join(base_path_components)}"
        command_commander = APICommander(
            api_endpoint=self.api_endpoint,
            path=base_path,
            headers=self._commander_headers,
            callers=self.api_options.callers,
            redacted_header_names=self.api_options.redacted_header_names,
        )

        _cmd_desc = ",".join(sorted(body.keys()))
        logger.info(f"command={_cmd_desc} on {self.__class__.__name__}")
        req_response = command_commander.request(
            payload=body,
            raise_api_errors=raise_api_errors,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        logger.info(f"command={_cmd_desc} on {self.__class__.__name__}")
        return req_response

    def get_database_admin(
        self,
        *,
        token: str | TokenProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> DatabaseAdmin:
        """
        Return a DatabaseAdmin object corresponding to this database, for
        use in admin tasks such as managing keyspaces.

        This method, depending on the environment where the database resides,
        returns an appropriate subclass of DatabaseAdmin.

        Args:
            token: an access token with enough permission on the database to
                perform the desired tasks. If omitted (as it can generally be done),
                the token of this Database is used.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            spawn_api_options: a specification - complete or partial - of the
                API Options to override the defaults.
                This allows for a deeper configuration of the database admin, e.g.
                concerning timeouts; if this is passed together with
                the equivalent named parameters, the latter will take precedence
                in their respective settings.

        Returns:
            A DatabaseAdmin instance targeting this database. More precisely,
            for Astra DB an instance of `AstraDBDatabaseAdmin` is returned;
            for other environments, an instance of `DataAPIDatabaseAdmin` is returned.

        Example:
            >>> my_db_admin = my_db.get_database_admin()
            >>> if "new_keyspace" not in my_db_admin.list_keyspaces():
            ...     my_db_admin.create_keyspace("new_keyspace")
            >>> my_db_admin.list_keyspaces()
            ['default_keyspace', 'new_keyspace']
        """

        # lazy importing here to avoid circular dependency
        from astrapy.admin.admin import AstraDBDatabaseAdmin, DataAPIDatabaseAdmin

        arg_api_options = APIOptions(
            token=token,
        )
        api_options = self.api_options.with_override(spawn_api_options).with_override(
            arg_api_options
        )

        if api_options.environment in Environment.astra_db_values:
            if parse_api_endpoint(self.api_endpoint) is None:
                raise InvalidEnvironmentException(
                    "Cannot use a nonstandard API endpoint for this operation."
                )
            return AstraDBDatabaseAdmin(
                api_endpoint=self.api_endpoint,
                api_options=api_options,
                spawner_database=self,
            )
        else:
            return DataAPIDatabaseAdmin(
                api_endpoint=self.api_endpoint,
                api_options=api_options,
                spawner_database=self,
            )


class AsyncDatabase:
    """
    A Data API database. This is the object for doing database-level
    DML, such as creating/deleting collections, and for obtaining Collection
    objects themselves. This class has an asynchronous interface.

    This class is not meant for direct instantiation by the user, rather
    it is usually obtained by invoking methods such as `get_async_database`
    of AstraDBClient.

    On Astra DB, an AsyncDatabase comes with an "API Endpoint", which implies
    an AsyncDatabase object instance reaches a specific region (relevant point in
    case of multi-region databases).

    An AsyncDatabase is also always set with a "working keyspace" on which all
    data operations are done (unless otherwise specified).

    Args:
        api_endpoint: the full "API Endpoint" string used to reach the Data API.
            Example: "https://<database_id>-<region>.apps.astra.datastax.com"
        keyspace: this is the keyspace all method calls will target, unless
            one is explicitly specified in the call. If no keyspace is supplied
            when creating a Database, on Astra DB the name "default_keyspace" is set,
            while on other environments the keyspace is left unspecified: in this case,
            most operations are unavailable until a keyspace is set (through an explicit
            `use_keyspace` invocation or equivalent).
        api_options: a complete specification of the API Options for this instance.

    Example:
        >>> from astrapy import DataAPIClient
        >>> my_client = astrapy.DataAPIClient()
        >>> my_db = my_client.get_async_database(
        ...    "https://01234567-....apps.astra.datastax.com",
        ...     token="AstraCS:...",
        ... )

    Note:
        creating an instance of AsyncDatabase does not trigger actual creation
        of the database itself, which should exist beforehand. To create databases,
        see the AstraDBAdmin class.
    """

    def __init__(
        self,
        *,
        api_endpoint: str,
        keyspace: str | None,
        api_options: FullAPIOptions,
    ) -> None:
        self.api_options = api_options
        self.api_endpoint = api_endpoint.strip("/")
        # enforce defaults if on Astra DB:
        self._using_keyspace: str | None
        if (
            keyspace is None
            and self.api_options.environment in Environment.astra_db_values
        ):
            self._using_keyspace = DEFAULT_ASTRA_DB_KEYSPACE
        else:
            self._using_keyspace = keyspace

        self._commander_headers = {
            DEFAULT_DATA_API_AUTH_HEADER: self.api_options.token.get_token(),
            **self.api_options.database_additional_headers,
        }
        self._name: str | None = None
        self._api_commander = self._get_api_commander(keyspace=self.keyspace)

    def __getattr__(self, collection_name: str) -> AsyncCollection[DefaultDocumentType]:
        return self.get_collection(name=collection_name)

    def __getitem__(self, collection_name: str) -> AsyncCollection[DefaultDocumentType]:
        return self.get_collection(name=collection_name)

    def __repr__(self) -> str:
        ep_desc = f'api_endpoint="{self.api_endpoint}"'
        keyspace_desc: str | None
        if self._using_keyspace is None:
            keyspace_desc = "keyspace not set"
        else:
            keyspace_desc = f'keyspace="{self._using_keyspace}"'
        api_options_desc = f"api_options={self.api_options}"
        parts = [
            pt for pt in [ep_desc, keyspace_desc, api_options_desc] if pt is not None
        ]
        return f"{self.__class__.__name__}({', '.join(parts)})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, AsyncDatabase):
            return all(
                [
                    self.api_endpoint == other.api_endpoint,
                    self.keyspace == other.keyspace,
                    self.api_options == other.api_options,
                ]
            )
        else:
            return False

    def _get_api_commander(self, keyspace: str | None) -> APICommander | None:
        """
        Instantiate a new APICommander based on the properties of this class
        and a provided keyspace.

        If keyspace is None, return None (signaling a "keyspace not set").
        """

        if keyspace is None:
            return None
        else:
            base_path_components = [
                comp
                for comp in (
                    ncomp.strip("/")
                    for ncomp in (
                        self.api_options.data_api_url_options.api_path,
                        self.api_options.data_api_url_options.api_version,
                        keyspace,
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

    def _get_driver_commander(self, keyspace: str | None) -> APICommander:
        """
        Building on _get_api_commander, fall back to class keyspace in
        creating/returning a commander, and in any case raise an error if not set.
        """
        driver_commander: APICommander | None
        if keyspace:
            driver_commander = self._get_api_commander(keyspace=keyspace)
        else:
            driver_commander = self._api_commander
        if driver_commander is None:
            raise ValueError(
                "No keyspace specified. This operation requires a keyspace to "
                "be set, e.g. through the `use_keyspace` method."
            )
        return driver_commander

    async def __aenter__(self) -> AsyncDatabase:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        if self._api_commander is not None:
            await self._api_commander.__aexit__(
                exc_type=exc_type,
                exc_value=exc_value,
                traceback=traceback,
            )

    def _copy(
        self,
        *,
        keyspace: str | None = None,
        token: str | TokenProvider | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncDatabase:
        arg_api_options = APIOptions(
            token=token,
        )
        final_api_options = self.api_options.with_override(api_options).with_override(
            arg_api_options
        )
        return AsyncDatabase(
            api_endpoint=self.api_endpoint,
            keyspace=keyspace or self.keyspace,
            api_options=final_api_options,
        )

    def with_options(
        self,
        *,
        keyspace: str | None = None,
        token: str | TokenProvider | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncDatabase:
        """
        Create a clone of this database with some changed attributes.

        Args:
            keyspace: this is the keyspace all method calls will target, unless
                one is explicitly specified in the call. If no keyspace is supplied
                when creating a Database, the name "default_keyspace" is set.
            token: an Access Token to the database. Example: `"AstraCS:xyz..."`.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            api_options: any additional options to set for the clone, in the form of
                an APIOptions instance (where one can set just the needed attributes).
                In case the same setting is also provided as named parameter,
                the latter takes precedence.

        Returns:
            a new `AsyncDatabase` instance.

        Example:
            >>> async_database_2 = async_database.with_options(
            ...     keyspace="the_other_keyspace",
            ...     token="AstraCS:xyz...",
            ... )
        """

        return self._copy(
            keyspace=keyspace,
            token=token,
            api_options=api_options,
        )

    def to_sync(
        self,
        *,
        keyspace: str | None = None,
        token: str | TokenProvider | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> Database:
        """
        Create a (synchronous) Database from this one. Save for the arguments
        explicitly provided as overrides, everything else is kept identical
        to this database in the copy.

        Args:
            keyspace: this is the keyspace all method calls will target, unless
                one is explicitly specified in the call. If no keyspace is supplied
                when creating a Database, the name "default_keyspace" is set.
            token: an Access Token to the database. Example: "AstraCS:xyz..."
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            api_options: any additional options to set for the result, in the form of
                an APIOptions instance (where one can set just the needed attributes).
                In case the same setting is also provided as named parameter,
                the latter takes precedence.

        Returns:
            the new copy, a `Database` instance.

        Example:
            >>> my_sync_db = async_database.to_sync()
            >>> my_sync_db.list_collection_names()
            ['a_collection', 'another_collection']
        """

        arg_api_options = APIOptions(
            token=token,
        )
        final_api_options = self.api_options.with_override(api_options).with_override(
            arg_api_options
        )
        return Database(
            api_endpoint=self.api_endpoint,
            keyspace=keyspace or self.keyspace,
            api_options=final_api_options,
        )

    def use_keyspace(self, keyspace: str) -> None:
        """
        Switch to a new working keyspace for this database.
        This method changes (mutates) the AsyncDatabase instance.

        Note that this method does not create the keyspace, which should exist
        already (created for instance with a `DatabaseAdmin.async_create_keyspace` call).

        Args:
            keyspace: the new keyspace to use as the database working keyspace.

        Returns:
            None.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> asyncio.run(async_database.list_collection_names())
            ['coll_1', 'coll_2']
            >>> async_database.use_keyspace("an_empty_keyspace")
            >>> asyncio.run(async_database.list_collection_names())
            []
        """
        logger.info(f"switching to keyspace '{keyspace}'")
        self._using_keyspace = keyspace
        self._api_commander = self._get_api_commander(keyspace=self.keyspace)

    async def info(
        self,
        *,
        database_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> AstraDBDatabaseInfo:
        """
        Additional information on the database as a AstraDBDatabaseInfo instance.

        Some of the returned properties are dynamic throughout the lifetime
        of the database (such as raw_info["keyspaces"]). For this reason,
        each invocation of this method triggers a new request to the DevOps API.

        Not available outside of Astra DB and when using custom domains.

        Args:
            database_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `database_admin_timeout_ms`.
            timeout_ms: an alias for `database_admin_timeout_ms`.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> asyncio.run(async_database.info()).region
            'eu-west-1'
            >>> asyncio.run(
            ...     async_database.info()
            ... ).raw_info['datacenters'][0]['dateCreated']
            '2023-01-30T12:34:56Z'

        Note:
            see the AstraDBDatabaseInfo documentation for a caveat about the difference
            between the `region` and the `raw["region"]` attributes.
        """

        if self.api_options.environment not in Environment.astra_db_values:
            raise InvalidEnvironmentException(
                "Environments outside of Astra DB are not supported."
            )
        elif parse_api_endpoint(self.api_endpoint) is None:
            raise InvalidEnvironmentException(
                "Cannot inspect a nonstandard API endpoint for properties."
            )

        _database_admin_timeout_ms, _da_label = _select_singlereq_timeout_da(
            timeout_options=self.api_options.timeout_options,
            database_admin_timeout_ms=database_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info("getting database info")
        database_info = await async_fetch_database_info(
            self.api_endpoint,
            keyspace=self.keyspace,
            request_timeout_ms=_database_admin_timeout_ms,
            api_options=self.api_options,
        )
        if database_info is not None:
            logger.info("finished getting database info")
            return database_info
        else:
            raise DevOpsAPIException("Failure while fetching database info.")

    @property
    def id(self) -> str:
        """
        The ID of this database.
        Not available outside of Astra DB and when using custom domains.

        Example:
            >>> my_async_database.id
            '01234567-89ab-cdef-0123-456789abcdef'
        """

        if self.api_options.environment in Environment.astra_db_values:
            parsed_api_endpoint = parse_api_endpoint(self.api_endpoint)
            if parsed_api_endpoint is not None:
                return parsed_api_endpoint.database_id
            else:
                raise InvalidEnvironmentException(
                    "Cannot inspect a nonstandard API endpoint for properties."
                )
        else:
            raise InvalidEnvironmentException(
                "Database is not in a supported environment for this operation."
            )

    @property
    def region(self) -> str:
        """
        The region where this database is located.

        The region is still well defined in case of multi-region databases,
        since a Database instance connects to exactly one of the regions
        (as specified by the API Endpoint).

        Not available outside of Astra DB and when using custom domains.

        Example:
            >>> my_async_database.region
            'us-west-2'
        """

        if self.api_options.environment in Environment.astra_db_values:
            parsed_api_endpoint = parse_api_endpoint(self.api_endpoint)
            if parsed_api_endpoint is not None:
                return parsed_api_endpoint.region
            else:
                raise InvalidEnvironmentException(
                    "Cannot inspect a nonstandard API endpoint for properties."
                )
        else:
            raise InvalidEnvironmentException(
                "Database is not in a supported environment for this operation."
            )

    async def name(self) -> str:
        """
        The name of this database. Note that this bears no unicity guarantees.

        Calling this method the first time involves a request
        to the DevOps API (the resulting database name is then cached).
        See the `info()` method for more details.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> asyncio.run(async_database.name())
            'the_application_database'
        """

        if self._name is None:
            self._name = (await self.info()).name
        return self._name

    @property
    def keyspace(self) -> str | None:
        """
        The keyspace this database uses as target for all commands when
        no method-call-specific keyspace is specified.

        Returns:
            the working keyspace (a string), or None if not set.

        Example:
            >>> async_database.keyspace
            'the_keyspace'
        """

        return self._using_keyspace

    @overload
    def get_collection(
        self,
        name: str,
        *,
        keyspace: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncCollection[DefaultDocumentType]: ...

    @overload
    def get_collection(
        self,
        name: str,
        *,
        document_type: type[DOC],
        keyspace: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncCollection[DOC]: ...

    def get_collection(
        self,
        name: str,
        *,
        document_type: type[Any] = DefaultDocumentType,
        keyspace: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncCollection[DOC]:
        """
        Spawn an `AsyncCollection` object instance representing a collection
        on this database.

        Creating an `AsyncCollection` instance does not have any effect on the
        actual state of the database: in other words, for the created
        `AsyncCollection` instance to be used meaningfully, the collection
        must exist already (for instance, it should have been created
        previously by calling the `create_collection` method).

        Args:
            name: the name of the collection.
            document_type: this parameter acts a formal specifier for the type checker.
                If omitted, the resulting AsyncCollection is implicitly
                an `AsyncCollection[dict[str, Any]]`. If provided, it must match the
                type hint specified in the assignment.
                See the examples below.
            keyspace: the keyspace containing the collection. If no keyspace
                is specified, the setting for this database is used.
            embedding_api_key: optional API key(s) for interacting with the collection.
                If an embedding service is configured, and this parameter is not None,
                each Data API call will include the necessary embedding-related headers
                as specified by this parameter. If a string is passed, it translates
                into the one "embedding api key" header
                (i.e. `astrapy.authentication.EmbeddingAPIKeyHeaderProvider`).
                For some vectorize providers/models, if using header-based
                authentication, specialized subclasses of
                `astrapy.authentication.EmbeddingHeadersProvider` should be supplied.
            reranking_api_key: optional API key(s) for interacting with the collection.
                If a reranker is configured for the collection, and this parameter
                is not None, Data API calls will include the appropriate
                reranker-related headers according to this parameter. Reranker services
                may not necessarily require this setting (e.g. if the service needs no
                authentication, or one is configured as part of the collection
                definition relying on a "shared secret").
                If a string is passed, it is translated into an instance of
                `astrapy.authentication.RerankingAPIKeyHeaderProvider`.
            spawn_api_options: a specification - complete or partial - of the
                API Options to override the defaults inherited from the Database.
                This allows for a deeper configuration of the collection, e.g.
                concerning timeouts; if this is passed together with
                the named timeout parameters, the latter will take precedence
                in their respective settings.

        Returns:
            an `AsyncCollection` instance, representing the desired collection
                (but without any form of validation).

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> async def count_docs(adb: AsyncDatabase, c_name: str) -> int:
            ...    async_col = adb.get_collection(c_name)
            ...    return await async_col.count_documents({}, upper_bound=100)
            ...
            >>> asyncio.run(count_docs(async_database, "my_collection"))
            45

        Note: the attribute and indexing syntax forms achieve the same effect
            as this method, returning an AsyncCollection.
            In other words, the following are equivalent:
                async_database.get_collection("coll_name")
                async_database.coll_name
                async_database["coll_name"]
        """

        # lazy importing here against circular-import error
        from astrapy.collection import AsyncCollection

        resulting_api_options = self.api_options.with_override(
            spawn_api_options,
        ).with_override(
            APIOptions(
                embedding_api_key=embedding_api_key,
                reranking_api_key=reranking_api_key,
            ),
        )

        _keyspace = keyspace or self.keyspace
        if _keyspace is None:
            raise ValueError(
                "No keyspace specified. This operation requires a keyspace to "
                "be set, e.g. through the `use_keyspace` method."
            )
        return AsyncCollection(
            database=self,
            name=name,
            keyspace=_keyspace,
            api_options=resulting_api_options,
        )

    @overload
    async def create_collection(
        self,
        name: str,
        *,
        definition: CollectionDefinition | dict[str, Any] | None = None,
        keyspace: str | None = None,
        collection_admin_timeout_ms: int | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncCollection[DefaultDocumentType]: ...

    @overload
    async def create_collection(
        self,
        name: str,
        *,
        definition: CollectionDefinition | dict[str, Any] | None = None,
        document_type: type[DOC],
        keyspace: str | None = None,
        collection_admin_timeout_ms: int | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncCollection[DOC]: ...

    async def create_collection(
        self,
        name: str,
        *,
        definition: CollectionDefinition | dict[str, Any] | None = None,
        document_type: type[Any] = DefaultDocumentType,
        keyspace: str | None = None,
        collection_admin_timeout_ms: int | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncCollection[DOC]:
        """
        Creates a collection on the database and return the AsyncCollection
        instance that represents it.

        This is a blocking operation: the method returns when the collection
        is ready to be used. As opposed to the `get_collection` instance,
        this method triggers causes the collection to be actually created on DB.

        Args:
            name: the name of the collection.
            definition: a complete collection definition for the table. This can be an
                instance of `CollectionDefinition` or an equivalent (nested) dictionary,
                in which case it will be parsed into a `CollectionDefinition`.
                See the `astrapy.info.CollectionDefinition` class and the
                `AsyncCollection` class for more details and ways to construct this object.
            document_type: this parameter acts a formal specifier for the type checker.
                If omitted, the resulting AsyncCollection is implicitly
                an `AsyncCollection[dict[str, Any]]`. If provided, it must match the
                type hint specified in the assignment.
                See the examples below.
            keyspace: the keyspace where the collection is to be created.
                If not specified, the general setting for this database is used.
            collection_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
            embedding_api_key: optional API key(s) for interacting with the collection.
                If an embedding service is configured, and this parameter is not None,
                each Data API call will include the necessary embedding-related headers
                as specified by this parameter. If a string is passed, it translates
                into the one "embedding api key" header
                (i.e. `astrapy.authentication.EmbeddingAPIKeyHeaderProvider`).
                For some vectorize providers/models, if using header-based authentication,
                specialized subclasses of `astrapy.authentication.EmbeddingHeadersProvider`
                should be supplied.
            reranking_api_key: optional API key(s) for interacting with the collection.
                If a reranker is configured for the collection, and this parameter
                is not None, Data API calls will include the appropriate
                reranker-related headers according to this parameter. Reranker services
                may not necessarily require this setting (e.g. if the service needs no
                authentication, or one is configured as part of the collection
                definition relying on a "shared secret").
                If a string is passed, it is translated into an instance of
                `astrapy.authentication.RerankingAPIKeyHeaderProvider`.
            spawn_api_options: a specification - complete or partial - of the
                API Options to override the defaults inherited from the Database.
                This allows for a deeper configuration of the collection, e.g.
                concerning timeouts; if this is passed together with
                the named timeout parameters, the latter will take precedence
                in their respective settings.

        Returns:
            an `AsyncCollection` instance, representing the newly-created collection.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> # Create a collection using the fluent syntax for its definition
            >>> from astrapy.constants import VectorMetric
            >>> from astrapy.info import CollectionDefinition
            >>>
            >>> collection_definition = (
            ...     CollectionDefinition.builder()
            ...     .set_vector_dimension(3)
            ...     .set_vector_metric(VectorMetric.DOT_PRODUCT)
            ...     .set_indexing("deny", ["annotations", "logs"])
            ...     .build()
            ... )
            >>> my_collection = asyncio.run(async_database.create_collection(
            ...     "my_events",
            ...     definition=collection_definition,
            ... ))
            >>>
            >>> # Create a collection with the definition as object
            >>> from astrapy.info import CollectionVectorOptions
            >>>
            >>> collection_definition_1 = CollectionDefinition(
            ...     vector=CollectionVectorOptions(
            ...         dimension=3,
            ...         metric=VectorMetric.DOT_PRODUCT,
            ...     ),
            ...     indexing={"deny": ["annotations", "logs"]},
            ... )
            >>> my_collection_1 = asyncio.run(async_database.create_collection(
            ...     "my_events",
            ...     definition=collection_definition_1,
            ... ))
            >>>
            >>>
            >>> # Create a collection with the definition as plain dictionary
            >>> collection_definition_2 = {
            ...     "indexing": {"deny": ["annotations", "logs"]},
            ...     "vector": {
            ...         "dimension": 3,
            ...         "metric": VectorMetric.DOT_PRODUCT,
            ...     },
            ... }
            >>> my_collection_2 = asyncio.run(async_database.create_collection(
            ...     "my_events",
            ...     definition=collection_definition_2,
            ... ))
        """

        cc_definition: dict[str, Any] = CollectionDefinition.coerce(
            definition or {}
        ).as_dict()
        if collection_admin_timeout_ms is not None:
            _collection_admin_timeout_ms = collection_admin_timeout_ms
            _ca_label = "collection_admin_timeout_ms"
        else:
            _collection_admin_timeout_ms = (
                self.api_options.timeout_options.collection_admin_timeout_ms
            )
            _ca_label = "collection_admin_timeout_ms"
        driver_commander = self._get_driver_commander(keyspace=keyspace)
        cc_payload = {
            "createCollection": {
                k: v
                for k, v in {
                    "name": name,
                    "options": cc_definition,
                }.items()
                if v is not None
                if v != {}
            }
        }
        logger.info(f"createCollection('{name}')")
        cc_response = await driver_commander.async_request(
            payload=cc_payload,
            timeout_context=_TimeoutContext(
                request_ms=_collection_admin_timeout_ms, label=_ca_label
            ),
        )
        if cc_response.get("status") != {"ok": 1}:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from createCollection API command.",
                raw_response=cc_response,
            )
        logger.info(f"finished createCollection('{name}')")
        return self.get_collection(
            name,
            document_type=document_type,
            keyspace=keyspace,
            embedding_api_key=embedding_api_key,
            reranking_api_key=reranking_api_key,
            spawn_api_options=spawn_api_options,
        )

    async def drop_collection(
        self,
        name: str,
        *,
        keyspace: str | None = None,
        collection_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        Drop a collection from the database, along with all documents therein.

        Args:
            name: the name of the collection to drop.
            keyspace: the keyspace where the collection resides. If not specified,
                the database working keyspace is assumed.
            collection_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `collection_admin_timeout_ms`.
            timeout_ms: an alias for `collection_admin_timeout_ms`.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> asyncio.run(async_database.list_collection_names())
            ['a_collection', 'my_v_col', 'another_col']
            >>> asyncio.run(async_database.drop_collection("my_v_col"))
            >>> asyncio.run(async_database.list_collection_names())
            ['a_collection', 'another_col']
        """

        _collection_admin_timeout_ms, _ca_label = _select_singlereq_timeout_ca(
            timeout_options=self.api_options.timeout_options,
            collection_admin_timeout_ms=collection_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        _keyspace = keyspace or self.keyspace
        driver_commander = self._get_driver_commander(keyspace=_keyspace)
        dc_payload = {"deleteCollection": {"name": name}}
        logger.info(f"deleteCollection('{name}')")
        dc_response = await driver_commander.async_request(
            payload=dc_payload,
            timeout_context=_TimeoutContext(
                request_ms=_collection_admin_timeout_ms, label=_ca_label
            ),
        )
        if dc_response.get("status") != {"ok": 1}:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from deleteCollection API command.",
                raw_response=dc_response,
            )
        logger.info(f"finished deleteCollection('{name}')")
        return dc_response.get("status", {})  # type: ignore[no-any-return]

    async def list_collections(
        self,
        *,
        keyspace: str | None = None,
        collection_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[CollectionDescriptor]:
        """
        List all collections in a given keyspace for this database.

        Args:
            keyspace: the keyspace to be inspected. If not specified,
                the general setting for this database is assumed.
            collection_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `collection_admin_timeout_ms`.
            timeout_ms: an alias for `collection_admin_timeout_ms`.

        Returns:
            a list of CollectionDescriptor instances one for each collection.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> async def a_list_colls(adb: AsyncDatabase) -> None:
            ...     a_coll_list = await adb.list_collections()
            ...     print("* list:", a_coll_list)
            ...     for coll in await adb.list_collections():
            ...         print("* coll:", coll)
            ...
            >>> asyncio.run(a_list_colls(async_database))
            * list: [CollectionDescriptor(name='my_v_col', options=CollectionDefinition())]
            * coll: CollectionDescriptor(name='my_v_col', options=CollectionDefinition())
        """

        _collection_admin_timeout_ms, _ca_label = _select_singlereq_timeout_ca(
            timeout_options=self.api_options.timeout_options,
            collection_admin_timeout_ms=collection_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        return await self._list_collections_ctx(
            keyspace=keyspace,
            timeout_context=_TimeoutContext(
                request_ms=_collection_admin_timeout_ms, label=_ca_label
            ),
        )

    async def _list_collections_ctx(
        self,
        *,
        keyspace: str | None,
        timeout_context: _TimeoutContext,
    ) -> list[CollectionDescriptor]:
        driver_commander = self._get_driver_commander(keyspace=keyspace)
        gc_payload = {"findCollections": {"options": {"explain": True}}}
        logger.info("findCollections")
        gc_response = await driver_commander.async_request(
            payload=gc_payload,
            timeout_context=timeout_context,
        )
        if "collections" not in gc_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from findCollections API command.",
                raw_response=gc_response,
            )
        else:
            # we know this is a list of dicts, to marshal into "descriptors"
            logger.info("finished findCollections")
            return [
                CollectionDescriptor._from_dict(col_dict)
                for col_dict in gc_response["status"]["collections"]
            ]

    async def list_collection_names(
        self,
        *,
        keyspace: str | None = None,
        collection_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[str]:
        """
        List the names of all collections in a given keyspace of this database.

        Args:
            keyspace: the keyspace to be inspected. If not specified,
                the general setting for this database is assumed.
            collection_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `collection_admin_timeout_ms`.
            timeout_ms: an alias for `collection_admin_timeout_ms`.

        Returns:
            a list of the collection names as strings, in no particular order.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> asyncio.run(async_database.list_collection_names())
            ['a_collection', 'another_col']
        """

        _collection_admin_timeout_ms, _ca_label = _select_singlereq_timeout_ca(
            timeout_options=self.api_options.timeout_options,
            collection_admin_timeout_ms=collection_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        driver_commander = self._get_driver_commander(keyspace=keyspace)
        gc_payload: dict[str, Any] = {"findCollections": {}}
        logger.info("findCollections")
        gc_response = await driver_commander.async_request(
            payload=gc_payload,
            timeout_context=_TimeoutContext(
                request_ms=_collection_admin_timeout_ms, label=_ca_label
            ),
        )
        if "collections" not in gc_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from findCollections API command.",
                raw_response=gc_response,
            )
        else:
            logger.info("finished findCollections")
            return gc_response["status"]["collections"]  # type: ignore[no-any-return]

    @overload
    def get_table(
        self,
        name: str,
        *,
        keyspace: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncTable[DefaultRowType]: ...

    @overload
    def get_table(
        self,
        name: str,
        *,
        row_type: type[ROW],
        keyspace: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncTable[ROW]: ...

    def get_table(
        self,
        name: str,
        *,
        row_type: type[Any] = DefaultRowType,
        keyspace: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncTable[ROW]:
        """
        Spawn an `AsyncTable` object instance representing a table
        on this database.

        Creating a `AsyncTable` instance does not have any effect on the
        actual state of the database: in other words, for the created
        `AsyncTable` instance to be used meaningfully, the table
        must exist already (for instance, it should have been created
        previously by calling the `create_table` method).

        Args:
            name: the name of the table.
            row_type: this parameter acts a formal specifier for the type checker.
                If omitted, the resulting AsyncTable is implicitly
                an `AsyncTable[dict[str, Any]]`. If provided, it must match
                the type hint specified in the assignment.
                See the examples below.
            keyspace: the keyspace containing the table. If no keyspace
                is specified, the general setting for this database is used.
            embedding_api_key: optional API key(s) for interacting with the table.
                If an embedding service is configured, and this parameter is not None,
                each Data API call will include the necessary embedding-related headers
                as specified by this parameter. If a string is passed, it translates
                into the one "embedding api key" header
                (i.e. `astrapy.authentication.EmbeddingAPIKeyHeaderProvider`).
                For some vectorize providers/models, if using header-based
                authentication, specialized subclasses of
                `astrapy.authentication.EmbeddingHeadersProvider` should be supplied.
            reranking_api_key: optional API key(s) for interacting with the table.
                If a reranker is configured for the table, and this parameter
                is not None, Data API calls will include the appropriate
                reranker-related headers according to this parameter. Reranker services
                may not necessarily require this setting (e.g. if the service needs no
                authentication, or one is configured as part of the table
                definition relying on a "shared secret").
                If a string is passed, it is translated into an instance of
                `astrapy.authentication.RerankingAPIKeyHeaderProvider`.
            spawn_api_options: a specification - complete or partial - of the
                API Options to override the defaults inherited from the Database.
                This allows for a deeper configuration of the table, e.g.
                concerning timeouts; if this is passed together with
                the named timeout parameters, the latter will take precedence
                in their respective settings.

        Returns:
            an `AsyncTable` instance, representing the desired table
                (but without any form of validation).

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> # Get an AsyncTable object (and read a property of it as an example):
            >>> my_async_table = async_database.get_table("games")
            >>> my_async_table.full_name
            'default_keyspace.games'
            >>>
            >>> # Get an AsyncTable object in a specific keyspace,
            >>> # and set an embedding API key to it:
            >>> my_other_async_table = async_database.get_table(
            ...     "tournaments",
            ...     keyspace="the_other_keyspace",
            ...     embedding_api_key="secret-012abc...",
            ... )
            >>> from astrapy import AsyncTable
            >>> MyCustomDictType = dict[str, int]
            >>>
            >>> # Get an AsyncTable object typed with a specific type for its rows:
            >>> my_typed_async_table: AsyncTable[MyCustomDictType] = async_database.get_table(
            ...     "games",
            ...     row_type=MyCustomDictType,
            ... )
        """

        # lazy importing here against circular-import error
        from astrapy.table import AsyncTable

        resulting_api_options = self.api_options.with_override(
            spawn_api_options,
        ).with_override(
            APIOptions(
                embedding_api_key=embedding_api_key,
                reranking_api_key=reranking_api_key,
            ),
        )

        _keyspace = keyspace or self.keyspace
        if _keyspace is None:
            raise ValueError(
                "No keyspace specified. This operation requires a keyspace to "
                "be set, e.g. through the `use_keyspace` method."
            )
        return AsyncTable[ROW](
            database=self,
            name=name,
            keyspace=_keyspace,
            api_options=resulting_api_options,
        )

    @overload
    async def create_table(
        self,
        name: str,
        *,
        definition: CreateTableDefinition | dict[str, Any],
        keyspace: str | None = None,
        if_not_exists: bool | None = None,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncTable[DefaultRowType]: ...

    @overload
    async def create_table(
        self,
        name: str,
        *,
        definition: CreateTableDefinition | dict[str, Any],
        row_type: type[ROW],
        keyspace: str | None = None,
        if_not_exists: bool | None = None,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncTable[ROW]: ...

    async def create_table(
        self,
        name: str,
        *,
        definition: CreateTableDefinition | dict[str, Any],
        row_type: type[Any] = DefaultRowType,
        keyspace: str | None = None,
        if_not_exists: bool | None = None,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncTable[ROW]:
        """
        Creates a table on the database and return the AsyncTable
        instance that represents it.

        This is a blocking operation: the method returns when the table
        is ready to be used. As opposed to the `get_table` method call,
        this method causes the table to be actually created on DB.

        Args:
            name: the name of the table.
            definition: a complete table definition for the table. This can be an
                instance of `CreateTableDefinition` or an equivalent (nested) dictionary,
                in which case it will be parsed into a `CreateTableDefinition`.
                See the `astrapy.info.CreateTableDefinition` class and the
                `AsyncTable` class for more details and ways to construct this object.
            row_type: this parameter acts a formal specifier for the type checker.
                If omitted, the resulting AsyncTable is implicitly
                an `AsyncTable[dict[str, Any]]`. If provided, it must match
                the type hint specified in the assignment.
                See the examples below.
            keyspace: the keyspace where the table is to be created.
                If not specified, the general setting for this database is used.
            if_not_exists: if set to True, the command will succeed even if a table
                with the specified name already exists (in which case no actual
                table creation takes place on the database). Defaults to False,
                i.e. an error is raised by the API in case of table-name collision.
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.
            embedding_api_key: optional API key(s) for interacting with the table.
                If an embedding service is configured, and this parameter is not None,
                each Data API call will include the necessary embedding-related headers
                as specified by this parameter. If a string is passed, it translates
                into the one "embedding api key" header
                (i.e. `astrapy.authentication.EmbeddingAPIKeyHeaderProvider`).
                For some vectorize providers/models, if using header-based authentication,
                specialized subclasses of `astrapy.authentication.EmbeddingHeadersProvider`
                should be supplied.
            reranking_api_key: optional API key(s) for interacting with the table.
                If a reranker is configured for the table, and this parameter
                is not None, Data API calls will include the appropriate
                reranker-related headers according to this parameter. Reranker services
                may not necessarily require this setting (e.g. if the service needs no
                authentication, or one is configured as part of the table
                definition relying on a "shared secret").
                If a string is passed, it is translated into an instance of
                `astrapy.authentication.RerankingAPIKeyHeaderProvider`.
            spawn_api_options: a specification - complete or partial - of the
                API Options to override the defaults inherited from the Database.
                This allows for a deeper configuration of the table, e.g.
                concerning timeouts; if this is passed together with
                the named timeout parameters, the latter will take precedence
                in their respective settings.

        Returns:
            an `AsyncTable` instance, representing the
            newly-created table.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> # Create a table using the fluent syntax for definition
            >>> from astrapy.constants import SortMode
            >>> from astrapy.info import (
            ...     CreateTableDefinition,
            ...     ColumnType,
            ... )
            >>> table_definition = (
            ...     CreateTableDefinition.builder()
            ...     .add_column("match_id", ColumnType.TEXT)
            ...     .add_column("round", ColumnType.INT)
            ...     .add_vector_column("m_vector", dimension=3)
            ...     .add_column("score", ColumnType.INT)
            ...     .add_column("when", ColumnType.TIMESTAMP)
            ...     .add_column("winner", ColumnType.TEXT)
            ...     .add_set_column("fighters", ColumnType.UUID)
            ...     .add_partition_by(["match_id"])
            ...     .add_partition_sort({"round": SortMode.ASCENDING})
            ...     .build()
            ... )
            >>> my_async_table = asyncio.run(async_database.create_table(
            ...     "games",
            ...     definition=table_definition,
            ... ))
            >>>
            >>> # Create a table with the definition as object
            >>> # (and do not raise an error if the table exists already)
            >>> from astrapy.info import (
            ...     CreateTableDefinition,
            ...     TablePrimaryKeyDescriptor,
            ...     TableScalarColumnTypeDescriptor,
            ...     TableValuedColumnType,
            ...     TableValuedColumnTypeDescriptor,
            ...     TableVectorColumnTypeDescriptor,
            ... )
            >>> table_definition_1 = CreateTableDefinition(
            ...     columns={
            ...         "match_id": TableScalarColumnTypeDescriptor(
            ...             ColumnType.TEXT,
            ...         ),
            ...         "round": TableScalarColumnTypeDescriptor(
            ...             ColumnType.INT,
            ...         ),
            ...         "m_vector": TableVectorColumnTypeDescriptor(
            ...             column_type="vector", dimension=3
            ...         ),
            ...         "score": TableScalarColumnTypeDescriptor(
            ...             ColumnType.INT,
            ...         ),
            ...         "when": TableScalarColumnTypeDescriptor(
            ...             ColumnType.TIMESTAMP,
            ...         ),
            ...         "winner": TableScalarColumnTypeDescriptor(
            ...             ColumnType.TEXT,
            ...         ),
            ...         "fighters": TableValuedColumnTypeDescriptor(
            ...             column_type=TableValuedColumnType.SET,
            ...             value_type=ColumnType.UUID,
            ...         ),
            ...     },
            ...     primary_key=TablePrimaryKeyDescriptor(
            ...         partition_by=["match_id"],
            ...         partition_sort={"round": SortMode.ASCENDING},
            ...     ),
            ... )
            >>> my_async_table_1 = asyncio.run(async_database.create_table(
            ...     "games",
            ...     definition=table_definition_1,
            ...     if_not_exists=True,
            ... ))
            >>>
            >>> # Create a table with the definition as plain dictionary
            >>> # (and do not raise an error if the table exists already)
            >>> table_definition_2 = {
            ...     "columns": {
            ...         "match_id": {"type": "text"},
            ...         "round": {"type": "int"},
            ...         "m_vector": {"type": "vector", "dimension": 3},
            ...         "score": {"type": "int"},
            ...         "when": {"type": "timestamp"},
            ...         "winner": {"type": "text"},
            ...         "fighters": {"type": "set", "valueType": "uuid"},
            ...     },
            ...     "primaryKey": {
            ...         "partitionBy": ["match_id"],
            ...         "partitionSort": {"round": 1},
            ...     },
            ... }
            >>> my_async_table_2 = asyncio.run(async_database.create_table(
            ...     "games",
            ...     definition=table_definition_2,
            ...     if_not_exists=True,
            ... ))
        """

        ct_options: dict[str, bool]
        if if_not_exists is not None:
            ct_options = {"ifNotExists": if_not_exists}
        else:
            ct_options = {}
        ct_definition: dict[str, Any] = CreateTableDefinition.coerce(
            definition
        ).as_dict()
        _table_admin_timeout_ms, _ta_label = _select_singlereq_timeout_ta(
            timeout_options=self.api_options.timeout_options,
            table_admin_timeout_ms=table_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        driver_commander = self._get_driver_commander(keyspace=keyspace)
        ct_payload = {
            "createTable": {
                k: v
                for k, v in {
                    "name": name,
                    "definition": ct_definition,
                    "options": ct_options,
                }.items()
                if v is not None
                if v != {}
            }
        }
        logger.info(f"createTable('{name}')")
        ct_response = await driver_commander.async_request(
            payload=ct_payload,
            timeout_context=_TimeoutContext(
                request_ms=_table_admin_timeout_ms, label=_ta_label
            ),
        )
        if ct_response.get("status") != {"ok": 1}:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from createTable API command.",
                raw_response=ct_response,
            )
        logger.info(f"finished createTable('{name}')")
        return self.get_table(
            name,
            row_type=row_type,
            keyspace=keyspace,
            embedding_api_key=embedding_api_key,
            reranking_api_key=reranking_api_key,
            spawn_api_options=spawn_api_options,
        )

    async def drop_table_index(
        self,
        name: str,
        *,
        keyspace: str | None = None,
        if_exists: bool | None = None,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        """
        Drops (deletes) an index (of any kind) from the table it is associated to.

        This is a blocking operation: the method returns once the index
        is deleted.

        Note:
            Although associated to a table, index names are unique across a keyspace.
            For this reason, no table name is required in this call.

        Args:
            name: the name of the index.
            keyspace: the keyspace to which the index belongs.
                If not specified, the general setting for this database is used.
            if_exists: if passed as True, trying to drop a non-existing index
                will not error, just silently do nothing instead. If not provided,
                the API default behaviour will hold.
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> # Drop an index from the keyspace:
            >>> await async_database.drop_table_index("score_index")
            >>> # Drop an index, unless it does not exist already:
            >>> await async_database.drop_table_index("score_index", if_exists=True)
        """

        _table_admin_timeout_ms, _ta_label = _select_singlereq_timeout_ta(
            timeout_options=self.api_options.timeout_options,
            table_admin_timeout_ms=table_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        di_options: dict[str, bool]
        if if_exists is not None:
            di_options = {"ifExists": if_exists}
        else:
            di_options = {}
        di_payload = {
            "dropIndex": {
                k: v
                for k, v in {
                    "name": name,
                    "options": di_options,
                }.items()
                if v is not None
                if v != {}
            }
        }
        driver_commander = self._get_driver_commander(keyspace=keyspace)
        logger.info(f"dropIndex('{name}')")
        di_response = await driver_commander.async_request(
            payload=di_payload,
            timeout_context=_TimeoutContext(
                request_ms=_table_admin_timeout_ms, label=_ta_label
            ),
        )
        if di_response.get("status") != {"ok": 1}:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from dropIndex API command.",
                raw_response=di_response,
            )
        logger.info(f"finished dropIndex('{name}')")

    async def drop_table(
        self,
        name: str,
        *,
        keyspace: str | None = None,
        if_exists: bool | None = None,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        Drop a table from the database, along with all rows therein and related indexes.

        Args:
            name: the name of the table to drop.
            keyspace: the keyspace where the table resides. If not specified,
                the database working keyspace is assumed.
            if_exists: if passed as True, trying to drop a non-existing table
                will not error, just silently do nothing instead. If not provided,
                the API default behaviour will hold.
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> asyncio.run(async_database.list_table_names())
            ['fighters', 'games']
            >>> asyncio.run(async_database.drop_table("fighters"))
            >>> asyncio.run(async_database.list_table_names())
            ['games']
            >>> # not erroring because of if_not_exists:
            >>> asyncio.run(async_database.drop_table("fighters", if_not_exists=True))
        """

        _table_admin_timeout_ms, _ta_label = _select_singlereq_timeout_ta(
            timeout_options=self.api_options.timeout_options,
            table_admin_timeout_ms=table_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        _keyspace = keyspace or self.keyspace
        dt_options: dict[str, bool]
        if if_exists is not None:
            dt_options = {"ifExists": if_exists}
        else:
            dt_options = {}
        driver_commander = self._get_driver_commander(keyspace=_keyspace)
        dt_payload = {
            "dropTable": {
                k: v
                for k, v in {
                    "name": name,
                    "options": dt_options,
                }.items()
                if v is not None
                if v != {}
            }
        }
        logger.info(f"dropTable('{name}')")
        dt_response = await driver_commander.async_request(
            payload=dt_payload,
            timeout_context=_TimeoutContext(
                request_ms=_table_admin_timeout_ms, label=_ta_label
            ),
        )
        if dt_response.get("status") != {"ok": 1}:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from dropTable API command.",
                raw_response=dt_response,
            )
        logger.info(f"finished dropTable('{name}')")
        return dt_response.get("status", {})  # type: ignore[no-any-return]

    async def list_tables(
        self,
        *,
        keyspace: str | None = None,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[ListTableDescriptor]:
        """
        List all tables in a given keyspace for this database.

        Args:
            keyspace: the keyspace to be inspected. If not specified,
                the general setting for this database is assumed.
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Returns:
            a list of ListTableDescriptor instances, one for each table.

        Example:
            >>> tables = asyncio.run(my_async_database.list_tables())
            >>> tables
            [ListTableDescriptor(name='fighters', definition=ListTableDefinition(...
            >>> tables[1].name
            'games'
            >>> tables[1].definition.columns
            {'match_id': TableScalarColumnTypeDescriptor(ColumnType.TEXT),...
            >>> tables[1].definition.columns['score']
            TableScalarColumnTypeDescriptor(ColumnType.INT)
            >>> tables[1].definition.primary_key.partition_by
            ['match_id']
            >>> tables[1].definition.primary_key.partition_sort
            {'round': 1}
        """

        _table_admin_timeout_ms, _ta_label = _select_singlereq_timeout_ta(
            timeout_options=self.api_options.timeout_options,
            table_admin_timeout_ms=table_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        return await self._list_tables_ctx(
            keyspace=keyspace,
            timeout_context=_TimeoutContext(
                request_ms=_table_admin_timeout_ms, label=_ta_label
            ),
        )

    async def _list_tables_ctx(
        self,
        *,
        keyspace: str | None,
        timeout_context: _TimeoutContext,
    ) -> list[ListTableDescriptor]:
        driver_commander = self._get_driver_commander(keyspace=keyspace)
        lt_payload = {"listTables": {"options": {"explain": True}}}
        logger.info("listTables")
        lt_response = await driver_commander.async_request(
            payload=lt_payload,
            timeout_context=timeout_context,
        )
        if "tables" not in lt_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from listTables API command.",
                raw_response=lt_response,
            )
        else:
            # we know this is a list of dicts, to marshal into "descriptors"
            logger.info("finished listTables")
            return [
                ListTableDescriptor.coerce(tab_dict)
                for tab_dict in lt_response["status"]["tables"]
            ]

    async def list_table_names(
        self,
        *,
        keyspace: str | None = None,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[str]:
        """
        List the names of all tables in a given keyspace of this database.

        Args:
            keyspace: the keyspace to be inspected. If not specified,
                the general setting for this database is assumed.
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Returns:
            a list of the table names as strings, in no particular order.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> async def destroy_temp_table(async_db: AsyncDatabase) -> None:
            ...     print(await async_db.list_table_names())
            ...     await async_db.drop_table("my_v_tab")
            ...     print(await async_db.list_table_names())
            ...
            >>> asyncio.run(destroy_temp_table(async_database))
            ['fighters', 'my_v_tab', 'games']
            ['fighters', 'games']
        """

        _table_admin_timeout_ms, _ta_label = _select_singlereq_timeout_ta(
            timeout_options=self.api_options.timeout_options,
            table_admin_timeout_ms=table_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        driver_commander = self._get_driver_commander(keyspace=keyspace)
        lt_payload: dict[str, Any] = {"listTables": {}}
        logger.info("listTables")
        lt_response = await driver_commander.async_request(
            payload=lt_payload,
            timeout_context=_TimeoutContext(
                request_ms=_table_admin_timeout_ms, label=_ta_label
            ),
        )
        if "tables" not in lt_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from listTables API command.",
                raw_response=lt_response,
            )
        else:
            logger.info("finished listTables")
            return lt_response["status"]["tables"]  # type: ignore[no-any-return]

    async def command(
        self,
        body: dict[str, Any],
        *,
        keyspace: str | None | UnsetType = _UNSET,
        collection_or_table_name: str | None = None,
        raise_api_errors: bool = True,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        Send a POST request to the Data API for this database with
        an arbitrary, caller-provided payload.

        Args:
            body: a JSON-serializable dictionary, the payload of the request.
            keyspace: the keyspace to use, if any. If a keyspace is employed,
                it is used to construct the full request URL. To run a command
                targeting no specific keyspace (rather, the database as a whole),
                pass an explicit `None`: the request URL will lack the suffix
                "/<keyspace>" component. If unspecified, the working keyspace of
                this database is used. If another keyspace is passed, it will be
                used instead of the database's working one.
            collection_or_table_name: if provided, the name is appended at the end
                of the endpoint. In this way, this method allows collection-
                and table-level arbitrary POST requests as well.
                This parameter cannot be used if `keyspace=None` is explicitly provided.
            raise_api_errors: if True, responses with a nonempty 'errors' field
                result in an astrapy exception being raised.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a dictionary with the response of the HTTP request.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> my_db.command({"findCollections": {}})
            {'status': {'collections': ['my_coll']}}
            >>> my_db.command({"countDocuments": {}}, collection_or_table_name="my_coll")
            {'status': {'count': 123}}
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        _keyspace: str | None
        if keyspace is None:
            if collection_or_table_name is not None:
                raise ValueError(
                    "Cannot pass collection_or_table_name to database "
                    "`command` on a no-keyspace command"
                )
            _keyspace = None
        else:
            if isinstance(keyspace, UnsetType):
                _keyspace = self.keyspace
            else:
                _keyspace = keyspace
        # build the ad-hoc-commander path with _keyspace and the coll.or.table
        base_path_components = [
            comp
            for comp in (
                ncomp.strip("/")
                for ncomp in (
                    self.api_options.data_api_url_options.api_path,
                    self.api_options.data_api_url_options.api_version,
                    _keyspace,
                    collection_or_table_name,
                )
                if ncomp is not None
            )
            if comp != ""
        ]
        base_path = f"/{'/'.join(base_path_components)}"
        command_commander = APICommander(
            api_endpoint=self.api_endpoint,
            path=base_path,
            headers=self._commander_headers,
            callers=self.api_options.callers,
            redacted_header_names=self.api_options.redacted_header_names,
        )

        _cmd_desc = ",".join(sorted(body.keys()))
        logger.info(f"command={_cmd_desc} on {self.__class__.__name__}")
        req_response = await command_commander.async_request(
            payload=body,
            raise_api_errors=raise_api_errors,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        logger.info(f"command={_cmd_desc} on {self.__class__.__name__}")
        return req_response

    def get_database_admin(
        self,
        *,
        token: str | TokenProvider | UnsetType = _UNSET,
        spawn_api_options: APIOptions | UnsetType = _UNSET,
    ) -> DatabaseAdmin:
        """
        Return a DatabaseAdmin object corresponding to this database, for
        use in admin tasks such as managing keyspaces.

        This method, depending on the environment where the database resides,
        returns an appropriate subclass of DatabaseAdmin.

        Args:
            token: an access token with enough permission on the database to
                perform the desired tasks. If omitted (as it can generally be done),
                the token of this Database is used.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            spawn_api_options: a specification - complete or partial - of the
                API Options to override the defaults.
                This allows for a deeper configuration of the database admin, e.g.
                concerning timeouts; if this is passed together with
                the equivalent named parameters, the latter will take precedence
                in their respective settings.

        Returns:
            A DatabaseAdmin instance targeting this database. More precisely,
            for Astra DB an instance of `AstraDBDatabaseAdmin` is returned;
            for other environments, an instance of `DataAPIDatabaseAdmin` is returned.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> my_db_admin = async_database.get_database_admin()
            >>> if "new_keyspace" not in my_db_admin.list_keyspaces():
            ...     my_db_admin.create_keyspace("new_keyspace")
            >>> my_db_admin.list_keyspaces()
            ['default_keyspace', 'new_keyspace']
        """

        # lazy importing here to avoid circular dependency
        from astrapy.admin.admin import AstraDBDatabaseAdmin, DataAPIDatabaseAdmin

        arg_api_options = APIOptions(
            token=token,
        )
        api_options = self.api_options.with_override(spawn_api_options).with_override(
            arg_api_options
        )

        if api_options.environment in Environment.astra_db_values:
            if parse_api_endpoint(self.api_endpoint) is None:
                raise InvalidEnvironmentException(
                    "Cannot use a nonstandard API endpoint for this operation."
                )
            return AstraDBDatabaseAdmin(
                api_endpoint=self.api_endpoint,
                api_options=api_options,
                spawner_database=self,
            )
        else:
            return DataAPIDatabaseAdmin(
                api_endpoint=self.api_endpoint,
                api_options=api_options,
                spawner_database=self,
            )
