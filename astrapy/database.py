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
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Type, Union

from astrapy.admin import (
    API_PATH_ENV_MAP,
    API_VERSION_ENV_MAP,
    fetch_database_info,
    parse_api_endpoint,
)
from astrapy.api_options import CollectionAPIOptions
from astrapy.authentication import (
    coerce_embedding_headers_provider,
    coerce_token_provider,
)
from astrapy.constants import Environment
from astrapy.core.db import AstraDB, AsyncAstraDB
from astrapy.cursors import AsyncCommandCursor, CommandCursor
from astrapy.exceptions import (
    CollectionAlreadyExistsException,
    DataAPIFaultyResponseException,
    DevOpsAPIException,
    MultiCallTimeoutManager,
    base_timeout_info,
    recast_method_async,
    recast_method_sync,
)
from astrapy.info import (
    CollectionDescriptor,
    CollectionVectorServiceOptions,
    DatabaseInfo,
)

if TYPE_CHECKING:
    from astrapy.admin import DatabaseAdmin
    from astrapy.authentication import EmbeddingHeadersProvider, TokenProvider
    from astrapy.collection import AsyncCollection, Collection


DEFAULT_ASTRA_DB_NAMESPACE = "default_keyspace"

logger = logging.getLogger(__name__)


def _validate_create_collection_options(
    dimension: Optional[int],
    metric: Optional[str],
    service: Optional[Union[CollectionVectorServiceOptions, Dict[str, Any]]],
    indexing: Optional[Dict[str, Any]],
    default_id_type: Optional[str],
    additional_options: Optional[Dict[str, Any]],
) -> None:
    if additional_options:
        if "vector" in additional_options:
            raise ValueError(
                "`additional_options` dict parameter to create_collection "
                "cannot have a `vector` key. Please use the specific "
                "method parameter."
            )
        if "indexing" in additional_options:
            raise ValueError(
                "`additional_options` dict parameter to create_collection "
                "cannot have a `indexing` key. Please use the specific "
                "method parameter."
            )
        if "defaultId" in additional_options and default_id_type is not None:
            # this leaves the workaround to pass more info in the defaultId
            # should that become part of the specs:
            raise ValueError(
                "`additional_options` dict parameter to create_collection "
                "cannot have a `defaultId` key when passing the "
                "`default_id_type` parameter as well."
            )
    is_vector: bool
    if service is not None or dimension is not None:
        is_vector = True
    else:
        is_vector = False
    if not is_vector and metric is not None:
        raise ValueError(
            "Cannot specify `metric` for non-vector collections in the "
            "create_collection method."
        )


class Database:
    """
    A Data API database. This is the object for doing database-level
    DML, such as creating/deleting collections, and for obtaining Collection
    objects themselves. This class has a synchronous interface.

    The usual way of obtaining one Database is through the `get_database`
    method of a `DataAPIClient`.

    On Astra DB, a Database comes with an "API Endpoint", which implies
    a Database object instance reaches a specific region (relevant point in
    case of multi-region databases).

    Args:
        api_endpoint: the full "API Endpoint" string used to reach the Data API.
            Example: "https://<database_id>-<region>.apps.astra.datastax.com"
        token: an Access Token to the database. Example: "AstraCS:xyz..."
            This can be either a literal token string or a subclass of
            `astrapy.authentication.TokenProvider`.
        namespace: this is the namespace all method calls will target, unless
            one is explicitly specified in the call. If no namespace is supplied
            when creating a Database, on Astra DB the name "default_namespace" is set,
            while on other environments the namespace is left unspecified: in this case,
            most operations are unavailable until a namespace is set (through an explicit
            `use_namespace` invocation or equivalent).
        caller_name: name of the application, or framework, on behalf of which
            the Data API calls are performed. This ends up in the request user-agent.
        caller_version: version of the caller.
        environment: a string representing the target Data API environment.
            It can be left unspecified for the default value of `Environment.PROD`;
            other values include `Environment.OTHER`, `Environment.DSE`.
        api_path: path to append to the API Endpoint. In typical usage, this
            should be left to its default (sensibly chosen based on the environment).
        api_version: version specifier to append to the API path. In typical
            usage, this should be left to its default of "v1".

    Example:
        >>> from astrapy import DataAPIClient
        >>> my_client = astrapy.DataAPIClient("AstraCS:...")
        >>> my_db = my_client.get_database(
        ...    "https://01234567-....apps.astra.datastax.com"
        ... )

    Note:
        creating an instance of Database does not trigger actual creation
        of the database itself, which should exist beforehand. To create databases,
        see the AstraDBAdmin class.
    """

    def __init__(
        self,
        api_endpoint: str,
        token: Optional[Union[str, TokenProvider]] = None,
        *,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
        environment: Optional[str] = None,
        api_path: Optional[str] = None,
        api_version: Optional[str] = None,
    ) -> None:
        self.environment = (environment or Environment.PROD).lower()
        #
        _api_path: Optional[str]
        _api_version: Optional[str]
        if api_path is None:
            _api_path = API_PATH_ENV_MAP[self.environment]
        else:
            _api_path = api_path
        if api_version is None:
            _api_version = API_VERSION_ENV_MAP[self.environment]
        else:
            _api_version = api_version
        self.token_provider = coerce_token_provider(token)
        self.api_endpoint = api_endpoint.strip("/")
        self.api_path = _api_path
        self.api_version = _api_version

        # enforce defaults if on Astra DB:
        self.using_namespace: Optional[str]
        if namespace is None and self.environment in Environment.astra_db_values:
            self.using_namespace = DEFAULT_ASTRA_DB_NAMESPACE
        else:
            self.using_namespace = namespace

        self.caller_name = caller_name
        self.caller_version = caller_version
        self._astra_db = self._refresh_astra_db()
        self._name: Optional[str] = None

    def __getattr__(self, collection_name: str) -> Collection:
        return self.get_collection(name=collection_name)

    def __getitem__(self, collection_name: str) -> Collection:
        return self.get_collection(name=collection_name)

    def __repr__(self) -> str:
        namespace_desc = self.namespace if self.namespace is not None else "(not set)"
        return (
            f'{self.__class__.__name__}(api_endpoint="{self.api_endpoint}", '
            f'token="{str(self.token_provider)[:12]}...", namespace="{namespace_desc}")'
        )

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Database):
            return all(
                [
                    self.token_provider == other.token_provider,
                    self.api_endpoint == other.api_endpoint,
                    self.api_path == other.api_path,
                    self.api_version == other.api_version,
                    self.namespace == other.namespace,
                    self.caller_name == other.caller_name,
                    self.caller_version == other.caller_version,
                ]
            )
        else:
            return False

    def _refresh_astra_db(self) -> AstraDB:
        """Re-instantiate a new (core) client based on the instance attributes."""
        logger.info("Instantiating a new (core) AstraDB")
        return AstraDB(
            token=self.token_provider.get_token(),
            api_endpoint=self.api_endpoint,
            api_path=self.api_path,
            api_version=self.api_version,
            namespace=self.namespace,
            caller_name=self.caller_name,
            caller_version=self.caller_version,
        )

    def _copy(
        self,
        *,
        api_endpoint: Optional[str] = None,
        token: Optional[Union[str, TokenProvider]] = None,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
        environment: Optional[str] = None,
        api_path: Optional[str] = None,
        api_version: Optional[str] = None,
    ) -> Database:
        return Database(
            api_endpoint=api_endpoint or self.api_endpoint,
            token=coerce_token_provider(token) or self.token_provider,
            namespace=namespace or self.namespace,
            caller_name=caller_name or self.caller_name,
            caller_version=caller_version or self.caller_version,
            environment=environment or self.environment,
            api_path=api_path or self.api_path,
            api_version=api_version or self.api_version,
        )

    def with_options(
        self,
        *,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> Database:
        """
        Create a clone of this database with some changed attributes.

        Args:
            namespace: this is the namespace all method calls will target, unless
                one is explicitly specified in the call. If no namespace is supplied
                when creating a Database, the name "default_namespace" is set.
            caller_name: name of the application, or framework, on behalf of which
                the Data API calls are performed. This ends up in the request user-agent.
            caller_version: version of the caller.

        Returns:
            a new `Database` instance.

        Example:
            >>> my_db_2 = my_db.with_options(
            ...     namespace="the_other_namespace",
            ...     caller_name="the_caller",
            ...     caller_version="0.1.0",
            ... )
        """

        return self._copy(
            namespace=namespace,
            caller_name=caller_name,
            caller_version=caller_version,
        )

    def to_async(
        self,
        *,
        api_endpoint: Optional[str] = None,
        token: Optional[Union[str, TokenProvider]] = None,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
        environment: Optional[str] = None,
        api_path: Optional[str] = None,
        api_version: Optional[str] = None,
    ) -> AsyncDatabase:
        """
        Create an AsyncDatabase from this one. Save for the arguments
        explicitly provided as overrides, everything else is kept identical
        to this database in the copy.

        Args:
            api_endpoint: the full "API Endpoint" string used to reach the Data API.
                Example: "https://<database_id>-<region>.apps.astra.datastax.com"
            token: an Access Token to the database. Example: "AstraCS:xyz..."
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            namespace: this is the namespace all method calls will target, unless
                one is explicitly specified in the call. If no namespace is supplied
                when creating a Database, the name "default_namespace" is set.
            caller_name: name of the application, or framework, on behalf of which
                the Data API calls are performed. This ends up in the request user-agent.
            caller_version: version of the caller.
            environment: a string representing the target Data API environment.
                Values are, for example, `Environment.PROD`, `Environment.OTHER`,
                or `Environment.DSE`.
            api_path: path to append to the API Endpoint. In typical usage, this
                should be left to its default of "/api/json".
            api_version: version specifier to append to the API path. In typical
                usage, this should be left to its default of "v1".

        Returns:
            the new copy, an `AsyncDatabase` instance.

        Example:
            >>> my_async_db = my_db.to_async()
            >>> asyncio.run(my_async_db.list_collection_names())
        """

        return AsyncDatabase(
            api_endpoint=api_endpoint or self.api_endpoint,
            token=coerce_token_provider(token) or self.token_provider,
            namespace=namespace or self.namespace,
            caller_name=caller_name or self.caller_name,
            caller_version=caller_version or self.caller_version,
            environment=environment or self.environment,
            api_path=api_path or self.api_path,
            api_version=api_version or self.api_version,
        )

    def set_caller(
        self,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> None:
        """
        Set a new identity for the application/framework on behalf of which
        the Data API calls are performed (the "caller").

        Args:
            caller_name: name of the application, or framework, on behalf of which
                the Data API calls are performed. This ends up in the request user-agent.
            caller_version: version of the caller.

        Example:
            >>> my_db.set_caller(caller_name="the_caller", caller_version="0.1.0")
        """

        logger.info(f"setting caller to {caller_name}/{caller_version}")
        self.caller_name = caller_name
        self.caller_version = caller_version
        self._astra_db = self._refresh_astra_db()

    def use_namespace(self, namespace: str) -> None:
        """
        Switch to a new working namespace for this database.
        This method changes (mutates) the Database instance.

        Note that this method does not create the namespace, which should exist
        already (created for instance with a `DatabaseAdmin.create_namespace` call).

        Args:
            namespace: the new namespace to use as the database working namespace.

        Returns:
            None.

        Example:
            >>> my_db.list_collection_names()
            ['coll_1', 'coll_2']
            >>> my_db.use_namespace("an_empty_namespace")
            >>> my_db.list_collection_names()
            []
        """
        logger.info(f"switching to namespace '{namespace}'")
        self.using_namespace = namespace
        self._astra_db = self._refresh_astra_db()

    def info(self) -> DatabaseInfo:
        """
        Additional information on the database as a DatabaseInfo instance.

        Some of the returned properties are dynamic throughout the lifetime
        of the database (such as raw_info["keyspaces"]). For this reason,
        each invocation of this method triggers a new request to the DevOps API.

        Example:
            >>> my_db.info().region
            'eu-west-1'

            >>> my_db.info().raw_info['datacenters'][0]['dateCreated']
            '2023-01-30T12:34:56Z'

        Note:
            see the DatabaseInfo documentation for a caveat about the difference
            between the `region` and the `raw_info["region"]` attributes.
        """

        logger.info("getting database info")
        database_info = fetch_database_info(
            self.api_endpoint,
            token=self.token_provider.get_token(),
            namespace=self.namespace,
        )
        if database_info is not None:
            logger.info("finished getting database info")
            return database_info
        else:
            raise DevOpsAPIException(
                "Database is not in a supported environment for this operation."
            )

    @property
    def id(self) -> str:
        """
        The ID of this database.

        Example:
            >>> my_db.id
            '01234567-89ab-cdef-0123-456789abcdef'
        """

        parsed_api_endpoint = parse_api_endpoint(self.api_endpoint)
        if parsed_api_endpoint is not None:
            return parsed_api_endpoint.database_id
        else:
            raise DevOpsAPIException(
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
    def namespace(self) -> Optional[str]:
        """
        The namespace this database uses as target for all commands when
        no method-call-specific namespace is specified.

        Returns:
            the working namespace (a string), or None if not set.

        Example:
            >>> my_db.namespace
            'the_keyspace'
        """

        return self.using_namespace

    def get_collection(
        self,
        name: str,
        *,
        namespace: Optional[str] = None,
        embedding_api_key: Optional[Union[str, EmbeddingHeadersProvider]] = None,
        collection_max_time_ms: Optional[int] = None,
    ) -> Collection:
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
            namespace: the namespace containing the collection. If no namespace
                is specified, the general setting for this database is used.
        embedding_api_key: optional API key(s) for interacting with the collection.
            If an embedding service is configured, and this parameter is not None,
            each Data API call will include the necessary embedding-related headers
            as specified by this parameter. If a string is passed, it translates
            into the one "embedding api key" header
            (i.e. `astrapy.authentication.EmbeddingAPIKeyHeaderProvider`).
            For some vectorize providers/models, if using header-based authentication,
            specialized subclasses of `astrapy.authentication.EmbeddingHeadersProvider`
            should be supplied.
        collection_max_time_ms: a default timeout, in millisecond, for the duration of each
            operation on the collection. Individual timeouts can be provided to
            each collection method call and will take precedence, with this value
            being an overall default.
            Note that for some methods involving multiple API calls (such as
            `find`, `delete_many`, `insert_many` and so on), it is strongly suggested
            to provide a specific timeout as the default one likely wouldn't make
            much sense.

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

        _namespace = namespace or self.namespace
        if _namespace is None:
            raise ValueError(
                "No namespace specified. This operation requires a namespace to "
                "be set, e.g. through the `use_namespace` method."
            )
        return Collection(
            self,
            name,
            namespace=_namespace,
            api_options=CollectionAPIOptions(
                embedding_api_key=coerce_embedding_headers_provider(embedding_api_key),
                max_time_ms=collection_max_time_ms,
            ),
        )

    @recast_method_sync
    def create_collection(
        self,
        name: str,
        *,
        namespace: Optional[str] = None,
        dimension: Optional[int] = None,
        metric: Optional[str] = None,
        service: Optional[Union[CollectionVectorServiceOptions, Dict[str, Any]]] = None,
        indexing: Optional[Dict[str, Any]] = None,
        default_id_type: Optional[str] = None,
        additional_options: Optional[Dict[str, Any]] = None,
        check_exists: Optional[bool] = None,
        max_time_ms: Optional[int] = None,
        embedding_api_key: Optional[Union[str, EmbeddingHeadersProvider]] = None,
        collection_max_time_ms: Optional[int] = None,
    ) -> Collection:
        """
        Creates a collection on the database and return the Collection
        instance that represents it.

        This is a blocking operation: the method returns when the collection
        is ready to be used. As opposed to the `get_collection` instance,
        this method triggers causes the collection to be actually created on DB.

        Args:
            name: the name of the collection.
            namespace: the namespace where the collection is to be created.
                If not specified, the general setting for this database is used.
            dimension: for vector collections, the dimension of the vectors
                (i.e. the number of their components).
            metric: the similarity metric used for vector searches.
                Allowed values are `VectorMetric.DOT_PRODUCT`, `VectorMetric.EUCLIDEAN`
                or `VectorMetric.COSINE` (default).
            service: a dictionary describing a service for
                embedding computation, e.g. `{"provider": "ab", "modelName": "xy"}`.
                Alternatively, a CollectionVectorServiceOptions object to the same effect.
            indexing: optional specification of the indexing options for
                the collection, in the form of a dictionary such as
                    {"deny": [...]}
                or
                    {"allow": [...]}
            default_id_type: this sets what type of IDs the API server will
                generate when inserting documents that do not specify their
                `_id` field explicitly. Can be set to any of the values
                `DefaultIdType.UUID`, `DefaultIdType.OBJECTID`,
                `DefaultIdType.UUIDV6`, `DefaultIdType.UUIDV7`,
                `DefaultIdType.DEFAULT`.
            additional_options: any further set of key-value pairs that will
                be added to the "options" part of the payload when sending
                the Data API command to create a collection.
            check_exists: whether to run an existence check for the collection
                name before attempting to create the collection:
                If check_exists is True, an error is raised when creating
                an existing collection.
                If it is False, the creation is attempted. In this case, for
                preexisting collections, the command will succeed or fail
                depending on whether the options match or not.
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
            embedding_api_key: optional API key(s) for interacting with the collection.
                If an embedding service is configured, and this parameter is not None,
                each Data API call will include the necessary embedding-related headers
                as specified by this parameter. If a string is passed, it translates
                into the one "embedding api key" header
                (i.e. `astrapy.authentication.EmbeddingAPIKeyHeaderProvider`).
                For some vectorize providers/models, if using header-based authentication,
                specialized subclasses of `astrapy.authentication.EmbeddingHeadersProvider`
                should be supplied.
            collection_max_time_ms: a default timeout, in millisecond, for the duration of each
                operation on the collection. Individual timeouts can be provided to
                each collection method call and will take precedence, with this value
                being an overall default.
                Note that for some methods involving multiple API calls (such as
                `find`, `delete_many`, `insert_many` and so on), it is strongly suggested
                to provide a specific timeout as the default one likely wouldn't make
                much sense.

        Returns:
            a (synchronous) `Collection` instance, representing the
            newly-created collection.

        Example:
            >>> new_col = my_db.create_collection("my_v_col", dimension=3)
            >>> new_col.insert_one({"name": "the_row", "$vector": [0.4, 0.5, 0.7]})
            InsertOneResult(raw_results=..., inserted_id='e22dd65e-...-...-...')

        Note:
            A collection is considered a vector collection if at least one of
            `dimension` or `service` are provided and not null. In that case,
            and only in that case, is `metric` an accepted parameter.
            Note, moreover, that if passing both these parameters, then
            the dimension must be compatible with the chosen service.
        """

        _validate_create_collection_options(
            dimension=dimension,
            metric=metric,
            service=service,
            indexing=indexing,
            default_id_type=default_id_type,
            additional_options=additional_options,
        )
        _options = {
            **(additional_options or {}),
            **({"indexing": indexing} if indexing else {}),
            **({"defaultId": {"type": default_id_type}} if default_id_type else {}),
        }

        timeout_manager = MultiCallTimeoutManager(overall_max_time_ms=max_time_ms)

        if check_exists is None:
            _check_exists = True
        else:
            _check_exists = check_exists
        existing_names: List[str]
        if _check_exists:
            logger.info(f"checking collection existence for '{name}'")
            existing_names = self.list_collection_names(
                namespace=namespace,
                max_time_ms=timeout_manager.remaining_timeout_ms(),
            )
        else:
            existing_names = []

        _namespace = namespace or self.namespace
        if _namespace is None:
            raise ValueError(
                "No namespace specified. This operation requires a namespace to "
                "be set, e.g. through the `use_namespace` method."
            )

        driver_db = self._astra_db.copy(namespace=_namespace)
        if name in existing_names:
            raise CollectionAlreadyExistsException(
                text=f"CollectionInvalid: collection {name} already exists",
                namespace=_namespace,
                collection_name=name,
            )

        service_dict: Optional[Dict[str, Any]]
        if service is not None:
            service_dict = service if isinstance(service, dict) else service.as_dict()
        else:
            service_dict = None

        logger.info(f"creating collection '{name}'")
        driver_db.create_collection(
            name,
            options=_options,
            dimension=dimension,
            metric=metric,
            service_dict=service_dict,
            timeout_info=timeout_manager.remaining_timeout_info(),
        )
        logger.info(f"finished creating collection '{name}'")
        return self.get_collection(
            name,
            namespace=namespace,
            embedding_api_key=coerce_embedding_headers_provider(embedding_api_key),
            collection_max_time_ms=collection_max_time_ms,
        )

    @recast_method_sync
    def drop_collection(
        self,
        name_or_collection: Union[str, Collection],
        *,
        max_time_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Drop a collection from the database, along with all documents therein.

        Args:
            name_or_collection: either the name of a collection or
                a `Collection` instance.
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.

        Returns:
            a dictionary in the form {"ok": 1} if the command succeeds.

        Example:
            >>> my_db.list_collection_names()
            ['a_collection', 'my_v_col', 'another_col']
            >>> my_db.drop_collection("my_v_col")
            {'ok': 1}
            >>> my_db.list_collection_names()
            ['a_collection', 'another_col']

        Note:
            when providing a collection name, it is assumed that the collection
            is to be found in the namespace set at database instance level.
        """

        # lazy importing here against circular-import error
        from astrapy.collection import Collection

        if isinstance(name_or_collection, Collection):
            _namespace = name_or_collection.namespace
            _name: str = name_or_collection.name
            logger.info(f"dropping collection '{_name}'")
            dc_response = self._astra_db.copy(namespace=_namespace).delete_collection(
                _name,
                timeout_info=base_timeout_info(max_time_ms),
            )
            logger.info(f"finished dropping collection '{_name}'")
            return dc_response.get("status", {})  # type: ignore[no-any-return]
        else:
            if self.namespace is None:
                raise ValueError(
                    "No namespace specified. This operation requires a namespace to "
                    "be set, e.g. through the `use_namespace` method."
                )
            logger.info(f"dropping collection '{name_or_collection}'")
            dc_response = self._astra_db.delete_collection(
                name_or_collection,
                timeout_info=base_timeout_info(max_time_ms),
            )
            logger.info(f"finished dropping collection '{name_or_collection}'")
            return dc_response.get("status", {})  # type: ignore[no-any-return]

    @recast_method_sync
    def list_collections(
        self,
        *,
        namespace: Optional[str] = None,
        max_time_ms: Optional[int] = None,
    ) -> CommandCursor[CollectionDescriptor]:
        """
        List all collections in a given namespace for this database.

        Args:
            namespace: the namespace to be inspected. If not specified,
                the general setting for this database is assumed.
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.

        Returns:
            a `CommandCursor` to iterate over CollectionDescriptor instances,
            each corresponding to a collection.

        Example:
            >>> ccur = my_db.list_collections()
            >>> ccur
            <astrapy.cursors.CommandCursor object at ...>
            >>> list(ccur)
            [CollectionDescriptor(name='my_v_col', options=CollectionOptions())]
            >>> for coll_dict in my_db.list_collections():
            ...     print(coll_dict)
            ...
            CollectionDescriptor(name='my_v_col', options=CollectionOptions())
        """

        _namespace = namespace or self.namespace
        if _namespace is None:
            raise ValueError(
                "No namespace specified. This operation requires a namespace to "
                "be set, e.g. through the `use_namespace` method."
            )

        driver_db = self._astra_db.copy(namespace=_namespace)
        logger.info("getting collections")
        gc_response = driver_db.get_collections(
            options={"explain": True}, timeout_info=base_timeout_info(max_time_ms)
        )
        if "collections" not in gc_response.get("status", {}):
            raise DataAPIFaultyResponseException(
                text="Faulty response from get_collections API command.",
                raw_response=gc_response,
            )
        else:
            # we know this is a list of dicts, to marshal into "descriptors"
            logger.info("finished getting collections")
            return CommandCursor(
                address=driver_db.base_url,
                items=[
                    CollectionDescriptor.from_dict(col_dict)
                    for col_dict in gc_response["status"]["collections"]
                ],
            )

    @recast_method_sync
    def list_collection_names(
        self,
        *,
        namespace: Optional[str] = None,
        max_time_ms: Optional[int] = None,
    ) -> List[str]:
        """
        List the names of all collections in a given namespace of this database.

        Args:
            namespace: the namespace to be inspected. If not specified,
                the general setting for this database is assumed.
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.

        Returns:
            a list of the collection names as strings, in no particular order.

        Example:
            >>> my_db.list_collection_names()
            ['a_collection', 'another_col']
        """

        _namespace = namespace or self.namespace
        if _namespace is None:
            raise ValueError(
                "No namespace specified. This operation requires a namespace to "
                "be set, e.g. through the `use_namespace` method."
            )

        logger.info("getting collection names")
        gc_response = self._astra_db.copy(namespace=_namespace).get_collections(
            timeout_info=base_timeout_info(max_time_ms)
        )
        if "collections" not in gc_response.get("status", {}):
            raise DataAPIFaultyResponseException(
                text="Faulty response from get_collections API command.",
                raw_response=gc_response,
            )
        else:
            # we know this is a list of strings
            logger.info("finished getting collection names")
            return gc_response["status"]["collections"]  # type: ignore[no-any-return]

    @recast_method_sync
    def command(
        self,
        body: Dict[str, Any],
        *,
        namespace: Optional[str] = None,
        collection_name: Optional[str] = None,
        max_time_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Send a POST request to the Data API for this database with
        an arbitrary, caller-provided payload.

        Args:
            body: a JSON-serializable dictionary, the payload of the request.
            namespace: the namespace to use. Requests always target a namespace:
                if not specified, the general setting for this database is assumed.
            collection_name: if provided, the collection name is appended at the end
                of the endpoint. In this way, this method allows collection-level
                arbitrary POST requests as well.
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.

        Returns:
            a dictionary with the response of the HTTP request.

        Example:
            >>> my_db.command({"findCollections": {}})
            {'status': {'collections': ['my_coll']}}
            >>> my_db.command({"countDocuments": {}}, collection_name="my_coll")
            {'status': {'count': 123}}
        """

        _namespace = namespace or self.namespace
        if _namespace is None:
            raise ValueError(
                "No namespace specified. This operation requires a namespace to "
                "be set, e.g. through the `use_namespace` method."
            )

        driver_db = self._astra_db.copy(namespace=_namespace)
        if collection_name:
            _collection = driver_db.collection(collection_name)
            logger.info(f"issuing custom command to API (on '{collection_name}')")
            req_response = _collection.post_raw_request(
                body=body,
                timeout_info=base_timeout_info(max_time_ms),
            )
            logger.info(
                f"finished issuing custom command to API (on '{collection_name}')"
            )
            return req_response
        else:
            logger.info("issuing custom command to API")
            req_response = driver_db.post_raw_request(
                body=body,
                timeout_info=base_timeout_info(max_time_ms),
            )
            logger.info("finished issuing custom command to API")
            return req_response

    def get_database_admin(
        self,
        *,
        token: Optional[Union[str, TokenProvider]] = None,
        dev_ops_url: Optional[str] = None,
        dev_ops_api_version: Optional[str] = None,
    ) -> DatabaseAdmin:
        """
        Return a DatabaseAdmin object corresponding to this database, for
        use in admin tasks such as managing namespaces.

        This method, depending on the environment where the database resides,
        returns an appropriate subclass of DatabaseAdmin.

        Args:
            token: an access token with enough permission on the database to
                perform the desired tasks. If omitted (as it can generally be done),
                the token of this Database is used.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            dev_ops_url: in case of custom deployments, this can be used to specify
                the URL to the DevOps API, such as "https://api.astra.datastax.com".
                Generally it can be omitted. The environment (prod/dev/...) is
                determined from the API Endpoint.
                Note that this parameter is allowed only for Astra DB environments.
            dev_ops_api_version: this can specify a custom version of the DevOps API
                (such as "v2"). Generally not needed.
                Note that this parameter is allowed only for Astra DB environments.

        Returns:
            A DatabaseAdmin instance targeting this database. More precisely,
            for Astra DB an instance of `AstraDBDatabaseAdmin` is returned;
            for other environments, an instance of `DataAPIDatabaseAdmin` is returned.

        Example:
            >>> my_db_admin = my_db.get_database_admin()
            >>> if "new_namespace" not in my_db_admin.list_namespaces():
            ...     my_db_admin.create_namespace("new_namespace")
            >>> my_db_admin.list_namespaces()
            ['default_keyspace', 'new_namespace']
        """

        # lazy importing here to avoid circular dependency
        from astrapy.admin import AstraDBDatabaseAdmin, DataAPIDatabaseAdmin

        if self.environment in Environment.astra_db_values:
            return AstraDBDatabaseAdmin(
                api_endpoint=self.api_endpoint,
                token=coerce_token_provider(token) or self.token_provider,
                environment=self.environment,
                caller_name=self.caller_name,
                caller_version=self.caller_version,
                dev_ops_url=dev_ops_url,
                dev_ops_api_version=dev_ops_api_version,
                spawner_database=self,
            )
        else:
            if dev_ops_url is not None:
                raise ValueError(
                    "Parameter `dev_ops_url` not supported outside of Astra DB."
                )
            if dev_ops_api_version is not None:
                raise ValueError(
                    "Parameter `dev_ops_api_version` not supported outside of Astra DB."
                )
            return DataAPIDatabaseAdmin(
                api_endpoint=self.api_endpoint,
                token=coerce_token_provider(token) or self.token_provider,
                environment=self.environment,
                api_path=self.api_path,
                api_version=self.api_version,
                caller_name=self.caller_name,
                caller_version=self.caller_version,
                spawner_database=self,
            )


class AsyncDatabase:
    """
    A Data API database. This is the object for doing database-level
    DML, such as creating/deleting collections, and for obtaining Collection
    objects themselves. This class has an asynchronous interface.

    The usual way of obtaining one AsyncDatabase is through the `get_async_database`
    method of a `DataAPIClient`.

    On Astra DB, an AsyncDatabase comes with an "API Endpoint", which implies
    an AsyncDatabase object instance reaches a specific region (relevant point in
    case of multi-region databases).

    Args:
        api_endpoint: the full "API Endpoint" string used to reach the Data API.
            Example: "https://<database_id>-<region>.apps.astra.datastax.com"
        token: an Access Token to the database. Example: "AstraCS:xyz..."
            This can be either a literal token string or a subclass of
            `astrapy.authentication.TokenProvider`.
        namespace: this is the namespace all method calls will target, unless
            one is explicitly specified in the call. If no namespace is supplied
            when creating a Database, on Astra DB the name "default_namespace" is set,
            while on other environments the namespace is left unspecified: in this case,
            most operations are unavailable until a namespace is set (through an explicit
            `use_namespace` invocation or equivalent).
        caller_name: name of the application, or framework, on behalf of which
            the Data API calls are performed. This ends up in the request user-agent.
        caller_version: version of the caller.
        environment: a string representing the target Data API environment.
            It can be left unspecified for the default value of `Environment.PROD`;
            other values include `Environment.OTHER`, `Environment.DSE`.
        api_path: path to append to the API Endpoint. In typical usage, this
            should be left to its default (sensibly chosen based on the environment).
        api_version: version specifier to append to the API path. In typical
            usage, this should be left to its default of "v1".

    Example:
        >>> from astrapy import DataAPIClient
        >>> my_client = astrapy.DataAPIClient("AstraCS:...")
        >>> my_db = my_client.get_async_database(
        ...    "https://01234567-....apps.astra.datastax.com"
        ... )

    Note:
        creating an instance of AsyncDatabase does not trigger actual creation
        of the database itself, which should exist beforehand. To create databases,
        see the AstraDBAdmin class.
    """

    def __init__(
        self,
        api_endpoint: str,
        token: Optional[Union[str, TokenProvider]] = None,
        *,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
        environment: Optional[str] = None,
        api_path: Optional[str] = None,
        api_version: Optional[str] = None,
    ) -> None:
        self.environment = (environment or Environment.PROD).lower()
        #
        _api_path: Optional[str]
        _api_version: Optional[str]
        if api_path is None:
            _api_path = API_PATH_ENV_MAP[self.environment]
        else:
            _api_path = api_path
        if api_version is None:
            _api_version = API_VERSION_ENV_MAP[self.environment]
        else:
            _api_version = api_version
        #
        self.token_provider = coerce_token_provider(token)
        self.api_endpoint = api_endpoint.strip("/")
        self.api_path = _api_path
        self.api_version = _api_version

        # enforce defaults if on Astra DB:
        self.using_namespace: Optional[str]
        if namespace is None and self.environment in Environment.astra_db_values:
            self.using_namespace = DEFAULT_ASTRA_DB_NAMESPACE
        else:
            self.using_namespace = namespace

        self.caller_name = caller_name
        self.caller_version = caller_version
        self._astra_db = self._refresh_astra_db()
        self._name: Optional[str] = None

    def __getattr__(self, collection_name: str) -> AsyncCollection:
        return self.to_sync().get_collection(name=collection_name).to_async()

    def __getitem__(self, collection_name: str) -> AsyncCollection:
        return self.to_sync().get_collection(name=collection_name).to_async()

    def __repr__(self) -> str:
        namespace_desc = self.namespace if self.namespace is not None else "(not set)"
        return (
            f'{self.__class__.__name__}(api_endpoint="{self.api_endpoint}", '
            f'token="{str(self.token_provider)[:12]}...", namespace="{namespace_desc}")'
        )

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, AsyncDatabase):
            return all(
                [
                    self.token_provider == other.token_provider,
                    self.api_endpoint == other.api_endpoint,
                    self.api_path == other.api_path,
                    self.api_version == other.api_version,
                    self.namespace == other.namespace,
                    self.caller_name == other.caller_name,
                    self.caller_version == other.caller_version,
                ]
            )
        else:
            return False

    async def __aenter__(self) -> AsyncDatabase:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> None:
        await self._astra_db.__aexit__(
            exc_type=exc_type,
            exc_value=exc_value,
            traceback=traceback,
        )

    def _refresh_astra_db(self) -> AsyncAstraDB:
        """Re-instantiate a new (core) client based on the instance attributes."""
        logger.info("Instantiating a new (core) AsyncAstraDB")
        return AsyncAstraDB(
            token=self.token_provider.get_token(),
            api_endpoint=self.api_endpoint,
            api_path=self.api_path,
            api_version=self.api_version,
            namespace=self.namespace,
            caller_name=self.caller_name,
            caller_version=self.caller_version,
        )

    def _copy(
        self,
        *,
        api_endpoint: Optional[str] = None,
        token: Optional[Union[str, TokenProvider]] = None,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
        environment: Optional[str] = None,
        api_path: Optional[str] = None,
        api_version: Optional[str] = None,
    ) -> AsyncDatabase:
        return AsyncDatabase(
            api_endpoint=api_endpoint or self.api_endpoint,
            token=coerce_token_provider(token) or self.token_provider,
            namespace=namespace or self.namespace,
            caller_name=caller_name or self.caller_name,
            caller_version=caller_version or self.caller_version,
            environment=environment or self.environment,
            api_path=api_path or self.api_path,
            api_version=api_version or self.api_version,
        )

    def with_options(
        self,
        *,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> AsyncDatabase:
        """
        Create a clone of this database with some changed attributes.

        Args:
            namespace: this is the namespace all method calls will target, unless
                one is explicitly specified in the call. If no namespace is supplied
                when creating a Database, the name "default_namespace" is set.
            caller_name: name of the application, or framework, on behalf of which
                the Data API calls are performed. This ends up in the request user-agent.
            caller_version: version of the caller.

        Returns:
            a new `AsyncDatabase` instance.

        Example:
            >>> my_async_db_2 = my_async_db.with_options(
            ...     namespace="the_other_namespace",
            ...     caller_name="the_caller",
            ...     caller_version="0.1.0",
            ... )
        """

        return self._copy(
            namespace=namespace,
            caller_name=caller_name,
            caller_version=caller_version,
        )

    def to_sync(
        self,
        *,
        api_endpoint: Optional[str] = None,
        token: Optional[Union[str, TokenProvider]] = None,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
        environment: Optional[str] = None,
        api_path: Optional[str] = None,
        api_version: Optional[str] = None,
    ) -> Database:
        """
        Create a (synchronous) Database from this one. Save for the arguments
        explicitly provided as overrides, everything else is kept identical
        to this database in the copy.

        Args:
            api_endpoint: the full "API Endpoint" string used to reach the Data API.
                Example: "https://<database_id>-<region>.apps.astra.datastax.com"
            token: an Access Token to the database. Example: "AstraCS:xyz..."
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            namespace: this is the namespace all method calls will target, unless
                one is explicitly specified in the call. If no namespace is supplied
                when creating a Database, the name "default_namespace" is set.
            caller_name: name of the application, or framework, on behalf of which
                the Data API calls are performed. This ends up in the request user-agent.
            caller_version: version of the caller.
            environment: a string representing the target Data API environment.
                Values are, for example, `Environment.PROD`, `Environment.OTHER`,
                or `Environment.DSE`.
            api_path: path to append to the API Endpoint. In typical usage, this
                should be left to its default of "/api/json".
            api_version: version specifier to append to the API path. In typical
                usage, this should be left to its default of "v1".

        Returns:
            the new copy, a `Database` instance.

        Example:
            >>> my_sync_db = my_async_db.to_sync()
            >>> my_sync_db.list_collection_names()
            ['a_collection', 'another_collection']
        """

        return Database(
            api_endpoint=api_endpoint or self.api_endpoint,
            token=coerce_token_provider(token) or self.token_provider,
            namespace=namespace or self.namespace,
            caller_name=caller_name or self.caller_name,
            caller_version=caller_version or self.caller_version,
            environment=environment or self.environment,
            api_path=api_path or self.api_path,
            api_version=api_version or self.api_version,
        )

    def set_caller(
        self,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> None:
        """
        Set a new identity for the application/framework on behalf of which
        the Data API calls are performed (the "caller").

        Args:
            caller_name: name of the application, or framework, on behalf of which
                the Data API calls are performed. This ends up in the request user-agent.
            caller_version: version of the caller.

        Example:
            >>> my_db.set_caller(caller_name="the_caller", caller_version="0.1.0")
        """

        logger.info(f"setting caller to {caller_name}/{caller_version}")
        self.caller_name = caller_name
        self.caller_version = caller_version
        self._astra_db = self._refresh_astra_db()

    def use_namespace(self, namespace: str) -> None:
        """
        Switch to a new working namespace for this database.
        This method changes (mutates) the AsyncDatabase instance.

        Note that this method does not create the namespace, which should exist
        already (created for instance with a `DatabaseAdmin.async_create_namespace` call).

        Args:
            namespace: the new namespace to use as the database working namespace.

        Returns:
            None.

        Example:
            >>> asyncio.run(my_async_db.list_collection_names())
            ['coll_1', 'coll_2']
            >>> my_async_db.use_namespace("an_empty_namespace")
            >>> asyncio.run(my_async_db.list_collection_names())
            []
        """
        logger.info(f"switching to namespace '{namespace}'")
        self.using_namespace = namespace
        self._astra_db = self._refresh_astra_db()

    def info(self) -> DatabaseInfo:
        """
        Additional information on the database as a DatabaseInfo instance.

        Some of the returned properties are dynamic throughout the lifetime
        of the database (such as raw_info["keyspaces"]). For this reason,
        each invocation of this method triggers a new request to the DevOps API.

        Example:
            >>> my_async_db.info().region
            'eu-west-1'

            >>> my_async_db.info().raw_info['datacenters'][0]['dateCreated']
            '2023-01-30T12:34:56Z'

        Note:
            see the DatabaseInfo documentation for a caveat about the difference
            between the `region` and the `raw_info["region"]` attributes.
        """

        logger.info("getting database info")
        database_info = fetch_database_info(
            self.api_endpoint,
            token=self.token_provider.get_token(),
            namespace=self.namespace,
        )
        if database_info is not None:
            logger.info("finished getting database info")
            return database_info
        else:
            raise DevOpsAPIException(
                "Database is not in a supported environment for this operation."
            )

    @property
    def id(self) -> str:
        """
        The ID of this database.

        Example:
            >>> my_async_db.id
            '01234567-89ab-cdef-0123-456789abcdef'
        """

        parsed_api_endpoint = parse_api_endpoint(self.api_endpoint)
        if parsed_api_endpoint is not None:
            return parsed_api_endpoint.database_id
        else:
            raise DevOpsAPIException(
                "Database is not in a supported environment for this operation."
            )

    def name(self) -> str:
        """
        The name of this database. Note that this bears no unicity guarantees.

        Calling this method the first time involves a request
        to the DevOps API (the resulting database name is then cached).
        See the `info()` method for more details.

        Example:
            >>> my_async_db.name()
            'the_application_database'
        """

        if self._name is None:
            self._name = self.info().name
        return self._name

    @property
    def namespace(self) -> Optional[str]:
        """
        The namespace this database uses as target for all commands when
        no method-call-specific namespace is specified.

        Returns:
            the working namespace (a string), or None if not set.

        Example:
            >>> my_async_db.namespace
            'the_keyspace'
        """

        return self.using_namespace

    async def get_collection(
        self,
        name: str,
        *,
        namespace: Optional[str] = None,
        embedding_api_key: Optional[Union[str, EmbeddingHeadersProvider]] = None,
        collection_max_time_ms: Optional[int] = None,
    ) -> AsyncCollection:
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
            namespace: the namespace containing the collection. If no namespace
                is specified, the setting for this database is used.
        embedding_api_key: optional API key(s) for interacting with the collection.
            If an embedding service is configured, and this parameter is not None,
            each Data API call will include the necessary embedding-related headers
            as specified by this parameter. If a string is passed, it translates
            into the one "embedding api key" header
            (i.e. `astrapy.authentication.EmbeddingAPIKeyHeaderProvider`).
            For some vectorize providers/models, if using header-based authentication,
            specialized subclasses of `astrapy.authentication.EmbeddingHeadersProvider`
            should be supplied.
        collection_max_time_ms: a default timeout, in millisecond, for the duration of each
            operation on the collection. Individual timeouts can be provided to
            each collection method call and will take precedence, with this value
            being an overall default.
            Note that for some methods involving multiple API calls (such as
            `find`, `delete_many`, `insert_many` and so on), it is strongly suggested
            to provide a specific timeout as the default one likely wouldn't make
            much sense.

        Returns:
            an `AsyncCollection` instance, representing the desired collection
                (but without any form of validation).

        Example:
            >>> async def count_docs(adb: AsyncDatabase, c_name: str) -> int:
            ...    async_col = await adb.get_collection(c_name)
            ...    return await async_col.count_documents({}, upper_bound=100)
            ...
            >>> asyncio.run(count_docs(my_async_db, "my_collection"))
            45

        Note: the attribute and indexing syntax forms achieve the same effect
            as this method, returning an AsyncCollection, albeit
            in a synchronous way. In other words, the following are equivalent:
                await my_async_db.get_collection("coll_name")
                my_async_db.coll_name
                my_async_db["coll_name"]
        """

        # lazy importing here against circular-import error
        from astrapy.collection import AsyncCollection

        _namespace = namespace or self.namespace
        if _namespace is None:
            raise ValueError(
                "No namespace specified. This operation requires a namespace to "
                "be set, e.g. through the `use_namespace` method."
            )
        return AsyncCollection(
            self,
            name,
            namespace=_namespace,
            api_options=CollectionAPIOptions(
                embedding_api_key=coerce_embedding_headers_provider(embedding_api_key),
                max_time_ms=collection_max_time_ms,
            ),
        )

    @recast_method_async
    async def create_collection(
        self,
        name: str,
        *,
        namespace: Optional[str] = None,
        dimension: Optional[int] = None,
        metric: Optional[str] = None,
        service: Optional[Union[CollectionVectorServiceOptions, Dict[str, Any]]] = None,
        indexing: Optional[Dict[str, Any]] = None,
        default_id_type: Optional[str] = None,
        additional_options: Optional[Dict[str, Any]] = None,
        check_exists: Optional[bool] = None,
        max_time_ms: Optional[int] = None,
        embedding_api_key: Optional[Union[str, EmbeddingHeadersProvider]] = None,
        collection_max_time_ms: Optional[int] = None,
    ) -> AsyncCollection:
        """
        Creates a collection on the database and return the AsyncCollection
        instance that represents it.

        This is a blocking operation: the method returns when the collection
        is ready to be used. As opposed to the `get_collection` instance,
        this method triggers causes the collection to be actually created on DB.

        Args:
            name: the name of the collection.
            namespace: the namespace where the collection is to be created.
                If not specified, the general setting for this database is used.
            dimension: for vector collections, the dimension of the vectors
                (i.e. the number of their components).
            metric: the similarity metric used for vector searches.
                Allowed values are `VectorMetric.DOT_PRODUCT`, `VectorMetric.EUCLIDEAN`
                or `VectorMetric.COSINE` (default).
            service: a dictionary describing a service for
                embedding computation, e.g. `{"provider": "ab", "modelName": "xy"}`.
                Alternatively, a CollectionVectorServiceOptions object to the same effect.
            indexing: optional specification of the indexing options for
                the collection, in the form of a dictionary such as
                    {"deny": [...]}
                or
                    {"allow": [...]}
            default_id_type: this sets what type of IDs the API server will
                generate when inserting documents that do not specify their
                `_id` field explicitly. Can be set to any of the values
                `DefaultIdType.UUID`, `DefaultIdType.OBJECTID`,
                `DefaultIdType.UUIDV6`, `DefaultIdType.UUIDV7`,
                `DefaultIdType.DEFAULT`.
            additional_options: any further set of key-value pairs that will
                be added to the "options" part of the payload when sending
                the Data API command to create a collection.
            check_exists: whether to run an existence check for the collection
                name before attempting to create the collection:
                If check_exists is True, an error is raised when creating
                an existing collection.
                If it is False, the creation is attempted. In this case, for
                preexisting collections, the command will succeed or fail
                depending on whether the options match or not.
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
            embedding_api_key: optional API key(s) for interacting with the collection.
                If an embedding service is configured, and this parameter is not None,
                each Data API call will include the necessary embedding-related headers
                as specified by this parameter. If a string is passed, it translates
                into the one "embedding api key" header
                (i.e. `astrapy.authentication.EmbeddingAPIKeyHeaderProvider`).
                For some vectorize providers/models, if using header-based authentication,
                specialized subclasses of `astrapy.authentication.EmbeddingHeadersProvider`
                should be supplied.
            collection_max_time_ms: a default timeout, in millisecond, for the duration of each
                operation on the collection. Individual timeouts can be provided to
                each collection method call and will take precedence, with this value
                being an overall default.
                Note that for some methods involving multiple API calls (such as
                `find`, `delete_many`, `insert_many` and so on), it is strongly suggested
                to provide a specific timeout as the default one likely wouldn't make
                much sense.

        Returns:
            an `AsyncCollection` instance, representing the newly-created collection.

        Example:
            >>> async def create_and_insert(adb: AsyncDatabase) -> Dict[str, Any]:
            ...     new_a_col = await adb.create_collection("my_v_col", dimension=3)
            ...     return await new_a_col.insert_one(
            ...         {"name": "the_row", "$vector": [0.4, 0.5, 0.7]},
            ...     )
            ...
            >>> asyncio.run(create_and_insert(my_async_db))
            InsertOneResult(raw_results=..., inserted_id='08f05ecf-...-...-...')

        Note:
            A collection is considered a vector collection if at least one of
            `dimension` or `service` are provided and not null. In that case,
            and only in that case, is `metric` an accepted parameter.
            Note, moreover, that if passing both these parameters, then
            the dimension must be compatible with the chosen service.
        """

        _validate_create_collection_options(
            dimension=dimension,
            metric=metric,
            service=service,
            indexing=indexing,
            default_id_type=default_id_type,
            additional_options=additional_options,
        )
        _options = {
            **(additional_options or {}),
            **({"indexing": indexing} if indexing else {}),
            **({"defaultId": {"type": default_id_type}} if default_id_type else {}),
        }

        timeout_manager = MultiCallTimeoutManager(overall_max_time_ms=max_time_ms)

        if check_exists is None:
            _check_exists = True
        else:
            _check_exists = check_exists
        existing_names: List[str]
        if _check_exists:
            logger.info(f"checking collection existence for '{name}'")
            existing_names = await self.list_collection_names(
                namespace=namespace,
                max_time_ms=timeout_manager.remaining_timeout_ms(),
            )
        else:
            existing_names = []

        _namespace = namespace or self.namespace
        if _namespace is None:
            raise ValueError(
                "No namespace specified. This operation requires a namespace to "
                "be set, e.g. through the `use_namespace` method."
            )

        driver_db = self._astra_db.copy(namespace=_namespace)
        if name in existing_names:
            raise CollectionAlreadyExistsException(
                text=f"CollectionInvalid: collection {name} already exists",
                namespace=_namespace,
                collection_name=name,
            )

        service_dict: Optional[Dict[str, Any]]
        if service is not None:
            service_dict = service if isinstance(service, dict) else service.as_dict()
        else:
            service_dict = None

        logger.info(f"creating collection '{name}'")
        await driver_db.create_collection(
            name,
            options=_options,
            dimension=dimension,
            metric=metric,
            service_dict=service_dict,
            timeout_info=timeout_manager.remaining_timeout_info(),
        )
        logger.info(f"finished creating collection '{name}'")
        return await self.get_collection(
            name,
            namespace=namespace,
            embedding_api_key=coerce_embedding_headers_provider(embedding_api_key),
            collection_max_time_ms=collection_max_time_ms,
        )

    @recast_method_async
    async def drop_collection(
        self,
        name_or_collection: Union[str, AsyncCollection],
        *,
        max_time_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Drop a collection from the database, along with all documents therein.

        Args:
            name_or_collection: either the name of a collection or
                an `AsyncCollection` instance.
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.

        Returns:
            a dictionary in the form {"ok": 1} if the command succeeds.

        Example:
            >>> asyncio.run(my_async_db.list_collection_names())
            ['a_collection', 'my_v_col', 'another_col']
            >>> asyncio.run(my_async_db.drop_collection("my_v_col"))
            {'ok': 1}
            >>> asyncio.run(my_async_db.list_collection_names())
            ['a_collection', 'another_col']

        Note:
            when providing a collection name, it is assumed that the collection
            is to be found in the namespace set at database instance level.
        """

        # lazy importing here against circular-import error
        from astrapy.collection import AsyncCollection

        if isinstance(name_or_collection, AsyncCollection):
            _namespace = name_or_collection.namespace
            _name = name_or_collection.name
            logger.info(f"dropping collection '{_name}'")
            dc_response = await self._astra_db.copy(
                namespace=_namespace
            ).delete_collection(
                _name,
                timeout_info=base_timeout_info(max_time_ms),
            )
            logger.info(f"finished dropping collection '{_name}'")
            return dc_response.get("status", {})  # type: ignore[no-any-return]
        else:
            if self.namespace is None:
                raise ValueError(
                    "No namespace specified. This operation requires a namespace to "
                    "be set, e.g. through the `use_namespace` method."
                )
            logger.info(f"dropping collection '{name_or_collection}'")
            dc_response = await self._astra_db.delete_collection(
                name_or_collection,
                timeout_info=base_timeout_info(max_time_ms),
            )
            logger.info(f"finished dropping collection '{name_or_collection}'")
            return dc_response.get("status", {})  # type: ignore[no-any-return]

    @recast_method_sync
    def list_collections(
        self,
        *,
        namespace: Optional[str] = None,
        max_time_ms: Optional[int] = None,
    ) -> AsyncCommandCursor[CollectionDescriptor]:
        """
        List all collections in a given namespace for this database.

        Args:
            namespace: the namespace to be inspected. If not specified,
                the general setting for this database is assumed.
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.

        Returns:
            an `AsyncCommandCursor` to iterate over CollectionDescriptor instances,
            each corresponding to a collection.

        Example:
            >>> async def a_list_colls(adb: AsyncDatabase) -> None:
            ...     a_ccur = adb.list_collections()
            ...     print("* a_ccur:", a_ccur)
            ...     print("* list:", [coll async for coll in a_ccur])
            ...     async for coll in adb.list_collections():
            ...         print("* coll:", coll)
            ...
            >>> asyncio.run(a_list_colls(my_async_db))
            * a_ccur: <astrapy.cursors.AsyncCommandCursor object at ...>
            * list: [CollectionDescriptor(name='my_v_col', options=CollectionOptions())]
            * coll: CollectionDescriptor(name='my_v_col', options=CollectionOptions())
        """

        _namespace = namespace or self.namespace
        if _namespace is None:
            raise ValueError(
                "No namespace specified. This operation requires a namespace to "
                "be set, e.g. through the `use_namespace` method."
            )

        driver_db = self._astra_db.copy(namespace=_namespace)
        logger.info("getting collections")
        gc_response = driver_db.to_sync().get_collections(
            options={"explain": True},
            timeout_info=base_timeout_info(max_time_ms),
        )
        if "collections" not in gc_response.get("status", {}):
            raise DataAPIFaultyResponseException(
                text="Faulty response from get_collections API command.",
                raw_response=gc_response,
            )
        else:
            # we know this is a list of dicts, to marshal into "descriptors"
            logger.info("finished getting collections")
            return AsyncCommandCursor(
                address=driver_db.base_url,
                items=[
                    CollectionDescriptor.from_dict(col_dict)
                    for col_dict in gc_response["status"]["collections"]
                ],
            )

    @recast_method_async
    async def list_collection_names(
        self,
        *,
        namespace: Optional[str] = None,
        max_time_ms: Optional[int] = None,
    ) -> List[str]:
        """
        List the names of all collections in a given namespace of this database.

        Args:
            namespace: the namespace to be inspected. If not specified,
                the general setting for this database is assumed.
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.

        Returns:
            a list of the collection names as strings, in no particular order.

        Example:
            >>> asyncio.run(my_async_db.list_collection_names())
            ['a_collection', 'another_col']
        """

        _namespace = namespace or self.namespace
        if _namespace is None:
            raise ValueError(
                "No namespace specified. This operation requires a namespace to "
                "be set, e.g. through the `use_namespace` method."
            )

        logger.info("getting collection names")
        gc_response = await self._astra_db.copy(namespace=_namespace).get_collections(
            timeout_info=base_timeout_info(max_time_ms)
        )
        if "collections" not in gc_response.get("status", {}):
            raise DataAPIFaultyResponseException(
                text="Faulty response from get_collections API command.",
                raw_response=gc_response,
            )
        else:
            # we know this is a list of strings
            logger.info("finished getting collection names")
            return gc_response["status"]["collections"]  # type: ignore[no-any-return]

    @recast_method_async
    async def command(
        self,
        body: Dict[str, Any],
        *,
        namespace: Optional[str] = None,
        collection_name: Optional[str] = None,
        max_time_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Send a POST request to the Data API for this database with
        an arbitrary, caller-provided payload.

        Args:
            body: a JSON-serializable dictionary, the payload of the request.
            namespace: the namespace to use. Requests always target a namespace:
                if not specified, the general setting for this database is assumed.
            collection_name: if provided, the collection name is appended at the end
                of the endpoint. In this way, this method allows collection-level
                arbitrary POST requests as well.
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.

        Returns:
            a dictionary with the response of the HTTP request.

        Example:
            >>> asyncio.run(my_async_db.command({"findCollections": {}}))
            {'status': {'collections': ['my_coll']}}
            >>> asyncio.run(my_async_db.command(
            ...     {"countDocuments": {}},
            ...     collection_name="my_coll",
            ... )
            {'status': {'count': 123}}
        """

        _namespace = namespace or self.namespace
        if _namespace is None:
            raise ValueError(
                "No namespace specified. This operation requires a namespace to "
                "be set, e.g. through the `use_namespace` method."
            )

        driver_db = self._astra_db.copy(namespace=_namespace)
        if collection_name:
            _collection = await driver_db.collection(collection_name)
            logger.info(f"issuing custom command to API (on '{collection_name}')")
            req_response = await _collection.post_raw_request(
                body=body,
                timeout_info=base_timeout_info(max_time_ms),
            )
            logger.info(
                f"finished issuing custom command to API (on '{collection_name}')"
            )
            return req_response
        else:
            logger.info("issuing custom command to API")
            req_response = await driver_db.post_raw_request(
                body=body,
                timeout_info=base_timeout_info(max_time_ms),
            )
            logger.info("finished issuing custom command to API")
            return req_response

    def get_database_admin(
        self,
        *,
        token: Optional[Union[str, TokenProvider]] = None,
        dev_ops_url: Optional[str] = None,
        dev_ops_api_version: Optional[str] = None,
    ) -> DatabaseAdmin:
        """
        Return a DatabaseAdmin object corresponding to this database, for
        use in admin tasks such as managing namespaces.

        This method, depending on the environment where the database resides,
        returns an appropriate subclass of DatabaseAdmin.

        Args:
            token: an access token with enough permission on the database to
                perform the desired tasks. If omitted (as it can generally be done),
                the token of this Database is used.
                This can be either a literal token string or a subclass of
                `astrapy.authentication.TokenProvider`.
            dev_ops_url: in case of custom deployments, this can be used to specify
                the URL to the DevOps API, such as "https://api.astra.datastax.com".
                Generally it can be omitted. The environment (prod/dev/...) is
                determined from the API Endpoint.
                Note that this parameter is allowed only for Astra DB environments.
            dev_ops_api_version: this can specify a custom version of the DevOps API
                (such as "v2"). Generally not needed.
                Note that this parameter is allowed only for Astra DB environments.

        Returns:
            A DatabaseAdmin instance targeting this database. More precisely,
            for Astra DB an instance of `AstraDBDatabaseAdmin` is returned;
            for other environments, an instance of `DataAPIDatabaseAdmin` is returned.

        Example:
            >>> my_db_admin = my_async_db.get_database_admin()
            >>> if "new_namespace" not in my_db_admin.list_namespaces():
            ...     my_db_admin.create_namespace("new_namespace")
            >>> my_db_admin.list_namespaces()
            ['default_keyspace', 'new_namespace']
        """

        # lazy importing here to avoid circular dependency
        from astrapy.admin import AstraDBDatabaseAdmin, DataAPIDatabaseAdmin

        if self.environment in Environment.astra_db_values:
            return AstraDBDatabaseAdmin(
                api_endpoint=self.api_endpoint,
                token=coerce_token_provider(token) or self.token_provider,
                environment=self.environment,
                caller_name=self.caller_name,
                caller_version=self.caller_version,
                dev_ops_url=dev_ops_url,
                dev_ops_api_version=dev_ops_api_version,
                spawner_database=self,
            )
        else:
            if dev_ops_url is not None:
                raise ValueError(
                    "Parameter `dev_ops_url` not supported outside of Astra DB."
                )
            if dev_ops_api_version is not None:
                raise ValueError(
                    "Parameter `dev_ops_api_version` not supported outside of Astra DB."
                )
            return DataAPIDatabaseAdmin(
                api_endpoint=self.api_endpoint,
                token=coerce_token_provider(token) or self.token_provider,
                environment=self.environment,
                api_path=self.api_path,
                api_version=self.api_version,
                caller_name=self.caller_name,
                caller_version=self.caller_version,
                spawner_database=self,
            )
