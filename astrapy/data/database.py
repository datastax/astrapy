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
from typing import TYPE_CHECKING, Any, Sequence

from astrapy.admin import fetch_database_info, parse_api_endpoint
from astrapy.authentication import (
    coerce_possible_embedding_headers_provider,
    coerce_possible_token_provider,
    redact_secret,
)
from astrapy.constants import CallerType, Environment
from astrapy.cursors import AsyncCommandCursor, CommandCursor
from astrapy.exceptions import (
    CollectionAlreadyExistsException,
    DataAPIFaultyResponseException,
    DevOpsAPIException,
    MultiCallTimeoutManager,
    base_timeout_info,
)
from astrapy.info import (
    CollectionDescriptor,
    CollectionVectorServiceOptions,
    DatabaseInfo,
)
from astrapy.settings.defaults import (
    DEFAULT_ASTRA_DB_KEYSPACE,
    DEFAULT_DATA_API_AUTH_HEADER,
)
from astrapy.utils.api_commander import APICommander
from astrapy.utils.api_options import (
    APIOptions,
    DataAPIURLOptions,
    DevOpsAPIURLOptions,
    FullAPIOptions,
    TimeoutOptions,
)
from astrapy.utils.unset import _UNSET, UnsetType

if TYPE_CHECKING:
    from astrapy.admin import DatabaseAdmin
    from astrapy.authentication import EmbeddingHeadersProvider, TokenProvider
    from astrapy.collection import AsyncCollection, Collection


logger = logging.getLogger(__name__)


def _normalize_create_collection_options(
    dimension: int | None,
    metric: str | None,
    service: CollectionVectorServiceOptions | dict[str, Any] | None,
    indexing: dict[str, Any] | None,
    default_id_type: str | None,
    additional_options: dict[str, Any] | None,
) -> dict[str, Any]:
    """Raise errors related to invalid input, and return a ready-to-send payload."""
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
    # prepare the payload
    service_dict: dict[str, Any] | None
    if service is not None:
        service_dict = service if isinstance(service, dict) else service.as_dict()
    else:
        service_dict = None
    vector_options = {
        k: v
        for k, v in {
            "dimension": dimension,
            "metric": metric,
            "service": service_dict,
        }.items()
        if v is not None
    }
    full_options0 = {
        k: v
        for k, v in {
            **({"indexing": indexing} if indexing else {}),
            **({"defaultId": {"type": default_id_type}} if default_id_type else {}),
            **({"vector": vector_options} if vector_options else {}),
        }.items()
        if v
    }
    overlap_keys = (full_options0).keys() & (additional_options or {}).keys()
    if overlap_keys:
        raise ValueError(
            "Gotten forbidden key(s) in additional_options: "
            f"{','.join(sorted(overlap_keys))}."
        )
    full_options = {
        **(additional_options or {}),
        **full_options0,
    }
    return full_options


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

    def __getattr__(self, collection_name: str) -> Collection:
        return self.get_collection(name=collection_name)

    def __getitem__(self, collection_name: str) -> Collection:
        return self.get_collection(name=collection_name)

    def __repr__(self) -> str:
        ep_desc = f'api_endpoint="{self.api_endpoint}"'
        token_desc: str | None
        if self.api_options.token:
            token_desc = f'token="{redact_secret(str(self.api_options.token), 15)}"'
        else:
            token_desc = None
        keyspace_desc: str | None
        if self.keyspace is None:
            keyspace_desc = "keyspace not set"
        else:
            keyspace_desc = f'keyspace="{self.keyspace}"'
        parts = [pt for pt in [ep_desc, token_desc, keyspace_desc] if pt is not None]
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
        api_endpoint: str | None = None,
        token: str | TokenProvider | UnsetType = _UNSET,
        keyspace: str | None = None,
        callers: Sequence[CallerType] | UnsetType = _UNSET,
        environment: str | UnsetType = _UNSET,
        api_path: str | None | UnsetType = _UNSET,
        api_version: str | None | UnsetType = _UNSET,
    ) -> Database:
        arg_api_options = APIOptions(
            token=coerce_possible_token_provider(token),
            callers=callers,
            environment=environment,
            data_api_url_options=DataAPIURLOptions(
                api_path=api_path,
                api_version=api_version,
            ),
        )
        api_options = self.api_options.with_override(arg_api_options)
        return Database(
            api_endpoint=api_endpoint or self.api_endpoint,
            keyspace=keyspace or self.keyspace,
            api_options=api_options,
        )

    def with_options(
        self,
        *,
        keyspace: str | None = None,
        callers: Sequence[CallerType] | UnsetType = _UNSET,
    ) -> Database:
        """
        Create a clone of this database with some changed attributes.

        Args:
            keyspace: this is the keyspace all method calls will target, unless
                one is explicitly specified in the call. If no keyspace is supplied
                when creating a Database, the name "default_keyspace" is set.
            callers: a list of caller identities, i.e. applications, or frameworks,
                on behalf of which the Data API calls are performed. These end up
                in the request user-agent.
                Each caller identity is a ("caller_name", "caller_version") pair.

        Returns:
            a new `Database` instance.

        Example:
            >>> my_db_2 = my_db.with_options(
            ...     keyspace="the_other_keyspace",
            ...     callers=[("the_caller", "0.1.0")],
            ... )
        """

        return self._copy(
            keyspace=keyspace,
            callers=callers,
        )

    def to_async(
        self,
        *,
        api_endpoint: str | None = None,
        token: str | TokenProvider | UnsetType = _UNSET,
        keyspace: str | None = None,
        callers: Sequence[CallerType] | UnsetType = _UNSET,
        environment: str | UnsetType = _UNSET,
        api_path: str | None | UnsetType = _UNSET,
        api_version: str | None | UnsetType = _UNSET,
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
            keyspace: this is the keyspace all method calls will target, unless
                one is explicitly specified in the call. If no keyspace is supplied
                when creating a Database, the name "default_keyspace" is set.
            callers: a list of caller identities, i.e. applications, or frameworks,
                on behalf of which the Data API calls are performed. These end up
                in the request user-agent.
                Each caller identity is a ("caller_name", "caller_version") pair.
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

        arg_api_options = APIOptions(
            token=coerce_possible_token_provider(token),
            callers=callers,
            environment=environment,
            data_api_url_options=DataAPIURLOptions(
                api_path=api_path,
                api_version=api_version,
            ),
        )
        api_options = self.api_options.with_override(arg_api_options)
        return AsyncDatabase(
            api_endpoint=api_endpoint or self.api_endpoint,
            keyspace=keyspace or self.keyspace,
            api_options=api_options,
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
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> DatabaseInfo:
        """
        Additional information on the database as a DatabaseInfo instance.

        Some of the returned properties are dynamic throughout the lifetime
        of the database (such as raw_info["keyspaces"]). For this reason,
        each invocation of this method triggers a new request to the DevOps API.

        Args:
            request_timeout_ms: a timeout, in milliseconds, for the DevOps API request.
            max_time_ms: an alias for `request_timeout_ms`.

        Example:
            >>> my_db.info().region
            'eu-west-1'

            >>> my_db.info().raw_info['datacenters'][0]['dateCreated']
            '2023-01-30T12:34:56Z'

        Note:
            see the DatabaseInfo documentation for a caveat about the difference
            between the `region` and the `raw_info["region"]` attributes.
        """

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )
        logger.info("getting database info")
        database_info = fetch_database_info(
            self.api_endpoint,
            token=self.api_options.token.get_token(),
            keyspace=self.keyspace,
            max_time_ms=_request_timeout_ms,
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

    def get_collection(
        self,
        name: str,
        *,
        keyspace: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        collection_request_timeout_ms: int | UnsetType = _UNSET,
        collection_max_time_ms: int | UnsetType = _UNSET,
        collection_api_options: APIOptions | UnsetType = _UNSET,
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
            collection_request_timeout_ms: a default timeout, in millisecond, for the
                duration of each request in the collection. For a more fine-grained
                control of collection timeouts (suggested e.g. with regard to
                methods involving multiple requests, such as `find`), use of the
                `collection_api_options` parameter is suggested; alternatively,
                bear in mind that individual collection methods also accept timeout
                parameters.
            collection_max_time_ms: an alias for `collection_request_timeout_ms`.
            collection_api_options: a specification - complete or partial - of the
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

        # this multiple-override implements the alias on timeout params
        resulting_api_options = (
            self.api_options.with_override(
                collection_api_options,
            )
            .with_override(
                APIOptions(
                    timeout_options=TimeoutOptions(
                        request_timeout_ms=collection_max_time_ms,
                    )
                ),
            )
            .with_override(
                APIOptions(
                    embedding_api_key=coerce_possible_embedding_headers_provider(
                        embedding_api_key
                    ),
                    timeout_options=TimeoutOptions(
                        request_timeout_ms=collection_request_timeout_ms,
                    ),
                ),
            )
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

    def create_collection(
        self,
        name: str,
        *,
        keyspace: str | None = None,
        dimension: int | None = None,
        metric: str | None = None,
        service: CollectionVectorServiceOptions | dict[str, Any] | None = None,
        indexing: dict[str, Any] | None = None,
        default_id_type: str | None = None,
        additional_options: dict[str, Any] | None = None,
        check_exists: bool | None = None,
        schema_operation_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        collection_request_timeout_ms: int | UnsetType = _UNSET,
        collection_max_time_ms: int | UnsetType = _UNSET,
        collection_api_options: APIOptions | UnsetType = _UNSET,
    ) -> Collection:
        """
        Creates a collection on the database and return the Collection
        instance that represents it.

        This is a blocking operation: the method returns when the collection
        is ready to be used. As opposed to the `get_collection` instance,
        this method triggers causes the collection to be actually created on DB.

        Args:
            name: the name of the collection.
            keyspace: the keyspace where the collection is to be created.
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
            schema_operation_timeout_ms: a timeout, in milliseconds, for the
                createCollection HTTP request.
            max_time_ms: an alias for `schema_operation_timeout_ms`.
            embedding_api_key: optional API key(s) for interacting with the collection.
                If an embedding service is configured, and this parameter is not None,
                each Data API call will include the necessary embedding-related headers
                as specified by this parameter. If a string is passed, it translates
                into the one "embedding api key" header
                (i.e. `astrapy.authentication.EmbeddingAPIKeyHeaderProvider`).
                For some vectorize providers/models, if using header-based authentication,
                specialized subclasses of `astrapy.authentication.EmbeddingHeadersProvider`
                should be supplied.
            collection_request_timeout_ms: a default timeout, in millisecond, for the
                duration of each request in the collection. For a more fine-grained
                control of collection timeouts (suggested e.g. with regard to
                methods involving multiple requests, such as `find`), use of the
                `collection_api_options` parameter is suggested; alternatively,
                bear in mind that individual collection methods also accept timeout
                parameters.
            collection_max_time_ms: an alias for `collection_request_timeout_ms`.
            collection_api_options: a specification - complete or partial - of the
                API Options to override the defaults inherited from the Database.
                This allows for a deeper configuration of the collection, e.g.
                concerning timeouts; if this is passed together with
                the named timeout parameters, the latter will take precedence
                in their respective settings.

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

        cc_options = _normalize_create_collection_options(
            dimension=dimension,
            metric=metric,
            service=service,
            indexing=indexing,
            default_id_type=default_id_type,
            additional_options=additional_options,
        )

        _schema_operation_timeout_ms = (
            schema_operation_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.schema_operation_timeout_ms
        )

        timeout_manager = MultiCallTimeoutManager(
            overall_max_time_ms=_schema_operation_timeout_ms
        )

        if check_exists is None:
            _check_exists = True
        else:
            _check_exists = check_exists
        if _check_exists:
            logger.info(f"checking collection existence for '{name}'")
            existing_names = self.list_collection_names(
                keyspace=keyspace,
                max_time_ms=timeout_manager.remaining_timeout_ms(),
            )
            if name in existing_names:
                raise CollectionAlreadyExistsException(
                    text=f"Collection {name} already exists",
                    keyspace=keyspace or self.keyspace or "(unspecified)",
                    collection_name=name,
                )

        driver_commander = self._get_driver_commander(keyspace=keyspace)
        cc_payload = {"createCollection": {"name": name, "options": cc_options}}
        logger.info(f"createCollection('{name}')")
        driver_commander.request(
            payload=cc_payload,
            timeout_info=timeout_manager.remaining_timeout_info(),
        )
        logger.info(f"finished createCollection('{name}')")
        return self.get_collection(
            name,
            keyspace=keyspace,
            embedding_api_key=embedding_api_key,
            collection_request_timeout_ms=collection_request_timeout_ms,
            collection_max_time_ms=collection_max_time_ms,
            collection_api_options=collection_api_options,
        )

    def drop_collection(
        self,
        name_or_collection: str | Collection,
        *,
        schema_operation_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        Drop a collection from the database, along with all documents therein.

        Args:
            name_or_collection: either the name of a collection or
                a `Collection` instance.
            schema_operation_timeout_ms: a timeout, in milliseconds, for
                the underlying schema-changing HTTP request.
            max_time_ms: an alias for `schema_operation_timeout_ms`.

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
            is to be found in the keyspace that was set at database instance level.
        """

        # lazy importing here against circular-import error
        from astrapy.collection import Collection

        _schema_operation_timeout_ms = (
            schema_operation_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.schema_operation_timeout_ms
        )

        _keyspace: str | None
        _collection_name: str
        if isinstance(name_or_collection, Collection):
            _keyspace = name_or_collection.keyspace
            _collection_name = name_or_collection.name
        else:
            _keyspace = self.keyspace
            _collection_name = name_or_collection
        driver_commander = self._get_driver_commander(keyspace=_keyspace)
        dc_payload = {"deleteCollection": {"name": _collection_name}}
        logger.info(f"deleteCollection('{_collection_name}')")
        dc_response = driver_commander.request(
            payload=dc_payload,
            timeout_info=base_timeout_info(_schema_operation_timeout_ms),
        )
        logger.info(f"finished deleteCollection('{_collection_name}')")
        return dc_response.get("status", {})  # type: ignore[no-any-return]

    def list_collections(
        self,
        *,
        keyspace: str | None = None,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> CommandCursor[CollectionDescriptor]:
        """
        List all collections in a given keyspace for this database.

        Args:
            keyspace: the keyspace to be inspected. If not specified,
                the general setting for this database is assumed.
            request_timeout_ms: a timeout, in milliseconds, for
                the underlying HTTP request.
            max_time_ms: an alias for `request_timeout_ms`.

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

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )

        driver_commander = self._get_driver_commander(keyspace=keyspace)
        gc_payload = {"findCollections": {"options": {"explain": True}}}
        logger.info("findCollections")
        gc_response = driver_commander.request(
            payload=gc_payload,
            timeout_info=base_timeout_info(_request_timeout_ms),
        )
        if "collections" not in gc_response.get("status", {}):
            raise DataAPIFaultyResponseException(
                text="Faulty response from get_collections API command.",
                raw_response=gc_response,
            )
        else:
            # we know this is a list of dicts, to marshal into "descriptors"
            logger.info("finished findCollections")
            return CommandCursor(
                address=driver_commander.full_path,
                items=[
                    CollectionDescriptor.from_dict(col_dict)
                    for col_dict in gc_response["status"]["collections"]
                ],
            )

    def list_collection_names(
        self,
        *,
        keyspace: str | None = None,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> list[str]:
        """
        List the names of all collections in a given keyspace of this database.

        Args:
            keyspace: the keyspace to be inspected. If not specified,
                the general setting for this database is assumed.
            request_timeout_ms: a timeout, in milliseconds, for
                the underlying HTTP request.
            max_time_ms: an alias for `request_timeout_ms`.

        Returns:
            a list of the collection names as strings, in no particular order.

        Example:
            >>> my_db.list_collection_names()
            ['a_collection', 'another_col']
        """

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )

        driver_commander = self._get_driver_commander(keyspace=keyspace)
        gc_payload: dict[str, Any] = {"findCollections": {}}
        logger.info("findCollections")
        gc_response = driver_commander.request(
            payload=gc_payload,
            timeout_info=base_timeout_info(_request_timeout_ms),
        )
        if "collections" not in gc_response.get("status", {}):
            raise DataAPIFaultyResponseException(
                text="Faulty response from get_collections API command.",
                raw_response=gc_response,
            )
        else:
            # we know this is a list of dicts, to marshal into "descriptors"
            logger.info("finished findCollections")
            return gc_response["status"]["collections"]  # type: ignore[no-any-return]

    def command(
        self,
        body: dict[str, Any],
        *,
        keyspace: str | None = None,
        collection_name: str | None = None,
        raise_api_errors: bool = True,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        Send a POST request to the Data API for this database with
        an arbitrary, caller-provided payload.

        Args:
            body: a JSON-serializable dictionary, the payload of the request.
            keyspace: the keyspace to use. Requests always target a keyspace:
                if not specified, the general setting for this database is assumed.
            collection_name: if provided, the collection name is appended at the end
                of the endpoint. In this way, this method allows collection-level
                arbitrary POST requests as well.
            raise_api_errors: if True, responses with a nonempty 'errors' field
                result in an astrapy exception being raised.
            request_timeout_ms: a timeout, in milliseconds, for
                the underlying HTTP request.
            max_time_ms: an alias for `request_timeout_ms`.

        Returns:
            a dictionary with the response of the HTTP request.

        Example:
            >>> my_db.command({"findCollections": {}})
            {'status': {'collections': ['my_coll']}}
            >>> my_db.command({"countDocuments": {}}, collection_name="my_coll")
            {'status': {'count': 123}}
        """

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )

        if collection_name:
            # if keyspace and collection_name both passed, a new database is needed
            _database: Database
            if keyspace:
                _database = self._copy(keyspace=keyspace)
            else:
                _database = self
            logger.info("deferring to collection " f"'{collection_name}' for command.")
            coll_req_response = _database.get_collection(collection_name).command(
                body=body,
                raise_api_errors=raise_api_errors,
                max_time_ms=_request_timeout_ms,
            )
            logger.info(
                "finished deferring to collection " f"'{collection_name}' for command."
            )
            return coll_req_response
        else:
            driver_commander = self._get_driver_commander(keyspace=keyspace)
            _cmd_desc = ",".join(sorted(body.keys()))
            logger.info(f"command={_cmd_desc} on {self.__class__.__name__}")
            req_response = driver_commander.request(
                payload=body,
                raise_api_errors=raise_api_errors,
                timeout_info=base_timeout_info(_request_timeout_ms),
            )
            logger.info(f"command={_cmd_desc} on {self.__class__.__name__}")
            return req_response

    def get_database_admin(
        self,
        *,
        token: str | TokenProvider | UnsetType = _UNSET,
        dev_ops_url: str | UnsetType = _UNSET,
        dev_ops_api_version: str | None | UnsetType = _UNSET,
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
            >>> if "new_keyspace" not in my_db_admin.list_keyspaces():
            ...     my_db_admin.create_keyspace("new_keyspace")
            >>> my_db_admin.list_keyspaces()
            ['default_keyspace', 'new_keyspace']
        """

        # lazy importing here to avoid circular dependency
        from astrapy.admin.admin import AstraDBDatabaseAdmin, DataAPIDatabaseAdmin

        arg_api_options = APIOptions(
            token=coerce_possible_token_provider(token),
            dev_ops_api_url_options=DevOpsAPIURLOptions(
                dev_ops_url=dev_ops_url,
                dev_ops_api_version=dev_ops_api_version,
            ),
        )
        api_options = self.api_options.with_override(arg_api_options)

        if api_options.environment in Environment.astra_db_values:
            return AstraDBDatabaseAdmin(
                api_endpoint=self.api_endpoint,
                api_options=api_options,
                spawner_database=self,
            )
        else:
            if not isinstance(dev_ops_url, UnsetType):
                raise ValueError(
                    "Parameter `dev_ops_url` not supported outside of Astra DB."
                )
            if not isinstance(dev_ops_api_version, UnsetType):
                raise ValueError(
                    "Parameter `dev_ops_api_version` not supported outside of Astra DB."
                )
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

    def __getattr__(self, collection_name: str) -> AsyncCollection:
        return self.to_sync().get_collection(name=collection_name).to_async()

    def __getitem__(self, collection_name: str) -> AsyncCollection:
        return self.to_sync().get_collection(name=collection_name).to_async()

    def __repr__(self) -> str:
        ep_desc = f'api_endpoint="{self.api_endpoint}"'
        token_desc: str | None
        if self.token_provider:
            token_desc = f'token="{redact_secret(str(self.api_options.token), 15)}"'
        else:
            token_desc = None
        keyspace_desc: str | None
        if self.keyspace is None:
            keyspace_desc = "keyspace not set"
        else:
            keyspace_desc = f'keyspace="{self.keyspace}"'
        parts = [pt for pt in [ep_desc, token_desc, keyspace_desc] if pt is not None]
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
        api_endpoint: str | None = None,
        token: str | TokenProvider | UnsetType = _UNSET,
        keyspace: str | None = None,
        callers: Sequence[CallerType] | UnsetType = _UNSET,
        environment: str | UnsetType = _UNSET,
        api_path: str | None | UnsetType = _UNSET,
        api_version: str | None | UnsetType = _UNSET,
    ) -> AsyncDatabase:
        arg_api_options = APIOptions(
            token=coerce_possible_token_provider(token),
            callers=callers,
            environment=environment,
            data_api_url_options=DataAPIURLOptions(
                api_path=api_path,
                api_version=api_version,
            ),
        )
        api_options = self.api_options.with_override(arg_api_options)
        return AsyncDatabase(
            api_endpoint=api_endpoint or self.api_endpoint,
            keyspace=keyspace or self.keyspace,
            api_options=api_options,
        )

    def with_options(
        self,
        *,
        keyspace: str | None = None,
        callers: Sequence[CallerType] | UnsetType = _UNSET,
    ) -> AsyncDatabase:
        """
        Create a clone of this database with some changed attributes.

        Args:
            keyspace: this is the keyspace all method calls will target, unless
                one is explicitly specified in the call. If no keyspace is supplied
                when creating a Database, the name "default_keyspace" is set.
            callers: a list of caller identities, i.e. applications, or frameworks,
                on behalf of which the Data API calls are performed. These end up
                in the request user-agent.
                Each caller identity is a ("caller_name", "caller_version") pair.

        Returns:
            a new `AsyncDatabase` instance.

        Example:
            >>> my_async_db_2 = my_async_db.with_options(
            ...     keyspace="the_other_keyspace",
            ...     callers=[("the_caller", "0.1.0")],
            ... )
        """

        return self._copy(
            keyspace=keyspace,
            callers=callers,
        )

    def to_sync(
        self,
        *,
        api_endpoint: str | None = None,
        token: str | TokenProvider | UnsetType = _UNSET,
        keyspace: str | None = None,
        callers: Sequence[CallerType] | UnsetType = _UNSET,
        environment: str | UnsetType = _UNSET,
        api_path: str | None | UnsetType = _UNSET,
        api_version: str | None | UnsetType = _UNSET,
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
            keyspace: this is the keyspace all method calls will target, unless
                one is explicitly specified in the call. If no keyspace is supplied
                when creating a Database, the name "default_keyspace" is set.
            callers: a list of caller identities, i.e. applications, or frameworks,
                on behalf of which the Data API calls are performed. These end up
                in the request user-agent.
                Each caller identity is a ("caller_name", "caller_version") pair.
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

        arg_api_options = APIOptions(
            token=coerce_possible_token_provider(token),
            callers=callers,
            environment=environment,
            data_api_url_options=DataAPIURLOptions(
                api_path=api_path,
                api_version=api_version,
            ),
        )
        api_options = self.api_options.with_override(arg_api_options)
        return Database(
            api_endpoint=api_endpoint or self.api_endpoint,
            keyspace=keyspace or self.keyspace,
            api_options=api_options,
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
            >>> asyncio.run(my_async_db.list_collection_names())
            ['coll_1', 'coll_2']
            >>> my_async_db.use_keyspace("an_empty_keyspace")
            >>> asyncio.run(my_async_db.list_collection_names())
            []
        """
        logger.info(f"switching to keyspace '{keyspace}'")
        self._using_keyspace = keyspace
        self._api_commander = self._get_api_commander(keyspace=self.keyspace)

    def info(
        self,
        *,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> DatabaseInfo:
        """
        Additional information on the database as a DatabaseInfo instance.

        Some of the returned properties are dynamic throughout the lifetime
        of the database (such as raw_info["keyspaces"]). For this reason,
        each invocation of this method triggers a new request to the DevOps API.

        Args:
            request_timeout_ms: a timeout, in milliseconds, for the DevOps API request.
            max_time_ms: an alias for `request_timeout_ms`.

        Example:
            >>> my_async_db.info().region
            'eu-west-1'

            >>> my_async_db.info().raw_info['datacenters'][0]['dateCreated']
            '2023-01-30T12:34:56Z'

        Note:
            see the DatabaseInfo documentation for a caveat about the difference
            between the `region` and the `raw_info["region"]` attributes.
        """

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )
        logger.info("getting database info")
        database_info = fetch_database_info(
            self.api_endpoint,
            token=self.api_options.token.get_token(),
            keyspace=self.keyspace,
            max_time_ms=_request_timeout_ms,
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
    def keyspace(self) -> str | None:
        """
        The keyspace this database uses as target for all commands when
        no method-call-specific keyspace is specified.

        Returns:
            the working keyspace (a string), or None if not set.

        Example:
            >>> my_async_db.keyspace
            'the_keyspace'
        """

        return self._using_keyspace

    async def get_collection(
        self,
        name: str,
        *,
        keyspace: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        collection_request_timeout_ms: int | UnsetType = _UNSET,
        collection_max_time_ms: int | UnsetType = _UNSET,
        collection_api_options: APIOptions | UnsetType = _UNSET,
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
            collection_request_timeout_ms: a default timeout, in millisecond, for the
                duration of each request in the collection. For a more fine-grained
                control of collection timeouts (suggested e.g. with regard to
                methods involving multiple requests, such as `find`), use of the
                `collection_api_options` parameter is suggested; alternatively,
                bear in mind that individual collection methods also accept timeout
                parameters.
            collection_max_time_ms: an alias for `collection_request_timeout_ms`.
            collection_api_options: a specification - complete or partial - of the
                API Options to override the defaults inherited from the Database.
                This allows for a deeper configuration of the collection, e.g.
                concerning timeouts; if this is passed together with
                the named timeout parameters, the latter will take precedence
                in their respective settings.

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

        # this multiple-override implements the alias on timeout params
        resulting_api_options = (
            self.api_options.with_override(
                collection_api_options,
            )
            .with_override(
                APIOptions(
                    timeout_options=TimeoutOptions(
                        request_timeout_ms=collection_max_time_ms,
                    )
                ),
            )
            .with_override(
                APIOptions(
                    embedding_api_key=coerce_possible_embedding_headers_provider(
                        embedding_api_key
                    ),
                    timeout_options=TimeoutOptions(
                        request_timeout_ms=collection_request_timeout_ms,
                    ),
                ),
            )
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

    async def create_collection(
        self,
        name: str,
        *,
        keyspace: str | None = None,
        dimension: int | None = None,
        metric: str | None = None,
        service: CollectionVectorServiceOptions | dict[str, Any] | None = None,
        indexing: dict[str, Any] | None = None,
        default_id_type: str | None = None,
        additional_options: dict[str, Any] | None = None,
        check_exists: bool | None = None,
        schema_operation_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        collection_request_timeout_ms: int | UnsetType = _UNSET,
        collection_max_time_ms: int | UnsetType = _UNSET,
        collection_api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncCollection:
        """
        Creates a collection on the database and return the AsyncCollection
        instance that represents it.

        This is a blocking operation: the method returns when the collection
        is ready to be used. As opposed to the `get_collection` instance,
        this method triggers causes the collection to be actually created on DB.

        Args:
            name: the name of the collection.
            keyspace: the keyspace where the collection is to be created.
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
            schema_operation_timeout_ms: a timeout, in milliseconds, for the
                createCollection HTTP request.
            max_time_ms: an alias for `schema_operation_timeout_ms`.
            embedding_api_key: optional API key(s) for interacting with the collection.
                If an embedding service is configured, and this parameter is not None,
                each Data API call will include the necessary embedding-related headers
                as specified by this parameter. If a string is passed, it translates
                into the one "embedding api key" header
                (i.e. `astrapy.authentication.EmbeddingAPIKeyHeaderProvider`).
                For some vectorize providers/models, if using header-based authentication,
                specialized subclasses of `astrapy.authentication.EmbeddingHeadersProvider`
                should be supplied.
            collection_request_timeout_ms: a default timeout, in millisecond, for the
                duration of each request in the collection. For a more fine-grained
                control of collection timeouts (suggested e.g. with regard to
                methods involving multiple requests, such as `find`), use of the
                `collection_api_options` parameter is suggested; alternatively,
                bear in mind that individual collection methods also accept timeout
                parameters.
            collection_max_time_ms: an alias for `collection_request_timeout_ms`.
            collection_api_options: a specification - complete or partial - of the
                API Options to override the defaults inherited from the Database.
                This allows for a deeper configuration of the collection, e.g.
                concerning timeouts; if this is passed together with
                the named timeout parameters, the latter will take precedence
                in their respective settings.

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

        cc_options = _normalize_create_collection_options(
            dimension=dimension,
            metric=metric,
            service=service,
            indexing=indexing,
            default_id_type=default_id_type,
            additional_options=additional_options,
        )

        _schema_operation_timeout_ms = (
            schema_operation_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.schema_operation_timeout_ms
        )

        timeout_manager = MultiCallTimeoutManager(
            overall_max_time_ms=_schema_operation_timeout_ms
        )

        if check_exists is None:
            _check_exists = True
        else:
            _check_exists = check_exists
        if _check_exists:
            logger.info(f"checking collection existence for '{name}'")
            existing_names = await self.list_collection_names(
                keyspace=keyspace,
                max_time_ms=timeout_manager.remaining_timeout_ms(),
            )
            if name in existing_names:
                raise CollectionAlreadyExistsException(
                    text=f"Collection {name} already exists",
                    keyspace=keyspace or self.keyspace or "(unspecified)",
                    collection_name=name,
                )

        driver_commander = self._get_driver_commander(keyspace=keyspace)
        cc_payload = {"createCollection": {"name": name, "options": cc_options}}
        logger.info(f"createCollection('{name}')")
        await driver_commander.async_request(
            payload=cc_payload,
            timeout_info=timeout_manager.remaining_timeout_info(),
        )
        logger.info(f"createCollection('{name}')")
        return await self.get_collection(
            name,
            keyspace=keyspace,
            embedding_api_key=embedding_api_key,
            collection_request_timeout_ms=collection_request_timeout_ms,
            collection_max_time_ms=collection_max_time_ms,
            collection_api_options=collection_api_options,
        )

    async def drop_collection(
        self,
        name_or_collection: str | AsyncCollection,
        *,
        schema_operation_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        Drop a collection from the database, along with all documents therein.

        Args:
            name_or_collection: either the name of a collection or
                an `AsyncCollection` instance.
            schema_operation_timeout_ms: a timeout, in milliseconds, for
                the underlying schema-changing HTTP request.
            max_time_ms: an alias for `schema_operation_timeout_ms`.

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
            is to be found in the keyspace that was set at database instance level.
        """

        # lazy importing here against circular-import error
        from astrapy.collection import AsyncCollection

        _schema_operation_timeout_ms = (
            schema_operation_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.schema_operation_timeout_ms
        )

        keyspace: str | None
        _collection_name: str
        if isinstance(name_or_collection, AsyncCollection):
            keyspace = name_or_collection.keyspace
            _collection_name = name_or_collection.name
        else:
            keyspace = self.keyspace
            _collection_name = name_or_collection
        driver_commander = self._get_driver_commander(keyspace=keyspace)
        dc_payload = {"deleteCollection": {"name": _collection_name}}
        logger.info(f"deleteCollection('{_collection_name}')")
        dc_response = await driver_commander.async_request(
            payload=dc_payload,
            timeout_info=base_timeout_info(_schema_operation_timeout_ms),
        )
        logger.info(f"finished deleteCollection('{_collection_name}')")
        return dc_response.get("status", {})  # type: ignore[no-any-return]

    def list_collections(
        self,
        *,
        keyspace: str | None = None,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> AsyncCommandCursor[CollectionDescriptor]:
        """
        List all collections in a given keyspace for this database.

        Args:
            keyspace: the keyspace to be inspected. If not specified,
                the general setting for this database is assumed.
            request_timeout_ms: a timeout, in milliseconds, for
                the underlying HTTP request.
            max_time_ms: an alias for `request_timeout_ms`.

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

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )

        driver_commander = self._get_driver_commander(keyspace=keyspace)
        gc_payload = {"findCollections": {"options": {"explain": True}}}
        logger.info("findCollections")
        gc_response = driver_commander.request(
            payload=gc_payload,
            timeout_info=base_timeout_info(_request_timeout_ms),
        )
        if "collections" not in gc_response.get("status", {}):
            raise DataAPIFaultyResponseException(
                text="Faulty response from get_collections API command.",
                raw_response=gc_response,
            )
        else:
            # we know this is a list of dicts, to marshal into "descriptors"
            logger.info("finished findCollections")
            return AsyncCommandCursor(
                address=driver_commander.full_path,
                items=[
                    CollectionDescriptor.from_dict(col_dict)
                    for col_dict in gc_response["status"]["collections"]
                ],
            )

    async def list_collection_names(
        self,
        *,
        keyspace: str | None = None,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> list[str]:
        """
        List the names of all collections in a given keyspace of this database.

        Args:
            keyspace: the keyspace to be inspected. If not specified,
                the general setting for this database is assumed.
            request_timeout_ms: a timeout, in milliseconds, for
                the underlying HTTP request.
            max_time_ms: an alias for `request_timeout_ms`.

        Returns:
            a list of the collection names as strings, in no particular order.

        Example:
            >>> asyncio.run(my_async_db.list_collection_names())
            ['a_collection', 'another_col']
        """

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )

        driver_commander = self._get_driver_commander(keyspace=keyspace)
        gc_payload: dict[str, Any] = {"findCollections": {}}
        logger.info("findCollections")
        gc_response = await driver_commander.async_request(
            payload=gc_payload,
            timeout_info=base_timeout_info(_request_timeout_ms),
        )
        if "collections" not in gc_response.get("status", {}):
            raise DataAPIFaultyResponseException(
                text="Faulty response from get_collections API command.",
                raw_response=gc_response,
            )
        else:
            # we know this is a list of dicts, to marshal into "descriptors"
            logger.info("finished findCollections")
            return gc_response["status"]["collections"]  # type: ignore[no-any-return]

    async def command(
        self,
        body: dict[str, Any],
        *,
        keyspace: str | None = None,
        collection_name: str | None = None,
        raise_api_errors: bool = True,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        Send a POST request to the Data API for this database with
        an arbitrary, caller-provided payload.

        Args:
            body: a JSON-serializable dictionary, the payload of the request.
            keyspace: the keyspace to use. Requests always target a keyspace:
                if not specified, the general setting for this database is assumed.
            collection_name: if provided, the collection name is appended at the end
                of the endpoint. In this way, this method allows collection-level
                arbitrary POST requests as well.
            raise_api_errors: if True, responses with a nonempty 'errors' field
                result in an astrapy exception being raised.
            request_timeout_ms: a timeout, in milliseconds, for
                the underlying HTTP request.
            max_time_ms: an alias for `request_timeout_ms`.

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

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )

        if collection_name:
            # if keyspace and collection_name both passed, a new database is needed
            _database: AsyncDatabase
            if keyspace:
                _database = self._copy(keyspace=keyspace)
            else:
                _database = self
            logger.info("deferring to collection " f"'{collection_name}' for command.")
            _collection = await _database.get_collection(collection_name)
            coll_req_response = await _collection.command(
                body=body,
                raise_api_errors=raise_api_errors,
                max_time_ms=_request_timeout_ms,
            )
            logger.info(
                "finished deferring to collection " f"'{collection_name}' for command."
            )
            return coll_req_response
        else:
            driver_commander = self._get_driver_commander(keyspace=keyspace)
            _cmd_desc = ",".join(sorted(body.keys()))
            logger.info(f"command={_cmd_desc} on {self.__class__.__name__}")
            req_response = await driver_commander.async_request(
                payload=body,
                raise_api_errors=raise_api_errors,
                timeout_info=base_timeout_info(_request_timeout_ms),
            )
            logger.info(f"command={_cmd_desc} on {self.__class__.__name__}")
            return req_response

    def get_database_admin(
        self,
        *,
        token: str | TokenProvider | UnsetType = _UNSET,
        dev_ops_url: str | UnsetType = _UNSET,
        dev_ops_api_version: str | None | UnsetType = _UNSET,
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
            >>> if "new_keyspace" not in my_db_admin.list_keyspaces():
            ...     my_db_admin.create_keyspace("new_keyspace")
            >>> my_db_admin.list_keyspaces()
            ['default_keyspace', 'new_keyspace']
        """

        # lazy importing here to avoid circular dependency
        from astrapy.admin.admin import AstraDBDatabaseAdmin, DataAPIDatabaseAdmin

        arg_api_options = APIOptions(
            token=coerce_possible_token_provider(token),
            dev_ops_api_url_options=DevOpsAPIURLOptions(
                dev_ops_url=dev_ops_url,
                dev_ops_api_version=dev_ops_api_version,
            ),
        )
        api_options = self.api_options.with_override(arg_api_options)

        if api_options.environment in Environment.astra_db_values:
            return AstraDBDatabaseAdmin(
                api_endpoint=self.api_endpoint,
                api_options=api_options,
                spawner_database=self,
            )
        else:
            if not isinstance(dev_ops_url, UnsetType):
                raise ValueError(
                    "Parameter `dev_ops_url` not supported outside of Astra DB."
                )
            if not isinstance(dev_ops_api_version, UnsetType):
                raise ValueError(
                    "Parameter `dev_ops_api_version` not supported outside of Astra DB."
                )
            return DataAPIDatabaseAdmin(
                api_endpoint=self.api_endpoint,
                api_options=api_options,
                spawner_database=self,
            )
