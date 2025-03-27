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
from concurrent.futures import ThreadPoolExecutor
from types import TracebackType
from typing import TYPE_CHECKING, Any, Generic, Iterable, overload

from astrapy.constants import (
    DOC,
    DOC2,
    FilterType,
    HybridSortType,
    ProjectionType,
    ReturnDocument,
    SortType,
    normalize_optional_projection,
)
from astrapy.data.utils.collection_converters import (
    postprocess_collection_response,
    preprocess_collection_payload,
)
from astrapy.data.utils.distinct_extractors import (
    _create_document_key_extractor,
    _hash_collection_document,
    _reduce_distinct_key_to_safe,
)
from astrapy.database import AsyncDatabase, Database
from astrapy.exceptions import (
    CollectionDeleteManyException,
    CollectionInsertManyException,
    CollectionUpdateManyException,
    DataAPIResponseException,
    MultiCallTimeoutManager,
    TooManyDocumentsToCountException,
    UnexpectedDataAPIResponseException,
    _first_valid_timeout,
    _select_singlereq_timeout_ca,
    _select_singlereq_timeout_gm,
    _TimeoutContext,
)
from astrapy.info import CollectionDefinition, CollectionInfo
from astrapy.results import (
    CollectionDeleteResult,
    CollectionInsertManyResult,
    CollectionInsertOneResult,
    CollectionUpdateResult,
)
from astrapy.settings.defaults import (
    DEFAULT_DATA_API_AUTH_HEADER,
    DEFAULT_INSERT_MANY_CHUNK_SIZE,
    DEFAULT_INSERT_MANY_CONCURRENCY,
)
from astrapy.utils.api_commander import APICommander
from astrapy.utils.api_options import APIOptions, FullAPIOptions
from astrapy.utils.meta import beta_method
from astrapy.utils.request_tools import HttpMethod
from astrapy.utils.unset import _UNSET, UnsetType

if TYPE_CHECKING:
    from astrapy.authentication import (
        EmbeddingHeadersProvider,
        RerankingHeadersProvider,
    )
    from astrapy.cursors import (
        AsyncCollectionFindAndRerankCursor,
        AsyncCollectionFindCursor,
        CollectionFindAndRerankCursor,
        CollectionFindCursor,
        RerankedResult,
    )


logger = logging.getLogger(__name__)


def _prepare_update_info(statuses: list[dict[str, Any]]) -> dict[str, Any]:
    reduced_status = {
        "matchedCount": sum(
            status["matchedCount"] for status in statuses if "matchedCount" in status
        ),
        "modifiedCount": sum(
            status["modifiedCount"] for status in statuses if "modifiedCount" in status
        ),
        "upsertedId": [
            status["upsertedId"] for status in statuses if "upsertedId" in status
        ],
    }
    if reduced_status["upsertedId"]:
        if len(reduced_status["upsertedId"]) == 1:
            ups_dict = {"upserted": reduced_status["upsertedId"][0]}
        else:
            ups_dict = {"upserteds": reduced_status["upsertedId"]}
    else:
        ups_dict = {}
    return {
        **{
            "n": reduced_status["matchedCount"] + len(reduced_status["upsertedId"]),
            "updatedExisting": reduced_status["modifiedCount"] > 0,
            "ok": 1.0,
            "nModified": reduced_status["modifiedCount"],
        },
        **ups_dict,
    }


def _is_vector_sort(sort: SortType | None) -> bool:
    if sort is None:
        return False
    else:
        return "$vector" in sort or "$vectorize" in sort


class Collection(Generic[DOC]):
    """
    A Data API collection, the object to interact with the Data API for unstructured
    (schemaless) data, especially for DDL operations.
    This class has a synchronous interface.

    This class is not meant for direct instantiation by the user, rather
    it is obtained by invoking methods such as `get_collection` of Database,
    wherefrom the Collection inherits its API options such as authentication
    token and API endpoint.

    Args:
        database: a Database object, instantiated earlier. This represents
            the database the collection belongs to.
        name: the collection name. This parameter should match an existing
            collection on the database.
        keyspace: this is the keyspace to which the collection belongs.
            If nothing is specified, the database's working keyspace is used.
        api_options: a complete specification of the API Options for this instance.

    Examples:
        >>> from astrapy import DataAPIClient
        >>> client = DataAPIClient()
        >>> database = client.get_database(
        ...     "https://01234567-....apps.astra.datastax.com",
        ...     token="AstraCS:..."
        ... )

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

        >>> # Get a reference to an existing collection
        >>> # (no checks are performed on DB)
        >>> my_collection_3a = database.get_collection("my_events")
        >>> my_collection_3b = database.my_events
        >>> my_collection_3c = database["my_events"]

    Note:
        creating an instance of Collection does not trigger actual creation
        of the collection on the database. The latter should have been created
        beforehand, e.g. through the `create_collection` method of a Database.
    """

    def __init__(
        self,
        *,
        database: Database,
        name: str,
        keyspace: str | None,
        api_options: FullAPIOptions,
    ) -> None:
        self.api_options = api_options
        self._name = name
        _keyspace = keyspace if keyspace is not None else database.keyspace

        if _keyspace is None:
            raise ValueError("Attempted to create Collection with 'keyspace' unset.")

        self._database = database._copy(
            keyspace=_keyspace, api_options=self.api_options
        )
        self._commander_headers = {
            **{DEFAULT_DATA_API_AUTH_HEADER: self.api_options.token.get_token()},
            **self.api_options.embedding_api_key.get_headers(),
            **self.api_options.reranking_api_key.get_headers(),
            **self.api_options.database_additional_headers,
        }
        self._api_commander = self._get_api_commander()

    def __repr__(self) -> str:
        _db_desc = f'database.api_endpoint="{self.database.api_endpoint}"'
        return (
            f'{self.__class__.__name__}(name="{self.name}", '
            f'keyspace="{self.keyspace}", {_db_desc}, '
            f"api_options={self.api_options})"
        )

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Collection):
            return all(
                [
                    self._name == other._name,
                    self._database == other._database,
                    self.api_options == other.api_options,
                ]
            )
        else:
            return False

    def __call__(self, *pargs: Any, **kwargs: Any) -> None:
        raise TypeError(
            f"'{self.__class__.__name__}' object is not callable. If you "
            f"meant to call the '{self.name}' method on a "
            f"'{self.database.__class__.__name__}' object "
            "it is failing because no such method exists."
        )

    def _get_api_commander(self) -> APICommander:
        """Instantiate a new APICommander based on the properties of this class."""

        if self._database.keyspace is None:
            raise ValueError(
                "No keyspace specified. Collection requires a keyspace to "
                "be set, e.g. through the `keyspace` constructor parameter."
            )

        base_path_components = [
            comp
            for comp in (
                ncomp.strip("/")
                for ncomp in (
                    self._database.api_options.data_api_url_options.api_path,
                    self._database.api_options.data_api_url_options.api_version,
                    self._database.keyspace,
                    self._name,
                )
                if ncomp is not None
            )
            if comp != ""
        ]
        base_path = f"/{'/'.join(base_path_components)}"
        api_commander = APICommander(
            api_endpoint=self._database.api_endpoint,
            path=base_path,
            headers=self._commander_headers,
            callers=self.api_options.callers,
            redacted_header_names=self.api_options.redacted_header_names,
            handle_decimals_writes=(
                self.api_options.serdes_options.use_decimals_in_collections
            ),
            handle_decimals_reads=(
                self.api_options.serdes_options.use_decimals_in_collections
            ),
        )
        return api_commander

    def _converted_request(
        self,
        *,
        http_method: str = HttpMethod.POST,
        payload: dict[str, Any] | None = None,
        additional_path: str | None = None,
        request_params: dict[str, Any] = {},
        raise_api_errors: bool = True,
        timeout_context: _TimeoutContext,
    ) -> dict[str, Any]:
        converted_payload = preprocess_collection_payload(
            payload, options=self.api_options.serdes_options
        )
        raw_response_json = self._api_commander.request(
            http_method=http_method,
            payload=converted_payload,
            additional_path=additional_path,
            request_params=request_params,
            raise_api_errors=raise_api_errors,
            timeout_context=timeout_context,
        )
        response_json = postprocess_collection_response(
            raw_response_json, options=self.api_options.serdes_options
        )
        return response_json

    def _copy(
        self: Collection[DOC],
        *,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> Collection[DOC]:
        arg_api_options = APIOptions(
            embedding_api_key=embedding_api_key,
            reranking_api_key=reranking_api_key,
        )
        final_api_options = self.api_options.with_override(api_options).with_override(
            arg_api_options
        )
        return Collection(
            database=self.database,
            name=self.name,
            keyspace=self.keyspace,
            api_options=final_api_options,
        )

    def with_options(
        self: Collection[DOC],
        *,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> Collection[DOC]:
        """
        Create a clone of this collection with some changed attributes.

        Args:
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
            api_options: any additional options to set for the clone, in the form of
                an APIOptions instance (where one can set just the needed attributes).
                In case the same setting is also provided as named parameter,
                the latter takes precedence.

        Returns:
            a new Collection instance.

        Example:
            >>> collection_with_api_key_configured = my_collection.with_options(
            ...     embedding_api_key="secret-key-0123abcd...",
            ... )
        """

        return self._copy(
            embedding_api_key=embedding_api_key,
            reranking_api_key=reranking_api_key,
            api_options=api_options,
        )

    def to_async(
        self: Collection[DOC],
        *,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncCollection[DOC]:
        """
        Create an AsyncCollection from this one. Save for the arguments
        explicitly provided as overrides, everything else is kept identical
        to this collection in the copy (the database is converted into
        an async object).

        Args:
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
            api_options: any additional options to set for the result, in the form of
                an APIOptions instance (where one can set just the needed attributes).
                In case the same setting is also provided as named parameter,
                the latter takes precedence.

        Returns:
            the new copy, an AsyncCollection instance.

        Example:
            >>> asyncio.run(my_coll.to_async().count_documents({},upper_bound=100))
            77
        """

        arg_api_options = APIOptions(
            embedding_api_key=embedding_api_key,
            reranking_api_key=reranking_api_key,
        )
        final_api_options = self.api_options.with_override(api_options).with_override(
            arg_api_options
        )
        return AsyncCollection(
            database=self.database.to_async(),
            name=self.name,
            keyspace=self.keyspace,
            api_options=final_api_options,
        )

    def options(
        self,
        *,
        collection_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> CollectionDefinition:
        """
        Get the collection options, i.e. its configuration as read from the database.

        The method issues a request to the Data API each time is invoked,
        without caching mechanisms: this ensures up-to-date information
        for usages such as real-time collection validation by the application.

        Args:
            collection_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `collection_admin_timeout_ms`.
            timeout_ms: an alias for `collection_admin_timeout_ms`.

        Returns:
            a CollectionDefinition instance describing the collection.
            (See also the database `list_collections` method.)

        Example:
            >>> my_coll.options()
            CollectionDefinition(vector=CollectionVectorOptions(dimension=3, metric='cosine'))
        """

        _collection_admin_timeout_ms, _ca_label = _select_singlereq_timeout_ca(
            timeout_options=self.api_options.timeout_options,
            collection_admin_timeout_ms=collection_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info(f"getting collections in search of '{self.name}'")
        self_descriptors = [
            coll_desc
            for coll_desc in self.database._list_collections_ctx(
                keyspace=None,
                timeout_context=_TimeoutContext(
                    request_ms=_collection_admin_timeout_ms,
                    label=_ca_label,
                ),
            )
            if coll_desc.name == self.name
        ]
        logger.info(f"finished getting collections in search of '{self.name}'")
        if self_descriptors:
            return self_descriptors[0].definition
        else:
            raise RuntimeError(
                f"Collection {self.keyspace}.{self.name} not found.",
            )

    def info(
        self,
        *,
        database_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> CollectionInfo:
        """
        Information on the collection (name, location, database), in the
        form of a CollectionInfo object.

        Not to be confused with the collection `options` method (related
        to the collection internal configuration).

        Args:
            database_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying DevOps API request.
                If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `database_admin_timeout_ms`.
            timeout_ms: an alias for `database_admin_timeout_ms`.

        Example:
            >>> my_coll.info().database_info.region
            'eu-west-1'
            >>> my_coll.info().full_name
            'default_keyspace.my_v_collection'

        Note:
            the returned CollectionInfo wraps, among other things,
            the database information: as such, calling this method
            triggers the same-named method of a Database object (which, in turn,
            performs a HTTP request to the DevOps API).
            See the documentation for `Database.info()` for more details.
        """

        return CollectionInfo(
            database_info=self.database.info(
                database_admin_timeout_ms=database_admin_timeout_ms,
                request_timeout_ms=request_timeout_ms,
                timeout_ms=timeout_ms,
            ),
            keyspace=self.keyspace,
            name=self.name,
            full_name=self.full_name,
        )

    @property
    def database(self) -> Database:
        """
        a Database object, the database this collection belongs to.

        Example:
            >>> my_coll.database.name
            'the_application_database'
        """

        return self._database

    @property
    def keyspace(self) -> str:
        """
        The keyspace this collection is in.

        Example:
            >>> my_coll.keyspace
            'default_keyspace'
        """

        _keyspace = self.database.keyspace
        if _keyspace is None:
            raise RuntimeError("The collection's DB is set with keyspace=None")
        return _keyspace

    @property
    def name(self) -> str:
        """
        The name of this collection.

        Example:
            >>> my_coll.name
            'my_v_collection'
        """

        return self._name

    @property
    def full_name(self) -> str:
        """
        The fully-qualified collection name within the database,
        in the form "keyspace.collection_name".

        Example:
            >>> my_coll.full_name
            'default_keyspace.my_v_collection'
        """

        return f"{self.keyspace}.{self.name}"

    def insert_one(
        self,
        document: DOC,
        *,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> CollectionInsertOneResult:
        """
        Insert a single document in the collection in an atomic operation.

        Args:
            document: the dictionary expressing the document to insert.
                The `_id` field of the document can be left out, in which
                case it will be created automatically.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a CollectionInsertOneResult object.

        Examples:
            >>> my_coll.count_documents({}, upper_bound=10)
            0
            >>> my_coll.insert_one(
            ...     {
            ...         "age": 30,
            ...         "name": "Smith",
            ...         "food": ["pear", "peach"],
            ...         "likes_fruit": True,
            ...     },
            ... )
            CollectionInsertOneResult(raw_results=..., inserted_id='ed4587a4-...-...-...')
            >>> my_coll.insert_one({"_id": "user-123", "age": 50, "name": "Maccio"})
            CollectionInsertOneResult(raw_results=..., inserted_id='user-123')
            >>> my_coll.count_documents({}, upper_bound=10)
            2

            >>> my_coll.insert_one({"tag": "v", "$vector": [10, 11]})
            CollectionInsertOneResult(...)

        Note:
            If an `_id` is explicitly provided, which corresponds to a document
            that exists already in the collection, an error is raised and
            the insertion fails.
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        io_payload = {"insertOne": {"document": document}}
        logger.info(f"insertOne on '{self.name}'")
        io_response = self._converted_request(
            payload=io_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        logger.info(f"finished insertOne on '{self.name}'")
        if "insertedIds" in io_response.get("status", {}):
            if io_response["status"]["insertedIds"]:
                inserted_id = io_response["status"]["insertedIds"][0]
                return CollectionInsertOneResult(
                    raw_results=[io_response],
                    inserted_id=inserted_id,
                )
            else:
                raise UnexpectedDataAPIResponseException(
                    text="Faulty response from insert_one API command.",
                    raw_response=io_response,
                )
        else:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from insert_one API command.",
                raw_response=io_response,
            )

    def insert_many(
        self,
        documents: Iterable[DOC],
        *,
        ordered: bool = False,
        chunk_size: int | None = None,
        concurrency: int | None = None,
        request_timeout_ms: int | None = None,
        general_method_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> CollectionInsertManyResult:
        """
        Insert a list of documents into the collection.
        This is not an atomic operation.

        Args:
            documents: an iterable of dictionaries, each a document to insert.
                Documents may specify their `_id` field or leave it out, in which
                case it will be added automatically.
            ordered: if False (default), the insertions can occur in arbitrary order
                and possibly concurrently. If True, they are processed sequentially.
                If there are no specific reasons against it, unordered insertions are to
                be preferred as they complete much faster.
            chunk_size: how many documents to include in a single API request.
                Exceeding the server maximum allowed value results in an error.
                Leave it unspecified (recommended) to use the system default.
            concurrency: maximum number of concurrent requests to the API at
                a given time. It cannot be more than one for ordered insertions.
            general_method_timeout_ms: a timeout, in milliseconds, for the whole
                requested operation (which may involve multiple API requests).
                If not passed, the collection-level setting is used instead.
            request_timeout_ms: a timeout, in milliseconds, for each API request.
                If not passed, the collection-level setting is used instead.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a CollectionInsertManyResult object.

        Examples:
            >>> my_coll.count_documents({}, upper_bound=10)
            0
            >>> my_coll.insert_many(
            ...     [{"a": 10}, {"a": 5}, {"b": [True, False, False]}],
            ...     ordered=True,
            ... )
            CollectionInsertManyResult(raw_results=..., inserted_ids=['184bb06f-...', '...', '...'])
            >>> my_coll.count_documents({}, upper_bound=100)
            3
            >>> my_coll.insert_many(
            ...     [{"seq": i} for i in range(50)],
            ...     concurrency=5,
            ... )
            CollectionInsertManyResult(raw_results=..., inserted_ids=[... ...])
            >>> my_coll.count_documents({}, upper_bound=100)
            53
            >>> my_coll.insert_many(
            ...     [
            ...         {"tag": "a", "$vector": [1, 2]},
            ...         {"tag": "b", "$vector": [3, 4]},
            ...     ]
            ... )
            CollectionInsertManyResult(...)

        Note:
            Unordered insertions are executed with some degree of concurrency,
            so it is usually better to prefer this mode unless the order in the
            document sequence is important.

        Note:
            A failure mode for this command is related to certain faulty documents
            found among those to insert: for example, a document may have an ID
            already found on the collection, or its vector dimension may not
            match the collection setting.

            For an ordered insertion, the method will raise an exception at
            the first such faulty document -- nevertheless, all documents processed
            until then will end up being written to the database.

            For unordered insertions, if the error stems from faulty documents
            the insertion proceeds until exhausting the input documents: then,
            an exception is raised -- and all insertable documents will have been
            written to the database, including those "after" the troublesome ones.

            Errors occurring during an insert_many operation, for that reason,
            may result in a `CollectionInsertManyException` being raised.
            This exception allows to inspect the list of document IDs that were
            successfully inserted, while accessing at the same time the underlying
            "root errors" that made the full method call to fail.
        """

        _general_method_timeout_ms, _gmt_label = _first_valid_timeout(
            (general_method_timeout_ms, "general_method_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (
                self.api_options.timeout_options.general_method_timeout_ms,
                "general_method_timeout_ms",
            ),
        )
        _request_timeout_ms, _rt_label = _first_valid_timeout(
            (request_timeout_ms, "request_timeout_ms"),
            (self.api_options.timeout_options.request_timeout_ms, "request_timeout_ms"),
        )
        if concurrency is None:
            if ordered:
                _concurrency = 1
            else:
                _concurrency = DEFAULT_INSERT_MANY_CONCURRENCY
        else:
            _concurrency = concurrency
        if _concurrency > 1 and ordered:
            raise ValueError("Cannot run ordered insert_many concurrently.")
        if chunk_size is None:
            _chunk_size = DEFAULT_INSERT_MANY_CHUNK_SIZE
        else:
            _chunk_size = chunk_size
        _documents = list(documents)
        logger.info(f"inserting {len(_documents)} documents in '{self.name}'")
        raw_results: list[dict[str, Any]] = []
        im_payloads: list[dict[str, Any]] = []
        timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=_general_method_timeout_ms,
            timeout_label=_gmt_label,
        )
        if ordered:
            options = {"ordered": True, "returnDocumentResponses": True}
            inserted_ids: list[Any] = []
            for i in range(0, len(_documents), _chunk_size):
                im_payload = {
                    "insertMany": {
                        "documents": _documents[i : i + _chunk_size],
                        "options": options,
                    },
                }
                logger.info(f"insertMany(chunk) on '{self.name}'")
                chunk_response = self._converted_request(
                    payload=im_payload,
                    raise_api_errors=False,
                    timeout_context=timeout_manager.remaining_timeout(
                        cap_time_ms=_request_timeout_ms,
                        cap_timeout_label=_rt_label,
                    ),
                )
                logger.info(f"finished insertMany(chunk) on '{self.name}'")
                # accumulate the results in this call
                chunk_inserted_ids = [
                    doc_resp["_id"]
                    for doc_resp in (chunk_response.get("status") or {}).get(
                        "documentResponses", []
                    )
                    if doc_resp["status"] == "OK"
                ]
                inserted_ids += chunk_inserted_ids
                raw_results += [chunk_response]
                im_payloads += [im_payload]
                # if errors, quit early
                if chunk_response.get("errors", []):
                    response_exception = DataAPIResponseException.from_response(
                        command=im_payload,
                        raw_response=chunk_response,
                    )
                    raise CollectionInsertManyException(
                        inserted_ids=inserted_ids, exceptions=[response_exception]
                    )

            # return
            full_result = CollectionInsertManyResult(
                raw_results=raw_results,
                inserted_ids=inserted_ids,
            )
            logger.info(
                f"finished inserting {len(_documents)} documents in '{self.name}'"
            )
            return full_result

        else:
            # unordered: concurrent or not, do all of them and parse the results
            options = {"ordered": False, "returnDocumentResponses": True}
            if _concurrency > 1:
                with ThreadPoolExecutor(max_workers=_concurrency) as executor:

                    def _chunk_insertor(
                        document_chunk: list[dict[str, Any]],
                    ) -> tuple[dict[str, Any], dict[str, Any]]:
                        im_payload = {
                            "insertMany": {
                                "documents": document_chunk,
                                "options": options,
                            },
                        }
                        logger.info(f"insertMany(chunk) on '{self.name}'")
                        im_response = self._converted_request(
                            payload=im_payload,
                            raise_api_errors=False,
                            timeout_context=timeout_manager.remaining_timeout(
                                cap_time_ms=_request_timeout_ms,
                                cap_timeout_label=_rt_label,
                            ),
                        )
                        logger.info(f"finished insertMany(chunk) on '{self.name}'")
                        return im_payload, im_response

                    raw_pl_results_pairs = list(
                        executor.map(
                            _chunk_insertor,
                            (
                                _documents[i : i + _chunk_size]
                                for i in range(0, len(_documents), _chunk_size)
                            ),
                        )
                    )
                    if raw_pl_results_pairs:
                        im_payloads, raw_results = list(zip(*raw_pl_results_pairs))
                    else:
                        im_payloads, raw_results = [], []

            else:
                for i in range(0, len(_documents), _chunk_size):
                    im_payload = {
                        "insertMany": {
                            "documents": _documents[i : i + _chunk_size],
                            "options": options,
                        },
                    }
                    logger.info(f"insertMany(chunk) on '{self.name}'")
                    im_response = self._converted_request(
                        payload=im_payload,
                        raise_api_errors=False,
                        timeout_context=timeout_manager.remaining_timeout(
                            cap_time_ms=_request_timeout_ms,
                            cap_timeout_label=_rt_label,
                        ),
                    )
                    logger.info(f"finished insertMany(chunk) on '{self.name}'")
                    raw_results.append(im_response)
                    im_payloads.append(im_payload)
            # recast raw_results
            inserted_ids = [
                doc_resp["_id"]
                for chunk_response in raw_results
                for doc_resp in (chunk_response.get("status") or {}).get(
                    "documentResponses", []
                )
                if doc_resp["status"] == "OK"
            ]

            # check-raise
            response_exceptions = [
                DataAPIResponseException.from_response(
                    command=chunk_payload,
                    raw_response=chunk_response,
                )
                for chunk_payload, chunk_response in zip(im_payloads, raw_results)
                if chunk_response.get("errors", [])
            ]
            if response_exceptions:
                raise CollectionInsertManyException(
                    inserted_ids=inserted_ids,
                    exceptions=response_exceptions,
                )

            # return
            full_result = CollectionInsertManyResult(
                raw_results=raw_results,
                inserted_ids=inserted_ids,
            )
            logger.info(
                f"finished inserting {len(_documents)} documents in '{self.name}'"
            )
            return full_result

    @overload
    def find(
        self,
        filter: FilterType | None = None,
        *,
        projection: ProjectionType | None = None,
        document_type: None = None,
        skip: int | None = None,
        limit: int | None = None,
        include_similarity: bool | None = None,
        include_sort_vector: bool | None = None,
        sort: SortType | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> CollectionFindCursor[DOC, DOC]: ...

    @overload
    def find(
        self,
        filter: FilterType | None = None,
        *,
        projection: ProjectionType | None = None,
        document_type: type[DOC2],
        skip: int | None = None,
        limit: int | None = None,
        include_similarity: bool | None = None,
        include_sort_vector: bool | None = None,
        sort: SortType | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> CollectionFindCursor[DOC, DOC2]: ...

    def find(
        self,
        filter: FilterType | None = None,
        *,
        projection: ProjectionType | None = None,
        document_type: type[DOC2] | None = None,
        skip: int | None = None,
        limit: int | None = None,
        include_similarity: bool | None = None,
        include_sort_vector: bool | None = None,
        sort: SortType | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> CollectionFindCursor[DOC, DOC2]:
        """
        Find documents on the collection, matching a certain provided filter.

        The method returns a cursor that can then be iterated over. Depending
        on the method call pattern, the iteration over all documents can reflect
        collection mutations occurred since the `find` method was called, or not.
        In cases where the cursor reflects mutations in real-time, it will iterate
        over cursors in an approximate way (i.e. exhibiting occasional skipped
        or duplicate documents). This happens when making use of the `sort`
        option in a non-vector-search manner.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
            projection: it controls which parts of the document are returned.
                It can be an allow-list: `{"f1": True, "f2": True}`,
                or a deny-list: `{"fx": False, "fy": False}`, but not a mixture
                (except for the `_id` and other special fields, which can be
                associated to both True or False independently of the rest
                of the specification).
                The special star-projections `{"*": True}` and `{"*": False}`
                have the effect of returning the whole document and `{}` respectively.
                For lists in documents, slice directives can be passed to select
                portions of the list: for instance, `{"array": {"$slice": 2}}`,
                `{"array": {"$slice": -2}}`, `{"array": {"$slice": [4, 2]}}` or
                `{"array": {"$slice": [-4, 2]}}`.
                An iterable over strings will be treated implicitly as an allow-list.
                The default projection (used if this parameter is not passed) does not
                necessarily include "special" fields such as `$vector` or `$vectorize`.
                See the Data API documentation for more on projections.
            document_type: this parameter acts a formal specifier for the type checker.
                If omitted, the resulting cursor is implicitly a
                `CollectionFindCursor[DOC, DOC]`, i.e. maintains the same type for
                the items it returns as that for the documents in the collection.
                Strictly typed code may want to specify this parameter especially when
                a projection is given.
            skip: with this integer parameter, what would be the first `skip`
                documents returned by the query are discarded, and the results
                start from the (skip+1)-th document.
                This parameter can be used only in conjunction with an explicit
                `sort` criterion of the ascending/descending type (i.e. it cannot
                be used when not sorting, nor with vector-based ANN search).
            limit: this (integer) parameter sets a limit over how many documents
                are returned. Once `limit` is reached (or the cursor is exhausted
                for lack of matching documents), nothing more is returned.
            include_similarity: a boolean to request the numeric value of the
                similarity to be returned as an added "$similarity" key in each
                returned document. It can be used meaningfully only in a vector
                search (see `sort`).
            include_sort_vector: a boolean to request the search query vector.
                If set to True (and if the invocation is a vector search), calling
                the `get_sort_vector` method on the returned cursor will yield
                the vector used for the ANN search.
            sort: with this dictionary parameter one can control the order
                the documents are returned. See the Note about sorting, as well as
                the one about upper bounds, for details.
                Vector-based ANN sorting is achieved by providing a "$vector"
                or a "$vectorize" key in `sort`.
            request_timeout_ms: a timeout, in milliseconds, for each single one
                of the underlying HTTP requests used to fetch documents as the
                cursor is iterated over.
                If not passed, the collection-level setting is used instead.
            timeout_ms: an alias for `request_timeout_ms`.

        Returns:
            a CollectionFindCursor object, that can be iterated over (and manipulated
            in several ways). The cursor, if needed, handles pagination under the hood
            as the documents are consumed.

        Examples:
            >>> filter = {"seq": {"$exists": True}}
            >>> for doc in my_coll.find(filter, projection={"seq": True}, limit=5):
            ...     print(doc["seq"])
            ...
            37
            35
            10
            36
            27
            >>> cursor1 = my_coll.find(
            ...     {},
            ...     limit=4,
            ...     sort={"seq": astrapy.constants.SortMode.DESCENDING},
            ... )
            >>> [doc["_id"] for doc in cursor1]
            ['97e85f81-...', '1581efe4-...', '...', '...']
            >>> cursor2 = my_coll.find({}, limit=3)

            >>> my_coll.insert_many([
            ...     {"tag": "A", "$vector": [4, 5]},
            ...     {"tag": "B", "$vector": [3, 4]},
            ...     {"tag": "C", "$vector": [3, 2]},
            ...     {"tag": "D", "$vector": [4, 1]},
            ...     {"tag": "E", "$vector": [2, 5]},
            ... ])
            >>> ann_tags = [
            ...     document["tag"]
            ...     for document in my_coll.find(
            ...         {},
            ...         sort={"$vector": [3, 3]},
            ...         limit=3,
            ...     )
            ... ]
            >>> ann_tags
            ['A', 'B', 'C']
            >>> # (assuming the collection has metric VectorMetric.COSINE)

            >>> cursor = my_coll.find(
            ...     sort={"$vector": [3, 3]},
            ...     limit=3,
            ...     include_sort_vector=True,
            ... )
            >>> cursor.get_sort_vector()
            [3.0, 3.0]
            >>> matches = cursor.to_list()
            >>> cursor.get_sort_vector()
            [3.0, 3.0]

        Note:
            The following are example values for the `sort` parameter.
            When no particular order is required:
                sort={}  # (default when parameter not provided)
            When sorting by a certain value in ascending/descending order:
                sort={"field": SortMode.ASCENDING}
                sort={"field": SortMode.DESCENDING}
            When sorting first by "field" and then by "subfield"
            (while modern Python versions preserve the order of dictionaries,
            it is suggested for clarity to employ a `collections.OrderedDict`
            in these cases):
                sort={
                    "field": SortMode.ASCENDING,
                    "subfield": SortMode.ASCENDING,
                }
            When running a vector similarity (ANN) search:
                sort={"$vector": [0.4, 0.15, -0.5]}

        Note:
            Some combinations of arguments impose an implicit upper bound on the
            number of documents that are returned by the Data API. More specifically:
            (a) Vector ANN searches cannot return more than a number of documents
            that at the time of writing is set to 1000 items.
            (b) When using a sort criterion of the ascending/descending type,
            the Data API will return a smaller number of documents, set to 20
            at the time of writing, and stop there. The returned documents are
            the top results across the whole collection according to the requested
            criterion.

        Note:
            When not specifying sorting criteria at all (by vector or otherwise),
            the cursor can scroll through an arbitrary number of documents as
            the Data API and the client periodically exchange new chunks of documents.
            It should be noted that the behavior of the cursor in the case documents
            have been added/removed after the `find` was started depends on database
            internals and it is not guaranteed, nor excluded, that such "real-time"
            changes in the data would be picked up by the cursor.
        """

        # lazy-import here to avoid circular import issues
        from astrapy.cursors import CollectionFindCursor

        _request_timeout_ms, _rt_label = _first_valid_timeout(
            (request_timeout_ms, "request_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (self.api_options.timeout_options.request_timeout_ms, "request_timeout_ms"),
        )
        return (
            CollectionFindCursor(
                collection=self,
                request_timeout_ms=_request_timeout_ms,
                overall_timeout_ms=None,
                request_timeout_label=_rt_label,
            )
            .filter(filter)
            .project(projection)
            .skip(skip)
            .limit(limit)
            .sort(sort)
            .include_similarity(include_similarity)
            .include_sort_vector(include_sort_vector)
        )

    def find_one(
        self,
        filter: FilterType | None = None,
        *,
        projection: ProjectionType | None = None,
        include_similarity: bool | None = None,
        sort: SortType | None = None,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> DOC | None:
        """
        Run a search, returning the first document in the collection that matches
        provided filters, if any is found.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
            projection: it controls which parts of the document are returned.
                It can be an allow-list: `{"f1": True, "f2": True}`,
                or a deny-list: `{"fx": False, "fy": False}`, but not a mixture
                (except for the `_id` and other special fields, which can be
                associated to both True or False independently of the rest
                of the specification).
                The special star-projections `{"*": True}` and `{"*": False}`
                have the effect of returning the whole document and `{}` respectively.
                For lists in documents, slice directives can be passed to select
                portions of the list: for instance, `{"array": {"$slice": 2}}`,
                `{"array": {"$slice": -2}}`, `{"array": {"$slice": [4, 2]}}` or
                `{"array": {"$slice": [-4, 2]}}`.
                An iterable over strings will be treated implicitly as an allow-list.
                The default projection (used if this parameter is not passed) does not
                necessarily include "special" fields such as `$vector` or `$vectorize`.
                See the Data API documentation for more on projections.
            include_similarity: a boolean to request the numeric value of the
                similarity to be returned as an added "$similarity" key in the
                returned document. It can be used meaningfully only in a vector
                search (see `sort`).
            sort: with this dictionary parameter one can control the order
                the documents are returned. See the Note about sorting for details.
                Vector-based ANN sorting is achieved by providing a "$vector"
                or a "$vectorize" key in `sort`.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a dictionary expressing the required document, otherwise None.

        Examples:
            >>> my_coll.find_one({})
            {'_id': '68d1e515-...', 'seq': 37}
            >>> my_coll.find_one({"seq": 10})
            {'_id': 'd560e217-...', 'seq': 10}
            >>> my_coll.find_one({"seq": 1011})
            >>> # (returns None for no matches)
            >>> my_coll.find_one({}, projection={"seq": False})
            {'_id': '68d1e515-...'}
            >>> my_coll.find_one(
            ...     {},
            ...     sort={"seq": astrapy.constants.SortMode.DESCENDING},
            ... )
            {'_id': '97e85f81-...', 'seq': 69}
            >>> my_coll.find_one({}, sort={"$vector": [1, 0]}, projection={"*": True})
            {'_id': '...', 'tag': 'D', '$vector': [4.0, 1.0]}

        Note:
            See the `find` method for more details on the accepted parameters
            (whereas `skip` and `limit` are not valid parameters for `find_one`).
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        fo_options = (
            None
            if include_similarity is None
            else {"includeSimilarity": include_similarity}
        )
        fo_payload = {
            "findOne": {
                k: v
                for k, v in {
                    "filter": filter,
                    "projection": normalize_optional_projection(projection),
                    "options": fo_options,
                    "sort": sort,
                }.items()
                if v is not None
            }
        }
        fo_response = self._converted_request(
            payload=fo_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        if "document" not in (fo_response.get("data") or {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from findOne API command.",
                raw_response=fo_response,
            )
        doc_response = fo_response["data"]["document"]
        if doc_response is None:
            return None
        return fo_response["data"]["document"]  # type: ignore[no-any-return]

    def distinct(
        self,
        key: str | Iterable[str | int],
        *,
        filter: FilterType | None = None,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[Any]:
        """
        Return a list of the unique values of `key` across the documents
        in the collection that match the provided filter.

        Args:
            key: the name of the field whose value is inspected across documents.
                Keys can be just field names (as is often the case), but
                the dot-notation is also accepted to mean subkeys or indices
                within lists (for example, "map_field.subkey" or "list_field.2").
                If a field has literal dots or ampersands in its name, this
                parameter must be escaped to be treated properly.
                The key can also be a list of strings and numbers, in which case
                no escape is necessary: each item in the list is a field name/index,
                for example ["map_field", "subkey"] or ["list_field", 2].
                If lists are encountered and no numeric index is specified,
                all items in the list are visited.
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
            general_method_timeout_ms: a timeout, in milliseconds, for the whole
                requested operation (which may involve multiple API requests).
                This method, being based on `find` (see) may entail successive HTTP API
                requests, depending on the amount of involved documents.
            request_timeout_ms: a timeout, in milliseconds, for each API request.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a list of all different values for `key` found across the documents
            that match the filter. The result list has no repeated items.

        Example:
            >>> my_coll.insert_many(
            ...     [
            ...         {"name": "Marco", "food": ["apple", "orange"], "city": "Helsinki"},
            ...         {"name": "Emma", "food": {"likes_fruit": True, "allergies": []}},
            ...     ]
            ... )
            CollectionInsertManyResult(raw_results=..., inserted_ids=['c5b99f37-...', 'd6416321-...'])
            >>> my_coll.distinct("name")
            ['Marco', 'Emma']
            >>> my_coll.distinct("city")
            ['Helsinki']
            >>> my_coll.distinct("food")
            ['apple', 'orange', {'likes_fruit': True, 'allergies': []}]
            >>> my_coll.distinct("food.1")
            ['orange']
            >>> my_coll.distinct("food.allergies")
            []
            >>> my_coll.distinct("food.likes_fruit")
            [True]

        Note:
            It must be kept in mind that `distinct` is a client-side operation,
            which effectively browses all required documents using the logic
            of the `find` method and collects the unique values found for `key`.
            As such, there may be performance, latency and ultimately
            billing implications if the amount of matching documents is large.

        Note:
            For details on the behaviour of "distinct" in conjunction with
            real-time changes in the collection contents, see the
            Note of the `find` command.
        """

        # lazy-import here to avoid circular import issues
        from astrapy.cursors import CollectionFindCursor

        _general_method_timeout_ms, _gmt_label = _first_valid_timeout(
            (general_method_timeout_ms, "general_method_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (
                self.api_options.timeout_options.general_method_timeout_ms,
                "general_method_timeout_ms",
            ),
        )
        _request_timeout_ms, _rt_label = _first_valid_timeout(
            (request_timeout_ms, "request_timeout_ms"),
            (self.api_options.timeout_options.request_timeout_ms, "request_timeout_ms"),
        )
        # preparing cursor:
        _extractor = _create_document_key_extractor(key)
        _key = _reduce_distinct_key_to_safe(key)
        # relaxing the type hint (limited to within this method body)
        f_cursor: CollectionFindCursor[dict[str, Any], dict[str, Any]] = (
            CollectionFindCursor(
                collection=self,
                request_timeout_ms=_request_timeout_ms,
                overall_timeout_ms=_general_method_timeout_ms,
                request_timeout_label=_rt_label,
                overall_timeout_label=_gmt_label,
            )  # type: ignore[assignment]
            .filter(filter)
            .project({_key: True})
        )
        # consuming it:
        _item_hashes = set()
        distinct_items: list[Any] = []
        logger.info(f"running distinct() on '{self.name}'")
        for document in f_cursor:
            for item in _extractor(document):
                _item_hash = _hash_collection_document(
                    item, options=self.api_options.serdes_options
                )
                if _item_hash not in _item_hashes:
                    _item_hashes.add(_item_hash)
                    distinct_items.append(item)
        logger.info(f"finished running distinct() on '{self.name}'")
        return distinct_items

    @overload
    def find_and_rerank(
        self,
        filter: FilterType | None = None,
        *,
        sort: HybridSortType,
        projection: ProjectionType | None = None,
        document_type: None = None,
        limit: int | None = None,
        hybrid_limits: int | dict[str, int] | None = None,
        include_scores: bool | None = None,
        include_sort_vector: bool | None = None,
        rerank_on: str | None = None,
        rerank_query: str | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> CollectionFindAndRerankCursor[DOC, RerankedResult[DOC]]: ...

    @overload
    def find_and_rerank(
        self,
        filter: FilterType | None = None,
        *,
        sort: HybridSortType,
        projection: ProjectionType | None = None,
        document_type: type[DOC2],
        limit: int | None = None,
        hybrid_limits: int | dict[str, int] | None = None,
        include_scores: bool | None = None,
        include_sort_vector: bool | None = None,
        rerank_on: str | None = None,
        rerank_query: str | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> CollectionFindAndRerankCursor[DOC, RerankedResult[DOC2]]: ...

    @beta_method
    def find_and_rerank(
        self,
        filter: FilterType | None = None,
        *,
        sort: HybridSortType,
        projection: ProjectionType | None = None,
        document_type: type[DOC2] | None = None,
        limit: int | None = None,
        hybrid_limits: int | dict[str, int] | None = None,
        include_scores: bool | None = None,
        include_sort_vector: bool | None = None,
        rerank_on: str | None = None,
        rerank_query: str | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> CollectionFindAndRerankCursor[DOC, RerankedResult[DOC2]]:
        """
        Find relevant documents, combining vector and lexical matches through reranking.

        For this method to succeed, the collection must be created with the required
        hybrid capabilities (see the `create_collection` method of the Database class).

        The method returns a cursor that can then be iterated over, which yields
        the resulting documents, generally paired with accompanying information
        such as scores.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
            sort: a clause specifying the criteria for selecting the top matching
                documents. This must provide enough information for both a lexical
                and a vector similarity to be performed (the latter either query text
                or by query vector, depending on the collection configuration).
                Examples are: `sort={"$hybrid": "xyz"}`,
                `sort={"$hybrid": {"$vectorize": "xyz", "$lexical": "abc"}}`,
                `sort={"$hybrid": {"$vector": DataAPIVector(...), "$lexical": "abc"}}`.
                Note this differs from the `sort` parameter for the `find` method.
            projection: it controls which parts of the document are returned.
                It can be an allow-list: `{"f1": True, "f2": True}`,
                or a deny-list: `{"fx": False, "fy": False}`, but not a mixture
                (except for the `_id` and other special fields, which can be
                associated to both True or False independently of the rest
                of the specification).
                The special star-projections `{"*": True}` and `{"*": False}`
                have the effect of returning the whole document and `{}` respectively.
                For lists in documents, slice directives can be passed to select
                portions of the list: for instance, `{"array": {"$slice": 2}}`,
                `{"array": {"$slice": -2}}`, `{"array": {"$slice": [4, 2]}}` or
                `{"array": {"$slice": [-4, 2]}}`.
                An iterable over strings will be treated implicitly as an allow-list.
                The default projection (used if this parameter is not passed) does not
                necessarily include "special" fields such as `$vector` or `$vectorize`.
                See the Data API documentation for more on projections.
            document_type: this parameter acts a formal specifier for the type checker.
                If omitted, the resulting cursor is implicitly a
                `CollectionFindAndRerankCursor[DOC, DOC]`, i.e. maintains the same type
                for the items it returns as that for the documents in the collection.
                Strictly typed code may want to specify this parameter especially when
                a projection is given.
            limit: maximum number of documents to return as the result of the final
                rerank step.
            hybrid_limits: this controls the amount of documents that are fetched by
                each of the individual retrieval operations that are combined in the
                rerank step. It can be either a number or a dictionary of strings to
                numbers, the latter case expressing different counts for the different
                retrievals. For example: `hybrid_limits=50`,
                `hybrid_limits={"$vector": 20, "$lexical": 10}`.
            include_scores: a boolean to request the scores to be returned along with
                the resulting documents. If this is set, the scores can be read in the
                the map `scores` attribute of each RerankedResult (the map is
                otherwise empty).
            include_sort_vector: a boolean to request the search query vector
                used for the vector-search part of the find operation.
                If set to True, calling the `get_sort_vector` method on the returned
                cursor will yield the vector used for the ANN search.
            rerank_on: for collections without a vectorize (server-side embeddings)
                service, this is used to specify the field name that is then used
                during reranking.
            rerank_query: for collections without a vectorize (server-side embeddings)
                service, this is used to specify the query text for the reranker.
            request_timeout_ms: a timeout, in milliseconds, for each single one
                of the underlying HTTP requests used to fetch documents as the
                cursor is iterated over.
                If not passed, the collection-level setting is used instead.
            timeout_ms: an alias for `request_timeout_ms`.

        Returns:
            a CollectionFindAndRerankCursor object, that can be iterated over (and
            manipulated in several ways).

        Examples:
            >>> # The following examples assume a collection with 'vectorize' and the
            >>> # necessary hybrid configuration; see below for a non-vectorize case.
            >>>
            >>> # Populate with documents
            >>> my_vectorize_coll.insert_many([
            ...     {
            ...         "_id": "A",
            ...         "wkd": "Mon",
            ...         "$vectorize": "Monday is green",
            ...         "$lexical": "Monday is green",
            ...     },
            ...     {
            ...         "_id": "B",
            ...         "wkd": "Tue",
            ...         "$vectorize": "Tuesday is pink",
            ...         "$lexical": "Tuesday is pink",
            ...     },
            ...     {
            ...         "_id": "C",
            ...         "wkd": "Wed",
            ...         "$vectorize": "Wednesday is cyan",
            ...         "$lexical": "Wednesday is cyan",
            ...     },
            ...     {
            ...         "_id": "D",
            ...         "wkd": "Thu",
            ...         "$vectorize": "Thursday is red",
            ...         "$lexical": "Thursday is red",
            ...     },
            ...     {
            ...         "_id": "E",
            ...         "wkd": "Fri",
            ...         "$vectorize": "Friday is orange",
            ...         "$lexical": "Friday is orange",
            ...     },
            ...     {
            ...         "_id": "F",
            ...         "wkd": "Sat",
            ...         "$vectorize": "Saturday is purple",
            ...         "$lexical": "Saturday is purple",
            ...     },
            ...     {
            ...         "_id": "G",
            ...         "wkd": "Sun",
            ...         "$vectorize": "Sunday is beige",
            ...         "$lexical": "Sunday is beige",
            ...     },
            ... ])
            CollectionInsertManyResult(inserted_ids=[A, B, C, D, E ... (7 total)], raw_results=...)
            >>>
            >>> # A simple invocation, consuming the cursor
            >>> # with a loop ('vectorize collection):
            >>> for r_result in my_vectorize_coll.find_and_rerank(
            ...     sort={"$hybrid": "Weekdays?"},
            ...     limit=2,
            ... ):
            ...     print(r_result.document)
            ...
            {'_id': 'C', 'wkd': 'Wed'}
            {'_id': 'A', 'wkd': 'Mon'}
            >>> # Additional arbitrary filtering predicates
            >>> # ('vectorize collection):
            >>> for r_result in my_vectorize_coll.find_and_rerank(
            ...     {"wkd": {"$ne": "Mon"}},
            ...     sort={"$hybrid": "Weekdays?"},
            ...     limit=2,
            ... ):
            ...     print(r_result.document)
            ...
            {'_id': 'C', 'wkd': 'Wed'}
            {'_id': 'B', 'wkd': 'Tue'}
            >>> # Fetch the scores with the documents ('vectorize collection):
            >>> scored_texts = [
            ...     (r_result.document["wkd"], r_result.scores["$rerank"])
            ...     for r_result in my_vectorize_coll.find_and_rerank(
            ...         sort={"$hybrid": "Weekdays?"},
            ...         limit=2,
            ...         include_scores=True,
            ...     )
            ... ]
            >>> print(scored_texts)
            [('Wed', -9.1015625), ('Mon', -10.2421875)]
            >>>
            >>> # Customize sub-search limits ('vectorize collection):
            >>> hits = my_vectorize_coll.find_and_rerank(
            ...     sort={"$hybrid": "Weekdays?"},
            ...     limit=2,
            ...     hybrid_limits=20,
            ... ).to_list()
            >>> print(", ".join(r_res.document["wkd"] for r_res in hits))
            Wed, Mon
            >>>
            >>> # Separate sub-search queries ('vectorize collection):
            >>> cursor = my_vectorize_coll.find_and_rerank(
            ...     sort={
            ...         "$hybrid": {
            ...             "$vectorize": "a week day",
            ...             "$lexical": "green",
            ...         },
            ...     },
            ...     limit=2,
            ...     hybrid_limits={"$lexical": 4, "$vector": 20},
            ... )
            >>> print(", ".join(r_res.document["wkd"] for r_res in cursor))
            Mon, Wed
            >>>
            >>> # Reading back the query vector used by
            >>> # the search ('vectorize collection):
            >>> cursor = my_vectorize_coll.find_and_rerank(
            ...     sort={"$hybrid": "Weekdays?"},
            ...     limit=2,
            ...     include_sort_vector=True
            ... )
            >>> sort_vector = cursor.get_sort_vector()
            >>> print(" ==> ".join(
            ...     r_res.document["wkd"] for r_res in cursor
            ... ))
            Wed ==> Mon
            >>> print(f"Sort vector={sort_vector}")
            Sort vector=[-0.0021172, -0.012057612, 0.010362527 ...]
            >>>
            >>>
            >>> # If the collection has no "vectorize", `rerank_query`
            >>> # and `rerank_on` must be passed. The following assumes a
            >>> # collection with a 3-dimensional vector and the setup for hybrid.
            >>>
            >>> # Populate with documents:
            >>> my_vector3d_coll.insert_many([
            ...     {
            ...         "_id": "A",
            ...         "wkd": "Mon",
            ...         "$vector": [0.1, 0.2, 0.3],
            ...         "$lexical": "Monday is green",
            ...     },
            ...     {
            ...         "_id": "B",
            ...         "wkd": "Tue",
            ...         "$vector": [0.2, 0.3, 0.4],
            ...         "$lexical": "Tuesday is pink",
            ...     },
            ...     {
            ...         "_id": "C",
            ...         "wkd": "Wed",
            ...         "$vector": [0.3, 0.4, 0.5],
            ...         "$lexical": "Wednesday is cyan",
            ...     },
            ...     {
            ...         "_id": "D",
            ...         "wkd": "Thu",
            ...         "$vector": [0.4, 0.5, 0.6],
            ...         "$lexical": "Thursday is red",
            ...     },
            ...     {
            ...         "_id": "E",
            ...         "wkd": "Fri",
            ...         "$vector": [0.5, 0.6, 0.7],
            ...         "$lexical": "Friday is orange",
            ...     },
            ...     {
            ...         "_id": "F",
            ...         "wkd": "Sat",
            ...         "$vector": [0.6, 0.7, 0.8],
            ...         "$lexical": "Saturday is purple",
            ...     },
            ...     {
            ...         "_id": "G",
            ...         "wkd": "Sun",
            ...         "$vector": [0.7, 0.8, 0.9],
            ...         "$lexical": "Sunday is beige",
            ...     },
            ... ])
            CollectionInsertManyResult(inserted_ids=[A, B, C, D, E ... (7 total)], raw_results=...)
            >>>
            >>> # A simple find_and_rerank call (collection without 'vectorize'):
            >>> for r_result in my_vector3d_coll.find_and_rerank(
            ...     sort={
            ...         "$hybrid": {
            ...             "$vector": [0.9, 0.8, 0.7],
            ...             "$lexical": "Weekdays?",
            ...         },
            ...     },
            ...     limit=2,
            ...     rerank_on="wkd",
            ...     rerank_query="week days",
            ... ):
            ...     print(r_result.document["wkd"])
            ...
            Mon
            Tue
        """

        # lazy-import here to avoid circular import issues
        from astrapy.cursors import CollectionFindAndRerankCursor

        _request_timeout_ms, _rt_label = _first_valid_timeout(
            (request_timeout_ms, "request_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (self.api_options.timeout_options.request_timeout_ms, "request_timeout_ms"),
        )
        return (
            CollectionFindAndRerankCursor(
                collection=self,
                request_timeout_ms=_request_timeout_ms,
                overall_timeout_ms=None,
                request_timeout_label=_rt_label,
            )
            .filter(filter)
            .project(projection)
            .limit(limit)
            .sort(sort)
            .hybrid_limits(hybrid_limits)
            .rerank_on(rerank_on)
            .rerank_query(rerank_query)
            .include_scores(include_scores)
            .include_sort_vector(include_sort_vector)
        )

    def count_documents(
        self,
        filter: FilterType,
        *,
        upper_bound: int,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> int:
        """
        Count the documents in the collection matching the specified filter.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
            upper_bound: a required ceiling on the result of the count operation.
                If the actual number of documents exceeds this value,
                an exception will be raised.
                Furthermore, if the actual number of documents exceeds the maximum
                count that the Data API can reach (regardless of upper_bound),
                an exception will be raised.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            the exact count of matching documents.

        Example:
            >>> my_coll.insert_many([{"seq": i} for i in range(20)])
            CollectionInsertManyResult(...)
            >>> my_coll.count_documents({}, upper_bound=100)
            20
            >>> my_coll.count_documents({"seq":{"$gt": 15}}, upper_bound=100)
            4
            >>> my_coll.count_documents({}, upper_bound=10)
            Traceback (most recent call last):
                ... ...
            astrapy.exceptions.TooManyDocumentsToCountException

        Note:
            Count operations are expensive: for this reason, the best practice
            is to provide a reasonable `upper_bound` according to the caller
            expectations. Moreover, indiscriminate usage of count operations
            for sizeable amounts of documents (i.e. in the thousands and more)
            is discouraged in favor of alternative application-specific solutions.
            Keep in mind that the Data API has a hard upper limit on the amount
            of documents it will count, and that an exception will be thrown
            by this method if this limit is encountered.
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        cd_payload = {"countDocuments": {"filter": filter}}
        logger.info(f"countDocuments on '{self.name}'")
        cd_response = self._converted_request(
            payload=cd_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        logger.info(f"finished countDocuments on '{self.name}'")
        if "count" in cd_response.get("status", {}):
            count: int = cd_response["status"]["count"]
            if cd_response["status"].get("moreData", False):
                raise TooManyDocumentsToCountException(
                    text=f"Document count exceeds {count}, the maximum allowed by the server",
                    server_max_count_exceeded=True,
                )
            else:
                if count > upper_bound:
                    raise TooManyDocumentsToCountException(
                        text="Document count exceeds required upper bound",
                        server_max_count_exceeded=False,
                    )
                else:
                    return count
        else:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from countDocuments API command.",
                raw_response=cd_response,
            )

    def estimated_document_count(
        self,
        *,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> int:
        """
        Query the API server for an estimate of the document count in the collection.

        Contrary to `count_documents`, this method has no filtering parameters.

        Args:
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a server-provided estimate count of the documents in the collection.

        Example:
            >>> my_coll.estimated_document_count()
            35700
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        ed_payload: dict[str, Any] = {"estimatedDocumentCount": {}}
        logger.info(f"estimatedDocumentCount on '{self.name}'")
        ed_response = self._converted_request(
            payload=ed_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        logger.info(f"finished estimatedDocumentCount on '{self.name}'")
        if "count" in ed_response.get("status", {}):
            count: int = ed_response["status"]["count"]
            return count
        else:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from estimatedDocumentCount API command.",
                raw_response=ed_response,
            )

    def find_one_and_replace(
        self,
        filter: FilterType,
        replacement: DOC,
        *,
        projection: ProjectionType | None = None,
        sort: SortType | None = None,
        upsert: bool = False,
        return_document: str = ReturnDocument.BEFORE,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> DOC | None:
        """
        Find a document on the collection and replace it entirely with a new one,
        optionally inserting a new one if no match is found.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
            replacement: the new document to write into the collection.
            projection: it controls which parts of the document are returned.
                It can be an allow-list: `{"f1": True, "f2": True}`,
                or a deny-list: `{"fx": False, "fy": False}`, but not a mixture
                (except for the `_id` and other special fields, which can be
                associated to both True or False independently of the rest
                of the specification).
                The special star-projections `{"*": True}` and `{"*": False}`
                have the effect of returning the whole document and `{}` respectively.
                For lists in documents, slice directives can be passed to select
                portions of the list: for instance, `{"array": {"$slice": 2}}`,
                `{"array": {"$slice": -2}}`, `{"array": {"$slice": [4, 2]}}` or
                `{"array": {"$slice": [-4, 2]}}`.
                An iterable over strings will be treated implicitly as an allow-list.
                The default projection (used if this parameter is not passed) does not
                necessarily include "special" fields such as `$vector` or `$vectorize`.
                See the Data API documentation for more on projections.
            sort: with this dictionary parameter one can control the sorting
                order of the documents matching the filter, effectively
                determining what document will come first and hence be the
                replaced one. See the `find` method for more on sorting.
                Vector-based ANN sorting is achieved by providing a "$vector"
                or a "$vectorize" key in `sort`.
            upsert: this parameter controls the behavior in absence of matches.
                If True, `replacement` is inserted as a new document
                if no matches are found on the collection. If False,
                the operation silently does nothing in case of no matches.
            return_document: a flag controlling what document is returned:
                if set to `ReturnDocument.BEFORE`, or the string "before",
                the document found on database is returned; if set to
                `ReturnDocument.AFTER`, or the string "after", the new
                document is returned. The default is "before".
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            A document (or a projection thereof, as required), either the one
            before the replace operation or the one after that.
            Alternatively, the method returns None to represent
            that no matching document was found, or that no replacement
            was inserted (depending on the `return_document` parameter).

        Example:
            >>> my_coll.insert_one({"_id": "rule1", "text": "all animals are equal"})
            CollectionInsertOneResult(...)
            >>> my_coll.find_one_and_replace(
            ...     {"_id": "rule1"},
            ...     {"text": "some animals are more equal!"},
            ... )
            {'_id': 'rule1', 'text': 'all animals are equal'}
            >>> my_coll.find_one_and_replace(
            ...     {"text": "some animals are more equal!"},
            ...     {"text": "and the pigs are the rulers"},
            ...     return_document=astrapy.constants.ReturnDocument.AFTER,
            ... )
            {'_id': 'rule1', 'text': 'and the pigs are the rulers'}
            >>> my_coll.find_one_and_replace(
            ...     {"_id": "rule2"},
            ...     {"text": "F=ma^2"},
            ...     return_document=astrapy.constants.ReturnDocument.AFTER,
            ... )
            >>> # (returns None for no matches)
            >>> my_coll.find_one_and_replace(
            ...     {"_id": "rule2"},
            ...     {"text": "F=ma"},
            ...     upsert=True,
            ...     return_document=astrapy.constants.ReturnDocument.AFTER,
            ...     projection={"_id": False},
            ... )
            {'text': 'F=ma'}
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        options = {
            "returnDocument": return_document,
            "upsert": upsert,
        }
        fo_payload = {
            "findOneAndReplace": {
                k: v
                for k, v in {
                    "filter": filter,
                    "projection": normalize_optional_projection(projection),
                    "replacement": replacement,
                    "options": options,
                    "sort": sort,
                }.items()
                if v is not None
            }
        }
        logger.info(f"findOneAndReplace on '{self.name}'")
        fo_response = self._converted_request(
            payload=fo_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        logger.info(f"finished findOneAndReplace on '{self.name}'")
        if "document" in fo_response.get("data", {}):
            ret_document = fo_response.get("data", {}).get("document")
            if ret_document is None:
                return None
            else:
                return ret_document  # type: ignore[no-any-return]
        else:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from find_one_and_replace API command.",
                raw_response=fo_response,
            )

    def replace_one(
        self,
        filter: FilterType,
        replacement: DOC,
        *,
        sort: SortType | None = None,
        upsert: bool = False,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> CollectionUpdateResult:
        """
        Replace a single document on the collection with a new one,
        optionally inserting a new one if no match is found.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
            replacement: the new document to write into the collection.
            sort: with this dictionary parameter one can control the sorting
                order of the documents matching the filter, effectively
                determining what document will come first and hence be the
                replaced one. See the `find` method for more on sorting.
                Vector-based ANN sorting is achieved by providing a "$vector"
                or a "$vectorize" key in `sort`.
            upsert: this parameter controls the behavior in absence of matches.
                If True, `replacement` is inserted as a new document
                if no matches are found on the collection. If False,
                the operation silently does nothing in case of no matches.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a CollectionUpdateResult object summarizing the outcome of
            the replace operation.

        Example:
            >>> my_coll.insert_one({"Marco": "Polo"})
            CollectionInsertOneResult(...)
            >>> my_coll.replace_one({"Marco": {"$exists": True}}, {"Buda": "Pest"})
            CollectionUpdateResult(raw_results=..., update_info={'n': 1, 'updatedExisting': True, 'ok': 1.0, 'nModified': 1})
            >>> my_coll.find_one({"Buda": "Pest"})
            {'_id': '8424905a-...', 'Buda': 'Pest'}
            >>> my_coll.replace_one({"Mirco": {"$exists": True}}, {"Oh": "yeah?"})
            CollectionUpdateResult(raw_results=..., update_info={'n': 0, 'updatedExisting': False, 'ok': 1.0, 'nModified': 0})
            >>> my_coll.replace_one({"Mirco": {"$exists": True}}, {"Oh": "yeah?"}, upsert=True)
            CollectionUpdateResult(raw_results=..., update_info={'n': 1, 'updatedExisting': False, 'ok': 1.0, 'nModified': 0, 'upserted': '931b47d6-...'})
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        options = {
            "upsert": upsert,
        }
        fo_payload = {
            "findOneAndReplace": {
                k: v
                for k, v in {
                    "filter": filter,
                    "replacement": replacement,
                    "options": options,
                    "sort": sort,
                }.items()
                if v is not None
            }
        }
        logger.info(f"findOneAndReplace on '{self.name}'")
        fo_response = self._converted_request(
            payload=fo_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        logger.info(f"finished findOneAndReplace on '{self.name}'")
        if "document" in fo_response.get("data", {}):
            fo_status = fo_response.get("status") or {}
            _update_info = _prepare_update_info([fo_status])
            return CollectionUpdateResult(
                raw_results=[fo_response],
                update_info=_update_info,
            )
        else:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from find_one_and_replace API command.",
                raw_response=fo_response,
            )

    def find_one_and_update(
        self,
        filter: FilterType,
        update: dict[str, Any],
        *,
        projection: ProjectionType | None = None,
        sort: SortType | None = None,
        upsert: bool = False,
        return_document: str = ReturnDocument.BEFORE,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> DOC | None:
        """
        Find a document on the collection and update it as requested,
        optionally inserting a new one if no match is found.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
            update: the update prescription to apply to the document, expressed
                as a dictionary as per Data API syntax. Examples are:
                    {"$set": {"field": "value}}
                    {"$inc": {"counter": 10}}
                    {"$unset": {"field": ""}}
                See the Data API documentation for the full syntax.
            projection: it controls which parts of the document are returned.
                It can be an allow-list: `{"f1": True, "f2": True}`,
                or a deny-list: `{"fx": False, "fy": False}`, but not a mixture
                (except for the `_id` and other special fields, which can be
                associated to both True or False independently of the rest
                of the specification).
                The special star-projections `{"*": True}` and `{"*": False}`
                have the effect of returning the whole document and `{}` respectively.
                For lists in documents, slice directives can be passed to select
                portions of the list: for instance, `{"array": {"$slice": 2}}`,
                `{"array": {"$slice": -2}}`, `{"array": {"$slice": [4, 2]}}` or
                `{"array": {"$slice": [-4, 2]}}`.
                An iterable over strings will be treated implicitly as an allow-list.
                The default projection (used if this parameter is not passed) does not
                necessarily include "special" fields such as `$vector` or `$vectorize`.
                See the Data API documentation for more on projections.
            sort: with this dictionary parameter one can control the sorting
                order of the documents matching the filter, effectively
                determining what document will come first and hence be the
                replaced one. See the `find` method for more on sorting.
                Vector-based ANN sorting is achieved by providing a "$vector"
                or a "$vectorize" key in `sort`.
            upsert: this parameter controls the behavior in absence of matches.
                If True, a new document (resulting from applying the `update`
                to an empty document) is inserted if no matches are found on
                the collection. If False, the operation silently does nothing
                in case of no matches.
            return_document: a flag controlling what document is returned:
                if set to `ReturnDocument.BEFORE`, or the string "before",
                the document found on database is returned; if set to
                `ReturnDocument.AFTER`, or the string "after", the new
                document is returned. The default is "before".
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            A document (or a projection thereof, as required), either the one
            before the replace operation or the one after that.
            Alternatively, the method returns None to represent
            that no matching document was found, or that no update
            was applied (depending on the `return_document` parameter).

        Example:
            >>> my_coll.insert_one({"Marco": "Polo"})
            CollectionInsertOneResult(...)
            >>> my_coll.find_one_and_update(
            ...     {"Marco": {"$exists": True}},
            ...     {"$set": {"title": "Mr."}},
            ... )
            {'_id': 'a80106f2-...', 'Marco': 'Polo'}
            >>> my_coll.find_one_and_update(
            ...     {"title": "Mr."},
            ...     {"$inc": {"rank": 3}},
            ...     projection=["title", "rank"],
            ...     return_document=astrapy.constants.ReturnDocument.AFTER,
            ... )
            {'_id': 'a80106f2-...', 'title': 'Mr.', 'rank': 3}
            >>> my_coll.find_one_and_update(
            ...     {"name": "Johnny"},
            ...     {"$set": {"rank": 0}},
            ...     return_document=astrapy.constants.ReturnDocument.AFTER,
            ... )
            >>> # (returns None for no matches)
            >>> my_coll.find_one_and_update(
            ...     {"name": "Johnny"},
            ...     {"$set": {"rank": 0}},
            ...     upsert=True,
            ...     return_document=astrapy.constants.ReturnDocument.AFTER,
            ... )
            {'_id': 'cb4ef2ab-...', 'name': 'Johnny', 'rank': 0}
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        options = {
            "returnDocument": return_document,
            "upsert": upsert,
        }
        fo_payload = {
            "findOneAndUpdate": {
                k: v
                for k, v in {
                    "filter": filter,
                    "update": update,
                    "options": options,
                    "sort": sort,
                    "projection": normalize_optional_projection(projection),
                }.items()
                if v is not None
            }
        }
        logger.info(f"findOneAndUpdate on '{self.name}'")
        fo_response = self._converted_request(
            payload=fo_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        logger.info(f"finished findOneAndUpdate on '{self.name}'")
        if "document" in fo_response.get("data", {}):
            ret_document = fo_response.get("data", {}).get("document")
            if ret_document is None:
                return None
            else:
                return ret_document  # type: ignore[no-any-return]
        else:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from find_one_and_update API command.",
                raw_response=fo_response,
            )

    def update_one(
        self,
        filter: FilterType,
        update: dict[str, Any],
        *,
        sort: SortType | None = None,
        upsert: bool = False,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> CollectionUpdateResult:
        """
        Update a single document on the collection as requested,
        optionally inserting a new one if no match is found.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
            update: the update prescription to apply to the document, expressed
                as a dictionary as per Data API syntax. Examples are:
                    {"$set": {"field": "value}}
                    {"$inc": {"counter": 10}}
                    {"$unset": {"field": ""}}
                See the Data API documentation for the full syntax.
            sort: with this dictionary parameter one can control the sorting
                order of the documents matching the filter, effectively
                determining what document will come first and hence be the
                replaced one. See the `find` method for more on sorting.
                Vector-based ANN sorting is achieved by providing a "$vector"
                or a "$vectorize" key in `sort`.
            upsert: this parameter controls the behavior in absence of matches.
                If True, a new document (resulting from applying the `update`
                to an empty document) is inserted if no matches are found on
                the collection. If False, the operation silently does nothing
                in case of no matches.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a CollectionUpdateResult object summarizing the outcome of
            the update operation.

        Example:
            >>> my_coll.insert_one({"Marco": "Polo"})
            CollectionInsertOneResult(...)
            >>> my_coll.update_one({"Marco": {"$exists": True}}, {"$inc": {"rank": 3}})
            CollectionUpdateResult(raw_results=..., update_info={'n': 1, 'updatedExisting': True, 'ok': 1.0, 'nModified': 1})
            >>> my_coll.update_one({"Mirko": {"$exists": True}}, {"$inc": {"rank": 3}})
            CollectionUpdateResult(raw_results=..., update_info={'n': 0, 'updatedExisting': False, 'ok': 1.0, 'nModified': 0})
            >>> my_coll.update_one({"Mirko": {"$exists": True}}, {"$inc": {"rank": 3}}, upsert=True)
            CollectionUpdateResult(raw_results=..., update_info={'n': 1, 'updatedExisting': False, 'ok': 1.0, 'nModified': 0, 'upserted': '2a45ff60-...'})
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        options = {
            "upsert": upsert,
        }
        uo_payload = {
            "updateOne": {
                k: v
                for k, v in {
                    "filter": filter,
                    "update": update,
                    "options": options,
                    "sort": sort,
                }.items()
                if v is not None
            }
        }
        logger.info(f"updateOne on '{self.name}'")
        uo_response = self._converted_request(
            payload=uo_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        logger.info(f"finished updateOne on '{self.name}'")
        if "status" in uo_response:
            uo_status = uo_response["status"]
            _update_info = _prepare_update_info([uo_status])
            return CollectionUpdateResult(
                raw_results=[uo_response],
                update_info=_update_info,
            )
        else:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from updateOne API command.",
                raw_response=uo_response,
            )

    def update_many(
        self,
        filter: FilterType,
        update: dict[str, Any],
        *,
        upsert: bool = False,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> CollectionUpdateResult:
        """
        Apply an update operation to all documents matching a condition,
        optionally inserting one documents in absence of matches.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
            update: the update prescription to apply to the documents, expressed
                as a dictionary as per Data API syntax. Examples are:
                    {"$set": {"field": "value}}
                    {"$inc": {"counter": 10}}
                    {"$unset": {"field": ""}}
                See the Data API documentation for the full syntax.
            upsert: this parameter controls the behavior in absence of matches.
                If True, a single new document (resulting from applying `update`
                to an empty document) is inserted if no matches are found on
                the collection. If False, the operation silently does nothing
                in case of no matches.
            general_method_timeout_ms: a timeout, in milliseconds, for the whole
                requested operation (which may involve multiple API requests).
                This method may entail successive HTTP API requests,
                depending on the amount of involved documents.
                If not passed, the collection-level setting is used instead.
            request_timeout_ms: a timeout, in milliseconds, for each API request.
                If not passed, the collection-level setting is used instead.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a CollectionUpdateResult object summarizing the outcome of
            the update operation.

        Example:
            >>> my_coll.insert_many([{"c": "red"}, {"c": "green"}, {"c": "blue"}])
            CollectionInsertManyResult(...)
            >>> my_coll.update_many({"c": {"$ne": "green"}}, {"$set": {"nongreen": True}})
            CollectionUpdateResult(raw_results=..., update_info={'n': 2, 'updatedExisting': True, 'ok': 1.0, 'nModified': 2})
            >>> my_coll.update_many({"c": "orange"}, {"$set": {"is_also_fruit": True}})
            CollectionUpdateResult(raw_results=..., update_info={'n': 0, 'updatedExisting': False, 'ok': 1.0, 'nModified': 0})
            >>> my_coll.update_many(
            ...     {"c": "orange"},
            ...     {"$set": {"is_also_fruit": True}},
            ...     upsert=True,
            ... )
            CollectionUpdateResult(raw_results=..., update_info={'n': 1, 'updatedExisting': False, 'ok': 1.0, 'nModified': 0, 'upserted': '46643050-...'})

        Note:
            Similarly to the case of `find` (see its docstring for more details),
            running this command while, at the same time, another process is
            inserting new documents which match the filter of the `update_many`
            can result in an unpredictable fraction of these documents being updated.
            In other words, it cannot be easily predicted whether a given
            newly-inserted document will be picked up by the update_many command or not.
        """

        _general_method_timeout_ms, _gmt_label = _first_valid_timeout(
            (general_method_timeout_ms, "general_method_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (
                self.api_options.timeout_options.general_method_timeout_ms,
                "general_method_timeout_ms",
            ),
        )
        _request_timeout_ms, _rt_label = _first_valid_timeout(
            (request_timeout_ms, "request_timeout_ms"),
            (self.api_options.timeout_options.request_timeout_ms, "request_timeout_ms"),
        )
        api_options = {
            "upsert": upsert,
        }
        page_state_options: dict[str, str] = {}
        um_responses: list[dict[str, Any]] = []
        um_statuses: list[dict[str, Any]] = []
        must_proceed = True
        logger.info(f"starting update_many on '{self.name}'")
        timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=_general_method_timeout_ms,
            timeout_label=_gmt_label,
        )
        while must_proceed:
            options = {**api_options, **page_state_options}
            this_um_payload = {
                "updateMany": {
                    k: v
                    for k, v in {
                        "filter": filter,
                        "update": update,
                        "options": options,
                    }.items()
                    if v is not None
                }
            }
            logger.info(f"updateMany on '{self.name}'")
            this_um_response = self._converted_request(
                payload=this_um_payload,
                raise_api_errors=False,
                timeout_context=timeout_manager.remaining_timeout(
                    cap_time_ms=_request_timeout_ms,
                    cap_timeout_label=_rt_label,
                ),
            )
            logger.info(f"finished updateMany on '{self.name}'")
            this_um_status = this_um_response.get("status") or {}
            #
            # if errors, quit early
            if this_um_response.get("errors", []):
                partial_update_info = _prepare_update_info(um_statuses)
                partial_result = CollectionUpdateResult(
                    raw_results=um_responses,
                    update_info=partial_update_info,
                )
                cause_exception = DataAPIResponseException.from_response(
                    command=this_um_payload,
                    raw_response=this_um_response,
                )
                raise CollectionUpdateManyException(
                    partial_result=partial_result,
                    cause=cause_exception,
                )
            else:
                if "status" not in this_um_response:
                    raise UnexpectedDataAPIResponseException(
                        text="Faulty response from update_many API command.",
                        raw_response=this_um_response,
                    )
                um_responses.append(this_um_response)
                um_statuses.append(this_um_status)
                next_page_state = this_um_status.get("nextPageState")
                if next_page_state is not None:
                    must_proceed = True
                    page_state_options = {"pageState": next_page_state}
                else:
                    must_proceed = False
                    page_state_options = {}

        update_info = _prepare_update_info(um_statuses)
        logger.info(f"finished update_many on '{self.name}'")
        return CollectionUpdateResult(
            raw_results=um_responses,
            update_info=update_info,
        )

    def find_one_and_delete(
        self,
        filter: FilterType,
        *,
        projection: ProjectionType | None = None,
        sort: SortType | None = None,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> DOC | None:
        """
        Find a document in the collection and delete it. The deleted document,
        however, is the return value of the method.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
            projection: it controls which parts of the document are returned.
                It can be an allow-list: `{"f1": True, "f2": True}`,
                or a deny-list: `{"fx": False, "fy": False}`, but not a mixture
                (except for the `_id` and other special fields, which can be
                associated to both True or False independently of the rest
                of the specification).
                The special star-projections `{"*": True}` and `{"*": False}`
                have the effect of returning the whole document and `{}` respectively.
                For lists in documents, slice directives can be passed to select
                portions of the list: for instance, `{"array": {"$slice": 2}}`,
                `{"array": {"$slice": -2}}`, `{"array": {"$slice": [4, 2]}}` or
                `{"array": {"$slice": [-4, 2]}}`.
                An iterable over strings will be treated implicitly as an allow-list.
                The default projection (used if this parameter is not passed) does not
                necessarily include "special" fields such as `$vector` or `$vectorize`.
                See the Data API documentation for more on projections.
            sort: with this dictionary parameter one can control the sorting
                order of the documents matching the filter, effectively
                determining what document will come first and hence be the
                deleted one. See the `find` method for more on sorting.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            Either the document (or a projection thereof, as requested), or None
            if no matches were found in the first place.

        Example:
            >>> my_coll.insert_many(
            ...     [
            ...         {"species": "swan", "class": "Aves"},
            ...         {"species": "frog", "class": "Amphibia"},
            ...     ],
            ... )
            CollectionInsertManyResult(...)
            >>> my_coll.find_one_and_delete(
            ...     {"species": {"$ne": "frog"}},
            ...     projection=["species"],
            ... )
            {'_id': '5997fb48-...', 'species': 'swan'}
            >>> my_coll.find_one_and_delete({"species": {"$ne": "frog"}})
            >>> # (returns None for no matches)
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        _projection = normalize_optional_projection(projection)
        fo_payload = {
            "findOneAndDelete": {
                k: v
                for k, v in {
                    "filter": filter,
                    "sort": sort,
                    "projection": _projection,
                }.items()
                if v is not None
            }
        }
        logger.info(f"findOneAndDelete on '{self.name}'")
        fo_response = self._converted_request(
            payload=fo_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        logger.info(f"finished findOneAndDelete on '{self.name}'")
        if "document" in fo_response.get("data", {}):
            document = fo_response["data"]["document"]
            return document  # type: ignore[no-any-return]
        else:
            deleted_count = fo_response.get("status", {}).get("deletedCount")
            if deleted_count == 0:
                return None
            else:
                raise UnexpectedDataAPIResponseException(
                    text="Faulty response from find_one_and_delete API command.",
                    raw_response=fo_response,
                )

    def delete_one(
        self,
        filter: FilterType,
        *,
        sort: SortType | None = None,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> CollectionDeleteResult:
        """
        Delete one document matching a provided filter.
        This method never deletes more than a single document, regardless
        of the number of matches to the provided filters.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
            sort: with this dictionary parameter one can control the sorting
                order of the documents matching the filter, effectively
                determining what document will come first and hence be the
                deleted one. See the `find` method for more on sorting.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a CollectionDeleteResult object summarizing the outcome of the
            delete operation.

        Example:
            >>> my_coll.insert_many([{"seq": 1}, {"seq": 0}, {"seq": 2}])
            CollectionInsertManyResult(...)
            >>> my_coll.delete_one({"seq": 1})
            CollectionDeleteResult(raw_results=..., deleted_count=1)
            >>> my_coll.distinct("seq")
            [0, 2]
            >>> my_coll.delete_one(
            ...     {"seq": {"$exists": True}},
            ...     sort={"seq": astrapy.constants.SortMode.DESCENDING},
            ... )
            CollectionDeleteResult(raw_results=..., deleted_count=1)
            >>> my_coll.distinct("seq")
            [0]
            >>> my_coll.delete_one({"seq": 2})
            CollectionDeleteResult(raw_results=..., deleted_count=0)
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        do_payload = {
            "deleteOne": {
                k: v
                for k, v in {
                    "filter": filter,
                    "sort": sort,
                }.items()
                if v is not None
            }
        }
        logger.info(f"deleteOne on '{self.name}'")
        do_response = self._converted_request(
            payload=do_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        logger.info(f"finished deleteOne on '{self.name}'")
        if "deletedCount" in do_response.get("status", {}):
            deleted_count = do_response["status"]["deletedCount"]
            return CollectionDeleteResult(
                deleted_count=deleted_count,
                raw_results=[do_response],
            )
        else:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from delete_one API command.",
                raw_response=do_response,
            )

    def delete_many(
        self,
        filter: FilterType,
        *,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> CollectionDeleteResult:
        """
        Delete all documents matching a provided filter.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
                Passing an empty filter, `{}`, completely erases all contents
                of the collection.
            general_method_timeout_ms: a timeout, in milliseconds, for the whole
                requested operation (which may involve multiple API requests).
                This method may entail successive HTTP API requests,
                depending on the amount of involved documents.
                If not passed, the collection-level setting is used instead.
            request_timeout_ms: a timeout, in milliseconds, for each API request.
                If not passed, the collection-level setting is used instead.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a CollectionDeleteResult object summarizing the outcome of the
            delete operation.

        Example:
            >>> my_coll.insert_many([{"seq": 1}, {"seq": 0}, {"seq": 2}])
            CollectionInsertManyResult(...)
            >>> my_coll.delete_many({"seq": {"$lte": 1}})
            CollectionDeleteResult(raw_results=..., deleted_count=2)
            >>> my_coll.distinct("seq")
            [2]
            >>> my_coll.delete_many({"seq": {"$lte": 1}})
            CollectionDeleteResult(raw_results=..., deleted_count=0)

        Note:
            This operation is in general not atomic. Depending on the amount
            of matching documents, it can keep running (in a blocking way)
            for a macroscopic time. In that case, new documents that are
            meanwhile inserted (e.g. from another process/application) will be
            deleted during the execution of this method call until the
            collection is devoid of matches.
            An exception is the `filter={}` case, whereby the operation is atomic.
        """

        _general_method_timeout_ms, _gmt_label = _first_valid_timeout(
            (general_method_timeout_ms, "general_method_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (
                self.api_options.timeout_options.general_method_timeout_ms,
                "general_method_timeout_ms",
            ),
        )
        _request_timeout_ms, _rt_label = _first_valid_timeout(
            (request_timeout_ms, "request_timeout_ms"),
            (self.api_options.timeout_options.request_timeout_ms, "request_timeout_ms"),
        )
        dm_responses: list[dict[str, Any]] = []
        deleted_count = 0
        must_proceed = True
        timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=_general_method_timeout_ms,
            timeout_label=_gmt_label,
        )
        this_dm_payload = {"deleteMany": {"filter": filter}}
        logger.info(f"starting delete_many on '{self.name}'")
        while must_proceed:
            logger.info(f"deleteMany on '{self.name}'")
            this_dm_response = self._converted_request(
                payload=this_dm_payload,
                raise_api_errors=False,
                timeout_context=timeout_manager.remaining_timeout(
                    cap_time_ms=_request_timeout_ms,
                    cap_timeout_label=_rt_label,
                ),
            )
            logger.info(f"finished deleteMany on '{self.name}'")
            # if errors, quit early
            if this_dm_response.get("errors", []):
                partial_result = CollectionDeleteResult(
                    deleted_count=deleted_count,
                    raw_results=dm_responses,
                )
                cause_exception = DataAPIResponseException.from_response(
                    command=this_dm_payload,
                    raw_response=this_dm_response,
                )
                raise CollectionDeleteManyException(
                    partial_result=partial_result,
                    cause=cause_exception,
                )
            else:
                this_dc = this_dm_response.get("status", {}).get("deletedCount")
                if this_dc is None:
                    raise UnexpectedDataAPIResponseException(
                        text="Faulty response from delete_many API command.",
                        raw_response=this_dm_response,
                    )
                dm_responses.append(this_dm_response)
                deleted_count += this_dc
                must_proceed = this_dm_response.get("status", {}).get("moreData", False)

        logger.info(f"finished delete_many on '{self.name}'")
        return CollectionDeleteResult(
            deleted_count=deleted_count,
            raw_results=dm_responses,
        )

    def drop(
        self,
        *,
        collection_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        """
        Drop the collection, i.e. delete it from the database along with
        all the documents it contains.

        Args:
            collection_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `collection_admin_timeout_ms`.
            timeout_ms: an alias for `collection_admin_timeout_ms`.

        Example:
            >>> my_coll.find_one({})
            {'_id': '...', 'a': 100}
            >>> my_coll.drop()
            >>> my_coll.find_one({})
            Traceback (most recent call last):
                ... ...
            astrapy.exceptions.DataAPIResponseException: Collection does not exist, collection name: my_collection

        Note:
            Use with caution.

        Note:
            Once the method succeeds, methods on this object can still be invoked:
            however, this hardly makes sense as the underlying actual collection
            is no more.
            It is responsibility of the developer to design a correct flow
            which avoids using a deceased collection any further.
        """

        logger.info(f"dropping collection '{self.name}' (self)")
        self.database.drop_collection(
            self.name,
            collection_admin_timeout_ms=collection_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info(f"finished dropping collection '{self.name}' (self)")

    def command(
        self,
        body: dict[str, Any] | None,
        *,
        raise_api_errors: bool = True,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        Send a POST request to the Data API for this collection with
        an arbitrary, caller-provided payload.
        No transformations or type conversions are made on the provided payload.

        Args:
            body: a JSON-serializable dictionary, the payload of the request.
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
            >>> my_coll.command({"countDocuments": {}})
            {'status': {'count': 123}}
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        _cmd_desc: str
        if body:
            _cmd_desc = ",".join(sorted(body.keys()))
        else:
            _cmd_desc = "(none)"
        logger.info(f"command={_cmd_desc} on '{self.name}'")
        command_result = self._api_commander.request(
            payload=body,
            raise_api_errors=raise_api_errors,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        logger.info(f"finished command={_cmd_desc} on '{self.name}'")
        return command_result


class AsyncCollection(Generic[DOC]):
    """
    A Data API collection, the object to interact with the Data API for unstructured
    (schemaless) data, especially for DDL operations.
    This class has an asynchronous interface for use with asyncio.

    This class is not meant for direct instantiation by the user, rather
    it is obtained by invoking methods such as `get_collection` of AsyncDatabase,
    wherefrom the AsyncCollection inherits its API options such as authentication
    token and API endpoint.

    Args:
        database: a Database object, instantiated earlier. This represents
            the database the collection belongs to.
        name: the collection name. This parameter should match an existing
            collection on the database.
        keyspace: this is the keyspace to which the collection belongs.
            If nothing is specified, the database's working keyspace is used.
        api_options: a complete specification of the API Options for this instance.

    Examples:
        >>> # NOTE: may require slight adaptation to an async context.
        >>>
        >>> from astrapy import DataAPIClient
        >>> client = DataAPIClient()
        >>> async_database = client.get_async_database(
        ...     "https://01234567-....apps.astra.datastax.com",
        ...     token="AstraCS:..."
        ... )

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
        >>> my_collection = await async_database.create_collection(
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
        >>> my_collection_1 = await async_database.create_collection(
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
        >>> my_collection_2 = await async_database.create_collection(
        ...     "my_events",
        ...     definition=collection_definition_2,
        ... )

        >>> # Get a reference to an existing collection
        >>> # (no checks are performed on DB)
        >>> my_collection_3a = async_database.get_collection("my_events")
        >>> my_collection_3b = async_database.my_events
        >>> my_collection_3c = async_database["my_events"]

    Note:
        creating an instance of AsyncCollection does not trigger actual creation
        of the collection on the database. The latter should have been created
        beforehand, e.g. through the `create_collection` method of an AsyncDatabase.
    """

    def __init__(
        self,
        *,
        database: AsyncDatabase,
        name: str,
        keyspace: str | None,
        api_options: FullAPIOptions,
    ) -> None:
        self.api_options = api_options
        self._name = name
        _keyspace = keyspace if keyspace is not None else database.keyspace

        if _keyspace is None:
            raise ValueError("Attempted to create Collection with 'keyspace' unset.")

        self._database = database._copy(
            keyspace=_keyspace, api_options=self.api_options
        )
        self._commander_headers = {
            **{DEFAULT_DATA_API_AUTH_HEADER: self.api_options.token.get_token()},
            **self.api_options.embedding_api_key.get_headers(),
            **self.api_options.reranking_api_key.get_headers(),
            **self.api_options.database_additional_headers,
        }
        self._api_commander = self._get_api_commander()

    def __repr__(self) -> str:
        _db_desc = f'database.api_endpoint="{self.database.api_endpoint}"'
        return (
            f'{self.__class__.__name__}(name="{self.name}", '
            f'keyspace="{self.keyspace}", {_db_desc}, '
            f"api_options={self.api_options})"
        )

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, AsyncCollection):
            return all(
                [
                    self._name == other._name,
                    self._database == other._database,
                    self.api_options == other.api_options,
                ]
            )
        else:
            return False

    def __call__(self, *pargs: Any, **kwargs: Any) -> None:
        raise TypeError(
            f"'{self.__class__.__name__}' object is not callable. If you "
            f"meant to call the '{self.name}' method on a "
            f"'{self.database.__class__.__name__}' object "
            "it is failing because no such method exists."
        )

    def _get_api_commander(self) -> APICommander:
        """Instantiate a new APICommander based on the properties of this class."""

        if self._database.keyspace is None:
            raise ValueError(
                "No keyspace specified. AsyncCollection requires a keyspace to "
                "be set, e.g. through the `keyspace` constructor parameter."
            )

        base_path_components = [
            comp
            for comp in (
                ncomp.strip("/")
                for ncomp in (
                    self._database.api_options.data_api_url_options.api_path,
                    self._database.api_options.data_api_url_options.api_version,
                    self._database.keyspace,
                    self._name,
                )
                if ncomp is not None
            )
            if comp != ""
        ]
        base_path = f"/{'/'.join(base_path_components)}"
        api_commander = APICommander(
            api_endpoint=self._database.api_endpoint,
            path=base_path,
            headers=self._commander_headers,
            callers=self.api_options.callers,
            redacted_header_names=self.api_options.redacted_header_names,
            handle_decimals_writes=(
                self.api_options.serdes_options.use_decimals_in_collections
            ),
            handle_decimals_reads=(
                self.api_options.serdes_options.use_decimals_in_collections
            ),
        )
        return api_commander

    async def __aenter__(self: AsyncCollection[DOC]) -> AsyncCollection[DOC]:
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

    async def _converted_request(
        self,
        *,
        http_method: str = HttpMethod.POST,
        payload: dict[str, Any] | None = None,
        additional_path: str | None = None,
        request_params: dict[str, Any] = {},
        raise_api_errors: bool = True,
        timeout_context: _TimeoutContext,
    ) -> dict[str, Any]:
        converted_payload = preprocess_collection_payload(
            payload, options=self.api_options.serdes_options
        )
        raw_response_json = await self._api_commander.async_request(
            http_method=http_method,
            payload=converted_payload,
            additional_path=additional_path,
            request_params=request_params,
            raise_api_errors=raise_api_errors,
            timeout_context=timeout_context,
        )
        response_json = postprocess_collection_response(
            raw_response_json, options=self.api_options.serdes_options
        )
        return response_json

    def _copy(
        self: AsyncCollection[DOC],
        *,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncCollection[DOC]:
        arg_api_options = APIOptions(
            embedding_api_key=embedding_api_key,
            reranking_api_key=reranking_api_key,
        )
        final_api_options = self.api_options.with_override(api_options).with_override(
            arg_api_options
        )
        return AsyncCollection(
            database=self.database,
            name=self.name,
            keyspace=self.keyspace,
            api_options=final_api_options,
        )

    def with_options(
        self: AsyncCollection[DOC],
        *,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncCollection[DOC]:
        """
        Create a clone of this collection with some changed attributes.

        Args:
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
            api_options: any additional options to set for the clone, in the form of
                an APIOptions instance (where one can set just the needed attributes).
                In case the same setting is also provided as named parameter,
                the latter takes precedence.

        Returns:
            a new AsyncCollection instance.

        Example:
            >>> collection_with_api_key_configured = my_async_collection.with_options(
            ...     embedding_api_key="secret-key-0123abcd...",
            ... )
        """

        return self._copy(
            embedding_api_key=embedding_api_key,
            reranking_api_key=reranking_api_key,
            api_options=api_options,
        )

    def to_sync(
        self: AsyncCollection[DOC],
        *,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> Collection[DOC]:
        """
        Create a Collection from this one. Save for the arguments
        explicitly provided as overrides, everything else is kept identical
        to this collection in the copy (the database is converted into
        a sync object).

        Args:
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
            api_options: any additional options to set for the result, in the form of
                an APIOptions instance (where one can set just the needed attributes).
                In case the same setting is also provided as named parameter,
                the latter takes precedence.

        Returns:
            the new copy, a Collection instance.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> my_async_coll.to_sync().count_documents({}, upper_bound=100)
            77
        """

        arg_api_options = APIOptions(
            embedding_api_key=embedding_api_key,
            reranking_api_key=reranking_api_key,
        )
        final_api_options = self.api_options.with_override(api_options).with_override(
            arg_api_options
        )
        return Collection(
            database=self.database.to_sync(),
            name=self.name,
            keyspace=self.keyspace,
            api_options=final_api_options,
        )

    async def options(
        self,
        *,
        collection_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> CollectionDefinition:
        """
        Get the collection options, i.e. its configuration as read from the database.

        The method issues a request to the Data API each time is invoked,
        without caching mechanisms: this ensures up-to-date information
        for usages such as real-time collection validation by the application.

        Args:
            collection_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `collection_admin_timeout_ms`.
            timeout_ms: an alias for `collection_admin_timeout_ms`.

        Returns:
            a CollectionDefinition instance describing the collection.
            (See also the database `list_collections` method.)

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> asyncio.run(my_async_coll.options())
            CollectionDefinition(vector=CollectionVectorOptions(dimension=3, metric='cosine'))
        """

        _collection_admin_timeout_ms, _ca_label = _select_singlereq_timeout_ca(
            timeout_options=self.api_options.timeout_options,
            collection_admin_timeout_ms=collection_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info(f"getting collections in search of '{self.name}'")
        self_descriptors = [
            coll_desc
            for coll_desc in await self.database._list_collections_ctx(
                keyspace=None,
                timeout_context=_TimeoutContext(
                    request_ms=_collection_admin_timeout_ms,
                    label=_ca_label,
                ),
            )
            if coll_desc.name == self.name
        ]
        logger.info(f"finished getting collections in search of '{self.name}'")
        if self_descriptors:
            return self_descriptors[0].definition
        else:
            raise RuntimeError(
                f"Collection {self.keyspace}.{self.name} not found.",
            )

    async def info(
        self,
        *,
        database_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> CollectionInfo:
        """
        Information on the collection (name, location, database), in the
        form of a CollectionInfo object.

        Not to be confused with the collection `options` method (related
        to the collection internal configuration).

        Args:
            database_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying DevOps API request.
                If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `database_admin_timeout_ms`.
            timeout_ms: an alias for `database_admin_timeout_ms`.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> asyncio.run(my_async_coll.info()).database_info.region
            'us-east1'
            >>> asyncio.run(my_async_coll.info()).full_name
            'default_keyspace.my_v_collection'

        Note:
            the returned CollectionInfo wraps, among other things,
            the database information: as such, calling this method
            triggers the same-named method of a Database object (which, in turn,
            performs a HTTP request to the DevOps API).
            See the documentation for `Database.info()` for more details.
        """

        db_info = await self.database.info(
            database_admin_timeout_ms=database_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        return CollectionInfo(
            database_info=db_info,
            keyspace=self.keyspace,
            name=self.name,
            full_name=self.full_name,
        )

    @property
    def database(self) -> AsyncDatabase:
        """
        a Database object, the database this collection belongs to.

        Example:
            >>> my_async_coll.database.name
            'the_db'
        """

        return self._database

    @property
    def keyspace(self) -> str:
        """
        The keyspace this collection is in.

        Example:
            >>> my_async_coll.keyspace
            'default_keyspace'
        """

        _keyspace = self.database.keyspace
        if _keyspace is None:
            raise RuntimeError("The collection's DB is set with keyspace=None")
        return _keyspace

    @property
    def name(self) -> str:
        """
        The name of this collection.

        Example:
            >>> my_async_coll.name
            'my_v_collection'
        """

        return self._name

    @property
    def full_name(self) -> str:
        """
        The fully-qualified collection name within the database,
        in the form "keyspace.collection_name".

        Example:
            >>> my_async_coll.full_name
            'default_keyspace.my_v_collection'
        """

        return f"{self.keyspace}.{self.name}"

    async def insert_one(
        self,
        document: DOC,
        *,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> CollectionInsertOneResult:
        """
        Insert a single document in the collection in an atomic operation.

        Args:
            document: the dictionary expressing the document to insert.
                The `_id` field of the document can be left out, in which
                case it will be created automatically.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a CollectionInsertOneResult object.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> async def write_and_count(acol: AsyncCollection) -> None:
            ...     count0 = await acol.count_documents({}, upper_bound=10)
            ...     print("count0", count0)
            ...     await acol.insert_one(
            ...         {
            ...             "age": 30,
            ...             "name": "Smith",
            ...             "food": ["pear", "peach"],
            ...             "likes_fruit": True,
            ...         },
            ...     )
            ...     await acol.insert_one({"_id": "user-123", "age": 50, "name": "Maccio"})
            ...     count1 = await acol.count_documents({}, upper_bound=10)
            ...     print("count1", count1)
            ...
            >>> asyncio.run(write_and_count(my_async_coll))
            count0 0
            count1 2

            >>> asyncio.run(my_async_coll.insert_one({"tag": v", "$vector": [10, 11]}))
            CollectionInsertOneResult(...)

        Note:
            If an `_id` is explicitly provided, which corresponds to a document
            that exists already in the collection, an error is raised and
            the insertion fails.
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        io_payload = {"insertOne": {"document": document}}
        logger.info(f"insertOne on '{self.name}'")
        io_response = await self._converted_request(
            payload=io_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        logger.info(f"finished insertOne on '{self.name}'")
        if "insertedIds" in io_response.get("status", {}):
            if io_response["status"]["insertedIds"]:
                inserted_id = io_response["status"]["insertedIds"][0]
                return CollectionInsertOneResult(
                    raw_results=[io_response],
                    inserted_id=inserted_id,
                )
            else:
                raise UnexpectedDataAPIResponseException(
                    text="Faulty response from insert_one API command.",
                    raw_response=io_response,
                )
        else:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from insert_one API command.",
                raw_response=io_response,
            )

    async def insert_many(
        self,
        documents: Iterable[DOC],
        *,
        ordered: bool = False,
        chunk_size: int | None = None,
        concurrency: int | None = None,
        request_timeout_ms: int | None = None,
        general_method_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> CollectionInsertManyResult:
        """
        Insert a list of documents into the collection.
        This is not an atomic operation.

        Args:
            documents: an iterable of dictionaries, each a document to insert.
                Documents may specify their `_id` field or leave it out, in which
                case it will be added automatically.
            ordered: if False (default), the insertions can occur in arbitrary order
                and possibly concurrently. If True, they are processed sequentially.
                If there are no specific reasons against it, unordered insertions are to
                be preferred as they complete much faster.
            chunk_size: how many documents to include in a single API request.
                Exceeding the server maximum allowed value results in an error.
                Leave it unspecified (recommended) to use the system default.
            concurrency: maximum number of concurrent requests to the API at
                a given time. It cannot be more than one for ordered insertions.
            request_timeout_ms: a timeout, in milliseconds, for each API request.
                If not passed, the collection-level setting is used instead.
            general_method_timeout_ms: a timeout, in milliseconds, for the whole
                requested operation (which may involve multiple API requests).
                If not passed, the collection-level setting is used instead.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a CollectionInsertManyResult object.

        Examples:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> async def write_and_count(acol: AsyncCollection) -> None:
            ...             count0 = await acol.count_documents({}, upper_bound=10)
            ...             print("count0", count0)
            ...             im_result1 = await acol.insert_many(
            ...                 [
            ...                     {"a": 10},
            ...                     {"a": 5},
            ...                     {"b": [True, False, False]},
            ...                 ],
            ...                 ordered=True,
            ...             )
            ...             print("inserted1", im_result1.inserted_ids)
            ...             count1 = await acol.count_documents({}, upper_bound=100)
            ...             print("count1", count1)
            ...             await acol.insert_many(
            ...                 [{"seq": i} for i in range(50)],
            ...                 concurrency=5,
            ...             )
            ...             count2 = await acol.count_documents({}, upper_bound=100)
            ...             print("count2", count2)
            ...
            >>> asyncio.run(write_and_count(my_async_coll))
            count0 0
            inserted1 ['e3c2a684-...', '1de4949f-...', '167dacc3-...']
            count1 3
            count2 53
            >>> asyncio.run(my_async_coll.insert_many(
            ...     [
            ...         {"tag": "a", "$vector": [1, 2]},
            ...         {"tag": "b", "$vector": [3, 4]},
            ...     ]
            ... ))
            CollectionInsertManyResult(...)

        Note:
            Unordered insertions are executed with some degree of concurrency,
            so it is usually better to prefer this mode unless the order in the
            document sequence is important.

        Note:
            A failure mode for this command is related to certain faulty documents
            found among those to insert: for example, a document may have an ID
            already found on the collection, or its vector dimension may not
            match the collection setting.

            For an ordered insertion, the method will raise an exception at
            the first such faulty document -- nevertheless, all documents processed
            until then will end up being written to the database.

            For unordered insertions, if the error stems from faulty documents
            the insertion proceeds until exhausting the input documents: then,
            an exception is raised -- and all insertable documents will have been
            written to the database, including those "after" the troublesome ones.

            Errors occurring during an insert_many operation, for that reason,
            may result in a `CollectionInsertManyException` being raised.
            This exception allows to inspect the list of document IDs that were
            successfully inserted, while accessing at the same time the underlying
            "root errors" that made the full method call to fail.
        """

        _general_method_timeout_ms, _gmt_label = _first_valid_timeout(
            (general_method_timeout_ms, "general_method_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (
                self.api_options.timeout_options.general_method_timeout_ms,
                "general_method_timeout_ms",
            ),
        )
        _request_timeout_ms, _rt_label = _first_valid_timeout(
            (request_timeout_ms, "request_timeout_ms"),
            (self.api_options.timeout_options.request_timeout_ms, "request_timeout_ms"),
        )
        if concurrency is None:
            if ordered:
                _concurrency = 1
            else:
                _concurrency = DEFAULT_INSERT_MANY_CONCURRENCY
        else:
            _concurrency = concurrency
        if _concurrency > 1 and ordered:
            raise ValueError("Cannot run ordered insert_many concurrently.")
        if chunk_size is None:
            _chunk_size = DEFAULT_INSERT_MANY_CHUNK_SIZE
        else:
            _chunk_size = chunk_size
        _documents = list(documents)
        logger.info(f"inserting {len(_documents)} documents in '{self.name}'")
        raw_results: list[dict[str, Any]] = []
        im_payloads: list[dict[str, Any]] = []
        timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=_general_method_timeout_ms,
            timeout_label=_gmt_label,
        )
        if ordered:
            options = {"ordered": True, "returnDocumentResponses": True}
            inserted_ids: list[Any] = []
            for i in range(0, len(_documents), _chunk_size):
                im_payload = {
                    "insertMany": {
                        "documents": _documents[i : i + _chunk_size],
                        "options": options,
                    },
                }
                logger.info(f"insertMany(chunk) on '{self.name}'")
                chunk_response = await self._converted_request(
                    payload=im_payload,
                    raise_api_errors=False,
                    timeout_context=timeout_manager.remaining_timeout(
                        cap_time_ms=_request_timeout_ms,
                        cap_timeout_label=_rt_label,
                    ),
                )
                logger.info(f"finished insertMany(chunk) on '{self.name}'")
                # accumulate the results in this call
                chunk_inserted_ids = [
                    doc_resp["_id"]
                    for doc_resp in (chunk_response.get("status") or {}).get(
                        "documentResponses", []
                    )
                    if doc_resp["status"] == "OK"
                ]
                inserted_ids += chunk_inserted_ids
                raw_results += [chunk_response]
                im_payloads += [im_payload]
                # if errors, quit early
                if chunk_response.get("errors", []):
                    response_exception = DataAPIResponseException.from_response(
                        command=im_payload,
                        raw_response=chunk_response,
                    )
                    raise CollectionInsertManyException(
                        inserted_ids=inserted_ids, exceptions=[response_exception]
                    )

            # return
            full_result = CollectionInsertManyResult(
                raw_results=raw_results,
                inserted_ids=inserted_ids,
            )
            logger.info(
                f"finished inserting {len(_documents)} documents in '{self.name}'"
            )
            return full_result

        else:
            # unordered: concurrent or not, do all of them and parse the results
            options = {"ordered": False, "returnDocumentResponses": True}

            sem = asyncio.Semaphore(_concurrency)

            async def concurrent_insert_chunk(
                document_chunk: list[DOC],
            ) -> tuple[dict[str, Any], dict[str, Any]]:
                async with sem:
                    im_payload = {
                        "insertMany": {
                            "documents": document_chunk,
                            "options": options,
                        },
                    }
                    logger.info(f"insertMany(chunk) on '{self.name}'")
                    im_response = await self._converted_request(
                        payload=im_payload,
                        raise_api_errors=False,
                        timeout_context=timeout_manager.remaining_timeout(
                            cap_time_ms=_request_timeout_ms,
                            cap_timeout_label=_rt_label,
                        ),
                    )
                    logger.info(f"finished insertMany(chunk) on '{self.name}'")
                    return im_payload, im_response

            raw_pl_results_pairs: list[tuple[dict[str, Any], dict[str, Any]]]
            if _concurrency > 1:
                tasks = [
                    asyncio.create_task(
                        concurrent_insert_chunk(_documents[i : i + _chunk_size])
                    )
                    for i in range(0, len(_documents), _chunk_size)
                ]
                raw_pl_results_pairs = await asyncio.gather(*tasks)
            else:
                raw_pl_results_pairs = [
                    await concurrent_insert_chunk(_documents[i : i + _chunk_size])
                    for i in range(0, len(_documents), _chunk_size)
                ]

            if raw_pl_results_pairs:
                im_payloads, raw_results = list(zip(*raw_pl_results_pairs))
            else:
                im_payloads, raw_results = [], []

            # recast raw_results
            inserted_ids = [
                doc_resp["_id"]
                for chunk_response in raw_results
                for doc_resp in (chunk_response.get("status") or {}).get(
                    "documentResponses", []
                )
                if doc_resp["status"] == "OK"
            ]

            # check-raise
            response_exceptions = [
                DataAPIResponseException.from_response(
                    command=chunk_payload,
                    raw_response=chunk_response,
                )
                for chunk_payload, chunk_response in zip(im_payloads, raw_results)
                if chunk_response.get("errors", [])
            ]
            if response_exceptions:
                raise CollectionInsertManyException(
                    inserted_ids=inserted_ids,
                    exceptions=response_exceptions,
                )

            # return
            full_result = CollectionInsertManyResult(
                raw_results=raw_results,
                inserted_ids=inserted_ids,
            )
            logger.info(
                f"finished inserting {len(_documents)} documents in '{self.name}'"
            )
            return full_result

    @overload
    def find(
        self,
        filter: FilterType | None = None,
        *,
        projection: ProjectionType | None = None,
        document_type: None = None,
        skip: int | None = None,
        limit: int | None = None,
        include_similarity: bool | None = None,
        include_sort_vector: bool | None = None,
        sort: SortType | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> AsyncCollectionFindCursor[DOC, DOC]: ...

    @overload
    def find(
        self,
        filter: FilterType | None = None,
        *,
        projection: ProjectionType | None = None,
        document_type: type[DOC2],
        skip: int | None = None,
        limit: int | None = None,
        include_similarity: bool | None = None,
        include_sort_vector: bool | None = None,
        sort: SortType | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> AsyncCollectionFindCursor[DOC, DOC2]: ...

    def find(
        self,
        filter: FilterType | None = None,
        *,
        projection: ProjectionType | None = None,
        document_type: type[DOC2] | None = None,
        skip: int | None = None,
        limit: int | None = None,
        include_similarity: bool | None = None,
        include_sort_vector: bool | None = None,
        sort: SortType | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> AsyncCollectionFindCursor[DOC, DOC2]:
        """
        Find documents on the collection, matching a certain provided filter.

        The method returns a cursor that can then be iterated over. Depending
        on the method call pattern, the iteration over all documents can reflect
        collection mutations occurred since the `find` method was called, or not.
        In cases where the cursor reflects mutations in real-time, it will iterate
        over cursors in an approximate way (i.e. exhibiting occasional skipped
        or duplicate documents). This happens when making use of the `sort`
        option in a non-vector-search manner.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
            projection: it controls which parts of the document are returned.
                It can be an allow-list: `{"f1": True, "f2": True}`,
                or a deny-list: `{"fx": False, "fy": False}`, but not a mixture
                (except for the `_id` and other special fields, which can be
                associated to both True or False independently of the rest
                of the specification).
                The special star-projections `{"*": True}` and `{"*": False}`
                have the effect of returning the whole document and `{}` respectively.
                For lists in documents, slice directives can be passed to select
                portions of the list: for instance, `{"array": {"$slice": 2}}`,
                `{"array": {"$slice": -2}}`, `{"array": {"$slice": [4, 2]}}` or
                `{"array": {"$slice": [-4, 2]}}`.
                An iterable over strings will be treated implicitly as an allow-list.
                The default projection (used if this parameter is not passed) does not
                necessarily include "special" fields such as `$vector` or `$vectorize`.
                See the Data API documentation for more on projections.
            document_type: this parameter acts a formal specifier for the type checker.
                If omitted, the resulting cursor is implicitly an
                `AsyncCollectionFindCursor[DOC, DOC]`, i.e. maintains the same type for
                the items it returns as that for the documents in the collection.
                Strictly typed code may want to specify this parameter especially when
                a projection is given.
            skip: with this integer parameter, what would be the first `skip`
                documents returned by the query are discarded, and the results
                start from the (skip+1)-th document.
                This parameter can be used only in conjunction with an explicit
                `sort` criterion of the ascending/descending type (i.e. it cannot
                be used when not sorting, nor with vector-based ANN search).
            limit: this (integer) parameter sets a limit over how many documents
                are returned. Once `limit` is reached (or the cursor is exhausted
                for lack of matching documents), nothing more is returned.
            include_similarity: a boolean to request the numeric value of the
                similarity to be returned as an added "$similarity" key in each
                returned document. It can be used meaningfully only in a vector
                search (see `sort`).
            include_sort_vector: a boolean to request the search query vector.
                If set to True (and if the invocation is a vector search), calling
                the `get_sort_vector` method on the returned cursor will yield
                the vector used for the ANN search.
            sort: with this dictionary parameter one can control the order
                the documents are returned. See the Note about sorting, as well as
                the one about upper bounds, for details.
                Vector-based ANN sorting is achieved by providing a "$vector"
                or a "$vectorize" key in `sort`.
            request_timeout_ms: a timeout, in milliseconds, for each single one
                of the underlying HTTP requests used to fetch documents as the
                cursor is iterated over.
                If not passed, the collection-level setting is used instead.
            timeout_ms: an alias for `request_timeout_ms`.

        Returns:
            a AsyncCollectionFindCursor object, that can be iterated over (and
            manipulated in several ways). The cursor, if needed, handles pagination
            under the hood as the documents are consumed.

        Examples:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> async def run_finds(acol: AsyncCollection) -> None:
            ...             filter = {"seq": {"$exists": True}}
            ...             print("find results 1:")
            ...             async for doc in acol.find(filter, projection={"seq": True}, limit=5):
            ...                 print(doc["seq"])
            ...             async_cursor1 = acol.find(
            ...                 {},
            ...                 limit=4,
            ...                 sort={"seq": astrapy.constants.SortMode.DESCENDING},
            ...             )
            ...             ids = [doc["_id"] async for doc in async_cursor1]
            ...             print("find results 2:", ids)
            ...
            >>> asyncio.run(run_finds(my_async_coll))
            find results 1:
            48
            35
            7
            11
            13
            find results 2: ['d656cd9d-...', '479c7ce8-...', '96dc87fd-...', '83f0a21f-...']

            >>> async def run_vector_finds(acol: AsyncCollection) -> None:
            ...     await acol.insert_many([
            ...         {"tag": "A", "$vector": [4, 5]},
            ...         {"tag": "B", "$vector": [3, 4]},
            ...         {"tag": "C", "$vector": [3, 2]},
            ...         {"tag": "D", "$vector": [4, 1]},
            ...         {"tag": "E", "$vector": [2, 5]},
            ...     ])
            ...     ann_tags = [
            ...         document["tag"]
            ...         async for document in acol.find(
            ...             {},
            ...             sort={"$vector": [3, 3]},
            ...             limit=3,
            ...         )
            ...     ]
            ...     return ann_tags
            ...
            >>> asyncio.run(run_vector_finds(my_async_coll))
            ['A', 'B', 'C']
            >>> # (assuming the collection has metric VectorMetric.COSINE)

            >>> async_cursor = my_async_coll.find(
            ...     sort={"$vector": [3, 3]},
            ...     limit=3,
            ...     include_sort_vector=True,
            ... )
            >>> asyncio.run(async_cursor.get_sort_vector())
            [3.0, 3.0]
            >>> asyncio.run(async_cursor.__anext__())
            {'_id': 'b13ce177-738e-47ec-bce1-77738ee7ec93', 'tag': 'A'}
            >>> asyncio.run(async_cursor.get_sort_vector())
            [3.0, 3.0]

        Note:
            The following are example values for the `sort` parameter.
            When no particular order is required:
                sort={}
            When sorting by a certain value in ascending/descending order:
                sort={"field": SortMode.ASCENDING}
                sort={"field": SortMode.DESCENDING}
            When sorting first by "field" and then by "subfield"
            (while modern Python versions preserve the order of dictionaries,
            it is suggested for clarity to employ a `collections.OrderedDict`
            in these cases):
                sort={
                    "field": SortMode.ASCENDING,
                    "subfield": SortMode.ASCENDING,
                }
            When running a vector similarity (ANN) search:
                sort={"$vector": [0.4, 0.15, -0.5]}

        Note:
            Some combinations of arguments impose an implicit upper bound on the
            number of documents that are returned by the Data API. More specifically:
            (a) Vector ANN searches cannot return more than a number of documents
            that at the time of writing is set to 1000 items.
            (b) When using a sort criterion of the ascending/descending type,
            the Data API will return a smaller number of documents, set to 20
            at the time of writing, and stop there. The returned documents are
            the top results across the whole collection according to the requested
            criterion.

        Note:
            When not specifying sorting criteria at all (by vector or otherwise),
            the cursor can scroll through an arbitrary number of documents as
            the Data API and the client periodically exchange new chunks of documents.
            It should be noted that the behavior of the cursor in the case documents
            have been added/removed after the `find` was started depends on database
            internals and it is not guaranteed, nor excluded, that such "real-time"
            changes in the data would be picked up by the cursor.
        """

        # lazy-import here to avoid circular import issues
        from astrapy.cursors import AsyncCollectionFindCursor

        _request_timeout_ms, _rt_label = _first_valid_timeout(
            (request_timeout_ms, "request_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (self.api_options.timeout_options.request_timeout_ms, "request_timeout_ms"),
        )
        return (
            AsyncCollectionFindCursor(
                collection=self,
                request_timeout_ms=_request_timeout_ms,
                overall_timeout_ms=None,
                request_timeout_label=_rt_label,
            )
            .filter(filter)
            .project(projection)
            .skip(skip)
            .limit(limit)
            .sort(sort)
            .include_similarity(include_similarity)
            .include_sort_vector(include_sort_vector)
        )

    async def find_one(
        self,
        filter: FilterType | None = None,
        *,
        projection: ProjectionType | None = None,
        include_similarity: bool | None = None,
        sort: SortType | None = None,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> DOC | None:
        """
        Run a search, returning the first document in the collection that matches
        provided filters, if any is found.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
            projection: it controls which parts of the document are returned.
                It can be an allow-list: `{"f1": True, "f2": True}`,
                or a deny-list: `{"fx": False, "fy": False}`, but not a mixture
                (except for the `_id` and other special fields, which can be
                associated to both True or False independently of the rest
                of the specification).
                The special star-projections `{"*": True}` and `{"*": False}`
                have the effect of returning the whole document and `{}` respectively.
                For lists in documents, slice directives can be passed to select
                portions of the list: for instance, `{"array": {"$slice": 2}}`,
                `{"array": {"$slice": -2}}`, `{"array": {"$slice": [4, 2]}}` or
                `{"array": {"$slice": [-4, 2]}}`.
                An iterable over strings will be treated implicitly as an allow-list.
                The default projection (used if this parameter is not passed) does not
                necessarily include "special" fields such as `$vector` or `$vectorize`.
                See the Data API documentation for more on projections.
            include_similarity: a boolean to request the numeric value of the
                similarity to be returned as an added "$similarity" key in the
                returned document. It can be used meaningfully only in a vector
                search (see `sort`).
            sort: with this dictionary parameter one can control the order
                the documents are returned. See the Note about sorting for details.
                Vector-based ANN sorting is achieved by providing a "$vector"
                or a "$vectorize" key in `sort`.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a dictionary expressing the required document, otherwise None.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> async def demo_find_one(acol: AsyncCollection) -> None:
            ....    print("Count:", await acol.count_documents({}, upper_bound=100))
            ...     result0 = await acol.find_one({})
            ...     print("result0", result0)
            ...     result1 = await acol.find_one({"seq": 10})
            ...     print("result1", result1)
            ...     result2 = await acol.find_one({"seq": 1011})
            ...     print("result2", result2)
            ...     result3 = await acol.find_one({}, projection={"seq": False})
            ...     print("result3", result3)
            ...     result4 = await acol.find_one(
            ...         {},
            ...         sort={"seq": astrapy.constants.SortMode.DESCENDING},
            ...     )
            ...     print("result4", result4)
            ...
            >>>
            >>> asyncio.run(demo_find_one(my_async_coll))
            Count: 50
            result0 {'_id': '479c7ce8-...', 'seq': 48}
            result1 {'_id': '93e992c4-...', 'seq': 10}
            result2 None
            result3 {'_id': '479c7ce8-...'}
            result4 {'_id': 'd656cd9d-...', 'seq': 49}

            >>> asyncio.run(my_async_coll.find_one(
            ...     {},
            ...     sort={"$vector": [1, 0]},
            ...     projection={"*": True},
            ... ))
            {'_id': '...', 'tag': 'D', '$vector': [4.0, 1.0]}

        Note:
            See the `find` method for more details on the accepted parameters
            (whereas `skip` and `limit` are not valid parameters for `find_one`).
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        fo_options = (
            None
            if include_similarity is None
            else {"includeSimilarity": include_similarity}
        )
        fo_payload = {
            "findOne": {
                k: v
                for k, v in {
                    "filter": filter,
                    "projection": normalize_optional_projection(projection),
                    "options": fo_options,
                    "sort": sort,
                }.items()
                if v is not None
            }
        }
        fo_response = await self._converted_request(
            payload=fo_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        if "document" not in (fo_response.get("data") or {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from findOne API command.",
                raw_response=fo_response,
            )
        doc_response = fo_response["data"]["document"]
        if doc_response is None:
            return None
        return fo_response["data"]["document"]  # type: ignore[no-any-return]

    async def distinct(
        self,
        key: str | Iterable[str | int],
        *,
        filter: FilterType | None = None,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[Any]:
        """
        Return a list of the unique values of `key` across the documents
        in the collection that match the provided filter.

        Args:
            key: the name of the field whose value is inspected across documents.
                Keys can be just field names (as is often the case), but
                the dot-notation is also accepted to mean subkeys or indices
                within lists (for example, "map_field.subkey" or "list_field.2").
                If a field has literal dots or ampersands in its name, this
                parameter must be escaped to be treated properly.
                The key can also be a list of strings and numbers, in which case
                no escape is necessary: each item in the list is a field name/index,
                for example ["map_field", "subkey"] or ["list_field", 2].
                If lists are encountered and no numeric index is specified,
                all items in the list are visited.
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
            general_method_timeout_ms: a timeout, in milliseconds, for the whole
                requested operation (which may involve multiple API requests).
                This method, being based on `find` (see) may entail successive HTTP API
                requests, depending on the amount of involved documents.
            request_timeout_ms: a timeout, in milliseconds, for each API request.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a list of all different values for `key` found across the documents
            that match the filter. The result list has no repeated items.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> async def run_distinct(acol: AsyncCollection) -> None:
            ...     await acol.insert_many(
            ...         [
            ...             {"name": "Marco", "food": ["apple", "orange"], "city": "Helsinki"},
            ...             {"name": "Emma", "food": {"likes_fruit": True, "allergies": []}},
            ...         ]
            ...     )
            ...     distinct0 = await acol.distinct("name")
            ...     print("distinct('name')", distinct0)
            ...     distinct1 = await acol.distinct("city")
            ...     print("distinct('city')", distinct1)
            ...     distinct2 = await acol.distinct("food")
            ...     print("distinct('food')", distinct2)
            ...     distinct3 = await acol.distinct("food.1")
            ...     print("distinct('food.1')", distinct3)
            ...     distinct4 = await acol.distinct("food.allergies")
            ...     print("distinct('food.allergies')", distinct4)
            ...     distinct5 = await acol.distinct("food.likes_fruit")
            ...     print("distinct('food.likes_fruit')", distinct5)
            ...
            >>> asyncio.run(run_distinct(my_async_coll))
            distinct('name') ['Emma', 'Marco']
            distinct('city') ['Helsinki']
            distinct('food') [{'likes_fruit': True, 'allergies': []}, 'apple', 'orange']
            distinct('food.1') ['orange']
            distinct('food.allergies') []
            distinct('food.likes_fruit') [True]

        Note:
            It must be kept in mind that `distinct` is a client-side operation,
            which effectively browses all required documents using the logic
            of the `find` method and collects the unique values found for `key`.
            As such, there may be performance, latency and ultimately
            billing implications if the amount of matching documents is large.

        Note:
            For details on the behaviour of "distinct" in conjunction with
            real-time changes in the collection contents, see the
            Note of the `find` command.
        """

        # lazy-import here to avoid circular import issues
        from astrapy.cursors import AsyncCollectionFindCursor

        _general_method_timeout_ms, _gmt_label = _first_valid_timeout(
            (general_method_timeout_ms, "general_method_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (
                self.api_options.timeout_options.general_method_timeout_ms,
                "general_method_timeout_ms",
            ),
        )
        _request_timeout_ms, _rt_label = _first_valid_timeout(
            (request_timeout_ms, "request_timeout_ms"),
            (self.api_options.timeout_options.request_timeout_ms, "request_timeout_ms"),
        )
        # preparing cursor:
        _extractor = _create_document_key_extractor(key)
        _key = _reduce_distinct_key_to_safe(key)
        # relaxing the type hint (limited to within this method body)
        f_cursor: AsyncCollectionFindCursor[dict[str, Any], dict[str, Any]] = (
            AsyncCollectionFindCursor(
                collection=self,
                request_timeout_ms=_request_timeout_ms,
                overall_timeout_ms=_general_method_timeout_ms,
                request_timeout_label=_rt_label,
                overall_timeout_label=_gmt_label,
            )  # type: ignore[assignment]
            .filter(filter)
            .project({_key: True})
        )
        # consuming it:
        _item_hashes = set()
        distinct_items: list[Any] = []
        logger.info(f"running distinct() on '{self.name}'")
        async for document in f_cursor:
            for item in _extractor(document):
                _item_hash = _hash_collection_document(
                    item, options=self.api_options.serdes_options
                )
                if _item_hash not in _item_hashes:
                    _item_hashes.add(_item_hash)
                    distinct_items.append(item)
        logger.info(f"finished running distinct() on '{self.name}'")
        return distinct_items

    @overload
    def find_and_rerank(
        self,
        filter: FilterType | None = None,
        *,
        sort: HybridSortType,
        projection: ProjectionType | None = None,
        document_type: None = None,
        limit: int | None = None,
        hybrid_limits: int | dict[str, int] | None = None,
        include_scores: bool | None = None,
        include_sort_vector: bool | None = None,
        rerank_on: str | None = None,
        rerank_query: str | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> AsyncCollectionFindAndRerankCursor[DOC, RerankedResult[DOC]]: ...

    @overload
    def find_and_rerank(
        self,
        filter: FilterType | None = None,
        *,
        sort: HybridSortType,
        projection: ProjectionType | None = None,
        document_type: type[DOC2],
        limit: int | None = None,
        hybrid_limits: int | dict[str, int] | None = None,
        include_scores: bool | None = None,
        include_sort_vector: bool | None = None,
        rerank_on: str | None = None,
        rerank_query: str | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> AsyncCollectionFindAndRerankCursor[DOC, RerankedResult[DOC2]]: ...

    @beta_method
    def find_and_rerank(
        self,
        filter: FilterType | None = None,
        *,
        sort: HybridSortType,
        projection: ProjectionType | None = None,
        document_type: type[DOC2] | None = None,
        limit: int | None = None,
        hybrid_limits: int | dict[str, int] | None = None,
        include_scores: bool | None = None,
        include_sort_vector: bool | None = None,
        rerank_on: str | None = None,
        rerank_query: str | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> AsyncCollectionFindAndRerankCursor[DOC, RerankedResult[DOC2]]:
        """
        Find relevant documents, combining vector and lexical matches through reranking.

        For this method to succeed, the collection must be created with the required
        hybrid capabilities (see the `create_collection` method of the Database class).

        The method returns a cursor that can then be iterated over, which yields
        the resulting documents, generally paired with accompanying information
        such as scores.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
            sort: a clause specifying the criteria for selecting the top matching
                documents. This must provide enough information for both a lexical
                and a vector similarity to be performed (the latter either query text
                or by query vector, depending on the collection configuration).
                Examples are: `sort={"$hybrid": "xyz"}`,
                `sort={"$hybrid": {"$vectorize": "xyz", "$lexical": "abc"}}`,
                `sort={"$hybrid": {"$vector": DataAPIVector(...), "$lexical": "abc"}}`.
                Note this differs from the `sort` parameter for the `find` method.
            projection: it controls which parts of the document are returned.
                It can be an allow-list: `{"f1": True, "f2": True}`,
                or a deny-list: `{"fx": False, "fy": False}`, but not a mixture
                (except for the `_id` and other special fields, which can be
                associated to both True or False independently of the rest
                of the specification).
                The special star-projections `{"*": True}` and `{"*": False}`
                have the effect of returning the whole document and `{}` respectively.
                For lists in documents, slice directives can be passed to select
                portions of the list: for instance, `{"array": {"$slice": 2}}`,
                `{"array": {"$slice": -2}}`, `{"array": {"$slice": [4, 2]}}` or
                `{"array": {"$slice": [-4, 2]}}`.
                An iterable over strings will be treated implicitly as an allow-list.
                The default projection (used if this parameter is not passed) does not
                necessarily include "special" fields such as `$vector` or `$vectorize`.
                See the Data API documentation for more on projections.
            document_type: this parameter acts a formal specifier for the type checker.
                If omitted, the resulting cursor is implicitly a
                `AsyncCollectionFindAndRerankCursor[DOC, DOC]`, i.e. maintains the same
                type for the items it returns as that for the documents in the
                collection. Strictly typed code may want to specify this parameter
                especially when a projection is given.
            limit: maximum number of documents to return as the result of the final
                rerank step.
            hybrid_limits: this controls the amount of documents that are fetched by
                each of the individual retrieval operations that are combined in the
                rerank step. It can be either a number or a dictionary of strings to
                numbers, the latter case expressing different counts for the different
                retrievals. For example: `hybrid_limits=50`,
                `hybrid_limits={"$vector": 20, "$lexical": 10}`.
            include_scores: a boolean to request the scores to be returned along with
                the resulting documents. If this is set, the scores can be read in the
                the map `scores` attribute of each RerankedResult (the map is
                otherwise empty).
            include_sort_vector: a boolean to request the search query vector
                used for the vector-search part of the find operation.
                If set to True, calling the `get_sort_vector` method on the returned
                cursor will yield the vector used for the ANN search.
            rerank_on: for collections without a vectorize (server-side embeddings)
                service, this is used to specify the field name that is then used
                during reranking.
            rerank_query: for collections without a vectorize (server-side embeddings)
                service, this is used to specify the query text for the reranker.
            request_timeout_ms: a timeout, in milliseconds, for each single one
                of the underlying HTTP requests used to fetch documents as the
                cursor is iterated over.
                If not passed, the collection-level setting is used instead.
            timeout_ms: an alias for `request_timeout_ms`.

        Returns:
            an AsyncCollectionFindAndRerankCursor object, that can be iterated over
            (and manipulated in several ways).

        Examples:
            >>> # NOTE: may require slight adaptation to an async context.
            >>> #       See the same method on Collection for more usage patterns.
            >>>
            >>> async def run_find_and_reranks(acol: AsyncCollection) -> None:
            ...     print("find results 1:")
            ...     async for r_res in acol.find_and_rerank(
            ...         sort={"$hybrid": "query text"},
            ...         limit=3,
            ...     ):
            ...         print(r_res.document["wkd"])
            ...     async_cursor1 = acol.find_and_rerank(
            ...         {"wkd": {"$ne": "Mon"}},
            ...         sort={"$hybrid": "query text"},
            ...         limit=3,
            ...     )
            ...     ids = [r_res.document["_id"] async for r_res in async_cursor1]
            ...     print("find results 2:", ids)
            ...
            >>> asyncio.run(run_find_and_reranks(my_async_coll))
            find results 1:
            Mon
            Thu
            Sat
            find results 2: ['D', 'F', 'B']
        """

        # lazy-import here to avoid circular import issues
        from astrapy.cursors import AsyncCollectionFindAndRerankCursor

        _request_timeout_ms, _rt_label = _first_valid_timeout(
            (request_timeout_ms, "request_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (self.api_options.timeout_options.request_timeout_ms, "request_timeout_ms"),
        )
        return (
            AsyncCollectionFindAndRerankCursor(
                collection=self,
                request_timeout_ms=_request_timeout_ms,
                overall_timeout_ms=None,
                request_timeout_label=_rt_label,
            )
            .filter(filter)
            .project(projection)
            .limit(limit)
            .sort(sort)
            .hybrid_limits(hybrid_limits)
            .rerank_on(rerank_on)
            .rerank_query(rerank_query)
            .include_scores(include_scores)
            .include_sort_vector(include_sort_vector)
        )

    async def count_documents(
        self,
        filter: FilterType,
        *,
        upper_bound: int,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> int:
        """
        Count the documents in the collection matching the specified filter.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
            upper_bound: a required ceiling on the result of the count operation.
                If the actual number of documents exceeds this value,
                an exception will be raised.
                Furthermore, if the actual number of documents exceeds the maximum
                count that the Data API can reach (regardless of upper_bound),
                an exception will be raised.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            the exact count of matching documents.

        Example:
            >>> async def do_count_docs(acol: AsyncCollection) -> None:
            ...     await acol.insert_many([{"seq": i} for i in range(20)])
            ...     count0 = await acol.count_documents({}, upper_bound=100)
            ...     print("count0", count0)
            ...     count1 = await acol.count_documents(
            ...         {"seq":{"$gt": 15}}, upper_bound=100
            ...     )
            ...     print("count1", count1)
            ...     count2 = await acol.count_documents({}, upper_bound=10)
            ...     print("count2", count2)
            ...
            >>> asyncio.run(do_count_docs(my_async_coll))
            count0 20
            count1 4
            Traceback (most recent call last):
                ... ...
            astrapy.exceptions.TooManyDocumentsToCountException

        Note:
            Count operations are expensive: for this reason, the best practice
            is to provide a reasonable `upper_bound` according to the caller
            expectations. Moreover, indiscriminate usage of count operations
            for sizeable amounts of documents (i.e. in the thousands and more)
            is discouraged in favor of alternative application-specific solutions.
            Keep in mind that the Data API has a hard upper limit on the amount
            of documents it will count, and that an exception will be thrown
            by this method if this limit is encountered.
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        cd_payload = {"countDocuments": {"filter": filter}}
        logger.info(f"countDocuments on '{self.name}'")
        cd_response = await self._converted_request(
            payload=cd_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        logger.info(f"finished countDocuments on '{self.name}'")
        if "count" in cd_response.get("status", {}):
            count: int = cd_response["status"]["count"]
            if cd_response["status"].get("moreData", False):
                raise TooManyDocumentsToCountException(
                    text=f"Document count exceeds {count}, the maximum allowed by the server",
                    server_max_count_exceeded=True,
                )
            else:
                if count > upper_bound:
                    raise TooManyDocumentsToCountException(
                        text="Document count exceeds required upper bound",
                        server_max_count_exceeded=False,
                    )
                else:
                    return count
        else:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from countDocuments API command.",
                raw_response=cd_response,
            )

    async def estimated_document_count(
        self,
        *,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> int:
        """
        Query the API server for an estimate of the document count in the collection.

        Contrary to `count_documents`, this method has no filtering parameters.

        Args:
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a server-provided estimate count of the documents in the collection.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> asyncio.run(my_async_coll.estimated_document_count())
            35700
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        ed_payload: dict[str, Any] = {"estimatedDocumentCount": {}}
        logger.info(f"estimatedDocumentCount on '{self.name}'")
        ed_response = await self._converted_request(
            payload=ed_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        logger.info(f"finished estimatedDocumentCount on '{self.name}'")
        if "count" in ed_response.get("status", {}):
            count: int = ed_response["status"]["count"]
            return count
        else:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from estimatedDocumentCount API command.",
                raw_response=ed_response,
            )

    async def find_one_and_replace(
        self,
        filter: FilterType,
        replacement: DOC,
        *,
        projection: ProjectionType | None = None,
        sort: SortType | None = None,
        upsert: bool = False,
        return_document: str = ReturnDocument.BEFORE,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> DOC | None:
        """
        Find a document on the collection and replace it entirely with a new one,
        optionally inserting a new one if no match is found.

        Args:

            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
            replacement: the new document to write into the collection.
            projection: it controls which parts of the document are returned.
                It can be an allow-list: `{"f1": True, "f2": True}`,
                or a deny-list: `{"fx": False, "fy": False}`, but not a mixture
                (except for the `_id` and other special fields, which can be
                associated to both True or False independently of the rest
                of the specification).
                The special star-projections `{"*": True}` and `{"*": False}`
                have the effect of returning the whole document and `{}` respectively.
                For lists in documents, slice directives can be passed to select
                portions of the list: for instance, `{"array": {"$slice": 2}}`,
                `{"array": {"$slice": -2}}`, `{"array": {"$slice": [4, 2]}}` or
                `{"array": {"$slice": [-4, 2]}}`.
                An iterable over strings will be treated implicitly as an allow-list.
                The default projection (used if this parameter is not passed) does not
                necessarily include "special" fields such as `$vector` or `$vectorize`.
                See the Data API documentation for more on projections.
            sort: with this dictionary parameter one can control the sorting
                order of the documents matching the filter, effectively
                determining what document will come first and hence be the
                replaced one. See the `find` method for more on sorting.
                Vector-based ANN sorting is achieved by providing a "$vector"
                or a "$vectorize" key in `sort`.
            upsert: this parameter controls the behavior in absence of matches.
                If True, `replacement` is inserted as a new document
                if no matches are found on the collection. If False,
                the operation silently does nothing in case of no matches.
            return_document: a flag controlling what document is returned:
                if set to `ReturnDocument.BEFORE`, or the string "before",
                the document found on database is returned; if set to
                `ReturnDocument.AFTER`, or the string "after", the new
                document is returned. The default is "before".
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            A document, either the one before the replace operation or the
            one after that. Alternatively, the method returns None to represent
            that no matching document was found, or that no replacement
            was inserted (depending on the `return_document` parameter).

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> async def do_find_one_and_replace(
            ...     acol: AsyncCollection
            ... ) -> None:
            ...     await acol.insert_one(
            ...         {"_id": "rule1", "text": "all animals are equal"}
            ...     )
            ...     result0 = await acol.find_one_and_replace(
            ...         {"_id": "rule1"},
            ...         {"text": "some animals are more equal!"},
            ...     )
            ...     print("result0", result0)
            ...     result1 = await acol.find_one_and_replace(
            ...         {"text": "some animals are more equal!"},
            ...         {"text": "and the pigs are the rulers"},
            ...         return_document=astrapy.constants.ReturnDocument.AFTER,
            ...     )
            ...     print("result1", result1)
            ...     result2 = await acol.find_one_and_replace(
            ...         {"_id": "rule2"},
            ...         {"text": "F=ma^2"},
            ...         return_document=astrapy.constants.ReturnDocument.AFTER,
            ...     )
            ...     print("result2", result2)
            ...     result3 = await acol.find_one_and_replace(
            ...         {"_id": "rule2"},
            ...         {"text": "F=ma"},
            ...         upsert=True,
            ...         return_document=astrapy.constants.ReturnDocument.AFTER,
            ...         projection={"_id": False},
            ...     )
            ...     print("result3", result3)
            ...
            >>> asyncio.run(do_find_one_and_replace(my_async_coll))
            result0 {'_id': 'rule1', 'text': 'all animals are equal'}
            result1 {'_id': 'rule1', 'text': 'and the pigs are the rulers'}
            result2 None
            result3 {'text': 'F=ma'}
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        options = {
            "returnDocument": return_document,
            "upsert": upsert,
        }
        fo_payload = {
            "findOneAndReplace": {
                k: v
                for k, v in {
                    "filter": filter,
                    "projection": normalize_optional_projection(projection),
                    "replacement": replacement,
                    "options": options,
                    "sort": sort,
                }.items()
                if v is not None
            }
        }
        logger.info(f"findOneAndReplace on '{self.name}'")
        fo_response = await self._converted_request(
            payload=fo_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        logger.info(f"finished findOneAndReplace on '{self.name}'")
        if "document" in fo_response.get("data", {}):
            ret_document = fo_response.get("data", {}).get("document")
            if ret_document is None:
                return None
            else:
                return ret_document  # type: ignore[no-any-return]
        else:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from find_one_and_replace API command.",
                raw_response=fo_response,
            )

    async def replace_one(
        self,
        filter: FilterType,
        replacement: DOC,
        *,
        sort: SortType | None = None,
        upsert: bool = False,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> CollectionUpdateResult:
        """
        Replace a single document on the collection with a new one,
        optionally inserting a new one if no match is found.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
            replacement: the new document to write into the collection.
            sort: with this dictionary parameter one can control the sorting
                order of the documents matching the filter, effectively
                determining what document will come first and hence be the
                replaced one. See the `find` method for more on sorting.
                Vector-based ANN sorting is achieved by providing a "$vector"
                or a "$vectorize" key in `sort`.
            upsert: this parameter controls the behavior in absence of matches.
                If True, `replacement` is inserted as a new document
                if no matches are found on the collection. If False,
                the operation silently does nothing in case of no matches.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a CollectionUpdateResult object summarizing the outcome of
            the replace operation.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> async def do_replace_one(acol: AsyncCollection) -> None:
            ...     await acol.insert_one({"Marco": "Polo"})
            ...     result0 = await acol.replace_one(
            ...         {"Marco": {"$exists": True}},
            ...         {"Buda": "Pest"},
            ...     )
            ...     print("result0.update_info", result0.update_info)
            ...     doc1 = await acol.find_one({"Buda": "Pest"})
            ...     print("doc1", doc1)
            ...     result1 = await acol.replace_one(
            ...         {"Mirco": {"$exists": True}},
            ...         {"Oh": "yeah?"},
            ...     )
            ...     print("result1.update_info", result1.update_info)
            ...     result2 = await acol.replace_one(
            ...         {"Mirco": {"$exists": True}},
            ...         {"Oh": "yeah?"},
            ...         upsert=True,
            ...     )
            ...     print("result2.update_info", result2.update_info)
            ...
            >>> asyncio.run(do_replace_one(my_async_coll))
            result0.update_info {'n': 1, 'updatedExisting': True, 'ok': 1.0, 'nModified': 1}
            doc1 {'_id': '6e669a5a-...', 'Buda': 'Pest'}
            result1.update_info {'n': 0, 'updatedExisting': False, 'ok': 1.0, 'nModified': 0}
            result2.update_info {'n': 1, 'updatedExisting': False, 'ok': 1.0, 'nModified': 0, 'upserted': '30e34e00-...'}
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        options = {
            "upsert": upsert,
        }
        fo_payload = {
            "findOneAndReplace": {
                k: v
                for k, v in {
                    "filter": filter,
                    "replacement": replacement,
                    "options": options,
                    "sort": sort,
                }.items()
                if v is not None
            }
        }
        logger.info(f"findOneAndReplace on '{self.name}'")
        fo_response = await self._converted_request(
            payload=fo_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        logger.info(f"finished findOneAndReplace on '{self.name}'")
        if "document" in fo_response.get("data", {}):
            fo_status = fo_response.get("status") or {}
            _update_info = _prepare_update_info([fo_status])
            return CollectionUpdateResult(
                raw_results=[fo_response],
                update_info=_update_info,
            )
        else:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from find_one_and_replace API command.",
                raw_response=fo_response,
            )

    async def find_one_and_update(
        self,
        filter: FilterType,
        update: dict[str, Any],
        *,
        projection: ProjectionType | None = None,
        sort: SortType | None = None,
        upsert: bool = False,
        return_document: str = ReturnDocument.BEFORE,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> DOC | None:
        """
        Find a document on the collection and update it as requested,
        optionally inserting a new one if no match is found.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
            update: the update prescription to apply to the document, expressed
                as a dictionary as per Data API syntax. Examples are:
                    {"$set": {"field": "value}}
                    {"$inc": {"counter": 10}}
                    {"$unset": {"field": ""}}
                See the Data API documentation for the full syntax.
            projection: it controls which parts of the document are returned.
                It can be an allow-list: `{"f1": True, "f2": True}`,
                or a deny-list: `{"fx": False, "fy": False}`, but not a mixture
                (except for the `_id` and other special fields, which can be
                associated to both True or False independently of the rest
                of the specification).
                The special star-projections `{"*": True}` and `{"*": False}`
                have the effect of returning the whole document and `{}` respectively.
                For lists in documents, slice directives can be passed to select
                portions of the list: for instance, `{"array": {"$slice": 2}}`,
                `{"array": {"$slice": -2}}`, `{"array": {"$slice": [4, 2]}}` or
                `{"array": {"$slice": [-4, 2]}}`.
                An iterable over strings will be treated implicitly as an allow-list.
                The default projection (used if this parameter is not passed) does not
                necessarily include "special" fields such as `$vector` or `$vectorize`.
                See the Data API documentation for more on projections.
            sort: with this dictionary parameter one can control the sorting
                order of the documents matching the filter, effectively
                determining what document will come first and hence be the
                replaced one. See the `find` method for more on sorting.
                Vector-based ANN sorting is achieved by providing a "$vector"
                or a "$vectorize" key in `sort`.
            upsert: this parameter controls the behavior in absence of matches.
                If True, a new document (resulting from applying the `update`
                to an empty document) is inserted if no matches are found on
                the collection. If False, the operation silently does nothing
                in case of no matches.
            return_document: a flag controlling what document is returned:
                if set to `ReturnDocument.BEFORE`, or the string "before",
                the document found on database is returned; if set to
                `ReturnDocument.AFTER`, or the string "after", the new
                document is returned. The default is "before".
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            A document (or a projection thereof, as required), either the one
            before the replace operation or the one after that.
            Alternatively, the method returns None to represent
            that no matching document was found, or that no update
            was applied (depending on the `return_document` parameter).

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> async def do_find_one_and_update(acol: AsyncCollection) -> None:
            ...     await acol.insert_one({"Marco": "Polo"})
            ...     result0 = await acol.find_one_and_update(
            ...         {"Marco": {"$exists": True}},
            ...         {"$set": {"title": "Mr."}},
            ...     )
            ...     print("result0", result0)
            ...     result1 = await acol.find_one_and_update(
            ...         {"title": "Mr."},
            ...         {"$inc": {"rank": 3}},
            ...         projection=["title", "rank"],
            ...         return_document=astrapy.constants.ReturnDocument.AFTER,
            ...     )
            ...     print("result1", result1)
            ...     result2 = await acol.find_one_and_update(
            ...         {"name": "Johnny"},
            ...         {"$set": {"rank": 0}},
            ...         return_document=astrapy.constants.ReturnDocument.AFTER,
            ...     )
            ...     print("result2", result2)
            ...     result3 = await acol.find_one_and_update(
            ...         {"name": "Johnny"},
            ...         {"$set": {"rank": 0}},
            ...         upsert=True,
            ...         return_document=astrapy.constants.ReturnDocument.AFTER,
            ...     )
            ...     print("result3", result3)
            ...
            >>> asyncio.run(do_find_one_and_update(my_async_coll))
            result0 {'_id': 'f7c936d3-b0a0-45eb-a676-e2829662a57c', 'Marco': 'Polo'}
            result1 {'_id': 'f7c936d3-b0a0-45eb-a676-e2829662a57c', 'title': 'Mr.', 'rank': 3}
            result2 None
            result3 {'_id': 'db3d678d-14d4-4caa-82d2-d5fb77dab7ec', 'name': 'Johnny', 'rank': 0}
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        options = {
            "returnDocument": return_document,
            "upsert": upsert,
        }
        fo_payload = {
            "findOneAndUpdate": {
                k: v
                for k, v in {
                    "filter": filter,
                    "update": update,
                    "options": options,
                    "sort": sort,
                    "projection": normalize_optional_projection(projection),
                }.items()
                if v is not None
            }
        }
        logger.info(f"findOneAndUpdate on '{self.name}'")
        fo_response = await self._converted_request(
            payload=fo_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        logger.info(f"finished findOneAndUpdate on '{self.name}'")
        if "document" in fo_response.get("data", {}):
            ret_document = fo_response.get("data", {}).get("document")
            if ret_document is None:
                return None
            else:
                return ret_document  # type: ignore[no-any-return]
        else:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from find_one_and_update API command.",
                raw_response=fo_response,
            )

    async def update_one(
        self,
        filter: FilterType,
        update: dict[str, Any],
        *,
        sort: SortType | None = None,
        upsert: bool = False,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> CollectionUpdateResult:
        """
        Update a single document on the collection as requested,
        optionally inserting a new one if no match is found.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
            update: the update prescription to apply to the document, expressed
                as a dictionary as per Data API syntax. Examples are:
                    {"$set": {"field": "value}}
                    {"$inc": {"counter": 10}}
                    {"$unset": {"field": ""}}
                See the Data API documentation for the full syntax.
            sort: with this dictionary parameter one can control the sorting
                order of the documents matching the filter, effectively
                determining what document will come first and hence be the
                replaced one. See the `find` method for more on sorting.
                Vector-based ANN sorting is achieved by providing a "$vector"
                or a "$vectorize" key in `sort`.
            upsert: this parameter controls the behavior in absence of matches.
                If True, a new document (resulting from applying the `update`
                to an empty document) is inserted if no matches are found on
                the collection. If False, the operation silently does nothing
                in case of no matches.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a CollectionUpdateResult object summarizing the outcome of
            the update operation.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> async def do_update_one(acol: AsyncCollection) -> None:
            ...     await acol.insert_one({"Marco": "Polo"})
            ...     result0 = await acol.update_one(
            ...         {"Marco": {"$exists": True}},
            ...         {"$inc": {"rank": 3}},
            ...     )
            ...     print("result0.update_info", result0.update_info)
            ...     result1 = await acol.update_one(
            ...         {"Mirko": {"$exists": True}},
            ...         {"$inc": {"rank": 3}},
            ...     )
            ...     print("result1.update_info", result1.update_info)
            ...     result2 = await acol.update_one(
            ...         {"Mirko": {"$exists": True}},
            ...         {"$inc": {"rank": 3}},
            ...         upsert=True,
            ...     )
            ...     print("result2.update_info", result2.update_info)
            ...
            >>> asyncio.run(do_update_one(my_async_coll))
            result0.update_info {'n': 1, 'updatedExisting': True, 'ok': 1.0, 'nModified': 1})
            result1.update_info {'n': 0, 'updatedExisting': False, 'ok': 1.0, 'nModified': 0})
            result2.update_info {'n': 1, 'updatedExisting': False, 'ok': 1.0, 'nModified': 0, 'upserted': '75748092-...'}
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        options = {
            "upsert": upsert,
        }
        uo_payload = {
            "updateOne": {
                k: v
                for k, v in {
                    "filter": filter,
                    "update": update,
                    "options": options,
                    "sort": sort,
                }.items()
                if v is not None
            }
        }
        logger.info(f"updateOne on '{self.name}'")
        uo_response = await self._converted_request(
            payload=uo_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        logger.info(f"finished updateOne on '{self.name}'")
        if "status" in uo_response:
            uo_status = uo_response["status"]
            _update_info = _prepare_update_info([uo_status])
            return CollectionUpdateResult(
                raw_results=[uo_response],
                update_info=_update_info,
            )
        else:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from updateOne API command.",
                raw_response=uo_response,
            )

    async def update_many(
        self,
        filter: FilterType,
        update: dict[str, Any],
        *,
        upsert: bool = False,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> CollectionUpdateResult:
        """
        Apply an update operation to all documents matching a condition,
        optionally inserting one documents in absence of matches.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
            update: the update prescription to apply to the documents, expressed
                as a dictionary as per Data API syntax. Examples are:
                    {"$set": {"field": "value}}
                    {"$inc": {"counter": 10}}
                    {"$unset": {"field": ""}}
                See the Data API documentation for the full syntax.
            upsert: this parameter controls the behavior in absence of matches.
                If True, a single new document (resulting from applying `update`
                to an empty document) is inserted if no matches are found on
                the collection. If False, the operation silently does nothing
                in case of no matches.
            general_method_timeout_ms: a timeout, in milliseconds, for the whole
                requested operation (which may involve multiple API requests).
                This method may entail successive HTTP API requests,
                depending on the amount of involved documents.
                If not passed, the collection-level setting is used instead.
            request_timeout_ms: a timeout, in milliseconds, for each API request.
                If not passed, the collection-level setting is used instead.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a CollectionUpdateResult object summarizing the outcome of
            the update operation.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> async def do_update_many(acol: AsyncCollection) -> None:
            ...     await acol.insert_many([{"c": "red"}, {"c": "green"}, {"c": "blue"}])
            ...     result0 = await acol.update_many(
            ...         {"c": {"$ne": "green"}},
            ...         {"$set": {"nongreen": True}},
            ...     )
            ...     print("result0.update_info", result0.update_info)
            ...     result1 = await acol.update_many(
            ...         {"c": "orange"},
            ...         {"$set": {"is_also_fruit": True}},
            ...     )
            ...     print("result1.update_info", result1.update_info)
            ...     result2 = await acol.update_many(
            ...         {"c": "orange"},
            ...         {"$set": {"is_also_fruit": True}},
            ...         upsert=True,
            ...     )
            ...     print("result2.update_info", result2.update_info)
            ...
            >>> asyncio.run(do_update_many(my_async_coll))
            result0.update_info {'n': 2, 'updatedExisting': True, 'ok': 1.0, 'nModified': 2}
            result1.update_info {'n': 0, 'updatedExisting': False, 'ok': 1.0, 'nModified': 0}
            result2.update_info {'n': 1, 'updatedExisting': False, 'ok': 1.0, 'nModified': 0, 'upserted': '79ffd5a3-ab99-4dff-a2a5-4aaa0e59e854'}

        Note:
            Similarly to the case of `find` (see its docstring for more details),
            running this command while, at the same time, another process is
            inserting new documents which match the filter of the `update_many`
            can result in an unpredictable fraction of these documents being updated.
            In other words, it cannot be easily predicted whether a given
            newly-inserted document will be picked up by the update_many command or not.
        """

        _general_method_timeout_ms, _gmt_label = _first_valid_timeout(
            (general_method_timeout_ms, "general_method_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (
                self.api_options.timeout_options.general_method_timeout_ms,
                "general_method_timeout_ms",
            ),
        )
        _request_timeout_ms, _rt_label = _first_valid_timeout(
            (request_timeout_ms, "request_timeout_ms"),
            (self.api_options.timeout_options.request_timeout_ms, "request_timeout_ms"),
        )
        api_options = {
            "upsert": upsert,
        }
        page_state_options: dict[str, str] = {}
        um_responses: list[dict[str, Any]] = []
        um_statuses: list[dict[str, Any]] = []
        must_proceed = True
        logger.info(f"starting update_many on '{self.name}'")
        timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=_general_method_timeout_ms,
            timeout_label=_gmt_label,
        )
        while must_proceed:
            options = {**api_options, **page_state_options}
            this_um_payload = {
                "updateMany": {
                    k: v
                    for k, v in {
                        "filter": filter,
                        "update": update,
                        "options": options,
                    }.items()
                    if v is not None
                }
            }
            logger.info(f"updateMany on '{self.name}'")
            this_um_response = await self._converted_request(
                payload=this_um_payload,
                raise_api_errors=False,
                timeout_context=timeout_manager.remaining_timeout(
                    cap_time_ms=_request_timeout_ms,
                    cap_timeout_label=_rt_label,
                ),
            )
            logger.info(f"finished updateMany on '{self.name}'")
            this_um_status = this_um_response.get("status") or {}
            #
            # if errors, quit early
            if this_um_response.get("errors", []):
                partial_update_info = _prepare_update_info(um_statuses)
                partial_result = CollectionUpdateResult(
                    raw_results=um_responses,
                    update_info=partial_update_info,
                )
                cause_exception = DataAPIResponseException.from_response(
                    command=this_um_payload,
                    raw_response=this_um_response,
                )
                raise CollectionUpdateManyException(
                    partial_result=partial_result,
                    cause=cause_exception,
                )
            else:
                if "status" not in this_um_response:
                    raise UnexpectedDataAPIResponseException(
                        text="Faulty response from update_many API command.",
                        raw_response=this_um_response,
                    )
                um_responses.append(this_um_response)
                um_statuses.append(this_um_status)
                next_page_state = this_um_status.get("nextPageState")
                if next_page_state is not None:
                    must_proceed = True
                    page_state_options = {"pageState": next_page_state}
                else:
                    must_proceed = False
                    page_state_options = {}

        update_info = _prepare_update_info(um_statuses)
        logger.info(f"finished update_many on '{self.name}'")
        return CollectionUpdateResult(
            raw_results=um_responses,
            update_info=update_info,
        )

    async def find_one_and_delete(
        self,
        filter: FilterType,
        *,
        projection: ProjectionType | None = None,
        sort: SortType | None = None,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> DOC | None:
        """
        Find a document in the collection and delete it. The deleted document,
        however, is the return value of the method.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
            projection: it controls which parts of the document are returned.
                It can be an allow-list: `{"f1": True, "f2": True}`,
                or a deny-list: `{"fx": False, "fy": False}`, but not a mixture
                (except for the `_id` and other special fields, which can be
                associated to both True or False independently of the rest
                of the specification).
                The special star-projections `{"*": True}` and `{"*": False}`
                have the effect of returning the whole document and `{}` respectively.
                For lists in documents, slice directives can be passed to select
                portions of the list: for instance, `{"array": {"$slice": 2}}`,
                `{"array": {"$slice": -2}}`, `{"array": {"$slice": [4, 2]}}` or
                `{"array": {"$slice": [-4, 2]}}`.
                An iterable over strings will be treated implicitly as an allow-list.
                The default projection (used if this parameter is not passed) does not
                necessarily include "special" fields such as `$vector` or `$vectorize`.
                See the Data API documentation for more on projections.
            sort: with this dictionary parameter one can control the sorting
                order of the documents matching the filter, effectively
                determining what document will come first and hence be the
                replaced one. See the `find` method for more on sorting.
                Vector-based ANN sorting is achieved by providing a "$vector"
                or a "$vectorize" key in `sort`.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            Either the document (or a projection thereof, as requested), or None
            if no matches were found in the first place.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> async def do_find_one_and_delete(acol: AsyncCollection) -> None:
            ...     await acol.insert_many(
            ...         [
            ...             {"species": "swan", "class": "Aves"},
            ...             {"species": "frog", "class": "Amphibia"},
            ...         ],
            ...     )
            ...     delete_result0 = await acol.find_one_and_delete(
            ...         {"species": {"$ne": "frog"}},
            ...         projection=["species"],
            ...     )
            ...     print("delete_result0", delete_result0)
            ...     delete_result1 = await acol.find_one_and_delete(
            ...         {"species": {"$ne": "frog"}},
            ...     )
            ...     print("delete_result1", delete_result1)
            ...
            >>> asyncio.run(do_find_one_and_delete(my_async_coll))
            delete_result0 {'_id': 'f335cd0f-...', 'species': 'swan'}
            delete_result1 None
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        _projection = normalize_optional_projection(projection)
        fo_payload = {
            "findOneAndDelete": {
                k: v
                for k, v in {
                    "filter": filter,
                    "sort": sort,
                    "projection": _projection,
                }.items()
                if v is not None
            }
        }
        logger.info(f"findOneAndDelete on '{self.name}'")
        fo_response = await self._converted_request(
            payload=fo_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        logger.info(f"finished findOneAndDelete on '{self.name}'")
        if "document" in fo_response.get("data", {}):
            document = fo_response["data"]["document"]
            return document  # type: ignore[no-any-return]
        else:
            deleted_count = fo_response.get("status", {}).get("deletedCount")
            if deleted_count == 0:
                return None
            else:
                raise UnexpectedDataAPIResponseException(
                    text="Faulty response from find_one_and_delete API command.",
                    raw_response=fo_response,
                )

    async def delete_one(
        self,
        filter: FilterType,
        *,
        sort: SortType | None = None,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> CollectionDeleteResult:
        """
        Delete one document matching a provided filter.
        This method never deletes more than a single document, regardless
        of the number of matches to the provided filters.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
            sort: with this dictionary parameter one can control the sorting
                order of the documents matching the filter, effectively
                determining what document will come first and hence be the
                replaced one. See the `find` method for more on sorting.
                Vector-based ANN sorting is achieved by providing a "$vector"
                or a "$vectorize" key in `sort`.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a CollectionDeleteResult object summarizing the outcome of the
            delete operation.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> asyncio.run(my_async_coll.insert_many(
            ...     [{"seq": 1}, {"seq": 0}, {"seq": 2}]
            ... ))
            CollectionInsertManyResult(...)
            >>> asyncio.run(my_async_coll.delete_one({"seq": 1}))
            CollectionDeleteResult(raw_results=..., deleted_count=1)
            >>> asyncio.run(my_async_coll.distinct("seq"))
            [0, 2]
            >>> asyncio.run(my_async_coll.delete_one(
            ...     {"seq": {"$exists": True}},
            ...     sort={"seq": astrapy.constants.SortMode.DESCENDING},
            ... ))
            CollectionDeleteResult(raw_results=..., deleted_count=1)
            >>> asyncio.run(my_async_coll.distinct("seq"))
            [0]
            >>> asyncio.run(my_async_coll.delete_one({"seq": 2}))
            CollectionDeleteResult(raw_results=..., deleted_count=0)
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        do_payload = {
            "deleteOne": {
                k: v
                for k, v in {
                    "filter": filter,
                    "sort": sort,
                }.items()
                if v is not None
            }
        }
        logger.info(f"deleteOne on '{self.name}'")
        do_response = await self._converted_request(
            payload=do_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        logger.info(f"finished deleteOne on '{self.name}'")
        if "deletedCount" in do_response.get("status", {}):
            deleted_count = do_response["status"]["deletedCount"]
            return CollectionDeleteResult(
                deleted_count=deleted_count,
                raw_results=[do_response],
            )
        else:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from delete_one API command.",
                raw_response=do_response,
            )

    async def delete_many(
        self,
        filter: FilterType,
        *,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> CollectionDeleteResult:
        """
        Delete all documents matching a provided filter.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
                Passing an empty filter, `{}`, completely erases all contents
                of the collection.
            general_method_timeout_ms: a timeout, in milliseconds, for the whole
                requested operation (which may involve multiple API requests).
                This method may entail successive HTTP API requests,
                depending on the amount of involved documents.
                If not passed, the collection-level setting is used instead.
            request_timeout_ms: a timeout, in milliseconds, for each API request.
                If not passed, the collection-level setting is used instead.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a CollectionDeleteResult object summarizing the outcome of the
            delete operation.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> async def do_delete_many(acol: AsyncCollection) -> None:
            ...     await acol.insert_many([{"seq": 1}, {"seq": 0}, {"seq": 2}])
            ...     delete_result0 = await acol.delete_many({"seq": {"$lte": 1}})
            ...     print("delete_result0.deleted_count", delete_result0.deleted_count)
            ...     distinct1 = await acol.distinct("seq")
            ...     print("distinct1", distinct1)
            ...     delete_result2 = await acol.delete_many({"seq": {"$lte": 1}})
            ...     print("delete_result2.deleted_count", delete_result2.deleted_count)
            ...
            >>> asyncio.run(do_delete_many(my_async_coll))
            delete_result0.deleted_count 2
            distinct1 [2]
            delete_result2.deleted_count 0

        Note:
            This operation is in general not atomic. Depending on the amount
            of matching documents, it can keep running (in a blocking way)
            for a macroscopic time. In that case, new documents that are
            meanwhile inserted (e.g. from another process/application) will be
            deleted during the execution of this method call until the
            collection is devoid of matches.
            An exception is the `filter={}` case, whereby the operation is atomic.
        """

        _general_method_timeout_ms, _gmt_label = _first_valid_timeout(
            (general_method_timeout_ms, "general_method_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (
                self.api_options.timeout_options.general_method_timeout_ms,
                "general_method_timeout_ms",
            ),
        )
        _request_timeout_ms, _rt_label = _first_valid_timeout(
            (request_timeout_ms, "request_timeout_ms"),
            (self.api_options.timeout_options.request_timeout_ms, "request_timeout_ms"),
        )
        dm_responses: list[dict[str, Any]] = []
        deleted_count = 0
        must_proceed = True
        timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=_general_method_timeout_ms,
            timeout_label=_gmt_label,
        )
        this_dm_payload = {"deleteMany": {"filter": filter}}
        logger.info(f"starting delete_many on '{self.name}'")
        while must_proceed:
            logger.info(f"deleteMany on '{self.name}'")
            this_dm_response = await self._converted_request(
                payload=this_dm_payload,
                raise_api_errors=False,
                timeout_context=timeout_manager.remaining_timeout(
                    cap_time_ms=_request_timeout_ms,
                    cap_timeout_label=_rt_label,
                ),
            )
            logger.info(f"finished deleteMany on '{self.name}'")
            # if errors, quit early
            if this_dm_response.get("errors", []):
                partial_result = CollectionDeleteResult(
                    deleted_count=deleted_count,
                    raw_results=dm_responses,
                )
                cause_exception = DataAPIResponseException.from_response(
                    command=this_dm_payload,
                    raw_response=this_dm_response,
                )
                raise CollectionDeleteManyException(
                    partial_result=partial_result,
                    cause=cause_exception,
                )
            else:
                this_dc = this_dm_response.get("status", {}).get("deletedCount")
                if this_dc is None:
                    raise UnexpectedDataAPIResponseException(
                        text="Faulty response from delete_many API command.",
                        raw_response=this_dm_response,
                    )
                dm_responses.append(this_dm_response)
                deleted_count += this_dc
                must_proceed = this_dm_response.get("status", {}).get("moreData", False)

        logger.info(f"finished delete_many on '{self.name}'")
        return CollectionDeleteResult(
            deleted_count=deleted_count,
            raw_results=dm_responses,
        )

    async def drop(
        self,
        *,
        collection_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        """
        Drop the collection, i.e. delete it from the database along with
        all the documents it contains.

        Args:
            collection_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `collection_admin_timeout_ms`.
            timeout_ms: an alias for `collection_admin_timeout_ms`.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> async def drop_and_check(acol: AsyncCollection) -> None:
            ...     doc0 = await acol.find_one({})
            ...     print("doc0", doc0)
            ...     await acol.drop()
            ...     doc1 = await acol.find_one({})
            ...
            >>> asyncio.run(drop_and_check(my_async_coll))
            doc0 {'_id': '...', 'z': -10}
            Traceback (most recent call last):
                ... ...
            astrapy.exceptions.DataAPIResponseException: Collection does not exist, ...

        Note:
            Use with caution.

        Note:
            Once the method succeeds, methods on this object can still be invoked:
            however, this hardly makes sense as the underlying actual collection
            is no more.
            It is responsibility of the developer to design a correct flow
            which avoids using a deceased collection any further.
        """

        logger.info(f"dropping collection '{self.name}' (self)")
        await self.database.drop_collection(
            self.name,
            collection_admin_timeout_ms=collection_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info(f"finished dropping collection '{self.name}' (self)")

    async def command(
        self,
        body: dict[str, Any] | None,
        *,
        raise_api_errors: bool = True,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        Send a POST request to the Data API for this collection with
        an arbitrary, caller-provided payload.
        No transformations or type conversions are made on the provided payload.

        Args:
            body: a JSON-serializable dictionary, the payload of the request.
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
            >>> asyncio.await(my_async_coll.command({"countDocuments": {}}))
            {'status': {'count': 123}}
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        _cmd_desc: str
        if body:
            _cmd_desc = ",".join(sorted(body.keys()))
        else:
            _cmd_desc = "(none)"
        logger.info(f"command={_cmd_desc} on '{self.name}'")
        command_result = await self._api_commander.async_request(
            payload=body,
            raise_api_errors=raise_api_errors,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        logger.info(f"finished command={_cmd_desc} on '{self.name}'")
        return command_result
