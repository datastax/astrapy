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
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from types import TracebackType
from typing import TYPE_CHECKING, Any, Iterable, Sequence

from astrapy.authentication import coerce_embedding_headers_provider
from astrapy.constants import (
    CallerType,
    DocumentType,
    FilterType,
    ProjectionType,
    ReturnDocument,
    SortType,
    normalize_optional_projection,
)
from astrapy.cursors import AsyncCursor, Cursor
from astrapy.database import AsyncDatabase, Database
from astrapy.exceptions import (
    CollectionNotFoundException,
    DataAPIFaultyResponseException,
    DeleteManyException,
    InsertManyException,
    MultiCallTimeoutManager,
    TooManyDocumentsToCountException,
    UpdateManyException,
    base_timeout_info,
)
from astrapy.info import CollectionInfo, CollectionOptions
from astrapy.results import (
    DeleteResult,
    InsertManyResult,
    InsertOneResult,
    UpdateResult,
)
from astrapy.settings.defaults import (
    DEFAULT_DATA_API_AUTH_HEADER,
    DEFAULT_INSERT_MANY_CHUNK_SIZE,
    DEFAULT_INSERT_MANY_CONCURRENCY,
)
from astrapy.utils.api_commander import APICommander
from astrapy.utils.api_options import CollectionAPIOptions

if TYPE_CHECKING:
    from astrapy.authentication import EmbeddingHeadersProvider


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


class Collection:
    """
    A Data API collection, the main object to interact with the Data API,
    especially for DDL operations.
    This class has a synchronous interface.

    A Collection is spawned from a Database object, from which it inherits
    the details on how to reach the API server (endpoint, authentication token).

    Args:
        database: a Database object, instantiated earlier. This represents
            the database the collection belongs to.
        name: the collection name. This parameter should match an existing
            collection on the database.
        keyspace: this is the keyspace to which the collection belongs.
            If not specified, the database's working keyspace is used.
        api_options: An instance of `astrapy.utils.api_options.CollectionAPIOptions`
            providing the general settings for interacting with the Data API.
        callers: a list of caller identities, i.e. applications, or frameworks,
            on behalf of which the Data API calls are performed. These end up
            in the request user-agent.
            Each caller identity is a ("caller_name", "caller_version") pair.

    Examples:
        >>> from astrapy import DataAPIClient, Collection
        >>> my_client = astrapy.DataAPIClient("AstraCS:...")
        >>> my_db = my_client.get_database(
        ...    "https://01234567-....apps.astra.datastax.com"
        ... )
        >>> my_coll_1 = Collection(database=my_db, name="my_collection")
        >>> my_coll_2 = my_db.create_collection(
        ...     "my_v_collection",
        ...     dimension=3,
        ...     metric="cosine",
        ... )
        >>> my_coll_3a = my_db.get_collection("my_already_existing_collection")
        >>> my_coll_3b = my_db.my_already_existing_collection
        >>> my_coll_3c = my_db["my_already_existing_collection"]

    Note:
        creating an instance of Collection does not trigger actual creation
        of the collection on the database. The latter should have been created
        beforehand, e.g. through the `create_collection` method of a Database.
    """

    def __init__(
        self,
        database: Database,
        name: str,
        *,
        keyspace: str | None = None,
        api_options: CollectionAPIOptions | None = None,
        callers: Sequence[CallerType] = [],
    ) -> None:
        if api_options is None:
            self.api_options = CollectionAPIOptions()
        else:
            self.api_options = api_options
        _keyspace = keyspace if keyspace is not None else database.keyspace
        if _keyspace is None:
            raise ValueError("Attempted to create Collection with 'keyspace' unset.")
        self._database = database._copy(
            keyspace=_keyspace,
            callers=callers,
        )
        self._name = name

        additional_headers = self.api_options.embedding_api_key.get_headers()
        self._commander_headers = {
            **{DEFAULT_DATA_API_AUTH_HEADER: self._database.token_provider.get_token()},
            **additional_headers,
        }

        self.callers = callers
        self._api_commander = self._get_api_commander()

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}(name="{self.name}", '
            f'keyspace="{self.keyspace}", database={self.database}, '
            f"api_options={self.api_options})"
        )

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Collection):
            return all(
                [
                    self._api_commander == other._api_commander,
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
                self._database.api_path.strip("/"),
                self._database.api_version.strip("/"),
                self._database.keyspace,
                self._name,
            )
            if comp != ""
        ]
        base_path = f"/{'/'.join(base_path_components)}"
        api_commander = APICommander(
            api_endpoint=self._database.api_endpoint,
            path=base_path,
            headers=self._commander_headers,
            callers=self.callers,
        )
        return api_commander

    def _copy(
        self,
        *,
        database: Database | None = None,
        name: str | None = None,
        keyspace: str | None = None,
        api_options: CollectionAPIOptions | None = None,
        callers: Sequence[CallerType] = [],
    ) -> Collection:
        return Collection(
            database=database or self.database._copy(),
            name=name or self.name,
            keyspace=keyspace or self.keyspace,
            api_options=self.api_options.with_override(api_options),
            callers=callers or self.callers,
        )

    def with_options(
        self,
        *,
        name: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | None = None,
        collection_max_time_ms: int | None = None,
        callers: Sequence[CallerType] = [],
    ) -> Collection:
        """
        Create a clone of this collection with some changed attributes.

        Args:
            name: the name of the collection. This parameter is useful to
                quickly spawn Collection instances each pointing to a different
                collection existing in the same keyspace.
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
            callers: a list of caller identities, i.e. applications, or frameworks,
                on behalf of which the Data API calls are performed. These end up
                in the request user-agent.
                Each caller identity is a ("caller_name", "caller_version") pair.

        Returns:
            a new Collection instance.

        Example:
            >>> my_other_coll = my_coll.with_options(
            ...     name="the_other_coll",
            ...     callers=[("caller_identity", "0.1.2")],
            ... )
        """

        _api_options = CollectionAPIOptions(
            embedding_api_key=coerce_embedding_headers_provider(embedding_api_key),
            max_time_ms=collection_max_time_ms,
        )

        return self._copy(
            name=name,
            api_options=_api_options,
            callers=callers,
        )

    def to_async(
        self,
        *,
        database: AsyncDatabase | None = None,
        name: str | None = None,
        keyspace: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | None = None,
        collection_max_time_ms: int | None = None,
        callers: Sequence[CallerType] = [],
    ) -> AsyncCollection:
        """
        Create an AsyncCollection from this one. Save for the arguments
        explicitly provided as overrides, everything else is kept identical
        to this collection in the copy (the database is converted into
        an async object).

        Args:
            database: an AsyncDatabase object, instantiated earlier.
                This represents the database the new collection belongs to.
            name: the collection name. This parameter should match an existing
                collection on the database.
            keyspace: this is the keyspace to which the collection belongs.
                If not specified, the database's working keyspace is used.
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
            callers: a list of caller identities, i.e. applications, or frameworks,
                on behalf of which the Data API calls are performed. These end up
                in the request user-agent.
                Each caller identity is a ("caller_name", "caller_version") pair.

        Returns:
            the new copy, an AsyncCollection instance.

        Example:
            >>> asyncio.run(my_coll.to_async().count_documents({},upper_bound=100))
            77
        """

        _api_options = CollectionAPIOptions(
            embedding_api_key=coerce_embedding_headers_provider(embedding_api_key),
            max_time_ms=collection_max_time_ms,
        )

        return AsyncCollection(
            database=database or self.database.to_async(),
            name=name or self.name,
            keyspace=keyspace or self.keyspace,
            api_options=self.api_options.with_override(_api_options),
            callers=callers or self.callers,
        )

    def options(self, *, max_time_ms: int | None = None) -> CollectionOptions:
        """
        Get the collection options, i.e. its configuration as read from the database.

        The method issues a request to the Data API each time is invoked,
        without caching mechanisms: this ensures up-to-date information
        for usages such as real-time collection validation by the application.

        Args:
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
                If not passed, the collection-level setting is used instead.

        Returns:
            a CollectionOptions instance describing the collection.
            (See also the database `list_collections` method.)

        Example:
            >>> my_coll.options()
            CollectionOptions(vector=CollectionVectorOptions(dimension=3, metric='cosine'))
        """

        _max_time_ms = max_time_ms or self.api_options.max_time_ms
        logger.info(f"getting collections in search of '{self.name}'")
        self_descriptors = [
            coll_desc
            for coll_desc in self.database.list_collections(max_time_ms=_max_time_ms)
            if coll_desc.name == self.name
        ]
        logger.info(f"finished getting collections in search of '{self.name}'")
        if self_descriptors:
            return self_descriptors[0].options
        else:
            raise CollectionNotFoundException(
                text=f"Collection {self.keyspace}.{self.name} not found.",
                keyspace=self.keyspace,
                collection_name=self.name,
            )

    def info(self) -> CollectionInfo:
        """
        Information on the collection (name, location, database), in the
        form of a CollectionInfo object.

        Not to be confused with the collection `options` method (related
        to the collection internal configuration).

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
            database_info=self.database.info(),
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
            raise ValueError("The collection's DB is set with keyspace=None")
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
        document: DocumentType,
        *,
        max_time_ms: int | None = None,
    ) -> InsertOneResult:
        """
        Insert a single document in the collection in an atomic operation.

        Args:
            document: the dictionary expressing the document to insert.
                The `_id` field of the document can be left out, in which
                case it will be created automatically.
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
                If not passed, the collection-level setting is used instead.

        Returns:
            an InsertOneResult object.

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
            InsertOneResult(raw_results=..., inserted_id='ed4587a4-...-...-...')
            >>> my_coll.insert_one({"_id": "user-123", "age": 50, "name": "Maccio"})
            InsertOneResult(raw_results=..., inserted_id='user-123')
            >>> my_coll.count_documents({}, upper_bound=10)
            2

            >>> my_coll.insert_one({"tag": "v", "$vector": [10, 11]})
            InsertOneResult(...)

        Note:
            If an `_id` is explicitly provided, which corresponds to a document
            that exists already in the collection, an error is raised and
            the insertion fails.
        """

        _max_time_ms = max_time_ms or self.api_options.max_time_ms
        io_payload = {"insertOne": {"document": document}}
        logger.info(f"insertOne on '{self.name}'")
        io_response = self._api_commander.request(
            payload=io_payload,
            timeout_info=base_timeout_info(_max_time_ms),
        )
        logger.info(f"finished insertOne on '{self.name}'")
        if "insertedIds" in io_response.get("status", {}):
            if io_response["status"]["insertedIds"]:
                inserted_id = io_response["status"]["insertedIds"][0]
                return InsertOneResult(
                    raw_results=[io_response],
                    inserted_id=inserted_id,
                )
            else:
                raise DataAPIFaultyResponseException(
                    text="Faulty response from insert_one API command.",
                    raw_response=io_response,
                )
        else:
            raise DataAPIFaultyResponseException(
                text="Faulty response from insert_one API command.",
                raw_response=io_response,
            )

    def insert_many(
        self,
        documents: Iterable[DocumentType],
        *,
        ordered: bool = False,
        chunk_size: int | None = None,
        concurrency: int | None = None,
        max_time_ms: int | None = None,
    ) -> InsertManyResult:
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
            max_time_ms: a timeout, in milliseconds, for the operation.
                If not passed, the collection-level setting is used instead:
                If many documents are being inserted, this method corresponds
                to several HTTP requests: in such cases one may want to specify
                a more tolerant timeout here.

        Returns:
            an InsertManyResult object.

        Examples:
            >>> my_coll.count_documents({}, upper_bound=10)
            0
            >>> my_coll.insert_many(
            ...     [{"a": 10}, {"a": 5}, {"b": [True, False, False]}],
            ...     ordered=True,
            ... )
            InsertManyResult(raw_results=..., inserted_ids=['184bb06f-...', '...', '...'])
            >>> my_coll.count_documents({}, upper_bound=100)
            3
            >>> my_coll.insert_many(
            ...     [{"seq": i} for i in range(50)],
            ...     concurrency=5,
            ... )
            InsertManyResult(raw_results=..., inserted_ids=[... ...])
            >>> my_coll.count_documents({}, upper_bound=100)
            53
            >>> my_coll.insert_many(
            ...     [
            ...         {"tag": "a", "$vector": [1, 2]},
            ...         {"tag": "b", "$vector": [3, 4]},
            ...     ]
            ... )
            InsertManyResult(...)

        Note:
            Unordered insertions are executed with some degree of concurrency,
            so it is usually better to prefer this mode unless the order in the
            document sequence is important.

        Note:
            A failure mode for this command is related to certain faulty documents
            found among those to insert: a document may have the an `_id` already
            present on the collection, or its vector dimension may not
            match the collection setting.

            For an ordered insertion, the method will raise an exception at
            the first such faulty document -- nevertheless, all documents processed
            until then will end up being written to the database.

            For unordered insertions, if the error stems from faulty documents
            the insertion proceeds until exhausting the input documents: then,
            an exception is raised -- and all insertable documents will have been
            written to the database, including those "after" the troublesome ones.

            If, on the other hand, there are errors not related to individual
            documents (such as a network connectivity error), the whole
            `insert_many` operation will stop in mid-way, an exception will be raised,
            and only a certain amount of the input documents will
            have made their way to the database.
        """

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
        _max_time_ms = max_time_ms or self.api_options.max_time_ms
        _documents = list(documents)
        logger.info(f"inserting {len(_documents)} documents in '{self.name}'")
        raw_results: list[dict[str, Any]] = []
        timeout_manager = MultiCallTimeoutManager(overall_max_time_ms=_max_time_ms)
        if ordered:
            options = {"ordered": True}
            inserted_ids: list[Any] = []
            for i in range(0, len(_documents), _chunk_size):
                im_payload = {
                    "insertMany": {
                        "documents": _documents[i : i + _chunk_size],
                        "options": options,
                    },
                }
                logger.info(f"insertMany on '{self.name}'")
                chunk_response = self._api_commander.request(
                    payload=im_payload,
                    raise_api_errors=False,
                    timeout_info=timeout_manager.remaining_timeout_info(),
                )
                logger.info(f"finished insertMany on '{self.name}'")
                # accumulate the results in this call
                chunk_inserted_ids = (chunk_response.get("status") or {}).get(
                    "insertedIds", []
                )
                inserted_ids += chunk_inserted_ids
                raw_results += [chunk_response]
                # if errors, quit early
                if chunk_response.get("errors", []):
                    partial_result = InsertManyResult(
                        raw_results=raw_results,
                        inserted_ids=inserted_ids,
                    )
                    raise InsertManyException.from_response(
                        command=None,
                        raw_response=chunk_response,
                        partial_result=partial_result,
                    )

            # return
            full_result = InsertManyResult(
                raw_results=raw_results,
                inserted_ids=inserted_ids,
            )
            logger.info(
                f"finished inserting {len(_documents)} documents in '{self.name}'"
            )
            return full_result

        else:
            # unordered: concurrent or not, do all of them and parse the results
            options = {"ordered": False}
            if _concurrency > 1:
                with ThreadPoolExecutor(max_workers=_concurrency) as executor:

                    def _chunk_insertor(
                        document_chunk: list[dict[str, Any]],
                    ) -> dict[str, Any]:
                        im_payload = {
                            "insertMany": {
                                "documents": document_chunk,
                                "options": options,
                            },
                        }
                        logger.info(f"insertMany(chunk) on '{self.name}'")
                        im_response = self._api_commander.request(
                            payload=im_payload,
                            raise_api_errors=False,
                            timeout_info=timeout_manager.remaining_timeout_info(),
                        )
                        logger.info(f"finished insertMany(chunk) on '{self.name}'")
                        return im_response

                    raw_results = list(
                        executor.map(
                            _chunk_insertor,
                            (
                                _documents[i : i + _chunk_size]
                                for i in range(0, len(_documents), _chunk_size)
                            ),
                        )
                    )
            else:
                for i in range(0, len(_documents), _chunk_size):
                    im_payload = {
                        "insertMany": {
                            "documents": _documents[i : i + _chunk_size],
                            "options": options,
                        },
                    }
                    logger.info(f"insertMany(chunk) on '{self.name}'")
                    im_response = self._api_commander.request(
                        payload=im_payload,
                        raise_api_errors=False,
                        timeout_info=timeout_manager.remaining_timeout_info(),
                    )
                    logger.info(f"finished insertMany(chunk) on '{self.name}'")
                    raw_results.append(im_response)
            # recast raw_results
            inserted_ids = [
                inserted_id
                for chunk_response in raw_results
                for inserted_id in (chunk_response.get("status") or {}).get(
                    "insertedIds", []
                )
            ]

            # check-raise
            if any(
                [chunk_response.get("errors", []) for chunk_response in raw_results]
            ):
                partial_result = InsertManyResult(
                    raw_results=raw_results,
                    inserted_ids=inserted_ids,
                )
                raise InsertManyException.from_responses(
                    commands=[None for _ in raw_results],
                    raw_responses=raw_results,
                    partial_result=partial_result,
                )

            # return
            full_result = InsertManyResult(
                raw_results=raw_results,
                inserted_ids=inserted_ids,
            )
            logger.info(
                f"finished inserting {len(_documents)} documents in '{self.name}'"
            )
            return full_result

    def find(
        self,
        filter: FilterType | None = None,
        *,
        projection: ProjectionType | None = None,
        skip: int | None = None,
        limit: int | None = None,
        include_similarity: bool | None = None,
        include_sort_vector: bool | None = None,
        sort: SortType | None = None,
        max_time_ms: int | None = None,
    ) -> Cursor:
        """
        Find documents on the collection, matching a certain provided filter.

        The method returns a Cursor that can then be iterated over. Depending
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
                returned document. Can only be used for vector ANN search, i.e.
                when either `vector` is supplied or the `sort` parameter has the
                shape {"$vector": ...}.
            include_sort_vector: a boolean to request query vector used in this search.
                If set to True (and if the invocation is a vector search), calling
                the `get_sort_vector` method on the returned cursor will yield
                the vector used for the ANN search.
            sort: with this dictionary parameter one can control the order
                the documents are returned. See the Note about sorting, as well as
                the one about upper bounds, for details.
                Vector-based ANN sorting is achieved by providing a "$vector"
                or a "$vectorize" key in `sort`.
            max_time_ms: a timeout, in milliseconds, for each single one
                of the underlying HTTP requests used to fetch documents as the
                cursor is iterated over.
                If not passed, the collection-level setting is used instead.

        Returns:
            a Cursor object representing iterations over the matching documents
            (see the Cursor object for how to use it. The simplest thing is to
            run a for loop: `for document in collection.sort(...):`).

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
            ...     sort={"seq": astrapy.constants.SortDocuments.DESCENDING},
            ... )
            >>> [doc["_id"] for doc in cursor1]
            ['97e85f81-...', '1581efe4-...', '...', '...']
            >>> cursor2 = my_coll.find({}, limit=3)
            >>> cursor2.distinct("seq")
            [37, 35, 10]

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
            >>> matches = list(cursor)
            >>> cursor.get_sort_vector()
            [3.0, 3.0]

        Note:
            The following are example values for the `sort` parameter.
            When no particular order is required:
                sort={}  # (default when parameter not provided)
            When sorting by a certain value in ascending/descending order:
                sort={"field": SortDocuments.ASCENDING}
                sort={"field": SortDocuments.DESCENDING}
            When sorting first by "field" and then by "subfield"
            (while modern Python versions preserve the order of dictionaries,
            it is suggested for clarity to employ a `collections.OrderedDict`
            in these cases):
                sort={
                    "field": SortDocuments.ASCENDING,
                    "subfield": SortDocuments.ASCENDING,
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
            These provisions should be kept in mind even when subsequently running
            a command such as `.distinct()` on a cursor.

        Note:
            When not specifying sorting criteria at all (by vector or otherwise),
            the cursor can scroll through an arbitrary number of documents as
            the Data API and the client periodically exchange new chunks of documents.
            It should be noted that the behavior of the cursor in the case documents
            have been added/removed after the `find` was started depends on database
            internals and it is not guaranteed, nor excluded, that such "real-time"
            changes in the data would be picked up by the cursor.
        """

        _max_time_ms = max_time_ms or self.api_options.max_time_ms
        if include_similarity is not None and not _is_vector_sort(sort):
            raise ValueError(
                "Cannot use `include_similarity` unless for vector search."
            )
        return (
            Cursor(
                collection=self,
                filter=filter,
                projection=projection,
                max_time_ms=_max_time_ms,
                overall_max_time_ms=None,
            )
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
        max_time_ms: int | None = None,
    ) -> DocumentType | None:
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
                returned document. Can only be used for vector ANN search, i.e.
                when either `vector` is supplied or the `sort` parameter has the
                shape {"$vector": ...}.
            sort: with this dictionary parameter one can control the order
                the documents are returned. See the Note about sorting for details.
                Vector-based ANN sorting is achieved by providing a "$vector"
                or a "$vectorize" key in `sort`.
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
                If not passed, the collection-level setting is used instead.

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
            ...     sort={"seq": astrapy.constants.SortDocuments.DESCENDING},
            ... )
            {'_id': '97e85f81-...', 'seq': 69}
            >>> my_coll.find_one({}, sort={"$vector": [1, 0]}, projection={"*": True})
            {'_id': '...', 'tag': 'D', '$vector': [4.0, 1.0]}

        Note:
            See the `find` method for more details on the accepted parameters
            (whereas `skip` and `limit` are not valid parameters for `find_one`).
        """

        _max_time_ms = max_time_ms or self.api_options.max_time_ms
        fo_cursor = self.find(
            filter=filter,
            projection=projection,
            skip=None,
            limit=1,
            include_similarity=include_similarity,
            sort=sort,
            max_time_ms=_max_time_ms,
        )
        try:
            document = fo_cursor.__next__()
            return document
        except StopIteration:
            return None

    def distinct(
        self,
        key: str,
        *,
        filter: FilterType | None = None,
        max_time_ms: int | None = None,
    ) -> list[Any]:
        """
        Return a list of the unique values of `key` across the documents
        in the collection that match the provided filter.

        Args:
            key: the name of the field whose value is inspected across documents.
                Keys can use dot-notation to descend to deeper document levels.
                Example of acceptable `key` values:
                    "field"
                    "field.subfield"
                    "field.3"
                    "field.3.subfield"
                If lists are encountered and no numeric index is specified,
                all items in the list are visited.
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
            max_time_ms: a timeout, in milliseconds, with the same meaning as for `find`.
                If not passed, the collection-level setting is used instead.

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
            InsertManyResult(raw_results=..., inserted_ids=['c5b99f37-...', 'd6416321-...'])
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

        _max_time_ms = max_time_ms or self.api_options.max_time_ms
        f_cursor = Cursor(
            collection=self,
            filter=filter,
            projection={key: True},
            max_time_ms=None,
            overall_max_time_ms=_max_time_ms,
        )
        return f_cursor.distinct(key)

    def count_documents(
        self,
        filter: FilterType,
        *,
        upper_bound: int,
        max_time_ms: int | None = None,
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
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
                If not passed, the collection-level setting is used instead.

        Returns:
            the exact count of matching documents.

        Example:
            >>> my_coll.insert_many([{"seq": i} for i in range(20)])
            InsertManyResult(...)
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

        _max_time_ms = max_time_ms or self.api_options.max_time_ms
        cd_payload = {"countDocuments": {"filter": filter}}
        logger.info(f"countDocuments on '{self.name}'")
        cd_response = self._api_commander.request(
            payload=cd_payload,
            timeout_info=base_timeout_info(_max_time_ms),
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
            raise DataAPIFaultyResponseException(
                text="Faulty response from count_documents API command.",
                raw_response=cd_response,
            )

    def estimated_document_count(
        self,
        *,
        max_time_ms: int | None = None,
    ) -> int:
        """
        Query the API server for an estimate of the document count in the collection.

        Contrary to `count_documents`, this method has no filtering parameters.

        Args:
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
                If not passed, the collection-level setting is used instead.

        Returns:
            a server-provided estimate count of the documents in the collection.

        Example:
            >>> my_coll.estimated_document_count()
            35700
        """
        _max_time_ms = max_time_ms or self.api_options.max_time_ms
        ed_payload: dict[str, Any] = {"estimatedDocumentCount": {}}
        logger.info(f"estimatedDocumentCount on '{self.name}'")
        ed_response = self._api_commander.request(
            payload=ed_payload,
            timeout_info=base_timeout_info(_max_time_ms),
        )
        logger.info(f"finished estimatedDocumentCount on '{self.name}'")
        if "count" in ed_response.get("status", {}):
            count: int = ed_response["status"]["count"]
            return count
        else:
            raise DataAPIFaultyResponseException(
                text="Faulty response from estimated_document_count API command.",
                raw_response=ed_response,
            )

    def find_one_and_replace(
        self,
        filter: FilterType,
        replacement: DocumentType,
        *,
        projection: ProjectionType | None = None,
        sort: SortType | None = None,
        upsert: bool = False,
        return_document: str = ReturnDocument.BEFORE,
        max_time_ms: int | None = None,
    ) -> DocumentType | None:
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
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
                If not passed, the collection-level setting is used instead.

        Returns:
            A document (or a projection thereof, as required), either the one
            before the replace operation or the one after that.
            Alternatively, the method returns None to represent
            that no matching document was found, or that no replacement
            was inserted (depending on the `return_document` parameter).

        Example:
            >>> my_coll.insert_one({"_id": "rule1", "text": "all animals are equal"})
            InsertOneResult(...)
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

        options = {
            "returnDocument": return_document,
            "upsert": upsert,
        }
        _max_time_ms = max_time_ms or self.api_options.max_time_ms
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
        fo_response = self._api_commander.request(
            payload=fo_payload,
            timeout_info=base_timeout_info(_max_time_ms),
        )
        logger.info(f"finished findOneAndReplace on '{self.name}'")
        if "document" in fo_response.get("data", {}):
            ret_document = fo_response.get("data", {}).get("document")
            if ret_document is None:
                return None
            else:
                return ret_document  # type: ignore[no-any-return]
        else:
            raise DataAPIFaultyResponseException(
                text="Faulty response from find_one_and_replace API command.",
                raw_response=fo_response,
            )

    def replace_one(
        self,
        filter: FilterType,
        replacement: DocumentType,
        *,
        sort: SortType | None = None,
        upsert: bool = False,
        max_time_ms: int | None = None,
    ) -> UpdateResult:
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
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
                If not passed, the collection-level setting is used instead.

        Returns:
            an UpdateResult object summarizing the outcome of the replace operation.

        Example:
            >>> my_coll.insert_one({"Marco": "Polo"})
            InsertOneResult(...)
            >>> my_coll.replace_one({"Marco": {"$exists": True}}, {"Buda": "Pest"})
            UpdateResult(raw_results=..., update_info={'n': 1, 'updatedExisting': True, 'ok': 1.0, 'nModified': 1})
            >>> my_coll.find_one({"Buda": "Pest"})
            {'_id': '8424905a-...', 'Buda': 'Pest'}
            >>> my_coll.replace_one({"Mirco": {"$exists": True}}, {"Oh": "yeah?"})
            UpdateResult(raw_results=..., update_info={'n': 0, 'updatedExisting': False, 'ok': 1.0, 'nModified': 0})
            >>> my_coll.replace_one({"Mirco": {"$exists": True}}, {"Oh": "yeah?"}, upsert=True)
            UpdateResult(raw_results=..., update_info={'n': 1, 'updatedExisting': False, 'ok': 1.0, 'nModified': 0, 'upserted': '931b47d6-...'})
        """

        options = {
            "upsert": upsert,
        }
        _max_time_ms = max_time_ms or self.api_options.max_time_ms
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
        fo_response = self._api_commander.request(
            payload=fo_payload,
            timeout_info=base_timeout_info(_max_time_ms),
        )
        logger.info(f"finished findOneAndReplace on '{self.name}'")
        if "document" in fo_response.get("data", {}):
            fo_status = fo_response.get("status") or {}
            _update_info = _prepare_update_info([fo_status])
            return UpdateResult(
                raw_results=[fo_response],
                update_info=_update_info,
            )
        else:
            raise DataAPIFaultyResponseException(
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
        max_time_ms: int | None = None,
    ) -> DocumentType | None:
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
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
                If not passed, the collection-level setting is used instead.

        Returns:
            A document (or a projection thereof, as required), either the one
            before the replace operation or the one after that.
            Alternatively, the method returns None to represent
            that no matching document was found, or that no update
            was applied (depending on the `return_document` parameter).

        Example:
            >>> my_coll.insert_one({"Marco": "Polo"})
            InsertOneResult(...)
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

        options = {
            "returnDocument": return_document,
            "upsert": upsert,
        }
        _max_time_ms = max_time_ms or self.api_options.max_time_ms
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
        fo_response = self._api_commander.request(
            payload=fo_payload,
            timeout_info=base_timeout_info(_max_time_ms),
        )
        logger.info(f"finished findOneAndUpdate on '{self.name}'")
        if "document" in fo_response.get("data", {}):
            ret_document = fo_response.get("data", {}).get("document")
            if ret_document is None:
                return None
            else:
                return ret_document  # type: ignore[no-any-return]
        else:
            raise DataAPIFaultyResponseException(
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
        max_time_ms: int | None = None,
    ) -> UpdateResult:
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
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
                If not passed, the collection-level setting is used instead.

        Returns:
            an UpdateResult object summarizing the outcome of the update operation.

        Example:
            >>> my_coll.insert_one({"Marco": "Polo"})
            InsertOneResult(...)
            >>> my_coll.update_one({"Marco": {"$exists": True}}, {"$inc": {"rank": 3}})
            UpdateResult(raw_results=..., update_info={'n': 1, 'updatedExisting': True, 'ok': 1.0, 'nModified': 1})
            >>> my_coll.update_one({"Mirko": {"$exists": True}}, {"$inc": {"rank": 3}})
            UpdateResult(raw_results=..., update_info={'n': 0, 'updatedExisting': False, 'ok': 1.0, 'nModified': 0})
            >>> my_coll.update_one({"Mirko": {"$exists": True}}, {"$inc": {"rank": 3}}, upsert=True)
            UpdateResult(raw_results=..., update_info={'n': 1, 'updatedExisting': False, 'ok': 1.0, 'nModified': 0, 'upserted': '2a45ff60-...'})
        """

        options = {
            "upsert": upsert,
        }
        _max_time_ms = max_time_ms or self.api_options.max_time_ms
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
        uo_response = self._api_commander.request(
            payload=uo_payload,
            timeout_info=base_timeout_info(_max_time_ms),
        )
        logger.info(f"finished updateOne on '{self.name}'")
        if "status" in uo_response:
            uo_status = uo_response["status"]
            _update_info = _prepare_update_info([uo_status])
            return UpdateResult(
                raw_results=[uo_response],
                update_info=_update_info,
            )
        else:
            raise DataAPIFaultyResponseException(
                text="Faulty response from update_one API command.",
                raw_response=uo_response,
            )

    def update_many(
        self,
        filter: FilterType,
        update: dict[str, Any],
        *,
        upsert: bool = False,
        max_time_ms: int | None = None,
    ) -> UpdateResult:
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
            max_time_ms: a timeout, in milliseconds, for the operation.
                If not passed, the collection-level setting is used instead:
                if a large number of document updates is anticipated, it is suggested
                to specify a larger timeout than in most other operations as the
                update will span several HTTP calls to the API in sequence.

        Returns:
            an UpdateResult object summarizing the outcome of the update operation.

        Example:
            >>> my_coll.insert_many([{"c": "red"}, {"c": "green"}, {"c": "blue"}])
            InsertManyResult(...)
            >>> my_coll.update_many({"c": {"$ne": "green"}}, {"$set": {"nongreen": True}})
            UpdateResult(raw_results=..., update_info={'n': 2, 'updatedExisting': True, 'ok': 1.0, 'nModified': 2})
            >>> my_coll.update_many({"c": "orange"}, {"$set": {"is_also_fruit": True}})
            UpdateResult(raw_results=..., update_info={'n': 0, 'updatedExisting': False, 'ok': 1.0, 'nModified': 0})
            >>> my_coll.update_many(
            ...     {"c": "orange"},
            ...     {"$set": {"is_also_fruit": True}},
            ...     upsert=True,
            ... )
            UpdateResult(raw_results=..., update_info={'n': 1, 'updatedExisting': False, 'ok': 1.0, 'nModified': 0, 'upserted': '46643050-...'})

        Note:
            Similarly to the case of `find` (see its docstring for more details),
            running this command while, at the same time, another process is
            inserting new documents which match the filter of the `update_many`
            can result in an unpredictable fraction of these documents being updated.
            In other words, it cannot be easily predicted whether a given
            newly-inserted document will be picked up by the update_many command or not.
        """

        api_options = {
            "upsert": upsert,
        }
        page_state_options: dict[str, str] = {}
        um_responses: list[dict[str, Any]] = []
        um_statuses: list[dict[str, Any]] = []
        must_proceed = True
        _max_time_ms = max_time_ms or self.api_options.max_time_ms
        logger.info(f"starting update_many on '{self.name}'")
        timeout_manager = MultiCallTimeoutManager(overall_max_time_ms=_max_time_ms)
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
            this_um_response = self._api_commander.request(
                payload=this_um_payload,
                timeout_info=timeout_manager.remaining_timeout_info(),
            )
            logger.info(f"finished updateMany on '{self.name}'")
            this_um_status = this_um_response.get("status") or {}
            #
            # if errors, quit early
            if this_um_response.get("errors", []):
                partial_update_info = _prepare_update_info(um_statuses)
                partial_result = UpdateResult(
                    raw_results=um_responses,
                    update_info=partial_update_info,
                )
                all_um_responses = um_responses + [this_um_response]
                raise UpdateManyException.from_responses(
                    commands=[None for _ in all_um_responses],
                    raw_responses=all_um_responses,
                    partial_result=partial_result,
                )
            else:
                if "status" not in this_um_response:
                    raise DataAPIFaultyResponseException(
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
        return UpdateResult(
            raw_results=um_responses,
            update_info=update_info,
        )

    def find_one_and_delete(
        self,
        filter: FilterType,
        *,
        projection: ProjectionType | None = None,
        sort: SortType | None = None,
        max_time_ms: int | None = None,
    ) -> DocumentType | None:
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
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
                If not passed, the collection-level setting is used instead.

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
            InsertManyResult(...)
            >>> my_coll.find_one_and_delete(
            ...     {"species": {"$ne": "frog"}},
            ...     projection=["species"],
            ... )
            {'_id': '5997fb48-...', 'species': 'swan'}
            >>> my_coll.find_one_and_delete({"species": {"$ne": "frog"}})
            >>> # (returns None for no matches)
        """

        _projection = normalize_optional_projection(projection)
        _max_time_ms = max_time_ms or self.api_options.max_time_ms
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
        fo_response = self._api_commander.request(
            payload=fo_payload,
            timeout_info=base_timeout_info(_max_time_ms),
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
                raise DataAPIFaultyResponseException(
                    text="Faulty response from find_one_and_delete API command.",
                    raw_response=fo_response,
                )

    def delete_one(
        self,
        filter: FilterType,
        *,
        sort: SortType | None = None,
        max_time_ms: int | None = None,
    ) -> DeleteResult:
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
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
                If not passed, the collection-level setting is used instead.

        Returns:
            a DeleteResult object summarizing the outcome of the delete operation.

        Example:
            >>> my_coll.insert_many([{"seq": 1}, {"seq": 0}, {"seq": 2}])
            InsertManyResult(...)
            >>> my_coll.delete_one({"seq": 1})
            DeleteResult(raw_results=..., deleted_count=1)
            >>> my_coll.distinct("seq")
            [0, 2]
            >>> my_coll.delete_one(
            ...     {"seq": {"$exists": True}},
            ...     sort={"seq": astrapy.constants.SortDocuments.DESCENDING},
            ... )
            DeleteResult(raw_results=..., deleted_count=1)
            >>> my_coll.distinct("seq")
            [0]
            >>> my_coll.delete_one({"seq": 2})
            DeleteResult(raw_results=..., deleted_count=0)
        """

        _max_time_ms = max_time_ms or self.api_options.max_time_ms
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
        do_response = self._api_commander.request(
            payload=do_payload,
            timeout_info=base_timeout_info(_max_time_ms),
        )
        logger.info(f"finished deleteOne on '{self.name}'")
        if "deletedCount" in do_response.get("status", {}):
            deleted_count = do_response["status"]["deletedCount"]
            return DeleteResult(
                deleted_count=deleted_count,
                raw_results=[do_response],
            )
        else:
            raise DataAPIFaultyResponseException(
                text="Faulty response from delete_one API command.",
                raw_response=do_response,
            )

    def delete_many(
        self,
        filter: FilterType,
        *,
        max_time_ms: int | None = None,
    ) -> DeleteResult:
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
            max_time_ms: a timeout, in milliseconds, for the operation.
                If not passed, the collection-level setting is used instead:
                keep in mind that this method entails successive HTTP requests
                to the API, depending on how many documents are to be deleted.
                For this reason, in most cases it is suggested to relax the
                timeout compared to other method calls.

        Returns:
            a DeleteResult object summarizing the outcome of the delete operation.

        Example:
            >>> my_coll.insert_many([{"seq": 1}, {"seq": 0}, {"seq": 2}])
            InsertManyResult(...)
            >>> my_coll.delete_many({"seq": {"$lte": 1}})
            DeleteResult(raw_results=..., deleted_count=2)
            >>> my_coll.distinct("seq")
            [2]
            >>> my_coll.delete_many({"seq": {"$lte": 1}})
            DeleteResult(raw_results=..., deleted_count=0)

        Note:
            This operation is in general not atomic. Depending on the amount
            of matching documents, it can keep running (in a blocking way)
            for a macroscopic time. In that case, new documents that are
            meanwhile inserted (e.g. from another process/application) will be
            deleted during the execution of this method call until the
            collection is devoid of matches.
            An exception is the `filter={}` case, whereby the operation is atomic.
        """
        dm_responses: list[dict[str, Any]] = []
        deleted_count = 0
        must_proceed = True
        _max_time_ms = max_time_ms or self.api_options.max_time_ms
        timeout_manager = MultiCallTimeoutManager(overall_max_time_ms=_max_time_ms)
        this_dm_payload = {"deleteMany": {"filter": filter}}
        logger.info(f"starting delete_many on '{self.name}'")
        while must_proceed:
            logger.info(f"deleteMany on '{self.name}'")
            this_dm_response = self._api_commander.request(
                payload=this_dm_payload,
                raise_api_errors=False,
                timeout_info=timeout_manager.remaining_timeout_info(),
            )
            logger.info(f"finished deleteMany on '{self.name}'")
            # if errors, quit early
            if this_dm_response.get("errors", []):
                partial_result = DeleteResult(
                    deleted_count=deleted_count,
                    raw_results=dm_responses,
                )
                all_dm_responses = dm_responses + [this_dm_response]
                raise DeleteManyException.from_responses(
                    commands=[None for _ in all_dm_responses],
                    raw_responses=all_dm_responses,
                    partial_result=partial_result,
                )
            else:
                this_dc = this_dm_response.get("status", {}).get("deletedCount")
                if this_dc is None:
                    raise DataAPIFaultyResponseException(
                        text="Faulty response from delete_many API command.",
                        raw_response=this_dm_response,
                    )
                dm_responses.append(this_dm_response)
                deleted_count += this_dc
                must_proceed = this_dm_response.get("status", {}).get("moreData", False)

        logger.info(f"finished delete_many on '{self.name}'")
        return DeleteResult(
            deleted_count=deleted_count,
            raw_results=dm_responses,
        )

    def drop(self, *, max_time_ms: int | None = None) -> dict[str, Any]:
        """
        Drop the collection, i.e. delete it from the database along with
        all the documents it contains.

        Args:
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
                If not passed, the collection-level setting is used instead.
                Remember there is not guarantee that a request that has
                timed out us not in fact honored.

        Returns:
            a dictionary of the form {"ok": 1} to signal successful deletion.

        Example:
            >>> my_coll.find_one({})
            {'_id': '...', 'a': 100}
            >>> my_coll.drop()
            {'ok': 1}
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

        _max_time_ms = max_time_ms or self.api_options.max_time_ms
        logger.info(f"dropping collection '{self.name}' (self)")
        drop_result = self.database.drop_collection(self, max_time_ms=_max_time_ms)
        logger.info(f"finished dropping collection '{self.name}' (self)")
        return drop_result

    def command(
        self,
        body: dict[str, Any],
        *,
        raise_api_errors: bool = True,
        max_time_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        Send a POST request to the Data API for this collection with
        an arbitrary, caller-provided payload.

        Args:
            body: a JSON-serializable dictionary, the payload of the request.
            raise_api_errors: if True, responses with a nonempty 'errors' field
                result in an astrapy exception being raised.
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
                If not passed, the collection-level setting is used instead.

        Returns:
            a dictionary with the response of the HTTP request.

        Example:
            >>> my_coll.command({"countDocuments": {}})
            {'status': {'count': 123}}
        """

        _max_time_ms = max_time_ms or self.api_options.max_time_ms
        _cmd_desc = ",".join(sorted(body.keys()))
        logger.info(f"command={_cmd_desc} on '{self.name}'")
        command_result = self._api_commander.request(
            payload=body,
            raise_api_errors=raise_api_errors,
            timeout_info=base_timeout_info(_max_time_ms),
        )
        logger.info(f"finished command={_cmd_desc} on '{self.name}'")
        return command_result


class AsyncCollection:
    """
    A Data API collection, the main object to interact with the Data API,
    especially for DDL operations.
    This class has an asynchronous interface for use with asyncio.

    An AsyncCollection is spawned from a Database object, from which it inherits
    the details on how to reach the API server (endpoint, authentication token).

    Args:
        database: a Database object, instantiated earlier. This represents
            the database the collection belongs to.
        name: the collection name. This parameter should match an existing
            collection on the database.
        keyspace: this is the keyspace to which the collection belongs.
            If not specified, the database's working keyspace is used.
        api_options: An instance of `astrapy.utils.api_options.CollectionAPIOptions`
            providing the general settings for interacting with the Data API.
        callers: a list of caller identities, i.e. applications, or frameworks,
            on behalf of which the Data API calls are performed. These end up
            in the request user-agent.
            Each caller identity is a ("caller_name", "caller_version") pair.

    Examples:
        >>> from astrapy import DataAPIClient, AsyncCollection
        >>> my_client = astrapy.DataAPIClient("AstraCS:...")
        >>> my_async_db = my_client.get_async_database(
        ...    "https://01234567-....apps.astra.datastax.com"
        ... )
        >>> my_async_coll_1 = AsyncCollection(database=my_async_db, name="my_collection")
        >>> my_async coll_2 = asyncio.run(my_async_db.create_collection(
        ...     "my_v_collection",
        ...     dimension=3,
        ...     metric="cosine",
        ... ))
        >>> my_async_coll_3a = asyncio.run(my_async_db.get_collection(
        ...     "my_already_existing_collection",
        ... ))
        >>> my_async_coll_3b = my_async_db.my_already_existing_collection
        >>> my_async_coll_3c = my_async_db["my_already_existing_collection"]

    Note:
        creating an instance of AsyncCollection does not trigger actual creation
        of the collection on the database. The latter should have been created
        beforehand, e.g. through the `create_collection` method of an AsyncDatabase.
    """

    def __init__(
        self,
        database: AsyncDatabase,
        name: str,
        *,
        keyspace: str | None = None,
        api_options: CollectionAPIOptions | None = None,
        callers: Sequence[CallerType] = [],
    ) -> None:
        if api_options is None:
            self.api_options = CollectionAPIOptions()
        else:
            self.api_options = api_options
        _keyspace = keyspace if keyspace is not None else database.keyspace
        if _keyspace is None:
            raise ValueError(
                "Attempted to create AsyncCollection with 'keyspace' unset."
            )
        self._database = database._copy(
            keyspace=_keyspace,
            callers=callers,
        )
        self._name = name

        additional_headers = self.api_options.embedding_api_key.get_headers()
        self._commander_headers = {
            **{DEFAULT_DATA_API_AUTH_HEADER: self._database.token_provider.get_token()},
            **additional_headers,
        }

        self.callers = callers
        self._api_commander = self._get_api_commander()

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}(name="{self.name}", '
            f'keyspace="{self.keyspace}", database={self.database}, '
            f"api_options={self.api_options})"
        )

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, AsyncCollection):
            return all(
                [
                    self._api_commander == other._api_commander,
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
                self._database.api_path.strip("/"),
                self._database.api_version.strip("/"),
                self._database.keyspace,
                self._name,
            )
            if comp != ""
        ]
        base_path = f"/{'/'.join(base_path_components)}"
        api_commander = APICommander(
            api_endpoint=self._database.api_endpoint,
            path=base_path,
            headers=self._commander_headers,
            callers=self.callers,
        )
        return api_commander

    async def __aenter__(self) -> AsyncCollection:
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
        database: AsyncDatabase | None = None,
        name: str | None = None,
        keyspace: str | None = None,
        api_options: CollectionAPIOptions | None = None,
        callers: Sequence[CallerType] = [],
    ) -> AsyncCollection:
        return AsyncCollection(
            database=database or self.database._copy(),
            name=name or self.name,
            keyspace=keyspace or self.keyspace,
            api_options=self.api_options.with_override(api_options),
            callers=callers or self.callers,
        )

    def with_options(
        self,
        *,
        name: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | None = None,
        collection_max_time_ms: int | None = None,
        callers: Sequence[CallerType] = [],
    ) -> AsyncCollection:
        """
        Create a clone of this collection with some changed attributes.

        Args:
            name: the name of the collection. This parameter is useful to
                quickly spawn AsyncCollection instances each pointing to a different
                collection existing in the same keyspace.
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
            callers: a list of caller identities, i.e. applications, or frameworks,
                on behalf of which the Data API calls are performed. These end up
                in the request user-agent.
                Each caller identity is a ("caller_name", "caller_version") pair.

        Returns:
            a new AsyncCollection instance.

        Example:
            >>> my_other_async_coll = my_async_coll.with_options(
            ...     name="the_other_coll",
            ...     callers=[("caller_identity", "0.1.2")],
            ... )
        """

        _api_options = CollectionAPIOptions(
            embedding_api_key=coerce_embedding_headers_provider(embedding_api_key),
            max_time_ms=collection_max_time_ms,
        )

        return self._copy(
            name=name,
            api_options=_api_options,
            callers=callers,
        )

    def to_sync(
        self,
        *,
        database: Database | None = None,
        name: str | None = None,
        keyspace: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | None = None,
        collection_max_time_ms: int | None = None,
        callers: Sequence[CallerType] = [],
    ) -> Collection:
        """
        Create a Collection from this one. Save for the arguments
        explicitly provided as overrides, everything else is kept identical
        to this collection in the copy (the database is converted into
        a sync object).

        Args:
            database: a Database object, instantiated earlier.
                This represents the database the new collection belongs to.
            name: the collection name. This parameter should match an existing
                collection on the database.
            keyspace: this is the keyspace to which the collection belongs.
                If not specified, the database's working keyspace is used.
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
            callers: a list of caller identities, i.e. applications, or frameworks,
                on behalf of which the Data API calls are performed. These end up
                in the request user-agent.
                Each caller identity is a ("caller_name", "caller_version") pair.

        Returns:
            the new copy, a Collection instance.

        Example:
            >>> my_async_coll.to_sync().count_documents({}, upper_bound=100)
            77
        """

        _api_options = CollectionAPIOptions(
            embedding_api_key=coerce_embedding_headers_provider(embedding_api_key),
            max_time_ms=collection_max_time_ms,
        )

        return Collection(
            database=database or self.database.to_sync(),
            name=name or self.name,
            keyspace=keyspace or self.keyspace,
            api_options=self.api_options.with_override(_api_options),
            callers=callers or self.callers,
        )

    async def options(self, *, max_time_ms: int | None = None) -> CollectionOptions:
        """
        Get the collection options, i.e. its configuration as read from the database.

        The method issues a request to the Data API each time is invoked,
        without caching mechanisms: this ensures up-to-date information
        for usages such as real-time collection validation by the application.

        Args:
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
                If not passed, the collection-level setting is used instead.

        Returns:
            a CollectionOptions instance describing the collection.
            (See also the database `list_collections` method.)

        Example:
            >>> asyncio.run(my_async_coll.options())
            CollectionOptions(vector=CollectionVectorOptions(dimension=3, metric='cosine'))
        """

        _max_time_ms = max_time_ms or self.api_options.max_time_ms
        logger.info(f"getting collections in search of '{self.name}'")
        self_descriptors = [
            coll_desc
            async for coll_desc in self.database.list_collections(
                max_time_ms=_max_time_ms
            )
            if coll_desc.name == self.name
        ]
        logger.info(f"finished getting collections in search of '{self.name}'")
        if self_descriptors:
            return self_descriptors[0].options
        else:
            raise CollectionNotFoundException(
                text=f"Collection {self.keyspace}.{self.name} not found.",
                keyspace=self.keyspace,
                collection_name=self.name,
            )

    def info(self) -> CollectionInfo:
        """
        Information on the collection (name, location, database), in the
        form of a CollectionInfo object.

        Not to be confused with the collection `options` method (related
        to the collection internal configuration).

        Example:
            >>> my_async_coll.info().database_info.region
            'us-east1'
            >>> my_async_coll.info().full_name
            'default_keyspace.my_v_collection'

        Note:
            the returned CollectionInfo wraps, among other things,
            the database information: as such, calling this method
            triggers the same-named method of a Database object (which, in turn,
            performs a HTTP request to the DevOps API).
            See the documentation for `Database.info()` for more details.
        """

        return CollectionInfo(
            database_info=self.database.info(),
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
            'quicktest'
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
            raise ValueError("The collection's DB is set with keyspace=None")
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
        document: DocumentType,
        *,
        max_time_ms: int | None = None,
    ) -> InsertOneResult:
        """
        Insert a single document in the collection in an atomic operation.

        Args:
            document: the dictionary expressing the document to insert.
                The `_id` field of the document can be left out, in which
                case it will be created automatically.
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
                If not passed, the collection-level setting is used instead.

        Returns:
            an InsertOneResult object.

        Example:
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
            InsertOneResult(...)

        Note:
            If an `_id` is explicitly provided, which corresponds to a document
            that exists already in the collection, an error is raised and
            the insertion fails.
        """

        _max_time_ms = max_time_ms or self.api_options.max_time_ms
        io_payload = {"insertOne": {"document": document}}
        logger.info(f"insertOne on '{self.name}'")
        io_response = await self._api_commander.async_request(
            payload=io_payload,
            timeout_info=base_timeout_info(_max_time_ms),
        )
        logger.info(f"finished insertOne on '{self.name}'")
        if "insertedIds" in io_response.get("status", {}):
            if io_response["status"]["insertedIds"]:
                inserted_id = io_response["status"]["insertedIds"][0]
                return InsertOneResult(
                    raw_results=[io_response],
                    inserted_id=inserted_id,
                )
            else:
                raise ValueError(
                    "Could not complete a insert_one operation. "
                    f"(gotten '${json.dumps(io_response)}')"
                )
        else:
            raise ValueError(
                "Could not complete a insert_one operation. "
                f"(gotten '${json.dumps(io_response)}')"
            )

    async def insert_many(
        self,
        documents: Iterable[DocumentType],
        *,
        ordered: bool = False,
        chunk_size: int | None = None,
        concurrency: int | None = None,
        max_time_ms: int | None = None,
    ) -> InsertManyResult:
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
            max_time_ms: a timeout, in milliseconds, for the operation.
                If not passed, the collection-level setting is used instead:
                If many documents are being inserted, this method corresponds
                to several HTTP requests: in such cases one may want to specify
                a more tolerant timeout here.
        Returns:
            an InsertManyResult object.

        Examples:
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
            InsertManyResult(...)

        Note:
            Unordered insertions are executed with some degree of concurrency,
            so it is usually better to prefer this mode unless the order in the
            document sequence is important.

        Note:
            A failure mode for this command is related to certain faulty documents
            found among those to insert: a document may have the an `_id` already
            present on the collection, or its vector dimension may not
            match the collection setting.

            For an ordered insertion, the method will raise an exception at
            the first such faulty document -- nevertheless, all documents processed
            until then will end up being written to the database.

            For unordered insertions, if the error stems from faulty documents
            the insertion proceeds until exhausting the input documents: then,
            an exception is raised -- and all insertable documents will have been
            written to the database, including those "after" the troublesome ones.

            If, on the other hand, there are errors not related to individual
            documents (such as a network connectivity error), the whole
            `insert_many` operation will stop in mid-way, an exception will be raised,
            and only a certain amount of the input documents will
            have made their way to the database.
        """

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
        _max_time_ms = max_time_ms or self.api_options.max_time_ms
        _documents = list(documents)
        logger.info(f"inserting {len(_documents)} documents in '{self.name}'")
        raw_results: list[dict[str, Any]] = []
        timeout_manager = MultiCallTimeoutManager(overall_max_time_ms=_max_time_ms)
        if ordered:
            options = {"ordered": True}
            inserted_ids: list[Any] = []
            for i in range(0, len(_documents), _chunk_size):
                im_payload = {
                    "insertMany": {
                        "documents": _documents[i : i + _chunk_size],
                        "options": options,
                    },
                }
                logger.info(f"insertMany on '{self.name}'")
                chunk_response = await self._api_commander.async_request(
                    payload=im_payload,
                    raise_api_errors=False,
                    timeout_info=timeout_manager.remaining_timeout_info(),
                )
                logger.info(f"finished insertMany on '{self.name}'")
                # accumulate the results in this call
                chunk_inserted_ids = (chunk_response.get("status") or {}).get(
                    "insertedIds", []
                )
                inserted_ids += chunk_inserted_ids
                raw_results += [chunk_response]
                # if errors, quit early
                if chunk_response.get("errors", []):
                    partial_result = InsertManyResult(
                        raw_results=raw_results,
                        inserted_ids=inserted_ids,
                    )
                    raise InsertManyException.from_response(
                        command=None,
                        raw_response=chunk_response,
                        partial_result=partial_result,
                    )

            # return
            full_result = InsertManyResult(
                raw_results=raw_results,
                inserted_ids=inserted_ids,
            )
            logger.info(
                f"finished inserting {len(_documents)} documents in '{self.name}'"
            )
            return full_result

        else:
            # unordered: concurrent or not, do all of them and parse the results
            options = {"ordered": False}

            sem = asyncio.Semaphore(_concurrency)

            async def concurrent_insert_chunk(
                document_chunk: list[DocumentType],
            ) -> dict[str, Any]:
                async with sem:
                    im_payload = {
                        "insertMany": {
                            "documents": document_chunk,
                            "options": options,
                        },
                    }
                    logger.info(f"insertMany(chunk) on '{self.name}'")
                    im_response = await self._api_commander.async_request(
                        payload=im_payload,
                        raise_api_errors=False,
                        timeout_info=timeout_manager.remaining_timeout_info(),
                    )
                    logger.info(f"finished insertMany(chunk) on '{self.name}'")
                    return im_response

            if _concurrency > 1:
                tasks = [
                    asyncio.create_task(
                        concurrent_insert_chunk(_documents[i : i + _chunk_size])
                    )
                    for i in range(0, len(_documents), _chunk_size)
                ]
                raw_results = await asyncio.gather(*tasks)
            else:
                raw_results = [
                    await concurrent_insert_chunk(_documents[i : i + _chunk_size])
                    for i in range(0, len(_documents), _chunk_size)
                ]

            # recast raw_results
            inserted_ids = [
                inserted_id
                for chunk_response in raw_results
                for inserted_id in (chunk_response.get("status") or {}).get(
                    "insertedIds", []
                )
            ]

            # check-raise
            if any(
                [chunk_response.get("errors", []) for chunk_response in raw_results]
            ):
                partial_result = InsertManyResult(
                    raw_results=raw_results,
                    inserted_ids=inserted_ids,
                )
                raise InsertManyException.from_responses(
                    commands=[None for _ in raw_results],
                    raw_responses=raw_results,
                    partial_result=partial_result,
                )

            # return
            full_result = InsertManyResult(
                raw_results=raw_results,
                inserted_ids=inserted_ids,
            )
            logger.info(
                f"finished inserting {len(_documents)} documents in '{self.name}'"
            )
            return full_result

    def find(
        self,
        filter: FilterType | None = None,
        *,
        projection: ProjectionType | None = None,
        skip: int | None = None,
        limit: int | None = None,
        include_similarity: bool | None = None,
        include_sort_vector: bool | None = None,
        sort: SortType | None = None,
        max_time_ms: int | None = None,
    ) -> AsyncCursor:
        """
        Find documents on the collection, matching a certain provided filter.

        The method returns a Cursor that can then be iterated over. Depending
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
                returned document. Can only be used for vector ANN search, i.e.
                when either `vector` is supplied or the `sort` parameter has the
                shape {"$vector": ...}.
            include_sort_vector: a boolean to request query vector used in this search.
                If set to True (and if the invocation is a vector search), calling
                the `get_sort_vector` method on the returned cursor will yield
                the vector used for the ANN search.
            sort: with this dictionary parameter one can control the order
                the documents are returned. See the Note about sorting, as well as
                the one about upper bounds, for details.
                Vector-based ANN sorting is achieved by providing a "$vector"
                or a "$vectorize" key in `sort`.
            max_time_ms: a timeout, in milliseconds, for each single one
                of the underlying HTTP requests used to fetch documents as the
                cursor is iterated over.
                If not passed, the collection-level setting is used instead.

        Returns:
            an AsyncCursor object representing iterations over the matching documents
            (see the AsyncCursor object for how to use it. The simplest thing is to
            run a for loop: `for document in collection.sort(...):`).

        Examples:
            >>> async def run_finds(acol: AsyncCollection) -> None:
            ...             filter = {"seq": {"$exists": True}}
            ...             print("find results 1:")
            ...             async for doc in acol.find(filter, projection={"seq": True}, limit=5):
            ...                 print(doc["seq"])
            ...             async_cursor1 = acol.find(
            ...                 {},
            ...                 limit=4,
            ...                 sort={"seq": astrapy.constants.SortDocuments.DESCENDING},
            ...             )
            ...             ids = [doc["_id"] async for doc in async_cursor1]
            ...             print("find results 2:", ids)
            ...             async_cursor2 = acol.find({}, limit=3)
            ...             seqs = await async_cursor2.distinct("seq")
            ...             print("distinct results 3:", seqs)
            ...
            >>> asyncio.run(run_finds(my_async_coll))
            find results 1:
            48
            35
            7
            11
            13
            find results 2: ['d656cd9d-...', '479c7ce8-...', '96dc87fd-...', '83f0a21f-...']
            distinct results 3: [48, 35, 7]

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
                sort={"field": SortDocuments.ASCENDING}
                sort={"field": SortDocuments.DESCENDING}
            When sorting first by "field" and then by "subfield"
            (while modern Python versions preserve the order of dictionaries,
            it is suggested for clarity to employ a `collections.OrderedDict`
            in these cases):
                sort={
                    "field": SortDocuments.ASCENDING,
                    "subfield": SortDocuments.ASCENDING,
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
            These provisions should be kept in mind even when subsequently running
            a command such as `.distinct()` on a cursor.

        Note:
            When not specifying sorting criteria at all (by vector or otherwise),
            the cursor can scroll through an arbitrary number of documents as
            the Data API and the client periodically exchange new chunks of documents.
            It should be noted that the behavior of the cursor in the case documents
            have been added/removed after the `find` was started depends on database
            internals and it is not guaranteed, nor excluded, that such "real-time"
            changes in the data would be picked up by the cursor.
        """

        _max_time_ms = max_time_ms or self.api_options.max_time_ms
        if include_similarity is not None and not _is_vector_sort(sort):
            raise ValueError(
                "Cannot use `include_similarity` unless for vector search."
            )
        return (
            AsyncCursor(
                collection=self,
                filter=filter,
                projection=projection,
                max_time_ms=_max_time_ms,
                overall_max_time_ms=None,
            )
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
        max_time_ms: int | None = None,
    ) -> DocumentType | None:
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
                returned document. Can only be used for vector ANN search, i.e.
                when either `vector` is supplied or the `sort` parameter has the
                shape {"$vector": ...}.
            sort: with this dictionary parameter one can control the order
                the documents are returned. See the Note about sorting for details.
                Vector-based ANN sorting is achieved by providing a "$vector"
                or a "$vectorize" key in `sort`.
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
                If not passed, the collection-level setting is used instead.

        Returns:
            a dictionary expressing the required document, otherwise None.

        Example:
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
            ...         sort={"seq": astrapy.constants.SortDocuments.DESCENDING},
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

        _max_time_ms = max_time_ms or self.api_options.max_time_ms
        fo_cursor = self.find(
            filter=filter,
            projection=projection,
            skip=None,
            limit=1,
            include_similarity=include_similarity,
            sort=sort,
            max_time_ms=_max_time_ms,
        )
        try:
            document = await fo_cursor.__anext__()
            return document
        except StopAsyncIteration:
            return None

    async def distinct(
        self,
        key: str,
        *,
        filter: FilterType | None = None,
        max_time_ms: int | None = None,
    ) -> list[Any]:
        """
        Return a list of the unique values of `key` across the documents
        in the collection that match the provided filter.

        Args:
            key: the name of the field whose value is inspected across documents.
                Keys can use dot-notation to descend to deeper document levels.
                Example of acceptable `key` values:
                    "field"
                    "field.subfield"
                    "field.3"
                    "field.3.subfield"
                If lists are encountered and no numeric index is specified,
                all items in the list are visited.
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$lt": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$lt": 100}}]}
                See the Data API documentation for the full set of operators.
            max_time_ms: a timeout, in milliseconds, with the same meaning as for `find`.
                If not passed, the collection-level setting is used instead.

        Returns:
            a list of all different values for `key` found across the documents
            that match the filter. The result list has no repeated items.

        Example:
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

        _max_time_ms = max_time_ms or self.api_options.max_time_ms
        f_cursor = AsyncCursor(
            collection=self,
            filter=filter,
            projection={key: True},
            max_time_ms=None,
            overall_max_time_ms=_max_time_ms,
        )
        return await f_cursor.distinct(key)

    async def count_documents(
        self,
        filter: FilterType,
        *,
        upper_bound: int,
        max_time_ms: int | None = None,
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
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
                If not passed, the collection-level setting is used instead.

        Returns:
            the exact count of matching documents.

        Example:
            >>> async def do_count_docs(acol: AsyncCollection) -> None:
            ...     await acol.insert_many([{"seq": i} for i in range(20)])
            ...     count0 = await acol.count_documents({}, upper_bound=100)
            ...     print("count0", count0)
            ...     count1 = await acol.count_documents({"seq":{"$gt": 15}}, upper_bound=100)
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

        _max_time_ms = max_time_ms or self.api_options.max_time_ms
        cd_payload = {"countDocuments": {"filter": filter}}
        logger.info(f"countDocuments on '{self.name}'")
        cd_response = await self._api_commander.async_request(
            payload=cd_payload,
            timeout_info=base_timeout_info(_max_time_ms),
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
            raise DataAPIFaultyResponseException(
                text="Faulty response from count_documents API command.",
                raw_response=cd_response,
            )

    async def estimated_document_count(
        self,
        *,
        max_time_ms: int | None = None,
    ) -> int:
        """
        Query the API server for an estimate of the document count in the collection.

        Contrary to `count_documents`, this method has no filtering parameters.

        Args:
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
                If not passed, the collection-level setting is used instead.

        Returns:
            a server-provided estimate count of the documents in the collection.

        Example:
            >>> asyncio.run(my_async_coll.estimated_document_count())
            35700
        """
        _max_time_ms = max_time_ms or self.api_options.max_time_ms
        ed_payload: dict[str, Any] = {"estimatedDocumentCount": {}}
        logger.info(f"estimatedDocumentCount on '{self.name}'")
        ed_response = await self._api_commander.async_request(
            payload=ed_payload,
            timeout_info=base_timeout_info(_max_time_ms),
        )
        logger.info(f"finished estimatedDocumentCount on '{self.name}'")
        if "count" in ed_response.get("status", {}):
            count: int = ed_response["status"]["count"]
            return count
        else:
            raise DataAPIFaultyResponseException(
                text="Faulty response from estimated_document_count API command.",
                raw_response=ed_response,
            )

    async def find_one_and_replace(
        self,
        filter: FilterType,
        replacement: DocumentType,
        *,
        projection: ProjectionType | None = None,
        sort: SortType | None = None,
        upsert: bool = False,
        return_document: str = ReturnDocument.BEFORE,
        max_time_ms: int | None = None,
    ) -> DocumentType | None:
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
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
                If not passed, the collection-level setting is used instead.

        Returns:
            A document, either the one before the replace operation or the
            one after that. Alternatively, the method returns None to represent
            that no matching document was found, or that no replacement
            was inserted (depending on the `return_document` parameter).

        Example:
            >>> async def do_find_one_and_replace(acol: AsyncCollection) -> None:
            ...             await acol.insert_one({"_id": "rule1", "text": "all animals are equal"})
            ...             result0 = await acol.find_one_and_replace(
            ...                 {"_id": "rule1"},
            ...                 {"text": "some animals are more equal!"},
            ...             )
            ...             print("result0", result0)
            ...             result1 = await acol.find_one_and_replace(
            ...                 {"text": "some animals are more equal!"},
            ...                 {"text": "and the pigs are the rulers"},
            ...                 return_document=astrapy.constants.ReturnDocument.AFTER,
            ...             )
            ...             print("result1", result1)
            ...             result2 = await acol.find_one_and_replace(
            ...                 {"_id": "rule2"},
            ...                 {"text": "F=ma^2"},
            ...                 return_document=astrapy.constants.ReturnDocument.AFTER,
            ...             )
            ...             print("result2", result2)
            ...             result3 = await acol.find_one_and_replace(
            ...                 {"_id": "rule2"},
            ...                 {"text": "F=ma"},
            ...                 upsert=True,
            ...                 return_document=astrapy.constants.ReturnDocument.AFTER,
            ...                 projection={"_id": False},
            ...             )
            ...             print("result3", result3)
            ...
            >>> asyncio.run(do_find_one_and_replace(my_async_coll))
            result0 {'_id': 'rule1', 'text': 'all animals are equal'}
            result1 {'_id': 'rule1', 'text': 'and the pigs are the rulers'}
            result2 None
            result3 {'text': 'F=ma'}
        """

        options = {
            "returnDocument": return_document,
            "upsert": upsert,
        }
        _max_time_ms = max_time_ms or self.api_options.max_time_ms
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
        fo_response = await self._api_commander.async_request(
            payload=fo_payload,
            timeout_info=base_timeout_info(_max_time_ms),
        )
        logger.info(f"finished findOneAndReplace on '{self.name}'")
        if "document" in fo_response.get("data", {}):
            ret_document = fo_response.get("data", {}).get("document")
            if ret_document is None:
                return None
            else:
                return ret_document  # type: ignore[no-any-return]
        else:
            raise DataAPIFaultyResponseException(
                text="Faulty response from find_one_and_replace API command.",
                raw_response=fo_response,
            )

    async def replace_one(
        self,
        filter: FilterType,
        replacement: DocumentType,
        *,
        sort: SortType | None = None,
        upsert: bool = False,
        max_time_ms: int | None = None,
    ) -> UpdateResult:
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
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
                If not passed, the collection-level setting is used instead.

        Returns:
            an UpdateResult object summarizing the outcome of the replace operation.

        Example:
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

        options = {
            "upsert": upsert,
        }
        _max_time_ms = max_time_ms or self.api_options.max_time_ms
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
        fo_response = await self._api_commander.async_request(
            payload=fo_payload,
            timeout_info=base_timeout_info(_max_time_ms),
        )
        logger.info(f"finished findOneAndReplace on '{self.name}'")
        if "document" in fo_response.get("data", {}):
            fo_status = fo_response.get("status") or {}
            _update_info = _prepare_update_info([fo_status])
            return UpdateResult(
                raw_results=[fo_response],
                update_info=_update_info,
            )
        else:
            raise DataAPIFaultyResponseException(
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
        max_time_ms: int | None = None,
    ) -> DocumentType | None:
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
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
                If not passed, the collection-level setting is used instead.

        Returns:
            A document (or a projection thereof, as required), either the one
            before the replace operation or the one after that.
            Alternatively, the method returns None to represent
            that no matching document was found, or that no update
            was applied (depending on the `return_document` parameter).

        Example:
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

        options = {
            "returnDocument": return_document,
            "upsert": upsert,
        }
        _max_time_ms = max_time_ms or self.api_options.max_time_ms
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
        fo_response = await self._api_commander.async_request(
            payload=fo_payload,
            timeout_info=base_timeout_info(_max_time_ms),
        )
        logger.info(f"finished findOneAndUpdate on '{self.name}'")
        if "document" in fo_response.get("data", {}):
            ret_document = fo_response.get("data", {}).get("document")
            if ret_document is None:
                return None
            else:
                return ret_document  # type: ignore[no-any-return]
        else:
            raise DataAPIFaultyResponseException(
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
        max_time_ms: int | None = None,
    ) -> UpdateResult:
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
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
                If not passed, the collection-level setting is used instead.

        Returns:
            an UpdateResult object summarizing the outcome of the update operation.

        Example:
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

        options = {
            "upsert": upsert,
        }
        _max_time_ms = max_time_ms or self.api_options.max_time_ms
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
        uo_response = await self._api_commander.async_request(
            payload=uo_payload,
            timeout_info=base_timeout_info(_max_time_ms),
        )
        logger.info(f"finished updateOne on '{self.name}'")
        if "status" in uo_response:
            uo_status = uo_response["status"]
            _update_info = _prepare_update_info([uo_status])
            return UpdateResult(
                raw_results=[uo_response],
                update_info=_update_info,
            )
        else:
            raise DataAPIFaultyResponseException(
                text="Faulty response from update_one API command.",
                raw_response=uo_response,
            )

    async def update_many(
        self,
        filter: FilterType,
        update: dict[str, Any],
        *,
        upsert: bool = False,
        max_time_ms: int | None = None,
    ) -> UpdateResult:
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
            max_time_ms: a timeout, in milliseconds, for the operation.
                If not passed, the collection-level setting is used instead:
                if a large number of document updates is anticipated, it is suggested
                to specify a larger timeout than in most other operations as the
                update will span several HTTP calls to the API in sequence.

        Returns:
            an UpdateResult object summarizing the outcome of the update operation.

        Example:
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

        api_options = {
            "upsert": upsert,
        }
        page_state_options: dict[str, str] = {}
        um_responses: list[dict[str, Any]] = []
        um_statuses: list[dict[str, Any]] = []
        must_proceed = True
        _max_time_ms = max_time_ms or self.api_options.max_time_ms
        logger.info(f"starting update_many on '{self.name}'")
        timeout_manager = MultiCallTimeoutManager(overall_max_time_ms=_max_time_ms)
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
            this_um_response = await self._api_commander.async_request(
                payload=this_um_payload,
                timeout_info=timeout_manager.remaining_timeout_info(),
            )
            logger.info(f"finished updateMany on '{self.name}'")
            this_um_status = this_um_response.get("status") or {}
            #
            # if errors, quit early
            if this_um_response.get("errors", []):
                partial_update_info = _prepare_update_info(um_statuses)
                partial_result = UpdateResult(
                    raw_results=um_responses,
                    update_info=partial_update_info,
                )
                all_um_responses = um_responses + [this_um_response]
                raise UpdateManyException.from_responses(
                    commands=[None for _ in all_um_responses],
                    raw_responses=all_um_responses,
                    partial_result=partial_result,
                )
            else:
                if "status" not in this_um_response:
                    raise DataAPIFaultyResponseException(
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
        return UpdateResult(
            raw_results=um_responses,
            update_info=update_info,
        )

    async def find_one_and_delete(
        self,
        filter: FilterType,
        *,
        projection: ProjectionType | None = None,
        sort: SortType | None = None,
        max_time_ms: int | None = None,
    ) -> DocumentType | None:
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
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
                If not passed, the collection-level setting is used instead.

        Returns:
            Either the document (or a projection thereof, as requested), or None
            if no matches were found in the first place.

        Example:
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

        _projection = normalize_optional_projection(projection)
        _max_time_ms = max_time_ms or self.api_options.max_time_ms
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
        fo_response = await self._api_commander.async_request(
            payload=fo_payload,
            timeout_info=base_timeout_info(_max_time_ms),
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
                raise DataAPIFaultyResponseException(
                    text="Faulty response from find_one_and_delete API command.",
                    raw_response=fo_response,
                )

    async def delete_one(
        self,
        filter: FilterType,
        *,
        sort: SortType | None = None,
        max_time_ms: int | None = None,
    ) -> DeleteResult:
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
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
                If not passed, the collection-level setting is used instead.

        Returns:
            a DeleteResult object summarizing the outcome of the delete operation.

        Example:
            >>> my_coll.insert_many([{"seq": 1}, {"seq": 0}, {"seq": 2}])
            InsertManyResult(...)
            >>> my_coll.delete_one({"seq": 1})
            DeleteResult(raw_results=..., deleted_count=1)
            >>> my_coll.distinct("seq")
            [0, 2]
            >>> my_coll.delete_one(
            ...     {"seq": {"$exists": True}},
            ...     sort={"seq": astrapy.constants.SortDocuments.DESCENDING},
            ... )
            DeleteResult(raw_results=..., deleted_count=1)
            >>> my_coll.distinct("seq")
            [0]
            >>> my_coll.delete_one({"seq": 2})
            DeleteResult(raw_results=..., deleted_count=0)
        """

        _max_time_ms = max_time_ms or self.api_options.max_time_ms
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
        do_response = await self._api_commander.async_request(
            payload=do_payload,
            timeout_info=base_timeout_info(_max_time_ms),
        )
        logger.info(f"finished deleteOne on '{self.name}'")
        if "deletedCount" in do_response.get("status", {}):
            deleted_count = do_response["status"]["deletedCount"]
            return DeleteResult(
                deleted_count=deleted_count,
                raw_results=[do_response],
            )
        else:
            raise DataAPIFaultyResponseException(
                text="Faulty response from delete_one API command.",
                raw_response=do_response,
            )

    async def delete_many(
        self,
        filter: FilterType,
        *,
        max_time_ms: int | None = None,
    ) -> DeleteResult:
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
            max_time_ms: a timeout, in milliseconds, for the operation.
                If not passed, the collection-level setting is used instead:
                keep in mind that this method entails successive HTTP requests
                to the API, depending on how many documents are to be deleted.
                For this reason, in most cases it is suggested to relax the
                timeout compared to other method calls.

        Returns:
            a DeleteResult object summarizing the outcome of the delete operation.

        Example:
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
        dm_responses: list[dict[str, Any]] = []
        deleted_count = 0
        must_proceed = True
        _max_time_ms = max_time_ms or self.api_options.max_time_ms
        timeout_manager = MultiCallTimeoutManager(overall_max_time_ms=_max_time_ms)
        this_dm_payload = {"deleteMany": {"filter": filter}}
        logger.info(f"starting delete_many on '{self.name}'")
        while must_proceed:
            logger.info(f"deleteMany on '{self.name}'")
            this_dm_response = await self._api_commander.async_request(
                payload=this_dm_payload,
                raise_api_errors=False,
                timeout_info=timeout_manager.remaining_timeout_info(),
            )
            logger.info(f"finished deleteMany on '{self.name}'")
            # if errors, quit early
            if this_dm_response.get("errors", []):
                partial_result = DeleteResult(
                    deleted_count=deleted_count,
                    raw_results=dm_responses,
                )
                all_dm_responses = dm_responses + [this_dm_response]
                raise DeleteManyException.from_responses(
                    commands=[None for _ in all_dm_responses],
                    raw_responses=all_dm_responses,
                    partial_result=partial_result,
                )
            else:
                this_dc = this_dm_response.get("status", {}).get("deletedCount")
                if this_dc is None:
                    raise DataAPIFaultyResponseException(
                        text="Faulty response from delete_many API command.",
                        raw_response=this_dm_response,
                    )
                dm_responses.append(this_dm_response)
                deleted_count += this_dc
                must_proceed = this_dm_response.get("status", {}).get("moreData", False)

        logger.info(f"finished delete_many on '{self.name}'")
        return DeleteResult(
            deleted_count=deleted_count,
            raw_results=dm_responses,
        )

    async def drop(self, *, max_time_ms: int | None = None) -> dict[str, Any]:
        """
        Drop the collection, i.e. delete it from the database along with
        all the documents it contains.

        Args:
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
                If not passed, the collection-level setting is used instead.
                Remember there is not guarantee that a request that has
                timed out us not in fact honored.

        Returns:
            a dictionary of the form {"ok": 1} to signal successful deletion.

        Example:
            >>> async def drop_and_check(acol: AsyncCollection) -> None:
            ...     doc0 = await acol.find_one({})
            ...     print("doc0", doc0)
            ...     drop_result = await acol.drop()
            ...     print("drop_result", drop_result)
            ...     doc1 = await acol.find_one({})
            ...
            >>> asyncio.run(drop_and_check(my_async_coll))
            doc0 {'_id': '...', 'z': -10}
            drop_result {'ok': 1}
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

        _max_time_ms = max_time_ms or self.api_options.max_time_ms
        logger.info(f"dropping collection '{self.name}' (self)")
        drop_result = await self.database.drop_collection(
            self, max_time_ms=_max_time_ms
        )
        logger.info(f"finished dropping collection '{self.name}' (self)")
        return drop_result

    async def command(
        self,
        body: dict[str, Any],
        *,
        raise_api_errors: bool = True,
        max_time_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        Send a POST request to the Data API for this collection with
        an arbitrary, caller-provided payload.

        Args:
            body: a JSON-serializable dictionary, the payload of the request.
            raise_api_errors: if True, responses with a nonempty 'errors' field
                result in an astrapy exception being raised.
            max_time_ms: a timeout, in milliseconds, for the underlying HTTP request.
                If not passed, the collection-level setting is used instead.

        Returns:
            a dictionary with the response of the HTTP request.

        Example:
            >>> asyncio.await(my_async_coll.command({"countDocuments": {}}))
            {'status': {'count': 123}}
        """

        _max_time_ms = max_time_ms or self.api_options.max_time_ms
        _cmd_desc = ",".join(sorted(body.keys()))
        logger.info(f"command={_cmd_desc} on '{self.name}'")
        command_result = await self._api_commander.async_request(
            payload=body,
            raise_api_errors=raise_api_errors,
            timeout_info=base_timeout_info(_max_time_ms),
        )
        logger.info(f"finished command={_cmd_desc} on '{self.name}'")
        return command_result
