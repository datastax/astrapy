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
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any, Dict, Iterable, List, Optional, Union, TYPE_CHECKING

from astrapy.core.db import (
    AstraDBCollection,
    AsyncAstraDBCollection,
)
from astrapy.core.defaults import MAX_INSERT_NUM_DOCUMENTS
from astrapy.exceptions import InsertManyException
from astrapy.constants import (
    DocumentType,
    FilterType,
    ProjectionType,
    ReturnDocument,
    SortType,
    normalize_optional_projection,
)
from astrapy.database import AsyncDatabase, Database
from astrapy.results import (
    DeleteResult,
    InsertManyResult,
    InsertOneResult,
    UpdateResult,
    BulkWriteResult,
)
from astrapy.cursors import AsyncCursor, Cursor
from astrapy.info import CollectionInfo


if TYPE_CHECKING:
    from astrapy.operations import AsyncBaseOperation, BaseOperation


INSERT_MANY_CONCURRENCY = 20
BULK_WRITE_CONCURRENCY = 10


def _prepare_update_info(status: Dict[str, Any]) -> Dict[str, Any]:
    return {
        **{
            "n": status.get("matchedCount") + (1 if "upsertedId" in status else 0),  # type: ignore[operator]
            "updatedExisting": (status.get("modifiedCount") or 0) > 0,
            "ok": 1.0,
            "nModified": status.get("modifiedCount"),
        },
        **({"upserted": status["upsertedId"]} if "upsertedId" in status else {}),
    }


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
        namespace: this is the namespace to which the collection belongs.
            This is generally not specified, in which case the general setting
            for the provided Database is used.
        caller_name: name of the application, or framework, on behalf of which
            the Data API calls are performed. This ends up in the request user-agent.
        caller_version: version of the caller.

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
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> None:
        self._astra_db_collection: AstraDBCollection = AstraDBCollection(
            collection_name=name,
            astra_db=database._astra_db,
            namespace=namespace,
            caller_name=caller_name,
            caller_version=caller_version,
        )
        # this comes after the above, lets AstraDBCollection resolve namespace
        self._database = database._copy(
            namespace=self._astra_db_collection.astra_db.namespace
        )

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}[_astra_db_collection="{self._astra_db_collection}"]'

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Collection):
            return self._astra_db_collection == other._astra_db_collection
        else:
            return False

    def __call__(self, *pargs: Any, **kwargs: Any) -> None:
        raise TypeError(
            f"'{self.__class__.__name__}' object is not callable. If you "
            f"meant to call the '{self.name}' method on a "
            f"'{self.database.__class__.__name__}' object "
            "it is failing because no such method exists."
        )

    def _copy(
        self,
        *,
        database: Optional[Database] = None,
        name: Optional[str] = None,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> Collection:
        return Collection(
            database=database or self.database,
            name=name or self.name,
            namespace=namespace or self.namespace,
            caller_name=caller_name or self._astra_db_collection.caller_name,
            caller_version=caller_version or self._astra_db_collection.caller_version,
        )

    def with_options(
        self,
        *,
        name: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> Collection:
        """
        Create a clone of this collections with some changed attributes.

        Args:
            name: the name of the collection. This parameter is useful to
                quickly spawn Collection instances each pointing to a different
                collection existing in the same namespace.
            caller_name: name of the application, or framework, on behalf of which
                the Data API calls are performed. This ends up in the request user-agent.
            caller_version: version of the caller.

        Returns:
            a new Collection instance.
        """

        return self._copy(
            name=name,
            caller_name=caller_name,
            caller_version=caller_version,
        )

    def to_async(
        self,
        *,
        database: Optional[AsyncDatabase] = None,
        name: Optional[str] = None,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
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
            namespace: this is the namespace to which the collection belongs.
                This is generally not specified, in which case the general setting
                for the provided Database is used.
            caller_name: name of the application, or framework, on behalf of which
                the Data API calls are performed. This ends up in the request user-agent.
            caller_version: version of the caller.

        Returns:
            the new copy, an AsyncCollection instance.
        """

        return AsyncCollection(
            database=database or self.database.to_async(),
            name=name or self.name,
            namespace=namespace or self.namespace,
            caller_name=caller_name or self._astra_db_collection.caller_name,
            caller_version=caller_version or self._astra_db_collection.caller_version,
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
        """

        self._astra_db_collection.set_caller(
            caller_name=caller_name,
            caller_version=caller_version,
        )

    def options(self) -> Dict[str, Any]:
        """
        Get the collection options, i.e. its configuration as read from the database.

        The method issues a request to the Data API each time is invoked,
        without caching mechanisms: this ensures up-to-date information
        for usages such as real-time collection validation by the application.

        Returns:
            a dictionary expressing the collection as a set of key-value pairs
            matching the arguments of a `create_collection` call.
            (See also the database `list_collections` method.)
        """

        self_dicts = [
            coll_dict
            for coll_dict in self.database.list_collections()
            if coll_dict["name"] == self.name
        ]
        if self_dicts:
            return self_dicts[0]
        else:
            raise ValueError(f"Collection {self.namespace}.{self.name} not found.")

    @property
    def info(self) -> CollectionInfo:
        """
        Information on the collection (name, location, database), in the
        form of a CollectionInfo object.

        Not to be confused with the collection `options` method (related
        to the collection internal configuration).
        """

        return CollectionInfo(
            database_info=self.database.info,
            namespace=self.namespace,
            name=self.name,
            full_name=self.full_name,
        )

    @property
    def database(self) -> Database:
        """
        a Database object, the database this collection belongs to.
        """

        return self._database

    @property
    def namespace(self) -> str:
        """
        The namespace this collection is in.
        """

        return self.database.namespace

    @property
    def name(self) -> str:
        """
        The name of this collection.
        """

        # type hint added as for some reason the typechecker gets lost
        return self._astra_db_collection.collection_name  # type: ignore[no-any-return, has-type]

    @property
    def full_name(self) -> str:
        """
        The fully-qualified collection name within the database,
        in the form "namespace.collection_name".
        """

        return f"{self.namespace}.{self.name}"

    def insert_one(
        self,
        document: DocumentType,
    ) -> InsertOneResult:
        """
        Insert a single document in the collection in an atomic operation.

        Args:
            document: the dictionary expressing the document to insert.
                The `_id` field of the document can be left out, in which
                case it will be created automatically.

        Returns:
            an InsertOneResult object.

        Note:
            If an `_id` is explicitly provided, which corresponds to a document
            that exists already in the collection, an error is raised and
            the insertion fails.
        """

        """
        Insert a single document in the collection in an atomic operation.

        Args:
            document: the dictionary expressing the document to insert.
                The `_id` field of the document can be left out, in which
                case it will be created automatically.

        Returns:
            an InsertOneResult object.

        Note:
            If an `_id` is explicitly provided, which corresponds to a document
            that exists already in the collection, an error is raised and
            the insertion fails.
        """

        io_response = self._astra_db_collection.insert_one(document)
        if "insertedIds" in io_response.get("status", {}):
            if io_response["status"]["insertedIds"]:
                inserted_id = io_response["status"]["insertedIds"][0]
                return InsertOneResult(
                    raw_result=io_response,
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

    def insert_many(
        self,
        documents: Iterable[DocumentType],
        *,
        ordered: bool = True,
        chunk_size: Optional[int] = None,
        concurrency: Optional[int] = None,
    ) -> InsertManyResult:
        """
        Insert a list of documents into the collection.
        This is not an atomic operation.

        Args:
            documents: an iterable of dictionaries, each a document to insert.
                Documents may specify their `_id` field or leave it out, in which
                case it will be added automatically.
            ordered: if True (default), the insertions are processed sequentially.
                If False, they can occur in arbitrary order and possibly concurrently.

        Returns:
            an InsertManyResult object.

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
                _concurrency = INSERT_MANY_CONCURRENCY
        else:
            _concurrency = concurrency
        if _concurrency > 1 and ordered:
            raise ValueError("Cannot run ordered insert_many concurrently.")
        if chunk_size is None:
            _chunk_size = MAX_INSERT_NUM_DOCUMENTS
        else:
            _chunk_size = chunk_size
        _documents = list(documents)  # TODO make this a chunked iterator
        # TODO handle the auto-inserted-ids here (chunk-wise better)
        raw_results: List[Dict[str, Any]] = []
        if ordered:
            options = {"ordered": True}
            inserted_ids: List[Any] = []
            for i in range(0, len(_documents), _chunk_size):
                chunk_response = self._astra_db_collection.insert_many(
                    documents=_documents[i : i + _chunk_size],
                    options=options,
                    partial_failures_allowed=True,
                )
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
                        command={"temporary TODO": True},
                        raw_response=chunk_response,
                        partial_result=partial_result,
                    )

            # return
            full_result = InsertManyResult(
                raw_results=raw_results,
                inserted_ids=inserted_ids,
            )
            return full_result

        else:
            # unordered: concurrent or not, do all of them and parse the results
            options = {"ordered": False}
            if _concurrency > 1:
                with ThreadPoolExecutor(max_workers=_concurrency) as executor:
                    _chunk_insertor = partial(
                        self._astra_db_collection.insert_many,
                        options=options,
                        partial_failures_allowed=True,
                    )
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
                raw_results = [
                    self._astra_db_collection.insert_many(
                        _documents[i : i + _chunk_size],
                        options=options,
                        partial_failures_allowed=True,
                    )
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
                    commands=[{"temporary TODO": True} for _ in raw_results],
                    raw_responses=raw_results,
                    partial_result=partial_result,
                )

            # return
            full_result = InsertManyResult(
                raw_results=raw_results,
                inserted_ids=inserted_ids,
            )
            return full_result

    def find(
        self,
        filter: Optional[FilterType] = None,
        *,
        projection: Optional[ProjectionType] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Optional[SortType] = None,
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
                    {"price": {"$le": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$le": 100}}]}
                See the Data API documentation for the full set of operators.
            projection: used to select a subset of fields in the documents being
                returned. The projection can be: an iterable over the field names
                to return; a dictionary {field_name: True} to positively select
                certain fields; or a dictionary {field_name: False} if one wants
                to discard some fields from the response.
                The default is to return the whole documents.
            skip: with this integer parameter, what would be the first `skip`
                documents returned by the query are discarded, and the results
                start from the (skip+1)-th document.
            limit: this (integer) parameter sets a limit over how many documents
                are returned. Once `limit` is reached (or the cursor is exhausted
                for lack of matching documents), nothing more is returned.
            sort: with this dictionary parameter one can control the order
                the documents are returned. See the Note about sorting for details.

        Returns:
            a Cursor object representing iterations over the matching documents
            (see the Cursor object for how to use it. The simplest thing is to
            run a for loop: `for document in collection.sort(...):`).

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
        """

        return (
            Cursor(
                collection=self,
                filter=filter,
                projection=projection,
            )
            .skip(skip)
            .limit(limit)
            .sort(sort)
        )

    def find_one(
        self,
        filter: Optional[FilterType] = None,
        *,
        projection: Optional[ProjectionType] = None,
        sort: Optional[SortType] = None,
    ) -> Union[DocumentType, None]:
        """
        Run a search, returning the first document in the collection that matches
        provided filters, if any is found.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$le": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$le": 100}}]}
                See the Data API documentation for the full set of operators.
            projection: used to select a subset of fields in the documents being
                returned. The projection can be: an iterable over the field names
                to return; a dictionary {field_name: True} to positively select
                certain fields; or a dictionary {field_name: False} if one wants
                to discard some fields from the response.
                The default is to return the whole documents.
            sort: with this dictionary parameter one can control the order
                the documents are returned. See the Note about sorting for details.

        Returns:
            a dictionary expressing the required document, otherwise None.

        Note:
            See the `find` method for more details on the accepted parameters
            (whereas `skip` and `limit` are not valid parameters for `find_one`).
        """

        fo_cursor = self.find(
            filter=filter,
            projection=projection,
            skip=None,
            limit=1,
            sort=sort,
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
        filter: Optional[FilterType] = None,
    ) -> List[Any]:
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
                if lists are encountered and no numeric index is specified,
                all items in the list are visited.
                Keys can use dot-notation to descend to deeper document levels.
                Example of acceptable `key` values:
                    "field"
                    "field.subfield"
                    "field.3"
                    "field.3.subfield"
                if lists are encountered and no numeric index is specified,
                all items in the list are visited.
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$le": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$le": 100}}]}
                See the Data API documentation for the full set of operators.

        Returns:
            a list of all different values for `key` found across the documents
            that match the filter. The result list has no repeated items.

        Note:
            It must be kept in mind that `distinct` is a client-side operation,
            which effectively browses all required documents using the logic
            of the `find` method and collects the unique values found for `key`.
            As such, there may be performance, latency and ultimately
            billing implications if the amount of matching documents is large.
        """

        return self.find(
            filter=filter,
            projection={key: True},
        ).distinct(key)

    def count_documents(
        self,
        filter: Dict[str, Any],
        upper_bound: int,
    ) -> int:
        """
        Count the documents in the collection matching the specified filter.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$le": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$le": 100}}]}
                See the Data API documentation for the full set of operators.
            upper_bound: a required ceiling on the result of the count operation.
                If the actual number of documents exceeds this value,
                an exception will be raised.
                Furthermore, if the actual number of documents exceeds the maximum
                count that the Data API can reach (regardless of upper_bound),
                an exception will be raised.

        Returns:
            the exact count of matching documents.

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

        cd_response = self._astra_db_collection.count_documents(filter=filter)
        if "count" in cd_response.get("status", {}):
            count: int = cd_response["status"]["count"]
            if cd_response["status"].get("moreData", False):
                raise ValueError(
                    f"Document count exceeds {count}, the maximum allowed by the server"
                )
            else:
                if count > upper_bound:
                    raise ValueError("Document count exceeds required upper bound")
                else:
                    return count
        else:
            raise ValueError(
                "Could not complete a count_documents operation. "
                f"(gotten '${json.dumps(cd_response)}')"
            )

    def find_one_and_replace(
        self,
        filter: Dict[str, Any],
        replacement: DocumentType,
        *,
        projection: Optional[ProjectionType] = None,
        sort: Optional[SortType] = None,
        upsert: bool = False,
        return_document: str = ReturnDocument.BEFORE,
    ) -> Union[DocumentType, None]:
        """
        Find a document on the collection and replace it entirely with a new one,
        optionally inserting a new document if no match is found.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$le": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$le": 100}}]}
                See the Data API documentation for the full set of operators.
            replacement: the new document to write into the collection.
            projection: used to select a subset of fields in the document being
                returned. The projection can be: an iterable over the field names
                to return; a dictionary {field_name: True} to positively select
                certain fields; or a dictionary {field_name: False} if one wants
                to discard some fields from the response.
                The default is to return the whole documents.
            sort: with this dictionary parameter one can control the sorting
                order of the documents matching the filter, effectively
                determining what document will come first and hence be the
                replaced one. See the `find` method for more on sorting.
            upsert: this parameter controls the behavior in absence of matches.
                If True, `replacement` is inserted as a new document
                if no matches are found on the collection. If False,
                the operation silently does nothing in case of no matches.
            return_document: a flag controlling what document is returned:
                if set to `ReturnDocument.BEFORE`, or the string "before",
                the document found on database is returned; if set to
                `ReturnDocument.AFTER`, or the string "after", the new
                document is returned. The default is "before".

        Returns:
            A document (or a projection thereof, as required), either the one
            before the replace operation or the one after that.
            Alternatively, the method returns None to represent
            that no matching document was found, or that no replacement
            was inserted (depending on the `return_document` parameter).
        """

        options = {
            "returnDocument": return_document,
            "upsert": upsert,
        }
        fo_response = self._astra_db_collection.find_one_and_replace(
            replacement=replacement,
            filter=filter,
            projection=normalize_optional_projection(projection),
            sort=sort,
            options=options,
        )
        if "document" in fo_response.get("data", {}):
            ret_document = fo_response.get("data", {}).get("document")
            if ret_document is None:
                return None
            else:
                return ret_document  # type: ignore[no-any-return]
        else:
            raise ValueError(
                "Could not complete a find_one_and_replace operation. "
                f"(gotten '${json.dumps(fo_response)}')"
            )

    def replace_one(
        self,
        filter: Dict[str, Any],
        replacement: DocumentType,
        *,
        upsert: bool = False,
    ) -> UpdateResult:
        """
        Replace a single document on the collection with a new one,
        optionally inserting a new document if no match is found.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$le": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$le": 100}}]}
                See the Data API documentation for the full set of operators.
            replacement: the new document to write into the collection.
            upsert: this parameter controls the behavior in absence of matches.
                If True, `replacement` is inserted as a new document
                if no matches are found on the collection. If False,
                the operation silently does nothing in case of no matches.

        Returns:
            an UpdateResult object summarizing the outcome of the replace operation.
        """

        options = {
            "upsert": upsert,
        }
        fo_response = self._astra_db_collection.find_one_and_replace(
            replacement=replacement,
            filter=filter,
            options=options,
        )
        if "document" in fo_response.get("data", {}):
            fo_status = fo_response.get("status") or {}
            _update_info = _prepare_update_info(fo_status)
            return UpdateResult(
                raw_result=fo_status,
                update_info=_update_info,
            )
        else:
            raise ValueError(
                "Could not complete a find_one_and_replace operation. "
                f"(gotten '${json.dumps(fo_response)}')"
            )

    def find_one_and_update(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        *,
        projection: Optional[ProjectionType] = None,
        sort: Optional[SortType] = None,
        upsert: bool = False,
        return_document: str = ReturnDocument.BEFORE,
    ) -> Union[DocumentType, None]:
        """
        Find a document on the collection and update it as requested,
        optionally inserting a new document if no match is found.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$le": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$le": 100}}]}
                See the Data API documentation for the full set of operators.
            update: the update prescription to apply to the document, expressed
                as a dictionary as per Data API syntax. Examples are:
                    {"$set": {"field": "value}}
                    {"$inc": {"counter": 10}}
                    {"$unset": {"field": ""}}
                See the Data API documentation for the full syntax.
            projection: used to select a subset of fields in the document being
                returned. The projection can be: an iterable over the field names
                to return; a dictionary {field_name: True} to positively select
                certain fields; or a dictionary {field_name: False} if one wants
                to discard some fields from the response.
                The default is to return the whole documents.
            sort: with this dictionary parameter one can control the sorting
                order of the documents matching the filter, effectively
                determining what document will come first and hence be the
                updated one. See the `find` method for more on sorting.
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

        Returns:
            A document (or a projection thereof, as required), either the one
            before the replace operation or the one after that.
            Alternatively, the method returns None to represent
            that no matching document was found, or that no update
            was applied (depending on the `return_document` parameter).
        """

        options = {
            "returnDocument": return_document,
            "upsert": upsert,
        }
        fo_response = self._astra_db_collection.find_one_and_update(
            update=update,
            filter=filter,
            projection=normalize_optional_projection(projection),
            sort=sort,
            options=options,
        )
        if "document" in fo_response.get("data", {}):
            ret_document = fo_response.get("data", {}).get("document")
            if ret_document is None:
                return None
            else:
                return ret_document  # type: ignore[no-any-return]
        else:
            raise ValueError(
                "Could not complete a find_one_and_update operation. "
                f"(gotten '${json.dumps(fo_response)}')"
            )

    def update_one(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        *,
        upsert: bool = False,
    ) -> UpdateResult:
        """
        Update a single document on the collection as requested,
        optionally inserting a new document if no match is found.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$le": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$le": 100}}]}
                See the Data API documentation for the full set of operators.
            update: the update prescription to apply to the document, expressed
                as a dictionary as per Data API syntax. Examples are:
                    {"$set": {"field": "value}}
                    {"$inc": {"counter": 10}}
                    {"$unset": {"field": ""}}
                See the Data API documentation for the full syntax.
            upsert: this parameter controls the behavior in absence of matches.
                If True, a new document (resulting from applying the `update`
                to an empty document) is inserted if no matches are found on
                the collection. If False, the operation silently does nothing
                in case of no matches.

        Returns:
            an UpdateResult object summarizing the outcome of the update operation.
        """

        options = {
            "upsert": upsert,
        }
        fo_response = self._astra_db_collection.find_one_and_update(
            update=update,
            filter=filter,
            options=options,
        )
        if "document" in fo_response.get("data", {}):
            fo_status = fo_response.get("status") or {}
            _update_info = _prepare_update_info(fo_status)
            return UpdateResult(
                raw_result=fo_status,
                update_info=_update_info,
            )
        else:
            raise ValueError(
                "Could not complete a find_one_and_update operation. "
                f"(gotten '${json.dumps(fo_response)}')"
            )

    def update_many(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        *,
        upsert: bool = False,
    ) -> UpdateResult:
        """
        Apply an update operations to all documents matching a condition,
        optionally inserting one documents in absence of matches.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$le": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$le": 100}}]}
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

        Returns:
            an UpdateResult object summarizing the outcome of the update operation.
        """

        options = {
            "upsert": upsert,
        }
        um_response = self._astra_db_collection.update_many(
            update=update,
            filter=filter,
            options=options,
        )
        um_status = um_response.get("status") or {}
        _update_info = _prepare_update_info(um_status)
        return UpdateResult(
            raw_result=um_status,
            update_info=_update_info,
        )

    def find_one_and_delete(
        self,
        filter: Dict[str, Any],
        *,
        projection: Optional[ProjectionType] = None,
        sort: Optional[SortType] = None,
    ) -> Union[DocumentType, None]:
        """
        Find a document in the collection and delete it. The deleted document,
        however, is the return value of the method.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$le": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$le": 100}}]}
                See the Data API documentation for the full set of operators.
            projection: used to select a subset of fields in the document being
                returned. The projection can be: an iterable over the field names
                to return; a dictionary {field_name: True} to positively select
                certain fields; or a dictionary {field_name: False} if one wants
                to discard some fields from the response.
                Note that the `_id` field will be returned with the document
                in any case, regardless of what the provided `projection` requires.
                The default is to return the whole documents.
            sort: with this dictionary parameter one can control the sorting
                order of the documents matching the filter, effectively
                determining what document will come first and hence be the
                deleted one. See the `find` method for more on sorting.

        Returns:
            Either the document (or a projection thereof, as requested), or None
            if no matches were found in the first place.

        Note:
            This operation is not atomic on the database.
            Internally, this method runs a `find_one` followed by a `delete_one`.
        """

        _projection = normalize_optional_projection(projection, ensure_fields={"_id"})
        target_document = self.find_one(
            filter=filter, projection=_projection, sort=sort
        )
        if target_document is not None:
            target_id = target_document["_id"]
            self.delete_one({"_id": target_id})
            # this is not an API atomic operation.
            # If someone deletes the document between the find and the delete,
            # this delete would silently be a no-op and we'd be returning the
            # document. By a 'infinitesimal' shift-backward of the time of this
            # operation, we recover a non-surprising behaviour. So:
            return target_document
        else:
            return target_document

    def delete_one(
        self,
        filter: Dict[str, Any],
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
                    {"price": {"$le": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$le": 100}}]}
                See the Data API documentation for the full set of operators.

        Returns:
            a DeleteResult object summarizing the outcome of the delete operation.
        """

        do_response = self._astra_db_collection.delete_one_by_predicate(filter=filter)
        if "deletedCount" in do_response.get("status", {}):
            deleted_count = do_response["status"]["deletedCount"]
            if deleted_count == -1:
                return DeleteResult(
                    deleted_count=None,
                    raw_results=[do_response],
                )
            else:
                # expected a non-negative integer:
                return DeleteResult(
                    deleted_count=deleted_count,
                    raw_results=[do_response],
                )
        else:
            raise ValueError(
                "Could not complete a delete_one operation. "
                f"(gotten '${json.dumps(do_response)}')"
            )

    def delete_many(
        self,
        filter: Dict[str, Any],
    ) -> DeleteResult:
        """
        Delete all documents matching a provided filter.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$le": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$le": 100}}]}
                See the Data API documentation for the full set of operators.
            The `delete_many` method does not accept an empty filter: see
            `delete_all` to completely erase all contents of a collection

        Returns:
            a DeleteResult object summarizing the outcome of the delete operation.

        Note:
            This operation is not atomic. Depending on the amount of matching
            documents, it can keep running (in a blocking way) for a macroscopic
            time. In that case, new documents that are meanwhile inserted
            (e.g. from another process/application) will be deleted during
            the execution of this method call.
        """

        if not filter:
            raise ValueError(
                "The `filter` parameter to method `delete_many` cannot be "
                "empty. In order to completely clear the contents of a "
                "collection, please use the `delete_all` method."
            )

        dm_responses = self._astra_db_collection.chunked_delete_many(filter=filter)
        deleted_counts = [
            resp["status"]["deletedCount"]
            for resp in dm_responses
            if "deletedCount" in resp.get("status", {})
        ]
        if deleted_counts:
            # the "-1" occurs when len(deleted_counts) == 1 only
            deleted_count = sum(deleted_counts)
            if deleted_count == -1:
                return DeleteResult(
                    deleted_count=None,
                    raw_results=dm_responses,
                )
            else:
                # per API specs, deleted_count has to be a non-negative integer.
                return DeleteResult(
                    deleted_count=deleted_count,
                    raw_results=dm_responses,
                )
        else:
            raise ValueError(
                "Could not complete a chunked_delete_many operation. "
                f"(gotten '${json.dumps(dm_responses)}')"
            )

    def delete_all(self) -> Dict[str, Any]:
        """
        Delete all documents in a collection.

        Returns:
            a dictionary of the form {"ok": 1} to signal successful deletion.

        Note:
            Use with caution.
        """

        dm_response = self._astra_db_collection.delete_many(filter={})
        deleted_count = dm_response["status"]["deletedCount"]
        if deleted_count == -1:
            return {"ok": 1}
        else:
            raise ValueError(
                "Could not complete a delete_many operation. "
                f"(gotten '${json.dumps(dm_response)}')"
            )

    def bulk_write(
        self,
        requests: Iterable[BaseOperation],
        *,
        ordered: bool = True,
    ) -> BulkWriteResult:
        """
        Execute an arbitrary amount of operations such as inserts, updates, deletes
        either sequentially or concurrently.

        This method does not execute atomically, i.e. individual operations are
        each performed in the same way as the corresponding collection method,
        and certainly each one is a different and unrelated database mutation.

        Args:
            requests: an iterable over concrete subclasses of `BaseOperation`,
                such as `InsertMany` or `ReplaceOne`. Each such object
                represents an operation ready to be executed on a collection,
                and is instantiated by passing the same parameters as one
                would the corresponding collection method.
            ordered: whether to launch the `requests` one after the other or
                in arbitrary order, possibly in a concurrent fashion. For
                performance reasons, `ordered=False` should be preferred
                when compatible with the needs of the application flow.

        Returns:
            A single BulkWriteResult summarizing the whole list of requested
            operations. The keys in the map attributes of BulkWriteResult
            (when present) are the integer indices of the corresponding operation
            in the `requests` iterable.
        """

        # lazy importing here against circular-import error
        from astrapy.operations import reduce_bulk_write_results

        if ordered:
            bulk_write_results = [
                operation.execute(self, operation_i)
                for operation_i, operation in enumerate(requests)
            ]
            return reduce_bulk_write_results(bulk_write_results)
        else:
            with ThreadPoolExecutor(max_workers=BULK_WRITE_CONCURRENCY) as executor:
                bulk_write_futures = [
                    executor.submit(
                        operation.execute,
                        self,
                        operation_i,
                    )
                    for operation_i, operation in enumerate(requests)
                ]
                bulk_write_results = [
                    bulk_write_future.result()
                    for bulk_write_future in bulk_write_futures
                ]
                return reduce_bulk_write_results(bulk_write_results)

    def drop(self) -> Dict[str, Any]:
        """
        Drop the collection, i.e. delete it from the database along with
        all the documents it contains.

        Returns:
            a dictionary of the form {"ok": 1} to signal successful deletion.

        Note:
            Use with caution.
        """

        return self.database.drop_collection(self)


class AsyncCollection:
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
        namespace: this is the namespace to which the collection belongs.
            This is generally not specified, in which case the general setting
            for the provided Database is used.
        caller_name: name of the application, or framework, on behalf of which
            the Data API calls are performed. This ends up in the request user-agent.
        caller_version: version of the caller.

    Note:
        creating an instance of Collection does not trigger actual creation
        of the collection on the database. The latter should have been created
        beforehand, e.g. through the `create_collection` method of a Database.
    """

    def __init__(
        self,
        database: AsyncDatabase,
        name: str,
        *,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> None:
        self._astra_db_collection: AsyncAstraDBCollection = AsyncAstraDBCollection(
            collection_name=name,
            astra_db=database._astra_db,
            namespace=namespace,
            caller_name=caller_name,
            caller_version=caller_version,
        )
        # this comes after the above, lets AstraDBCollection resolve namespace
        self._database = database._copy(
            namespace=self._astra_db_collection.astra_db.namespace
        )

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}[_astra_db_collection="{self._astra_db_collection}"]'

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, AsyncCollection):
            return self._astra_db_collection == other._astra_db_collection
        else:
            return False

    def __call__(self, *pargs: Any, **kwargs: Any) -> None:
        raise TypeError(
            f"'{self.__class__.__name__}' object is not callable. If you "
            f"meant to call the '{self.name}' method on a "
            f"'{self.database.__class__.__name__}' object "
            "it is failing because no such method exists."
        )

    def _copy(
        self,
        *,
        database: Optional[AsyncDatabase] = None,
        name: Optional[str] = None,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> AsyncCollection:
        return AsyncCollection(
            database=database or self.database,
            name=name or self.name,
            namespace=namespace or self.namespace,
            caller_name=caller_name or self._astra_db_collection.caller_name,
            caller_version=caller_version or self._astra_db_collection.caller_version,
        )

    def with_options(
        self,
        *,
        name: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> AsyncCollection:
        """
        Create a clone of this collections with some changed attributes.

        Args:
            name: the name of the collection. This parameter is useful to
                quickly spawn AsyncCollection instances each pointing to a different
                collection existing in the same namespace.
            caller_name: name of the application, or framework, on behalf of which
                the Data API calls are performed. This ends up in the request user-agent.
            caller_version: version of the caller.

        Returns:
            a new AsyncCollection instance.
        """

        return self._copy(
            name=name,
            caller_name=caller_name,
            caller_version=caller_version,
        )

    def to_sync(
        self,
        *,
        database: Optional[Database] = None,
        name: Optional[str] = None,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
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
            namespace: this is the namespace to which the collection belongs.
                This is generally not specified, in which case the general setting
                for the provided Database is used.
            caller_name: name of the application, or framework, on behalf of which
                the Data API calls are performed. This ends up in the request user-agent.
            caller_version: version of the caller.

        Returns:
            the new copy, a Collection instance.
        """

        return Collection(
            database=database or self.database.to_sync(),
            name=name or self.name,
            namespace=namespace or self.namespace,
            caller_name=caller_name or self._astra_db_collection.caller_name,
            caller_version=caller_version or self._astra_db_collection.caller_version,
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
        """

        self._astra_db_collection.set_caller(
            caller_name=caller_name,
            caller_version=caller_version,
        )

    async def options(self) -> Dict[str, Any]:
        """
        Get the collection options, i.e. its configuration as read from the database.

        The method issues a request to the Data API each time is invoked,
        without caching mechanisms: this ensures up-to-date information
        for usages such as real-time collection validation by the application.

        Returns:
            a dictionary expressing the collection as a set of key-value pairs
            matching the arguments of a `create_collection` call.
            (See also the database `list_collections` method.)
        """

        self_dicts = [
            coll_dict
            async for coll_dict in self.database.list_collections()
            if coll_dict["name"] == self.name
        ]
        if self_dicts:
            return self_dicts[0]
        else:
            raise ValueError(f"Collection {self.namespace}.{self.name} not found.")

    @property
    def info(self) -> CollectionInfo:
        """
        Information on the collection (name, location, database), in the
        form of a CollectionInfo object.

        Not to be confused with the collection `options` method (related
        to the collection internal configuration).
        """

        return CollectionInfo(
            database_info=self.database.info,
            namespace=self.namespace,
            name=self.name,
            full_name=self.full_name,
        )

    @property
    def database(self) -> AsyncDatabase:
        """
        a Database object, the database this collection belongs to.
        """

        return self._database

    @property
    def namespace(self) -> str:
        """
        The namespace this collection is in.
        """

        return self.database.namespace

    @property
    def name(self) -> str:
        """
        The name of this collection.
        """

        # type hint added as for some reason the typechecker gets lost
        return self._astra_db_collection.collection_name  # type: ignore[no-any-return, has-type]

    @property
    def full_name(self) -> str:
        """
        The fully-qualified collection name within the database,
        in the form "namespace.collection_name".
        """

        return f"{self.namespace}.{self.name}"

    async def insert_one(
        self,
        document: DocumentType,
    ) -> InsertOneResult:
        """
        Insert a single document in the collection in an atomic operation.

        Args:
            document: the dictionary expressing the document to insert.
                The `_id` field of the document can be left out, in which
                case it will be created automatically.

        Returns:
            an InsertOneResult object.

        Note:
            If an `_id` is explicitly provided, which corresponds to a document
            that exists already in the collection, an error is raised and
            the insertion fails.
        """

        io_response = await self._astra_db_collection.insert_one(document)
        if "insertedIds" in io_response.get("status", {}):
            if io_response["status"]["insertedIds"]:
                inserted_id = io_response["status"]["insertedIds"][0]
                return InsertOneResult(
                    raw_result=io_response,
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
        ordered: bool = True,
        chunk_size: Optional[int] = None,
        concurrency: Optional[int] = None,
    ) -> InsertManyResult:
        """
        Insert a list of documents into the collection.
        This is not an atomic operation.

        Args:
            documents: an iterable of dictionaries, each a document to insert.
                Documents may specify their `_id` field or leave it out, in which
                case it will be added automatically.
            ordered: if True (default), the insertions are processed sequentially.
                If False, they can occur in arbitrary order and possibly concurrently.

        Returns:
            an InsertManyResult object.

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
                _concurrency = INSERT_MANY_CONCURRENCY
        else:
            _concurrency = concurrency
        if _concurrency > 1 and ordered:
            raise ValueError("Cannot run ordered insert_many concurrently.")
        if chunk_size is None:
            _chunk_size = MAX_INSERT_NUM_DOCUMENTS
        else:
            _chunk_size = chunk_size
        _documents = list(documents)  # TODO make this a chunked iterator
        # TODO handle the auto-inserted-ids here (chunk-wise better)
        raw_results: List[Dict[str, Any]] = []
        if ordered:
            options = {"ordered": True}
            inserted_ids: List[Any] = []
            for i in range(0, len(_documents), _chunk_size):
                chunk_response = await self._astra_db_collection.insert_many(
                    documents=_documents[i : i + _chunk_size],
                    options=options,
                    partial_failures_allowed=True,
                )
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
                        command={"temporary TODO": True},
                        raw_response=chunk_response,
                        partial_result=partial_result,
                    )

            # return
            full_result = InsertManyResult(
                raw_results=raw_results,
                inserted_ids=inserted_ids,
            )
            return full_result

        else:
            # unordered: concurrent or not, do all of them and parse the results
            options = {"ordered": False}

            sem = asyncio.Semaphore(_concurrency)

            async def concurrent_insert_chunk(
                document_chunk: List[DocumentType],
            ) -> Dict[str, Any]:
                async with sem:
                    return await self._astra_db_collection.insert_many(
                        document_chunk,
                        options=options,
                        partial_failures_allowed=True,
                    )

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
                    commands=[{"temporary TODO": True} for _ in raw_results],
                    raw_responses=raw_results,
                    partial_result=partial_result,
                )

            # return
            full_result = InsertManyResult(
                raw_results=raw_results,
                inserted_ids=inserted_ids,
            )
            return full_result

        """
        if ordered:
            cim_responses = await self._astra_db_collection.chunked_insert_many(
                documents=list(documents),
                options={"ordered": True},
                partial_failures_allowed=False,
                concurrency=1,
            )
        else:
            # unordered insertion: can do chunks concurrently
            cim_responses = await self._astra_db_collection.chunked_insert_many(
                documents=list(documents),
                options={"ordered": False},
                partial_failures_allowed=True,
                concurrency=INSERT_MANY_CONCURRENCY,
            )
        _exceptions = [cim_r for cim_r in cim_responses if isinstance(cim_r, Exception)]
        _errors_in_response = [
            err
            for response in cim_responses
            if isinstance(response, dict)
            for err in (response.get("errors") or [])
        ]
        if _exceptions:
            raise _exceptions[0]
        elif _errors_in_response:
            raise ValueError(str(_errors_in_response[0]))
        else:
            inserted_ids = [
                ins_id
                for response in cim_responses
                if isinstance(response, dict)
                for ins_id in (response.get("status") or {}).get("insertedIds", [])
            ]
            return InsertManyResult(
                # if we are here, cim_responses are all dicts (no exceptions)
                raw_results=cim_responses,  # type: ignore[arg-type]
                inserted_ids=inserted_ids,
            )
        """

    def find(
        self,
        filter: Optional[FilterType] = None,
        *,
        projection: Optional[ProjectionType] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Optional[SortType] = None,
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
                    {"price": {"$le": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$le": 100}}]}
                See the Data API documentation for the full set of operators.
            projection: used to select a subset of fields in the documents being
                returned. The projection can be: an iterable over the field names
                to return; a dictionary {field_name: True} to positively select
                certain fields; or a dictionary {field_name: False} if one wants
                to discard some fields from the response.
                The default is to return the whole documents.
            skip: with this integer parameter, what would be the first `skip`
                documents returned by the query are discarded, and the results
                start from the (skip+1)-th document.
            limit: this (integer) parameter sets a limit over how many documents
                are returned. Once `limit` is reached (or the cursor is exhausted
                for lack of matching documents), nothing more is returned.
            sort: with this dictionary parameter one can control the order
                the documents are returned. See the Note about sorting for details.

        Returns:
            an AsyncCursor object representing iterations over the matching documents
            (see the AsyncCursor object for how to use it. The simplest thing is to
            run a for loop: `for document in collection.sort(...):`).

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
        """

        return (
            AsyncCursor(
                collection=self,
                filter=filter,
                projection=projection,
            )
            .skip(skip)
            .limit(limit)
            .sort(sort)
        )

    async def find_one(
        self,
        filter: Optional[FilterType] = None,
        *,
        projection: Optional[ProjectionType] = None,
        sort: Optional[SortType] = None,
    ) -> Union[DocumentType, None]:
        """
        Run a search, returning the first document in the collection that matches
        provided filters, if any is found.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$le": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$le": 100}}]}
                See the Data API documentation for the full set of operators.
            projection: used to select a subset of fields in the documents being
                returned. The projection can be: an iterable over the field names
                to return; a dictionary {field_name: True} to positively select
                certain fields; or a dictionary {field_name: False} if one wants
                to discard some fields from the response.
                The default is to return the whole documents.
            sort: with this dictionary parameter one can control the order
                the documents are returned. See the Note about sorting for details.

        Returns:
            a dictionary expressing the required document, otherwise None.

        Note:
            See the `find` method for more details on the accepted parameters
            (whereas `skip` and `limit` are not valid parameters for `find_one`).
        """

        fo_cursor = self.find(
            filter=filter,
            projection=projection,
            skip=None,
            limit=1,
            sort=sort,
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
        filter: Optional[FilterType] = None,
    ) -> List[Any]:
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
                if lists are encountered and no numeric index is specified,
                all items in the list are visited.
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$le": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$le": 100}}]}
                See the Data API documentation for the full set of operators.

        Returns:
            a list of all different values for `key` found across the documents
            that match the filter. The result list has no repeated items.

        Note:
            It must be kept in mind that `distinct` is a client-side operation,
            which effectively browses all required documents using the logic
            of the `find` method and collects the unique values found for `key`.
            As such, there may be performance, latency and ultimately
            billing implications if the amount of matching documents is large.
        """

        cursor = self.find(
            filter=filter,
            projection={key: True},
        )
        return await cursor.distinct(key)

    async def count_documents(
        self,
        filter: Dict[str, Any],
        upper_bound: int,
    ) -> int:
        """
        Count the documents in the collection matching the specified filter.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$le": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$le": 100}}]}
                See the Data API documentation for the full set of operators.
            upper_bound: a required ceiling on the result of the count operation.
                If the actual number of documents exceeds this value,
                an exception will be raised.
                Furthermore, if the actual number of documents exceeds the maximum
                count that the Data API can reach (regardless of upper_bound),
                an exception will be raised.

        Returns:
            the exact count of matching documents.

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

        cd_response = await self._astra_db_collection.count_documents(filter=filter)
        if "count" in cd_response.get("status", {}):
            count: int = cd_response["status"]["count"]
            if cd_response["status"].get("moreData", False):
                raise ValueError(
                    f"Document count exceeds {count}, the maximum allowed by the server"
                )
            else:
                if count > upper_bound:
                    raise ValueError("Document count exceeds required upper bound")
                else:
                    return count
        else:
            raise ValueError(
                "Could not complete a count_documents operation. "
                f"(gotten '${json.dumps(cd_response)}')"
            )

    async def find_one_and_replace(
        self,
        filter: Dict[str, Any],
        replacement: DocumentType,
        *,
        projection: Optional[ProjectionType] = None,
        sort: Optional[SortType] = None,
        upsert: bool = False,
        return_document: str = ReturnDocument.BEFORE,
    ) -> Union[DocumentType, None]:
        """
        Find a document on the collection and replace it entirely with a new one,
        optionally inserting a new document if no match is found.

        Args:

            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$le": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$le": 100}}]}
                See the Data API documentation for the full set of operators.
            replacement: the new document to write into the collection.
            projection: used to select a subset of fields in the document being
                returned. The projection can be: an iterable over the field names
                to return; a dictionary {field_name: True} to positively select
                certain fields; or a dictionary {field_name: False} if one wants
                to discard some fields from the response.
                The default is to return the whole documents.
            sort: with this dictionary parameter one can control the sorting
                order of the documents matching the filter, effectively
                determining what document will come first and hence be the
                replaced one. See the `find` method for more on sorting.
            upsert: this parameter controls the behavior in absence of matches.
                If True, `replacement` is inserted as a new document
                if no matches are found on the collection. If False,
                the operation silently does nothing in case of no matches.
            return_document: a flag controlling what document is returned:
                if set to `ReturnDocument.BEFORE`, or the string "before",
                the document found on database is returned; if set to
                `ReturnDocument.AFTER`, or the string "after", the new
                document is returned. The default is "before".

        Returns:
            A document, either the one before the replace operation or the
            one after that. Alternatively, the method returns None to represent
            that no matching document was found, or that no replacement
            was inserted (depending on the `return_document` parameter).
        """

        options = {
            "returnDocument": return_document,
            "upsert": upsert,
        }
        fo_response = await self._astra_db_collection.find_one_and_replace(
            replacement=replacement,
            filter=filter,
            projection=normalize_optional_projection(projection),
            sort=sort,
            options=options,
        )
        if "document" in fo_response.get("data", {}):
            ret_document = fo_response.get("data", {}).get("document")
            if ret_document is None:
                return None
            else:
                return ret_document  # type: ignore[no-any-return]
        else:
            raise ValueError(
                "Could not complete a find_one_and_replace operation. "
                f"(gotten '${json.dumps(fo_response)}')"
            )

    async def replace_one(
        self,
        filter: Dict[str, Any],
        replacement: DocumentType,
        *,
        upsert: bool = False,
    ) -> UpdateResult:
        """
        Replace a single document on the collection with a new one,
        optionally inserting a new document if no match is found.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$le": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$le": 100}}]}
                See the Data API documentation for the full set of operators.
            replacement: the new document to write into the collection.
            upsert: this parameter controls the behavior in absence of matches.
                If True, `replacement` is inserted as a new document
                if no matches are found on the collection. If False,
                the operation silently does nothing in case of no matches.

        Returns:
            an UpdateResult object summarizing the outcome of the replace operation.
        """

        options = {
            "upsert": upsert,
        }
        fo_response = await self._astra_db_collection.find_one_and_replace(
            replacement=replacement,
            filter=filter,
            options=options,
        )
        if "document" in fo_response.get("data", {}):
            fo_status = fo_response.get("status") or {}
            _update_info = _prepare_update_info(fo_status)
            return UpdateResult(
                raw_result=fo_status,
                update_info=_update_info,
            )
        else:
            raise ValueError(
                "Could not complete a find_one_and_replace operation. "
                f"(gotten '${json.dumps(fo_response)}')"
            )

    async def find_one_and_update(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        *,
        projection: Optional[ProjectionType] = None,
        sort: Optional[SortType] = None,
        upsert: bool = False,
        return_document: str = ReturnDocument.BEFORE,
    ) -> Union[DocumentType, None]:
        """
        Find a document on the collection and update it as requested,
        optionally inserting a new document if no match is found.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$le": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$le": 100}}]}
                See the Data API documentation for the full set of operators.
            update: the update prescription to apply to the document, expressed
                as a dictionary as per Data API syntax. Examples are:
                    {"$set": {"field": "value}}
                    {"$inc": {"counter": 10}}
                    {"$unset": {"field": ""}}
                See the Data API documentation for the full syntax.
            projection: used to select a subset of fields in the document being
                returned. The projection can be: an iterable over the field names
                to return; a dictionary {field_name: True} to positively select
                certain fields; or a dictionary {field_name: False} if one wants
                to discard some fields from the response.
                The default is to return the whole documents.
            sort: with this dictionary parameter one can control the sorting
                order of the documents matching the filter, effectively
                determining what document will come first and hence be the
                updated one. See the `find` method for more on sorting.
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

        Returns:
            A document (or a projection thereof, as required), either the one
            before the replace operation or the one after that.
            Alternatively, the method returns None to represent
            that no matching document was found, or that no update
            was applied (depending on the `return_document` parameter).
        """

        options = {
            "returnDocument": return_document,
            "upsert": upsert,
        }
        fo_response = await self._astra_db_collection.find_one_and_update(
            update=update,
            filter=filter,
            projection=normalize_optional_projection(projection),
            sort=sort,
            options=options,
        )
        if "document" in fo_response.get("data", {}):
            ret_document = fo_response.get("data", {}).get("document")
            if ret_document is None:
                return None
            else:
                return ret_document  # type: ignore[no-any-return]
        else:
            raise ValueError(
                "Could not complete a find_one_and_update operation. "
                f"(gotten '${json.dumps(fo_response)}')"
            )

    async def update_one(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        *,
        upsert: bool = False,
    ) -> UpdateResult:
        """
        Update a single document on the collection as requested,
        optionally inserting a new document if no match is found.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$le": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$le": 100}}]}
                See the Data API documentation for the full set of operators.
            update: the update prescription to apply to the document, expressed
                as a dictionary as per Data API syntax. Examples are:
                    {"$set": {"field": "value}}
                    {"$inc": {"counter": 10}}
                    {"$unset": {"field": ""}}
                See the Data API documentation for the full syntax.
            upsert: this parameter controls the behavior in absence of matches.
                If True, a new document (resulting from applying the `update`
                to an empty document) is inserted if no matches are found on
                the collection. If False, the operation silently does nothing
                in case of no matches.

        Returns:
            an UpdateResult object summarizing the outcome of the update operation.
        """

        options = {
            "upsert": upsert,
        }
        fo_response = await self._astra_db_collection.find_one_and_update(
            update=update,
            filter=filter,
            options=options,
        )
        if "document" in fo_response.get("data", {}):
            fo_status = fo_response.get("status") or {}
            _update_info = _prepare_update_info(fo_status)
            return UpdateResult(
                raw_result=fo_status,
                update_info=_update_info,
            )
        else:
            raise ValueError(
                "Could not complete a find_one_and_update operation. "
                f"(gotten '${json.dumps(fo_response)}')"
            )

    async def update_many(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        *,
        upsert: bool = False,
    ) -> UpdateResult:
        """
        Apply an update operations to all documents matching a condition,
        optionally inserting one documents in absence of matches.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$le": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$le": 100}}]}
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

        Returns:
            an UpdateResult object summarizing the outcome of the update operation.
        """

        options = {
            "upsert": upsert,
        }
        um_response = await self._astra_db_collection.update_many(
            update=update,
            filter=filter,
            options=options,
        )
        um_status = um_response.get("status") or {}
        _update_info = _prepare_update_info(um_status)
        return UpdateResult(
            raw_result=um_status,
            update_info=_update_info,
        )

    async def find_one_and_delete(
        self,
        filter: Dict[str, Any],
        *,
        projection: Optional[ProjectionType] = None,
        sort: Optional[SortType] = None,
    ) -> Union[DocumentType, None]:
        """
        Find a document in the collection and delete it. The deleted document,
        however, is the return value of the method.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$le": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$le": 100}}]}
                See the Data API documentation for the full set of operators.
            projection: used to select a subset of fields in the document being
                returned. The projection can be: an iterable over the field names
                to return; a dictionary {field_name: True} to positively select
                certain fields; or a dictionary {field_name: False} if one wants
                to discard some fields from the response.
                Note that the `_id` field will be returned with the document
                in any case, regardless of what the provided `projection` requires.
                The default is to return the whole documents.
            sort: with this dictionary parameter one can control the sorting
                order of the documents matching the filter, effectively
                determining what document will come first and hence be the
                deleted one. See the `find` method for more on sorting.

        Returns:
            Either the document (or a projection thereof, as requested), or None
            if no matches were found in the first place.

        Note:
            This operation is not atomic on the database.
            Internally, this method runs a `find_one` followed by a `delete_one`.
        """

        _projection = normalize_optional_projection(projection, ensure_fields={"_id"})
        target_document = await self.find_one(
            filter=filter, projection=_projection, sort=sort
        )
        if target_document is not None:
            target_id = target_document["_id"]
            await self.delete_one({"_id": target_id})
            # this is not an API atomic operation.
            # If someone deletes the document between the find and the delete,
            # this delete would silently be a no-op and we'd be returning the
            # document. By a 'infinitesimal' shift-backward of the time of this
            # operation, we recover a non-surprising behaviour. So:
            return target_document
        else:
            return target_document

    async def delete_one(
        self,
        filter: Dict[str, Any],
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
                    {"price": {"$le": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$le": 100}}]}
                See the Data API documentation for the full set of operators.

        Returns:
            a DeleteResult object summarizing the outcome of the delete operation.
        """

        do_response = await self._astra_db_collection.delete_one_by_predicate(
            filter=filter
        )
        if "deletedCount" in do_response.get("status", {}):
            deleted_count = do_response["status"]["deletedCount"]
            if deleted_count == -1:
                return DeleteResult(
                    deleted_count=None,
                    raw_results=[do_response],
                )
            else:
                # expected a non-negative integer:
                return DeleteResult(
                    deleted_count=deleted_count,
                    raw_results=[do_response],
                )
        else:
            raise ValueError(
                "Could not complete a delete_one operation. "
                f"(gotten '${json.dumps(do_response)}')"
            )

    async def delete_many(
        self,
        filter: Dict[str, Any],
        *,
        let: Optional[int] = None,
    ) -> DeleteResult:
        """
        Delete all documents matching a provided filter.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"price": {"$le": 100}}
                    {"$and": [{"name": "John"}, {"price": {"$le": 100}}]}
                See the Data API documentation for the full set of operators.
            The `delete_many` method does not accept an empty filter: see
            `delete_all` to completely erase all contents of a collection

        Returns:
            a DeleteResult object summarizing the outcome of the delete operation.

        Note:
            This operation is not atomic. Depending on the amount of matching
            documents, it can keep running (in a blocking way) for a macroscopic
            time. In that case, new documents that are meanwhile inserted
            (e.g. from another process/application) will be deleted during
            the execution of this method call.
        """

        if not filter:
            raise ValueError(
                "The `filter` parameter to method `delete_many` cannot be "
                "empty. In order to completely clear the contents of a "
                "collection, please use the `delete_all` method."
            )

        dm_responses = await self._astra_db_collection.chunked_delete_many(
            filter=filter
        )
        deleted_counts = [
            resp["status"]["deletedCount"]
            for resp in dm_responses
            if "deletedCount" in resp.get("status", {})
        ]
        if deleted_counts:
            # the "-1" occurs when len(deleted_counts) == 1 only
            deleted_count = sum(deleted_counts)
            if deleted_count == -1:
                return DeleteResult(
                    deleted_count=None,
                    raw_results=dm_responses,
                )
            else:
                # per API specs, deleted_count has to be a non-negative integer.
                return DeleteResult(
                    deleted_count=deleted_count,
                    raw_results=dm_responses,
                )
        else:
            raise ValueError(
                "Could not complete a chunked_delete_many operation. "
                f"(gotten '${json.dumps(dm_responses)}')"
            )

    async def delete_all(self) -> Dict[str, Any]:
        """
        Delete all documents in a collection.

        Returns:
            a dictionary of the form {"ok": 1} to signal successful deletion.

        Note:
            Use with caution.
        """

        dm_response = await self._astra_db_collection.delete_many(filter={})
        deleted_count = dm_response["status"]["deletedCount"]
        if deleted_count == -1:
            return {"ok": 1}
        else:
            raise ValueError(
                "Could not complete a delete_many operation. "
                f"(gotten '${json.dumps(dm_response)}')"
            )

    async def bulk_write(
        self,
        requests: Iterable[AsyncBaseOperation],
        *,
        ordered: bool = True,
    ) -> BulkWriteResult:
        """
        Execute an arbitrary amount of operations such as inserts, updates, deletes
        either sequentially or concurrently.

        This method does not execute atomically, i.e. individual operations are
        each performed in the same way as the corresponding collection method,
        and certainly each one is a different and unrelated database mutation.

        Args:
            requests: an iterable over concrete subclasses of `BaseOperation`,
                such as `InsertMany` or `ReplaceOne`. Each such object
                represents an operation ready to be executed on a collection,
                and is instantiated by passing the same parameters as one
                would the corresponding collection method.
            ordered: whether to launch the `requests` one after the other or
                in arbitrary order, possibly in a concurrent fashion. For
                performance reasons, `ordered=False` should be preferred
                when compatible with the needs of the application flow.

        Returns:
            A single BulkWriteResult summarizing the whole list of requested
            operations. The keys in the map attributes of BulkWriteResult
            (when present) are the integer indices of the corresponding operation
            in the `requests` iterable.
        """

        # lazy importing here against circular-import error
        from astrapy.operations import reduce_bulk_write_results

        if ordered:
            bulk_write_results = [
                await operation.execute(self, operation_i)
                for operation_i, operation in enumerate(requests)
            ]
            return reduce_bulk_write_results(bulk_write_results)
        else:
            sem = asyncio.Semaphore(BULK_WRITE_CONCURRENCY)

            async def concurrent_execute_operation(
                operation: AsyncBaseOperation,
                collection: AsyncCollection,
                index_in_bulk_write: int,
            ) -> BulkWriteResult:
                async with sem:
                    return await operation.execute(
                        collection=collection, index_in_bulk_write=index_in_bulk_write
                    )

            tasks = [
                asyncio.create_task(
                    concurrent_execute_operation(operation, self, operation_i)
                )
                for operation_i, operation in enumerate(requests)
            ]
            bulk_write_results = await asyncio.gather(*tasks)
            return reduce_bulk_write_results(bulk_write_results)

    async def drop(self) -> Dict[str, Any]:
        """
        Drop the collection, i.e. delete it from the database along with
        all the documents it contains.

        Returns:
            a dictionary of the form {"ok": 1} to signal successful deletion.

        Note:
            Use with caution.
        """

        return await self.database.drop_collection(self)
