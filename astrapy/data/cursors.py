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

import hashlib
import json
import logging
import time
from collections.abc import AsyncIterator
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    Iterator,
    Optional,
    Tuple,
    TypeVar,
)

from astrapy.constants import (
    DocumentType,
    ProjectionType,
    normalize_optional_projection,
)
from astrapy.exceptions import (
    CursorIsStartedException,
    DataAPIFaultyResponseException,
    DataAPITimeoutException,
)
from astrapy.utils.transform_payload import normalize_payload_value

if TYPE_CHECKING:
    from astrapy.collection import AsyncCollection, Collection


logger = logging.getLogger(__name__)


BC = TypeVar("BC", bound="BaseCursor")
IndexPairType = Tuple[str, Optional[int]]


class CursorState(Enum):
    # Iteration over results has not started yet (alive=T, started=F)
    IDLE = "idle"
    # Iteration has started, *can* still yield results (alive=T, started=T)
    STARTED = "started"
    # Finished/forcibly stopped. Won't return more documents (alive=F)
    CLOSED = "closed"


def _maybe_valid_list_index(key_block: str) -> int | None:
    # '0', '1' is good. '00', '01', '-30' are not.
    try:
        kb_index = int(key_block)
        if kb_index >= 0 and key_block == str(kb_index):
            return kb_index
        else:
            return None
    except ValueError:
        return None


def _create_document_key_extractor(
    key: str,
) -> Callable[[dict[str, Any]], Iterable[Any]]:
    key_blocks0: list[IndexPairType] = [
        (kb_str, _maybe_valid_list_index(kb_str)) for kb_str in key.split(".")
    ]
    if key_blocks0 == []:
        raise ValueError("Field path specification cannot be empty")
    if any(kb[0] == "" for kb in key_blocks0):
        raise ValueError("Field path components cannot be empty")

    def _extract_with_key_blocks(
        key_blocks: list[IndexPairType], value: Any
    ) -> Iterable[Any]:
        if key_blocks == []:
            if isinstance(value, list):
                for item in value:
                    yield item
            else:
                yield value
            return
        else:
            # go deeper as requested
            rest_key_blocks = key_blocks[1:]
            key_block = key_blocks[0]
            k_str, k_int = key_block
            if isinstance(value, dict):
                if k_str in value:
                    new_value = value[k_str]
                    for item in _extract_with_key_blocks(rest_key_blocks, new_value):
                        yield item
                return
            elif isinstance(value, list):
                if k_int is not None:
                    if len(value) > k_int:
                        new_value = value[k_int]
                        for item in _extract_with_key_blocks(
                            rest_key_blocks, new_value
                        ):
                            yield item
                    else:
                        # list has no such element. Nothing to extract.
                        return
                else:
                    for item in value:
                        for item in _extract_with_key_blocks(key_blocks, item):
                            yield item
                return
            else:
                # keyblocks are deeper than the document. Nothing to extract.
                return

    def _item_extractor(document: dict[str, Any]) -> Iterable[Any]:
        return _extract_with_key_blocks(key_blocks=key_blocks0, value=document)

    return _item_extractor


def _reduce_distinct_key_to_safe(distinct_key: str) -> str:
    """
    In light of the twofold interpretation of "0" as index and dict key
    in selection (for distinct), and the auto-unroll of lists, it is not
    safe to go beyond the first level. See this example:
        document = {'x': [{'y': 'Y', '0': 'ZERO'}]}
        key = "x.0"
    With full key as projection, we would lose the `"y": "Y"` part (mistakenly).
    """
    blocks = distinct_key.split(".")
    valid_portion = []
    for block in blocks:
        if _maybe_valid_list_index(block) is None:
            valid_portion.append(block)
        else:
            break
    return ".".join(valid_portion)


def _hash_document(document: dict[str, Any]) -> str:
    _normalized_item = normalize_payload_value(path=[], value=document)
    _normalized_json = json.dumps(
        _normalized_item, sort_keys=True, separators=(",", ":")
    )
    _item_hash = hashlib.md5(_normalized_json.encode()).hexdigest()
    return _item_hash


class _LookAheadIterator:
    """
    A class that allows to anticipate reading one element to ensure a call
    is made and 'global' (find-wide) properties are read off the first response.
    """

    def __init__(self, iterator: Iterator[DocumentType]):
        self.iterator = iterator
        self.preread_item: DocumentType | None = None
        self.has_preread = False
        self.preread_exhausted = False

    def __iter__(self) -> Iterator[DocumentType]:
        return self

    def __next__(self) -> DocumentType:
        if self.has_preread:
            self.has_preread = False
            if self.preread_exhausted:
                raise StopIteration
            # if this runs, preread_item is filled with a document:
            return self.preread_item  # type: ignore[return-value]
        else:
            return next(self.iterator)

    def preread(self) -> None:
        if not self.has_preread and not self.preread_exhausted:
            try:
                self.preread_item = next(self.iterator)
                self.has_preread = True
            except StopIteration:
                self.preread_item = None
                self.has_preread = False
                self.preread_exhausted = True


class _AsyncLookAheadIterator:
    """
    A class that allows to anticipate reading one element to ensure a call
    is made and 'global' (find-wide) properties are read off the first response.
    """

    def __init__(self, async_iterator: AsyncIterator[DocumentType]):
        self.async_iterator = async_iterator
        self.preread_item: DocumentType | None = None
        self.has_preread = False
        self.preread_exhausted = False

    def __aiter__(self) -> AsyncIterator[DocumentType]:
        return self

    async def __anext__(self) -> DocumentType:
        if self.has_preread:
            self.has_preread = False
            if self.preread_exhausted:
                raise StopAsyncIteration
            # if this runs, preread_item is filled with a document:
            return self.preread_item  # type: ignore[return-value]
        else:
            return await self.async_iterator.__anext__()

    async def preread(self) -> None:
        if not self.has_preread and not self.preread_exhausted:
            try:
                self.preread_item = await self.async_iterator.__anext__()
                self.has_preread = True
            except StopAsyncIteration:
                self.preread_item = None
                self.has_preread = False
                self.preread_exhausted = True


class BaseCursor:
    """
    Represents a generic Cursor over query results, regardless of whether
    synchronous or asynchronous. It cannot be instantiated.

    See classes Cursor and AsyncCursor for more information.
    """

    _collection: Collection | AsyncCollection
    _filter: dict[str, Any] | None
    _projection: ProjectionType | None
    _max_time_ms: int | None
    _overall_max_time_ms: int | None
    _started_time_s: float | None
    _limit: int | None
    _skip: int | None
    _include_similarity: bool | None
    _include_sort_vector: bool | None
    _sort: dict[str, Any] | None
    _state: CursorState
    _consumed: int
    _iterator: _LookAheadIterator | _AsyncLookAheadIterator | None = None
    _api_response_status: dict[str, Any] | None

    def __init__(
        self,
        collection: Collection | AsyncCollection,
        filter: dict[str, Any] | None,
        projection: ProjectionType | None,
        max_time_ms: int | None,
        overall_max_time_ms: int | None,
    ) -> None:
        raise NotImplementedError

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}("{self._collection.name}", '
            f"{self.state}, "
            f"consumed so far: {self.consumed})"
        )

    def _item_at_index(self, index: int) -> DocumentType:
        # deferred to subclasses
        raise NotImplementedError

    def _ensure_alive(self) -> None:
        if not self.alive:
            raise CursorIsStartedException(
                text="Cursor not alive.",
                cursor_state=self.state,
            )

    def _ensure_idle(self) -> None:
        if self._state != CursorState.IDLE:
            raise CursorIsStartedException(
                text="Cursor started already.",
                cursor_state=self.state,
            )

    def _copy(
        self: BC,
        *,
        projection: ProjectionType | None = None,
        max_time_ms: int | None = None,
        overall_max_time_ms: int | None = None,
        limit: int | None = None,
        skip: int | None = None,
        include_similarity: bool | None = None,
        include_sort_vector: bool | None = None,
        sort: dict[str, Any] | None = None,
    ) -> BC:
        new_cursor = self.__class__(
            collection=self._collection,
            filter=self._filter,
            projection=projection or self._projection,
            max_time_ms=max_time_ms or self._max_time_ms,
            overall_max_time_ms=overall_max_time_ms or self._overall_max_time_ms,
        )
        # Cursor treated as mutable within this function scope:
        new_cursor._limit = limit if limit is not None else self._limit
        new_cursor._skip = skip if skip is not None else self._skip
        new_cursor._include_similarity = (
            include_similarity
            if include_similarity is not None
            else self._include_similarity
        )
        new_cursor._include_sort_vector = (
            include_sort_vector
            if include_sort_vector is not None
            else self._include_sort_vector
        )
        new_cursor._sort = sort if sort is not None else self._sort
        return new_cursor

    @property
    def state(self) -> str:
        """
        The current state of this cursor, which can be one of
        the astrapy.cursors.CursorState enum.
        """

        return self._state.value

    @property
    def address(self) -> str:
        """
        The API endpoint used by this cursor when issuing
        requests to the database.
        """

        return self._collection._api_commander.full_path

    @property
    def alive(self) -> bool:
        """
        Whether the cursor has the potential to yield more data.
        """

        return self._state != CursorState.CLOSED

    def clone(self: BC) -> BC:
        """
        Clone the cursor into a new, fresh one.

        Returns:
            a copy of this cursor, reset to its pristine state,
            i.e. fully un-consumed.
        """

        return self._copy()

    def close(self) -> None:
        """
        Stop/kill the cursor, regardless of its status.
        """

        self._state = CursorState.CLOSED

    @property
    def cursor_id(self) -> int:
        """
        An integer uniquely identifying this cursor.
        """

        return id(self)

    def limit(self: BC, limit: int | None) -> BC:
        """
        Set a new `limit` value for this cursor.

        Args:
            limit: the new value to set

        Returns:
            this cursor itself.
        """

        self._ensure_idle()
        self._ensure_alive()
        self._limit = limit if limit != 0 else None
        return self

    def include_similarity(self: BC, include_similarity: bool | None) -> BC:
        """
        Set a new `include_similarity` value for this cursor.

        Args:
            include_similarity: the new value to set

        Returns:
            this cursor itself.
        """

        self._ensure_idle()
        self._ensure_alive()
        self._include_similarity = include_similarity
        return self

    def include_sort_vector(self: BC, include_sort_vector: bool | None) -> BC:
        """
        Set a new `include_sort_vector` value for this cursor.

        Args:
            include_sort_vector: the new value to set

        Returns:
            this cursor itself.
        """

        self._ensure_idle()
        self._ensure_alive()
        self._include_sort_vector = include_sort_vector
        return self

    @property
    def consumed(self) -> int:
        """
        The number of documents consumed so far (by the code consuming the cursor).
        """

        return self._consumed

    def rewind(self: BC) -> BC:
        """
        Reset the cursor to its pristine state, i.e. fully unconsumed.

        Returns:
            this cursor itself.
        """

        self._state = CursorState.IDLE
        self._consumed = 0
        self._iterator = None
        return self

    def skip(self: BC, skip: int | None) -> BC:
        """
        Set a new `skip` value for this cursor.

        Args:
            skip: the new value to set

        Returns:
            this cursor itself.

        Note:
            This parameter can be used only in conjunction with an explicit
            `sort` criterion of the ascending/descending type (i.e. it cannot
            be used when not sorting, nor with vector-based ANN search).
        """
        self._ensure_idle()
        self._ensure_alive()
        self._skip = skip
        return self

    def sort(
        self: BC,
        sort: dict[str, Any] | None,
    ) -> BC:
        """
        Set a new `sort` value for this cursor.

        Args:
            sort: the new sorting prescription to set

        Returns:
            this cursor itself.

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
        """

        self._ensure_idle()
        self._ensure_alive()
        self._sort = sort
        return self


class Cursor(BaseCursor):
    """
    Represents a (synchronous) cursor over documents in a collection.
    A cursor is iterated over, e.g. with a for loop, and keeps track of
    its progress.

    Generally cursors are not supposed to be instantiated directly,
    rather they are obtained by invoking the `find` method on a collection.

    Attributes:
        collection: the collection to find documents in
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
                The default is to return the whole documents.
            max_time_ms: a timeout, in milliseconds, for each single one
                of the underlying HTTP requests used to fetch documents as the
                cursor is iterated over.

    Note:
        When not specifying sorting criteria at all (by vector or otherwise),
        the cursor can scroll through an arbitrary number of documents as
        the Data API and the client periodically exchange new chunks of documents.
        It should be noted that the behavior of the cursor in the case documents
        have been added/removed after the cursor was started depends on database
        internals and it is not guaranteed, nor excluded, that such "real-time"
        changes in the data would be picked up by the cursor.
    """

    def __init__(
        self,
        collection: Collection,
        filter: dict[str, Any] | None,
        projection: ProjectionType | None,
        max_time_ms: int | None,
        overall_max_time_ms: int | None,
    ) -> None:
        self._collection: Collection = collection
        self._filter = filter
        self._projection = projection
        self._overall_max_time_ms = overall_max_time_ms
        if overall_max_time_ms is not None and max_time_ms is not None:
            self._max_time_ms = min(max_time_ms, overall_max_time_ms)
        else:
            self._max_time_ms = max_time_ms
        self._limit: int | None = None
        self._skip: int | None = None
        self._include_similarity: bool | None = None
        self._include_sort_vector: bool | None = None
        self._sort: dict[str, Any] | None = None
        self._state = CursorState.IDLE
        self._consumed = 0
        #
        self._iterator: _LookAheadIterator | None = None
        self._api_response_status: dict[str, Any] | None = None

    def __iter__(self) -> Cursor:
        self._ensure_alive()
        if self._iterator is None:
            self._iterator = self._create_iterator()
            self._state = CursorState.STARTED
        return self

    def __next__(self) -> DocumentType:
        if not self.alive:
            # keep raising once exhausted:
            raise StopIteration
        if self._iterator is None:
            self._iterator = self._create_iterator()
            self._state = CursorState.STARTED
        # check for overall timing out
        if self._overall_max_time_ms is not None:
            _elapsed = time.time() - self._started_time_s  # type: ignore[operator]
            if _elapsed > (self._overall_max_time_ms / 1000.0):
                raise DataAPITimeoutException(
                    text="Cursor timed out.",
                    timeout_type="generic",
                    endpoint=None,
                    raw_payload=None,
                )
        try:
            next_item = self._iterator.__next__()
            self._consumed = self._consumed + 1
            return next_item
        except StopIteration:
            self.close()
            raise

    def get_sort_vector(self) -> list[float] | None:
        """
        Return the vector used in this ANN search, if applicable.
        If this is not an ANN search, or it was invoked without the
        `include_sort_vector` parameter, return None.

        Invoking this method on a pristine cursor will trigger an API call
        to get the first page of results.
        """

        if self._iterator is None:
            self._iterator = self._create_iterator()
            self._state = CursorState.STARTED
        self._iterator.preread()
        if self._api_response_status:
            return self._api_response_status.get("sortVector")
        else:
            return None

    def _item_at_index(self, index: int) -> DocumentType:
        finder_cursor = self._copy().skip(index).limit(1)
        items = list(finder_cursor)
        if items:
            return items[0]
        else:
            raise IndexError("no such item for Cursor instance")

    def _create_iterator(self) -> _LookAheadIterator:
        self._ensure_idle()
        self._ensure_alive()
        _options_0 = {
            k: v
            for k, v in {
                "limit": self._limit,
                "skip": self._skip,
                "includeSimilarity": self._include_similarity,
                "includeSortVector": self._include_sort_vector,
            }.items()
            if v is not None
        }
        _projection = normalize_optional_projection(self._projection)
        _sort = self._sort or {}
        _filter = self._filter or {}
        f0_payload = {
            "find": {
                k: v
                for k, v in {
                    "filter": _filter,
                    "projection": _projection,
                    "options": _options_0,
                    "sort": _sort,
                }.items()
                if v
            }
        }

        def _find_iterator() -> Iterator[DocumentType]:
            next_page_state: str | None = None
            #
            resp_0 = self._collection.command(
                body=f0_payload,
                max_time_ms=self._max_time_ms,
            )
            self._api_response_status = resp_0.get("status")
            if "nextPageState" not in resp_0.get("data", {}):
                raise DataAPIFaultyResponseException(
                    text="Faulty response from find API command (no 'nextPageState').",
                    raw_response=resp_0,
                )
            next_page_state = resp_0["data"]["nextPageState"]
            if "documents" not in resp_0.get("data", {}):
                raise DataAPIFaultyResponseException(
                    text="Faulty response from find API command (no 'documents').",
                    raw_response=resp_0,
                )
            for doc in resp_0["data"]["documents"]:
                yield doc
            while next_page_state is not None:
                _options_n = {**_options_0, **{"pageState": next_page_state}}
                fn_payload = {
                    "find": {
                        k: v
                        for k, v in {
                            "filter": _filter,
                            "projection": _projection,
                            "options": _options_n,
                            "sort": _sort,
                        }.items()
                        if v
                    }
                }
                resp_n = self._collection.command(
                    body=fn_payload,
                    max_time_ms=self._max_time_ms,
                )
                self._api_response_status = resp_n.get("status")
                if "nextPageState" not in resp_n.get("data", {}):
                    raise DataAPIFaultyResponseException(
                        text="Faulty response from find API command (no 'nextPageState').",
                        raw_response=resp_n,
                    )
                next_page_state = resp_n["data"]["nextPageState"]
                if "documents" not in resp_n.get("data", {}):
                    raise DataAPIFaultyResponseException(
                        text="Faulty response from find API command (no 'documents').",
                        raw_response=resp_n,
                    )
                for doc in resp_n["data"]["documents"]:
                    yield doc

        logger.info(f"creating iterator on '{self._collection.name}'")
        iterator = _find_iterator()
        logger.info(f"finished creating iterator on '{self._collection.name}'")
        self._started_time_s = time.time()
        return _LookAheadIterator(iterator)

    @property
    def data_source(self) -> Collection:
        """
        The (synchronous) collection this cursor is targeting.
        """

        return self._collection

    def distinct(self, key: str, max_time_ms: int | None = None) -> list[Any]:
        """
        Compute a list of unique values for a specific field across all
        documents the cursor iterates through.

        Invoking this method has no effect on the cursor state, i.e.
        the position of the cursor is unchanged.

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
            max_time_ms: a timeout, in milliseconds, for the operation.

        Note:
            this operation works at client-side by scrolling through all
            documents matching the cursor parameters (such as `filter`).
            Please be aware of this fact, especially for a very large
            amount of documents, for this may have implications on latency,
            network traffic and possibly billing.
        """

        _item_hashes = set()
        distinct_items = []

        _extractor = _create_document_key_extractor(key)
        _key = _reduce_distinct_key_to_safe(key)

        if _key == "":
            raise ValueError(
                "The 'key' parameter for distinct cannot be empty "
                "or start with a list index."
            )

        d_cursor = self._copy(
            projection={_key: True},
            overall_max_time_ms=max_time_ms,
        )
        logger.info(f"running distinct() on '{self._collection.name}'")
        for document in d_cursor:
            for item in _extractor(document):
                _item_hash = _hash_document(item)
                if _item_hash not in _item_hashes:
                    _item_hashes.add(_item_hash)
                    distinct_items.append(item)

        logger.info(f"finished running distinct() on '{self._collection.name}'")
        return distinct_items


class AsyncCursor(BaseCursor):
    """
    Represents a (asynchronous) cursor over documents in a collection.
    An asynchronous cursor is iterated over, e.g. with a for loop,
    and keeps track of its progress.

    Generally cursors are not supposed to be instantiated directly,
    rather they are obtained by invoking the `find` method on a collection.

    Attributes:
        collection: the collection to find documents in
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
                The default is to return the whole documents.
            max_time_ms: a timeout, in milliseconds, for each single one
                of the underlying HTTP requests used to fetch documents as the
                cursor is iterated over.

    Note:
        When not specifying sorting criteria at all (by vector or otherwise),
        the cursor can scroll through an arbitrary number of documents as
        the Data API and the client periodically exchange new chunks of documents.
        It should be noted that the behavior of the cursor in the case documents
        have been added/removed after the cursor was started depends on database
        internals and it is not guaranteed, nor excluded, that such "real-time"
        changes in the data would be picked up by the cursor.
    """

    def __init__(
        self,
        collection: AsyncCollection,
        filter: dict[str, Any] | None,
        projection: ProjectionType | None,
        max_time_ms: int | None,
        overall_max_time_ms: int | None,
    ) -> None:
        self._collection: AsyncCollection = collection
        self._filter = filter
        self._projection = projection
        self._overall_max_time_ms = overall_max_time_ms
        if overall_max_time_ms is not None and max_time_ms is not None:
            self._max_time_ms = min(max_time_ms, overall_max_time_ms)
        else:
            self._max_time_ms = max_time_ms
        self._limit: int | None = None
        self._skip: int | None = None
        self._include_similarity: bool | None = None
        self._include_sort_vector: bool | None = None
        self._sort: dict[str, Any] | None = None
        self._state = CursorState.IDLE
        self._consumed = 0
        #
        self._iterator: _AsyncLookAheadIterator | None = None
        self._api_response_status: dict[str, Any] | None = None

    def __aiter__(self) -> AsyncCursor:
        self._ensure_alive()
        if self._iterator is None:
            self._iterator = self._create_iterator()
            self._state = CursorState.STARTED
        return self

    async def __anext__(self) -> DocumentType:
        if not self.alive:
            # keep raising once exhausted:
            raise StopAsyncIteration
        if self._iterator is None:
            self._iterator = self._create_iterator()
            self._state = CursorState.STARTED
        # check for overall timing out
        if self._overall_max_time_ms is not None:
            _elapsed = time.time() - self._started_time_s  # type: ignore[operator]
            if _elapsed > (self._overall_max_time_ms / 1000.0):
                raise DataAPITimeoutException(
                    text="Cursor timed out.",
                    timeout_type="generic",
                    endpoint=None,
                    raw_payload=None,
                )
        try:
            next_item = await self._iterator.__anext__()
            self._consumed = self._consumed + 1
            return next_item
        except StopAsyncIteration:
            self.close()
            raise

    async def get_sort_vector(self) -> list[float] | None:
        """
        Return the vector used in this ANN search, if applicable.
        If this is not an ANN search, or it was invoked without the
        `include_sort_vector` parameter, return None.

        Invoking this method on a pristine cursor will trigger an API call
        to get the first page of results.
        """

        if self._iterator is None:
            self._iterator = self._create_iterator()
            self._state = CursorState.STARTED
        await self._iterator.preread()
        if self._api_response_status:
            return self._api_response_status.get("sortVector")
        else:
            return None

    def _item_at_index(self, index: int) -> DocumentType:
        finder_cursor = self._to_sync().skip(index).limit(1)
        items = list(finder_cursor)
        if items:
            return items[0]
        else:
            raise IndexError("no such item for AsyncCursor instance")

    def _create_iterator(self) -> _AsyncLookAheadIterator:
        self._ensure_idle()
        self._ensure_alive()
        _options_0 = {
            k: v
            for k, v in {
                "limit": self._limit,
                "skip": self._skip,
                "includeSimilarity": self._include_similarity,
                "includeSortVector": self._include_sort_vector,
            }.items()
            if v is not None
        }
        _projection = normalize_optional_projection(self._projection)
        _sort = self._sort or {}
        _filter = self._filter or {}
        f0_payload = {
            "find": {
                k: v
                for k, v in {
                    "filter": _filter,
                    "projection": _projection,
                    "options": _options_0,
                    "sort": _sort,
                }.items()
                if v
            }
        }

        async def _find_iterator() -> AsyncIterator[DocumentType]:
            resp_0 = await self._collection.command(
                body=f0_payload,
                max_time_ms=self._max_time_ms,
            )
            self._api_response_status = resp_0.get("status")
            if "nextPageState" not in resp_0.get("data", {}):
                raise DataAPIFaultyResponseException(
                    text="Faulty response from find API command (no 'nextPageState').",
                    raw_response=resp_0,
                )
            next_page_state = resp_0["data"]["nextPageState"]
            if "documents" not in resp_0.get("data", {}):
                raise DataAPIFaultyResponseException(
                    text="Faulty response from find API command (no 'documents').",
                    raw_response=resp_0,
                )
            for doc in resp_0["data"]["documents"]:
                yield doc
            while next_page_state is not None:
                _options_n = {**_options_0, **{"pageState": next_page_state}}
                fn_payload = {
                    "find": {
                        k: v
                        for k, v in {
                            "filter": _filter,
                            "projection": _projection,
                            "options": _options_n,
                            "sort": _sort,
                        }.items()
                        if v
                    }
                }
                resp_n = await self._collection.command(
                    body=fn_payload,
                    max_time_ms=self._max_time_ms,
                )
                self._api_response_status = resp_n.get("status")
                if "nextPageState" not in resp_n.get("data", {}):
                    raise DataAPIFaultyResponseException(
                        text="Faulty response from find API command (no 'nextPageState').",
                        raw_response=resp_n,
                    )
                next_page_state = resp_n["data"]["nextPageState"]
                if "documents" not in resp_n.get("data", {}):
                    raise DataAPIFaultyResponseException(
                        text="Faulty response from find API command (no 'documents').",
                        raw_response=resp_n,
                    )
                for doc in resp_n["data"]["documents"]:
                    yield doc

        logger.info(f"creating iterator on '{self._collection.name}'")
        iterator = _find_iterator()
        logger.info(f"finished creating iterator on '{self._collection.name}'")
        self._started_time_s = time.time()
        return _AsyncLookAheadIterator(iterator)

    def _to_sync(
        self: AsyncCursor,
        *,
        limit: int | None = None,
        skip: int | None = None,
        include_similarity: bool | None = None,
        include_sort_vector: bool | None = None,
        sort: dict[str, Any] | None = None,
    ) -> Cursor:
        new_cursor = Cursor(
            collection=self._collection.to_sync(),
            filter=self._filter,
            projection=self._projection,
            max_time_ms=self._max_time_ms,
            overall_max_time_ms=self._overall_max_time_ms,
        )
        # Cursor treated as mutable within this function scope:
        new_cursor._limit = limit if limit is not None else self._limit
        new_cursor._skip = skip if skip is not None else self._skip
        new_cursor._include_similarity = (
            include_similarity
            if include_similarity is not None
            else self._include_similarity
        )
        new_cursor._include_sort_vector = (
            include_sort_vector
            if include_sort_vector is not None
            else self._include_sort_vector
        )
        new_cursor._sort = sort if sort is not None else self._sort
        return new_cursor

    @property
    def data_source(self) -> AsyncCollection:
        """
        The (asynchronous) collection this cursor is targeting.
        """

        return self._collection

    async def distinct(self, key: str, max_time_ms: int | None = None) -> list[Any]:
        """
        Compute a list of unique values for a specific field across all
        documents the cursor iterates through.

        Invoking this method has no effect on the cursor state, i.e.
        the position of the cursor is unchanged.

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
            max_time_ms: a timeout, in milliseconds, for the operation.

        Note:
            this operation works at client-side by scrolling through all
            documents matching the cursor parameters (such as `filter`).
            Please be aware of this fact, especially for a very large
            amount of documents, for this may have implications on latency,
            network traffic and possibly billing.
        """

        _item_hashes = set()
        distinct_items = []

        _extractor = _create_document_key_extractor(key)
        _key = _reduce_distinct_key_to_safe(key)

        d_cursor = self._copy(
            projection={_key: True},
            overall_max_time_ms=max_time_ms,
        )
        logger.info(f"running distinct() on '{self._collection.name}'")
        async for document in d_cursor:
            for item in _extractor(document):
                _item_hash = _hash_document(item)
                if _item_hash not in _item_hashes:
                    _item_hashes.add(_item_hash)
                    distinct_items.append(item)

        logger.info(f"finished running distinct() on '{self._collection.name}'")
        return distinct_items
