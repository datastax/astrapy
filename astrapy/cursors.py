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
from collections.abc import Iterator, AsyncIterator
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
    TYPE_CHECKING,
)

from astrapy.core.utils import _normalize_payload_value
from astrapy.constants import (
    DocumentType,
    ProjectionType,
    normalize_optional_projection,
)

if TYPE_CHECKING:
    from astrapy.collection import AsyncCollection, Collection


BC = TypeVar("BC", bound="BaseCursor")
T = TypeVar("T")
IndexPairType = Tuple[str, Optional[int]]

FIND_PREFETCH = 20


def _maybe_valid_list_index(key_block: str) -> Optional[int]:
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
) -> Callable[[Dict[str, Any]], Iterable[Any]]:

    key_blocks0: List[IndexPairType] = [
        (kb_str, _maybe_valid_list_index(kb_str)) for kb_str in key.split(".")
    ]
    if key_blocks0 == []:
        raise ValueError("Field path specification cannot be empty")
    if any(kb[0] == "" for kb in key_blocks0):
        raise ValueError("Field path components cannot be empty")

    def _extract_with_key_blocks(
        key_blocks: List[IndexPairType], value: Any
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
                if k_int is not None and len(value) > k_int:
                    new_value = value[k_int]
                    for item in _extract_with_key_blocks(rest_key_blocks, new_value):
                        yield item
                else:
                    for item in value:
                        for item in _extract_with_key_blocks(key_blocks, item):
                            yield item
                return
            else:
                # keyblocks are deeper than the document. Nothing to extract.
                return

    def _item_extractor(document: Dict[str, Any]) -> Iterable[Any]:
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
    return distinct_key.split(".")[0]


def _hash_document(document: Dict[str, Any]) -> str:
    _normalized_item = _normalize_payload_value(path=[], value=document)
    _normalized_json = json.dumps(
        _normalized_item, sort_keys=True, separators=(",", ":")
    )
    _item_hash = hashlib.md5(_normalized_json.encode()).hexdigest()
    return _item_hash


class BaseCursor:
    """
    Represents a generic Cursor over query results, regardless of whether
    synchronous or asynchronous. It cannot be instantiated.
    """

    _collection: Union[Collection, AsyncCollection]
    _filter: Optional[Dict[str, Any]]
    _projection: Optional[ProjectionType]
    _limit: Optional[int]
    _skip: Optional[int]
    _sort: Optional[Dict[str, Any]]
    _started: bool
    _retrieved: int
    _alive: bool
    _iterator: Optional[Union[Iterator[DocumentType], AsyncIterator[DocumentType]]] = (
        None
    )

    def __init__(
        self,
        collection: Union[Collection, AsyncCollection],
        filter: Optional[Dict[str, Any]],
        projection: Optional[ProjectionType],
    ) -> None:
        raise NotImplementedError

    def __getitem__(self: BC, index: Union[int, slice]) -> Union[BC, DocumentType]:
        self._ensure_not_started()
        self._ensure_alive()
        if isinstance(index, int):
            # In this case, a separate cursor is run, not touching self
            return self._item_at_index(index)
        elif isinstance(index, slice):
            start = index.start
            stop = index.stop
            step = index.step
            if step is not None and step != 1:
                raise ValueError("Cursor slicing cannot have arbitrary step")
            _skip = start
            _limit = stop - start
            return self.limit(_limit).skip(_skip)
        else:
            raise TypeError(
                f"cursor indices must be integers or slices, not {type(index).__name__}"
            )

    def __repr__(self) -> str:
        _state_desc: str
        if self._started:
            if self._alive:
                _state_desc = "running"
            else:
                _state_desc = "exhausted"
        else:
            _state_desc = "new"
        return (
            f'{self.__class__.__name__}("{self._collection.name}", '
            f"{_state_desc}, "
            f"retrieved: {self.retrieved})"
        )

    def _item_at_index(self, index: int) -> DocumentType:
        # deferred to subclasses
        raise NotImplementedError

    def _ensure_alive(self) -> None:
        if not self._alive:
            raise ValueError("Cursor is closed.")

    def _ensure_not_started(self) -> None:
        if self._started:
            raise ValueError("Cursor has already been used")

    def _copy(
        self: BC,
        *,
        projection: Optional[ProjectionType] = None,
        limit: Optional[int] = None,
        skip: Optional[int] = None,
        started: Optional[bool] = None,
        sort: Optional[Dict[str, Any]] = None,
    ) -> BC:
        new_cursor = self.__class__(
            collection=self._collection,
            filter=self._filter,
            projection=projection or self._projection,
        )
        # Cursor treated as mutable within this function scope:
        new_cursor._limit = limit if limit is not None else self._limit
        new_cursor._skip = skip if skip is not None else self._skip
        new_cursor._started = started if started is not None else self._started
        new_cursor._sort = sort if sort is not None else self._sort
        if started is False:
            new_cursor._retrieved = 0
            new_cursor._alive = True
        else:
            new_cursor._retrieved = self._retrieved
            new_cursor._alive = self._alive
        return new_cursor

    @property
    def address(self) -> str:
        """
        The API endpoint used by this cursor when issuing
        requests to the database.
        """

        return self._collection._astra_db_collection.base_path

    @property
    def alive(self) -> bool:
        """
        Whether the cursor has the potential to yield more data.
        """

        return self._alive

    def clone(self: BC) -> BC:
        """
        Clone the cursor into a new, fresh one.

        Returns:
            a copy of this cursor, reset to its pristine state,
            i.e. fully un-consumed.
        """

        return self._copy(started=False)

    def close(self) -> None:
        """
        Stop/kill the cursor, regardless of its status.
        """

        self._alive = False

    @property
    def cursor_id(self) -> int:
        """
        An integer uniquely identifying this cursor.
        """

        return id(self)

    def limit(self: BC, limit: Optional[int]) -> BC:
        """
        Set a new `limit` value for this cursor.

        Args:
            limit: the new value to set

        Returns:
            this cursor itself.
        """

        self._ensure_not_started()
        self._ensure_alive()
        self._limit = limit if limit != 0 else None
        return self

    @property
    def retrieved(self) -> int:
        """
        The number of documents retrieved so far.
        """

        return self._retrieved

    def rewind(self: BC) -> BC:
        """
        Reset the cursor to its pristine state, i.e. fully unconsumed.

        Returns:
            this cursor itself.
        """

        self._started = False
        self._retrieved = 0
        self._alive = True
        self._iterator = None
        return self

    def skip(self: BC, skip: Optional[int]) -> BC:
        """
        Set a new `skip` value for this cursor.

        Args:
            skip: the new value to set

        Returns:
            this cursor itself.
        """
        self._ensure_not_started()
        self._ensure_alive()
        self._skip = skip
        return self

    def sort(
        self: BC,
        sort: Optional[Dict[str, Any]],
    ) -> BC:
        """
        Set a new `sort` value for this cursor.

        Args:
            sort: the new sorting prescription to set

        Returns:
            this cursor itself.
        """

        self._ensure_not_started()
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
    """

    def __init__(
        self,
        collection: Collection,
        filter: Optional[Dict[str, Any]],
        projection: Optional[ProjectionType],
    ) -> None:
        self._collection: Collection = collection
        self._filter = filter
        self._projection = projection
        self._limit: Optional[int] = None
        self._skip: Optional[int] = None
        self._sort: Optional[Dict[str, Any]] = None
        self._started = False
        self._retrieved = 0
        self._alive = True
        #
        self._iterator: Optional[Iterator[DocumentType]] = None

    def __iter__(self) -> Cursor:
        self._ensure_alive()
        if self._iterator is None:
            self._iterator = self._create_iterator()
            self._started = True
        return self

    def __next__(self) -> DocumentType:
        if not self.alive:
            # keep raising once exhausted:
            raise StopIteration
        if self._iterator is None:
            self._iterator = self._create_iterator()
            self._started = True
        try:
            next_item = self._iterator.__next__()
            self._retrieved = self._retrieved + 1
            return next_item
        except StopIteration:
            self._alive = False
            raise

    def _item_at_index(self, index: int) -> DocumentType:
        finder_cursor = self._copy().skip(index).limit(1)
        items = list(finder_cursor)
        if items:
            return items[0]
        else:
            raise IndexError("no such item for Cursor instance")

    def _create_iterator(self) -> Iterator[DocumentType]:
        self._ensure_not_started()
        self._ensure_alive()
        _options = {
            k: v
            for k, v in {
                "limit": self._limit,
                "skip": self._skip,
            }.items()
            if v is not None
        }

        # recast parameters for paginated_find call
        pf_projection: Optional[Dict[str, bool]] = normalize_optional_projection(
            self._projection
        )
        pf_sort: Optional[Dict[str, int]]
        if self._sort:
            pf_sort = dict(self._sort)
        else:
            pf_sort = None

        iterator = self._collection._astra_db_collection.paginated_find(
            filter=self._filter,
            projection=pf_projection,
            sort=pf_sort,
            options=_options,
            prefetched=FIND_PREFETCH,
        )
        return iterator

    @property
    def collection(self) -> Collection:
        """
        The (synchronous) collection this cursor is targeting.
        """

        return self._collection

    def distinct(self, key: str) -> List[Any]:
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

        d_cursor = self._copy(projection={_key: True}, started=False)
        for document in d_cursor:
            for item in _extractor(document):
                _item_hash = _hash_document(item)
                if _item_hash not in _item_hashes:
                    _item_hashes.add(_item_hash)
                    distinct_items.append(item)

        return distinct_items


class AsyncCursor(BaseCursor):
    """
    Represents a (asynchronous) cursor over documents in a collection.
    An asynchronous cursor is iterated over, e.g. with a for loop,
    and keeps track of its progress.

    Generally cursors are not supposed to be instantiated directly,
    rather they are obtained by invoking the `find` method on a collection.
    """

    def __init__(
        self,
        collection: AsyncCollection,
        filter: Optional[Dict[str, Any]],
        projection: Optional[ProjectionType],
    ) -> None:
        self._collection: AsyncCollection = collection
        self._filter = filter
        self._projection = projection
        self._limit: Optional[int] = None
        self._skip: Optional[int] = None
        self._sort: Optional[Dict[str, Any]] = None
        self._started = False
        self._retrieved = 0
        self._alive = True
        #
        self._iterator: Optional[AsyncIterator[DocumentType]] = None

    def __aiter__(self) -> AsyncCursor:
        self._ensure_alive()
        if self._iterator is None:
            self._iterator = self._create_iterator()
            self._started = True
        return self

    async def __anext__(self) -> DocumentType:
        if not self.alive:
            # keep raising once exhausted:
            raise StopAsyncIteration
        if self._iterator is None:
            self._iterator = self._create_iterator()
            self._started = True
        try:
            next_item = await self._iterator.__anext__()
            self._retrieved = self._retrieved + 1
            return next_item
        except StopAsyncIteration:
            self._alive = False
            raise

    def _item_at_index(self, index: int) -> DocumentType:
        finder_cursor = self._to_sync().skip(index).limit(1)
        items = list(finder_cursor)
        if items:
            return items[0]
        else:
            raise IndexError("no such item for AsyncCursor instance")

    def _create_iterator(self) -> AsyncIterator[DocumentType]:
        self._ensure_not_started()
        self._ensure_alive()
        _options = {
            k: v
            for k, v in {
                "limit": self._limit,
                "skip": self._skip,
            }.items()
            if v is not None
        }

        # recast parameters for paginated_find call
        pf_projection: Optional[Dict[str, bool]] = normalize_optional_projection(
            self._projection
        )
        pf_sort: Optional[Dict[str, int]]
        if self._sort:
            pf_sort = dict(self._sort)
        else:
            pf_sort = None

        iterator = self._collection._astra_db_collection.paginated_find(
            filter=self._filter,
            projection=pf_projection,
            sort=pf_sort,
            options=_options,
            prefetched=FIND_PREFETCH,
        )
        return iterator

    def _to_sync(
        self: AsyncCursor,
        *,
        limit: Optional[int] = None,
        skip: Optional[int] = None,
        started: Optional[bool] = None,
        sort: Optional[Dict[str, Any]] = None,
    ) -> Cursor:
        new_cursor = Cursor(
            collection=self._collection.to_sync(),
            filter=self._filter,
            projection=self._projection,
        )
        # Cursor treated as mutable within this function scope:
        new_cursor._limit = limit if limit is not None else self._limit
        new_cursor._skip = skip if skip is not None else self._skip
        new_cursor._started = started if started is not None else self._started
        new_cursor._sort = sort if sort is not None else self._sort
        if started is False:
            new_cursor._retrieved = 0
            new_cursor._alive = True
        else:
            new_cursor._retrieved = self._retrieved
            new_cursor._alive = self._alive
        return new_cursor

    @property
    def collection(self) -> AsyncCollection:
        """
        The (asynchronous) collection this cursor is targeting.
        """

        return self._collection

    async def distinct(self, key: str) -> List[Any]:
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

        d_cursor = self._copy(projection={_key: True}, started=False)
        async for document in d_cursor:
            for item in _extractor(document):
                _item_hash = _hash_document(item)
                if _item_hash not in _item_hashes:
                    _item_hashes.add(_item_hash)
                    distinct_items.append(item)

        return distinct_items


class CommandCursor(Generic[T]):
    """
    A (synchronous) cursor over the results of a Data API command
    (as opposed to a cursor over data as one would get with a `find` method).

    Command cursors are iterated over, e.g. with a for loop.

    Generally command cursors are not supposed to be instantiated directly,
    rather they are obtained by invoking methods on a collection/database
    (such as the database `list_collections` method).
    """

    def __init__(self, address: str, items: List[T]) -> None:
        self._address = address
        self.items = items
        self.iterable = items.__iter__()
        self._alive = True

    def __iter__(self) -> CommandCursor[T]:
        self._ensure_alive()
        return self

    def __next__(self) -> T:
        try:
            item = self.iterable.__next__()
            return item
        except StopIteration:
            self._alive = False
            raise

    @property
    def address(self) -> str:
        """
        The API endpoint used by this cursor when issuing
        requests to the database.
        """

        return self._address

    @property
    def alive(self) -> bool:
        """
        Whether the cursor has the potential to yield more data.
        """

        return self._alive

    @property
    def cursor_id(self) -> int:
        """
        An integer uniquely identifying this cursor.
        """

        return id(self)

    def _ensure_alive(self) -> None:
        if not self._alive:
            raise ValueError("Cursor is closed.")

    def try_next(self) -> T:
        """
        An alias for the `__next__` method, used by the iterator protocol.
        """

        return self.__next__()

    def close(self) -> None:
        """
        Stop/kill the cursor, regardless of its status.
        """

        self._alive = False


class AsyncCommandCursor(Generic[T]):
    """
    A (asynchronous) cursor over the results of a Data API command
    (as opposed to a cursor over data as one would get with a `find` method).

    Asynchronous command cursors are iterated over, e.g. with an async for loop.

    Generally command cursors are not supposed to be instantiated directly,
    rather they are obtained by invoking methods on a collection/database
    (such as the database `list_collections` method).
    """

    def __init__(self, address: str, items: List[T]) -> None:
        self._address = address
        self.items = items
        self.iterable = items.__iter__()
        self._alive = True

    def __aiter__(self) -> AsyncCommandCursor[T]:
        self._ensure_alive()
        return self

    async def __anext__(self) -> T:
        try:
            item = self.iterable.__next__()
            return item
        except StopIteration:
            self._alive = False
            raise StopAsyncIteration

    @property
    def address(self) -> str:
        """
        The API endpoint used by this cursor when issuing
        requests to the database.
        """

        return self._address

    @property
    def alive(self) -> bool:
        """
        Whether the cursor has the potential to yield more data.
        """

        return self._alive

    @property
    def cursor_id(self) -> int:
        """
        An integer uniquely identifying this cursor.
        """

        return id(self)

    def _ensure_alive(self) -> None:
        if not self._alive:
            raise ValueError("Cursor is closed.")

    async def try_next(self) -> T:
        """
        An alias for the `__next__` method, used by the iterator protocol.
        """

        return await self.__anext__()

    def close(self) -> None:
        """
        Stop/kill the cursor, regardless of its status.
        """

        self._alive = False