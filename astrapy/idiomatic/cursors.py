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

from collections.abc import Iterator, AsyncIterator
from typing import (
    Any,
    Dict,
    List,
    Optional,
    TypeVar,
    Union,
    TYPE_CHECKING,
)

from astrapy.idiomatic.types import DocumentType, ProjectionType

if TYPE_CHECKING:
    from astrapy.idiomatic.collection import AsyncCollection, Collection


BC = TypeVar("BC", bound="BaseCursor")

FIND_PREFETCH = 20


class BaseCursor:
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
        # subclasses must implement this
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
        limit: Optional[int] = None,
        skip: Optional[int] = None,
        started: Optional[bool] = None,
        sort: Optional[Dict[str, Any]] = None,
    ) -> BC:
        new_cursor = self.__class__(
            collection=self._collection,
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
    def address(self) -> str:
        """Return the api_endpoint used by this cursor."""
        return self._collection._astra_db_collection.base_path

    @property
    def alive(self) -> bool:
        return self._alive

    def clone(self: BC) -> BC:
        return self._copy(started=False)

    def close(self) -> None:
        self._alive = False

    @property
    def cursor_id(self) -> int:
        return id(self)

    def limit(self: BC, limit: Optional[int]) -> BC:
        self._ensure_not_started()
        self._ensure_alive()
        self._limit = limit if limit != 0 else None
        return self

    @property
    def retrieved(self) -> int:
        return self._retrieved

    def rewind(self: BC) -> BC:
        self._started = False
        self._retrieved = 0
        self._alive = True
        self._iterator = None
        return self

    def skip(self: BC, skip: Optional[int]) -> BC:
        self._ensure_not_started()
        self._ensure_alive()
        self._skip = skip
        return self

    def sort(
        self: BC,
        sort: Optional[Dict[str, Any]],
    ) -> BC:
        self._ensure_not_started()
        self._ensure_alive()
        self._sort = sort
        return self


class Cursor(BaseCursor):
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
        pf_projection: Optional[Dict[str, bool]]
        if self._projection:
            if isinstance(self._projection, dict):
                pf_projection = self._projection
            else:
                # an iterable over strings
                pf_projection = {field: True for field in self._projection}
        else:
            pf_projection = None
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
        return self._collection

    def distinct(self, key: str) -> List[Any]:
        """
        This works on a fresh pristine copy of the cursor
        and never touches self in any way.
        """
        return list(
            {document[key] for document in self._copy(started=False) if key in document}
        )


class AsyncCursor(BaseCursor):
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
        pf_projection: Optional[Dict[str, bool]]
        if self._projection:
            if isinstance(self._projection, dict):
                pf_projection = self._projection
            else:
                # an iterable over strings
                pf_projection = {field: True for field in self._projection}
        else:
            pf_projection = None
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
        return self._collection

    async def distinct(self, key: str) -> List[Any]:
        """
        This works on a fresh pristine copy of the cursor
        and never touches self in any way.
        """
        return list(
            {
                document[key]
                async for document in self._copy(started=False)
                if key in document
            }
        )
