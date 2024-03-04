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

from collections.abc import Iterator
from typing import (
    Any,
    Dict,
    Optional,
    Union,
    TYPE_CHECKING,
)

from astrapy.idiomatic.types import DocumentType, ProjectionType

if TYPE_CHECKING:
    from astrapy.idiomatic.collection import Collection


FIND_PREFETCH = 20


class Cursor:
    def __init__(
        self,
        collection: Collection,
        filter: Optional[Dict[str, Any]],
        projection: Optional[ProjectionType],
    ) -> None:
        self._collection = collection
        self._filter = filter
        self._projection = projection
        self._limit: Optional[int] = None
        self._skip: Optional[int] = None
        self._sort: Optional[Dict[str, Any]] = None
        self._started = False
        self._retrieved = 0
        self._alive = True
        # mutable field; and never cloned
        self._iterator: Optional[Iterator[DocumentType]] = None

    def __iter__(self) -> Cursor:
        self._ensure_alive()
        if self._iterator is None:
            self._iterator = self._create_iterator()
            self._started = True
        return self

    def __getitem__(self, index: Union[int, slice]) -> Union[Cursor, DocumentType]:
        self._ensure_not_started()
        self._ensure_alive()
        if isinstance(index, int):
            # In this case, a separate cursor is run
            finder_cursor = self._copy().skip(index).limit(1)
            items = list(finder_cursor)
            if items:
                return items[0]
            else:
                raise IndexError
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
            raise ValueError("Unsupported indexing type for Cursor")

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
            f'Cursor("{self._collection.name}", '
            f"{_state_desc}, "
            f"retrieved: {self.retrieved})"
        )

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

    def _ensure_alive(self):
        if not self._alive:
            raise ValueError("Cursor is closed.")

    def _ensure_not_started(self):
        if self._started:
            raise ValueError("Cursor has already been used")

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

    def _copy(
        self,
        *,
        limit: Optional[int] = None,
        skip: Optional[int] = None,
        started: Optional[bool] = None,
        sort: Optional[Dict[str, Any]] = None,
    ) -> Cursor:
        new_cursor = Cursor(
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

    def clone(self) -> Cursor:
        return self._copy(started=False)

    def close(self) -> None:
        self._alive = False

    @property
    def collection(self) -> Collection:
        return self._collection

    @property
    def cursor_id(self) -> int:
        return id(self)

    def distinct(self) -> None:
        raise NotImplementedError

    def limit(self, limit: Optional[int]) -> Cursor:
        self._ensure_not_started()
        self._ensure_alive()
        self._limit = limit if limit != 0 else None
        return self

    @property
    def retrieved(self) -> int:
        return self._retrieved

    def rewind(self) -> Cursor:
        self._started = False
        self._retrieved = 0
        self._alive = True
        self._iterator = None
        return self

    def skip(self, skip: Optional[int]) -> Cursor:
        self._ensure_not_started()
        self._ensure_alive()
        self._skip = skip
        return self

    def sort(
        self,
        sort: Optional[Dict[str, Any]],
    ) -> Cursor:
        self._ensure_not_started()
        self._ensure_alive()
        self._sort = sort
        return self
