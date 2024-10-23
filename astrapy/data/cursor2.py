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

from abc import ABC, abstractmethod
from copy import deepcopy
from enum import Enum
from typing import Any, Generic, TypeVar
from typing_extensions import override

from astrapy.collection import AsyncCollection, Collection
from astrapy.constants import (
    FilterType,
    ProjectionType,
    normalize_optional_projection,
)
from astrapy.exceptions import (
    CursorIsStartedException,
    # DataAPIFaultyResponseException,
    # DataAPITimeoutException,
)
# from astrapy.table import Table

# DOC = TypeVar("DOC", bound=DocumentType)
# COL = TypeVar("COL", Collection, Table)

TRAW = TypeVar("TRAW")


class CursorState(Enum):
    # Iteration over results has not started yet (alive=T, started=F)
    IDLE = "idle"
    # Iteration has started, *can* still yield results (alive=T, started=T)
    STARTED = "started"
    # Finished/forcibly stopped. Won't return more documents (alive=F)
    CLOSED = "closed"


class _BufferedCursor(Generic[TRAW]):
    _state: CursorState
    _buffer: list[TRAW]
    _pages_retrieved: int
    _consumed: int
    _next_page_state: str | None
    _last_response_status: dict[str, Any] | None

    def __init__(self) -> None:
        self._state = CursorState.IDLE
        self._buffer = []
        self._pages_retrieved = 0
        self._consumed = 0
        self._next_page_state = None
        self._last_response_status = None

    def _ensure_alive(self) -> None:
        if not self.alive:
            raise CursorIsStartedException(
                text="Cursor not alive.",
                cursor_state=self._state.value,
            )

    def _ensure_idle(self) -> None:
        if self._state != CursorState.IDLE:
            raise CursorIsStartedException(
                text="Cursor started already.",
                cursor_state=self._state.value,
            )

    @property
    def state(self) -> CursorState:
        """
        The current state of this cursor, which can be one of
        the astrapy.cursors.CursorState enum.
        """

        return self._state

    @property
    def alive(self) -> bool:
        """
        Whether the cursor has the potential to yield more data.
        """

        return self._state != CursorState.CLOSED

    @property
    def consumed(self) -> int:
        return self._consumed

    @property
    def cursor_id(self) -> int:
        """
        An integer uniquely identifying this cursor.
        """

        return id(self)

    @property
    def buffered_count(self) -> int:
        return len(self._buffer)

    def consume_buffer(self, n: int) -> list[TRAW]:
        """
        Consume (return) up to the requested number of buffered documents,
        but not triggering new page fetches from the Data API.
        """
        self._ensure_alive()
        if n < 0:
            raise ValueError("A negative amount of items was requested.")
        returned, remaining = self._buffer[:n], self._buffer[n:]
        self._buffer = remaining
        self._consumed += len(returned)
        return returned


class _QueryEngine(ABC, Generic[TRAW]):
    @abstractmethod
    def fetch_page(
        self, page_state: str | None
    ) -> tuple[list[TRAW], str | None, dict[str, Any] | None]:
        """Run a query for one page and return (entries, next-page-state, response.status)."""
        ...

    @abstractmethod
    async def async_fetch_page(
        self, page_state: str | None
    ) -> tuple[list[TRAW], str | None, dict[str, Any] | None]:
        """Run a query for one page and return (entries, next-page-state, response.status)."""
        ...


class _CollectionQueryEngine(Generic[TRAW], _QueryEngine[TRAW]):
    collection: Collection[TRAW] | None
    async_collection: AsyncCollection[TRAW] | None
    f_r_subpayload: dict[str, Any]
    f_options0: dict[str, Any]

    def __init__(
        self,
        collection: Collection[TRAW] | None,
        async_collection: AsyncCollection[TRAW] | None,
        filter: FilterType | None,
        projection: ProjectionType | None,
        sort: dict[str, Any] | None,
        limit: int | None,
        include_similarity: bool | None,
        include_sort_vector: bool | None,
        skip: int | None,
    ) -> None:
        self.collection = collection
        self.async_collection = async_collection
        self.f_r_subpayload = {
            k: v
            for k, v in {
                "filter": filter,
                "projection": normalize_optional_projection(projection),
                "sort": sort,
            }.items()
            if v is not None
        }
        self.f_options0 = {
            k: v
            for k, v in {
                "limit": limit if limit != 0 else None,
                "skip": skip,
                "includeSimilarity": include_similarity,
                "includeSortVector": include_sort_vector,
            }.items()
            if v is not None
        }

    @override
    def fetch_page(
        self, page_state: str | None
    ) -> tuple[list[TRAW], str | None, dict[str, Any] | None]:
        if self.collection is None:
            raise ValueError("Query engine has no sync collection.")
        f_payload = {
            "find": {
                **self.f_r_subpayload,
                "options": {
                    **self.f_options0,
                    **({"pageState": page_state} if page_state else {}),
                },
            },
        }
        f_response = self.collection.command(
            body=f_payload,
            # request_timeout_ms=...,
        )
        # if "documents" not in resp_n.get("data", {}):
        #     raise DataAPIFaultyResponseException(
        #         text="Faulty response from find API command (no 'documents').",
        #         raw_response=resp_n,
        #     )
        p_documents = f_response["data"]["documents"]
        n_p_state = f_response["data"]["nextPageState"]
        p_r_status = f_response.get("status")
        return (p_documents, n_p_state, p_r_status)

    @override
    async def async_fetch_page(
        self, page_state: str | None
    ) -> tuple[list[TRAW], str | None, dict[str, Any] | None]:
        if self.async_collection is None:
            raise ValueError("Query engine has no async collection.")
        f_payload = {
            "find": {
                **self.f_r_subpayload,
                "options": {
                    **self.f_options0,
                    **({"pageState": page_state} if page_state else {}),
                },
            },
        }
        f_response = await self.async_collection.command(
            body=f_payload,
            # request_timeout_ms=...,
        )
        # if "documents" not in resp_n.get("data", {}):
        #     raise DataAPIFaultyResponseException(
        #         text="Faulty response from find API command (no 'documents').",
        #         raw_response=resp_n,
        #     )
        p_documents = f_response["data"]["documents"]
        n_p_state = f_response["data"]["nextPageState"]
        p_r_status = f_response.get("status")
        return (p_documents, n_p_state, p_r_status)


class CollectionCursor(Generic[TRAW], _BufferedCursor[TRAW]):
    _query_engine: _CollectionQueryEngine[TRAW]
    filter: FilterType | None
    projection: ProjectionType | None
    sort: dict[str, Any] | None
    limit: int | None
    include_similarity: bool | None
    include_sort_vector: bool | None
    skip: int | None

    def __init__(
        self,
        collection: Collection[TRAW],
        filter: FilterType | None,
        projection: ProjectionType | None,
        sort: dict[str, Any] | None,
        limit: int | None,
        include_similarity: bool | None,
        include_sort_vector: bool | None,
        skip: int | None,
    ) -> None:
        self._filter = deepcopy(filter)
        self._projection = projection
        self._sort = sort
        self._limit = limit
        self._include_similarity = include_similarity
        self._include_sort_vector = include_sort_vector
        self._skip = skip
        self._query_engine = _CollectionQueryEngine(
            collection=collection,
            async_collection=None,
            filter=self._filter,
            projection=self._projection,
            sort=self._sort,
            limit=self._limit,
            include_similarity=self._include_similarity,
            include_sort_vector=self._include_sort_vector,
            skip=self._skip,
        )
        _BufferedCursor.__init__(self)

    def _try_ensure_fill_buffer(self) -> None:
        """
        If buffer is empty, try to fill with next page, if applicable.
        If not possible, silently do nothing.
        """

        if not self._buffer:
            if self._next_page_state is not None or self._state == CursorState.IDLE: 
                new_buffer, next_page_state, resp_status = self._query_engine.fetch_page(
                    page_state=self._next_page_state,
                )
                self._next_page_state = next_page_state
                self._last_response_status = resp_status
                self._pages_retrieved += 1
                self._buffer = new_buffer

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}("{self.data_source.name}", '
            f"{self._state.value}, "
            f"consumed so far: {self.consumed})"
        )

    def __iter__(self: CollectionCursor[TRAW]) -> CollectionCursor[TRAW]:
        self._ensure_alive()
        return self

    def __next__(self) -> TRAW:
        if not self.alive:
            raise StopIteration
        self._try_ensure_fill_buffer()
        if not self._buffer:
            self._state = CursorState.CLOSED
            raise StopIteration
        self._state = CursorState.STARTED
        # consume one item from buffer
        traw0, rest_buffer = self._buffer[0], self._buffer[1:]
        self._buffer = rest_buffer
        self._consumed += 1
        return traw0

    def has_next(self) -> bool:
        self._ensure_alive()
        self._try_ensure_fill_buffer()
        return len(self._buffer) > 0

    @property
    def data_source(self) -> Collection[TRAW]:
        if self._query_engine.collection is None:
            raise ValueError("Query engine has no collection.")
        return self._query_engine.collection

    @property
    def keyspace(self) -> str:
        return self.data_source.keyspace


class AsyncCollectionCursor(Generic[TRAW], _BufferedCursor[TRAW]):
    _query_engine: _CollectionQueryEngine[TRAW]
    filter: FilterType | None
    projection: ProjectionType | None
    sort: dict[str, Any] | None
    limit: int | None
    include_similarity: bool | None
    include_sort_vector: bool | None
    skip: int | None

    def __init__(
        self,
        collection: AsyncCollection[TRAW],
        filter: FilterType | None,
        projection: ProjectionType | None,
        sort: dict[str, Any] | None,
        limit: int | None,
        include_similarity: bool | None,
        include_sort_vector: bool | None,
        skip: int | None,
    ) -> None:
        self._filter = deepcopy(filter)
        self._projection = projection
        self._sort = sort
        self._limit = limit
        self._include_similarity = include_similarity
        self._include_sort_vector = include_sort_vector
        self._skip = skip
        self._query_engine = _CollectionQueryEngine(
            collection=None,
            async_collection=collection,
            filter=self._filter,
            projection=self._projection,
            sort=self._sort,
            limit=self._limit,
            include_similarity=self._include_similarity,
            include_sort_vector=self._include_sort_vector,
            skip=self._skip,
        )
        _BufferedCursor.__init__(self)

    async def _try_ensure_fill_buffer(self) -> None:
        """
        If buffer is empty, try to fill with next page, if applicable.
        If not possible, silently do nothing.
        """
        if not self._buffer:
            if self._next_page_state is not None or self._state == CursorState.IDLE: 
                new_buffer, next_page_state, resp_status = await self._query_engine.async_fetch_page(
                    page_state=self._next_page_state,
                )
                self._next_page_state = next_page_state
                self._last_response_status = resp_status
                self._pages_retrieved += 1
                self._buffer = new_buffer

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}("{self.data_source.name}", '
            f"{self._state.value}, "
            f"consumed so far: {self.consumed})"
        )

    def __aiter__(self: AsyncCollectionCursor[TRAW]) -> AsyncCollectionCursor[TRAW]:
        self._ensure_alive()
        return self

    async def __anext__(self) -> TRAW:
        if not self.alive:
            raise StopAsyncIteration
        await self._try_ensure_fill_buffer()
        if not self._buffer:
            self._state = CursorState.CLOSED
            raise StopAsyncIteration
        self._state = CursorState.STARTED
        # consume one item from buffer
        traw0, rest_buffer = self._buffer[0], self._buffer[1:]
        self._buffer = rest_buffer
        self._consumed += 1
        return traw0

    async def has_next(self) -> bool:
        self._ensure_alive()
        await self._try_ensure_fill_buffer()
        return len(self._buffer) > 0

    @property
    def data_source(self) -> AsyncCollection[TRAW]:
        if self._query_engine.async_collection is None:
            raise ValueError("Query engine has no async collection.")
        return self._query_engine.async_collection


    @property
    def keyspace(self) -> str:
        return self.data_source.keyspace
