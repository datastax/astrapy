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
from typing import Any, Callable, Generic, TypeVar, cast

from typing_extensions import override

from astrapy import AsyncCollection, AsyncTable, Collection, Table
from astrapy.constants import (
    FilterType,
    ProjectionType,
    normalize_optional_projection,
)
from astrapy.exceptions import (
    CursorException,
    DataAPIFaultyResponseException,
    MultiCallTimeoutManager,
)
from astrapy.utils.unset import _UNSET, UnsetType

# A cursor reads TRAW from DB and maps them to T if any mapping.
# A new cursor returned by .map will map to TNEW
TRAW = TypeVar("TRAW")
T = TypeVar("T")
TNEW = TypeVar("TNEW")


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
        self.rewind()

    def _ensure_alive(self) -> None:
        if not self.alive:
            raise CursorException(
                text="Cursor is stopped.",
                cursor_state=self._state.value,
            )

    def _ensure_idle(self) -> None:
        if self._state != CursorState.IDLE:
            raise CursorException(
                text="Cursor is not idle anymore.",
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

    def close(self) -> None:
        self._state = CursorState.CLOSED
        self._buffer = []

    def rewind(self) -> None:
        self._state = CursorState.IDLE
        self._buffer = []
        self._pages_retrieved = 0
        self._consumed = 0
        self._next_page_state = None
        self._last_response_status = None

    def consume_buffer(self, n: int | None = None) -> list[TRAW]:
        """
        Consume (return) up to the requested number of buffered documents,
        but not triggering new page fetches from the Data API.

        Returns empty list (without errors): if the buffer is empty; if the
        cursor is idle; if the cursor is closed.

        Args:
            TODO
        """
        _n = n if n is not None else len(self._buffer)
        if _n < 0:
            raise ValueError("A negative amount of items was requested.")
        returned, remaining = self._buffer[:_n], self._buffer[_n:]
        self._buffer = remaining
        self._consumed += len(returned)
        return returned


class _QueryEngine(ABC, Generic[TRAW]):
    @abstractmethod
    def fetch_page(
        self,
        *,
        page_state: str | None,
        request_timeout_ms: int | None,
    ) -> tuple[list[TRAW], str | None, dict[str, Any] | None]:
        """Run a query for one page and return (entries, next-page-state, response.status)."""
        ...

    @abstractmethod
    async def async_fetch_page(
        self,
        *,
        page_state: str | None,
        request_timeout_ms: int | None,
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
        *,
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
        self,
        *,
        page_state: str | None,
        request_timeout_ms: int | None,
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
            request_timeout_ms=request_timeout_ms,
        )
        if "documents" not in f_response.get("data", {}):
            raise DataAPIFaultyResponseException(
                text="Faulty response from find API command (no 'documents').",
                raw_response=f_response,
            )
        p_documents = f_response["data"]["documents"]
        n_p_state = f_response["data"]["nextPageState"]
        p_r_status = f_response.get("status")
        return (p_documents, n_p_state, p_r_status)

    @override
    async def async_fetch_page(
        self,
        *,
        page_state: str | None,
        request_timeout_ms: int | None,
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
            request_timeout_ms=request_timeout_ms,
        )
        if "documents" not in f_response.get("data", {}):
            raise DataAPIFaultyResponseException(
                text="Faulty response from find API command (no 'documents').",
                raw_response=f_response,
            )
        p_documents = f_response["data"]["documents"]
        n_p_state = f_response["data"]["nextPageState"]
        p_r_status = f_response.get("status")
        return (p_documents, n_p_state, p_r_status)


class _TableQueryEngine(Generic[TRAW], _QueryEngine[TRAW]):
    table: Table[TRAW] | None
    async_table: AsyncTable[TRAW] | None
    f_r_subpayload: dict[str, Any]
    f_options0: dict[str, Any]

    def __init__(
        self,
        *,
        table: Table[TRAW] | None,
        async_table: AsyncTable[TRAW] | None,
        filter: FilterType | None,
        projection: ProjectionType | None,
        sort: dict[str, Any] | None,
        limit: int | None,
        include_similarity: bool | None,
        include_sort_vector: bool | None,
        skip: int | None,
    ) -> None:
        self.table = table
        self.async_table = async_table
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
        self,
        *,
        page_state: str | None,
        request_timeout_ms: int | None,
    ) -> tuple[list[TRAW], str | None, dict[str, Any] | None]:
        if self.table is None:
            raise ValueError("Query engine has no sync table.")
        f_payload = {
            "find": {
                **self.f_r_subpayload,
                "options": {
                    **self.f_options0,
                    **({"pageState": page_state} if page_state else {}),
                },
            },
        }
        f_response = self.table.command(
            body=f_payload,
            request_timeout_ms=request_timeout_ms,
        )
        if "documents" not in f_response.get("data", {}):
            raise DataAPIFaultyResponseException(
                text="Faulty response from find API command (no 'documents').",
                raw_response=f_response,
            )
        p_documents = f_response["data"]["documents"]
        n_p_state = f_response["data"]["nextPageState"]
        p_r_status = f_response.get("status")
        return (p_documents, n_p_state, p_r_status)

    @override
    async def async_fetch_page(
        self,
        *,
        page_state: str | None,
        request_timeout_ms: int | None,
    ) -> tuple[list[TRAW], str | None, dict[str, Any] | None]:
        if self.async_table is None:
            raise ValueError("Query engine has no async table.")
        f_payload = {
            "find": {
                **self.f_r_subpayload,
                "options": {
                    **self.f_options0,
                    **({"pageState": page_state} if page_state else {}),
                },
            },
        }
        f_response = await self.async_table.command(
            body=f_payload,
            request_timeout_ms=request_timeout_ms,
        )
        if "documents" not in f_response.get("data", {}):
            raise DataAPIFaultyResponseException(
                text="Faulty response from find API command (no 'documents').",
                raw_response=f_response,
            )
        p_documents = f_response["data"]["documents"]
        n_p_state = f_response["data"]["nextPageState"]
        p_r_status = f_response.get("status")
        return (p_documents, n_p_state, p_r_status)


class CollectionCursor(Generic[TRAW, T], _BufferedCursor[TRAW]):
    _query_engine: _CollectionQueryEngine[TRAW]
    _request_timeout_ms: int | None | None
    _overall_timeout_ms: int | None
    _timeout_manager: MultiCallTimeoutManager
    _filter: FilterType | None
    _projection: ProjectionType | None
    _sort: dict[str, Any] | None
    _limit: int | None
    _include_similarity: bool | None
    _include_sort_vector: bool | None
    _skip: int | None
    _mapper: Callable[[TRAW], T]

    def __init__(
        self,
        *,
        collection: Collection[TRAW],
        request_timeout_ms: int | None,
        overall_timeout_ms: int | None,
        filter: FilterType | None = None,
        projection: ProjectionType | None = None,
        sort: dict[str, Any] | None = None,
        limit: int | None = None,
        include_similarity: bool | None = None,
        include_sort_vector: bool | None = None,
        skip: int | None = None,
        mapper: Callable[[TRAW], T] | None = None,
    ) -> None:
        self._filter = deepcopy(filter)
        self._projection = projection
        self._sort = sort
        self._limit = limit
        self._include_similarity = include_similarity
        self._include_sort_vector = include_sort_vector
        self._skip = skip
        if mapper is None:

            def _identity(document: TRAW) -> T:
                return cast(T, document)

            self._mapper = _identity
        else:
            self._mapper = mapper
        self._request_timeout_ms = request_timeout_ms
        self._overall_timeout_ms = overall_timeout_ms
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
        self._timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=self._overall_timeout_ms,
        )

    def _copy(
        self: CollectionCursor[TRAW, T],
        *,
        request_timeout_ms: int | None | UnsetType = _UNSET,
        overall_timeout_ms: int | None | UnsetType = _UNSET,
        filter: FilterType | None | UnsetType = _UNSET,
        projection: ProjectionType | None | UnsetType = _UNSET,
        sort: dict[str, Any] | None | UnsetType = _UNSET,
        limit: int | None | UnsetType = _UNSET,
        include_similarity: bool | None | UnsetType = _UNSET,
        include_sort_vector: bool | None | UnsetType = _UNSET,
        skip: int | None | UnsetType = _UNSET,
    ) -> CollectionCursor[TRAW, T]:
        if self._query_engine.collection is None:
            raise ValueError("Query engine has no collection.")
        return CollectionCursor(
            collection=self._query_engine.collection,
            request_timeout_ms=self._request_timeout_ms
            if isinstance(request_timeout_ms, UnsetType)
            else request_timeout_ms,
            overall_timeout_ms=self._overall_timeout_ms
            if isinstance(overall_timeout_ms, UnsetType)
            else overall_timeout_ms,
            filter=self._filter if isinstance(filter, UnsetType) else filter,
            projection=self._projection
            if isinstance(projection, UnsetType)
            else projection,
            sort=self._sort if isinstance(sort, UnsetType) else sort,
            limit=self._limit if isinstance(limit, UnsetType) else limit,
            include_similarity=self._include_similarity
            if isinstance(include_similarity, UnsetType)
            else include_similarity,
            include_sort_vector=self._include_sort_vector
            if isinstance(include_sort_vector, UnsetType)
            else include_sort_vector,
            skip=self._skip if isinstance(skip, UnsetType) else skip,
            mapper=self._mapper,
        )

    def _try_ensure_fill_buffer(self) -> None:
        """
        If buffer is empty, try to fill with next page, if applicable.
        If not possible, silently do nothing.
        """

        if self._state == CursorState.CLOSED:
            return
        if not self._buffer:
            if self._next_page_state is not None or self._state == CursorState.IDLE:
                new_buffer, next_page_state, resp_status = (
                    self._query_engine.fetch_page(
                        page_state=self._next_page_state,
                        request_timeout_ms=self._timeout_manager.remaining_timeout_ms(
                            cap_time_ms=self._request_timeout_ms,
                        ),
                    )
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

    def __iter__(self: CollectionCursor[TRAW, T]) -> CollectionCursor[TRAW, T]:
        self._ensure_alive()
        return self

    def __next__(self) -> T:
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
        return self._mapper(traw0)

    @property
    def data_source(self) -> Collection[TRAW]:
        if self._query_engine.collection is None:
            raise ValueError("Query engine has no collection.")
        return self._query_engine.collection

    @property
    def keyspace(self) -> str:
        return self.data_source.keyspace

    def clone(self) -> CollectionCursor[TRAW, TRAW]:
        """TODO. A new rewound cursor. Also: strips away any mapping."""
        if self._query_engine.collection is None:
            raise ValueError("Query engine has no collection.")
        return CollectionCursor(
            collection=self._query_engine.collection,
            request_timeout_ms=self._request_timeout_ms,
            overall_timeout_ms=self._overall_timeout_ms,
            filter=self._filter,
            projection=self._projection,
            sort=self._sort,
            limit=self._limit,
            include_similarity=self._include_similarity,
            include_sort_vector=self._include_sort_vector,
            skip=self._skip,
            mapper=None,
        )

    def filter(self, filter: FilterType | None) -> CollectionCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(filter=filter)

    def project(self, projection: ProjectionType | None) -> CollectionCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(projection=projection)

    def sort(self, sort: dict[str, Any] | None) -> CollectionCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(sort=sort)

    def limit(self, limit: int | None) -> CollectionCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(limit=limit)

    def include_similarity(
        self, include_similarity: bool | None
    ) -> CollectionCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(include_similarity=include_similarity)

    def include_sort_vector(
        self, include_sort_vector: bool | None
    ) -> CollectionCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(include_sort_vector=include_sort_vector)

    def skip(self, skip: int | None) -> CollectionCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(skip=skip)

    def map(self, mapper: Callable[[T], TNEW]) -> CollectionCursor[TRAW, TNEW]:
        # cannot happen once started consuming
        self._ensure_idle()
        if self._query_engine.collection is None:
            raise ValueError("Query engine has no collection.")
        composite_mapper: Callable[[TRAW], TNEW]
        if self._mapper is not None:

            def _composite(document: TRAW) -> TNEW:
                return mapper(self._mapper(document))

            composite_mapper = _composite
        else:
            composite_mapper = mapper
        return CollectionCursor(
            collection=self._query_engine.collection,
            request_timeout_ms=self._request_timeout_ms,
            overall_timeout_ms=self._overall_timeout_ms,
            filter=self._filter,
            projection=self._projection,
            sort=self._sort,
            limit=self._limit,
            include_similarity=self._include_similarity,
            include_sort_vector=self._include_sort_vector,
            skip=self._skip,
            mapper=composite_mapper,
        )

    def for_each(
        self,
        function: Callable[[T], Any],
        *,
        data_operation_timeout_ms: int | None = None,
    ) -> None:
        _cursor = self._copy(overall_timeout_ms=data_operation_timeout_ms)
        for document in _cursor:
            function(document)
        self.close()

    def to_list(
        self,
        *,
        data_operation_timeout_ms: int | None = None,
    ) -> list[T]:
        _cursor = self._copy(overall_timeout_ms=data_operation_timeout_ms)
        documents = [document for document in _cursor]
        self.close()
        return documents

    def has_next(self) -> bool:
        if self._state == CursorState.IDLE:
            raise CursorException(
                text="Cannot call has_next on an idle cursor.",
                cursor_state=self._state.value,
            )
        if self._state == CursorState.CLOSED:
            return False
        self._try_ensure_fill_buffer()
        return len(self._buffer) > 0

    def get_sort_vector(self) -> list[float] | None:
        """
        Return the vector used in this ANN search, if applicable.
        If this is not an ANN search, or it was invoked without the
        `include_sort_vector` parameter, return None.

        Invoking this method on a still-idle cursor will trigger an API call
        to get the first page of results.
        """

        self._try_ensure_fill_buffer()
        if self._last_response_status:
            return self._last_response_status.get("sortVector")
        else:
            return None


class AsyncCollectionCursor(Generic[TRAW, T], _BufferedCursor[TRAW]):
    _query_engine: _CollectionQueryEngine[TRAW]
    _request_timeout_ms: int | None
    _overall_timeout_ms: int | None
    _timeout_manager: MultiCallTimeoutManager
    _filter: FilterType | None
    _projection: ProjectionType | None
    _sort: dict[str, Any] | None
    _limit: int | None
    _include_similarity: bool | None
    _include_sort_vector: bool | None
    _skip: int | None
    _mapper: Callable[[TRAW], T]

    def __init__(
        self,
        *,
        collection: AsyncCollection[TRAW],
        request_timeout_ms: int | None,
        overall_timeout_ms: int | None,
        filter: FilterType | None = None,
        projection: ProjectionType | None = None,
        sort: dict[str, Any] | None = None,
        limit: int | None = None,
        include_similarity: bool | None = None,
        include_sort_vector: bool | None = None,
        skip: int | None = None,
        mapper: Callable[[TRAW], T] | None = None,
    ) -> None:
        self._filter = deepcopy(filter)
        self._projection = projection
        self._sort = sort
        self._limit = limit
        self._include_similarity = include_similarity
        self._include_sort_vector = include_sort_vector
        self._skip = skip
        if mapper is None:

            def _identity(document: TRAW) -> T:
                return cast(T, document)

            self._mapper = _identity
        else:
            self._mapper = mapper
        self._request_timeout_ms = request_timeout_ms
        self._overall_timeout_ms = overall_timeout_ms
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
        self._timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=self._overall_timeout_ms,
        )

    def _copy(
        self: AsyncCollectionCursor[TRAW, T],
        *,
        request_timeout_ms: int | None | UnsetType = _UNSET,
        overall_timeout_ms: int | None | UnsetType = _UNSET,
        filter: FilterType | None | UnsetType = _UNSET,
        projection: ProjectionType | None | UnsetType = _UNSET,
        sort: dict[str, Any] | None | UnsetType = _UNSET,
        limit: int | None | UnsetType = _UNSET,
        include_similarity: bool | None | UnsetType = _UNSET,
        include_sort_vector: bool | None | UnsetType = _UNSET,
        skip: int | None | UnsetType = _UNSET,
    ) -> AsyncCollectionCursor[TRAW, T]:
        if self._query_engine.async_collection is None:
            raise ValueError("Query engine has no async collection.")
        return AsyncCollectionCursor(
            collection=self._query_engine.async_collection,
            request_timeout_ms=self._request_timeout_ms
            if isinstance(request_timeout_ms, UnsetType)
            else request_timeout_ms,
            overall_timeout_ms=self._overall_timeout_ms
            if isinstance(overall_timeout_ms, UnsetType)
            else overall_timeout_ms,
            filter=self._filter if isinstance(filter, UnsetType) else filter,
            projection=self._projection
            if isinstance(projection, UnsetType)
            else projection,
            sort=self._sort if isinstance(sort, UnsetType) else sort,
            limit=self._limit if isinstance(limit, UnsetType) else limit,
            include_similarity=self._include_similarity
            if isinstance(include_similarity, UnsetType)
            else include_similarity,
            include_sort_vector=self._include_sort_vector
            if isinstance(include_sort_vector, UnsetType)
            else include_sort_vector,
            skip=self._skip if isinstance(skip, UnsetType) else skip,
        )

    async def _try_ensure_fill_buffer(self) -> None:
        """
        If buffer is empty, try to fill with next page, if applicable.
        If not possible, silently do nothing.
        """

        if self._state == CursorState.CLOSED:
            return
        if not self._buffer:
            if self._next_page_state is not None or self._state == CursorState.IDLE:
                (
                    new_buffer,
                    next_page_state,
                    resp_status,
                ) = await self._query_engine.async_fetch_page(
                    page_state=self._next_page_state,
                    request_timeout_ms=self._timeout_manager.remaining_timeout_ms(
                        cap_time_ms=self._request_timeout_ms,
                    ),
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

    def __aiter__(
        self: AsyncCollectionCursor[TRAW, T],
    ) -> AsyncCollectionCursor[TRAW, T]:
        self._ensure_alive()
        return self

    async def __anext__(self) -> T:
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
        return self._mapper(traw0)

    @property
    def data_source(self) -> AsyncCollection[TRAW]:
        if self._query_engine.async_collection is None:
            raise ValueError("Query engine has no async collection.")
        return self._query_engine.async_collection

    @property
    def keyspace(self) -> str:
        return self.data_source.keyspace

    def clone(self) -> AsyncCollectionCursor[TRAW, TRAW]:
        """TODO. A new rewound cursor. Also: strips away any mapping."""
        if self._query_engine.async_collection is None:
            raise ValueError("Query engine has no async collection.")
        return AsyncCollectionCursor(
            collection=self._query_engine.async_collection,
            request_timeout_ms=self._request_timeout_ms,
            overall_timeout_ms=self._overall_timeout_ms,
            filter=self._filter,
            projection=self._projection,
            sort=self._sort,
            limit=self._limit,
            include_similarity=self._include_similarity,
            include_sort_vector=self._include_sort_vector,
            skip=self._skip,
            mapper=None,
        )

    def filter(self, filter: FilterType | None) -> AsyncCollectionCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(filter=filter)

    def project(
        self, projection: ProjectionType | None
    ) -> AsyncCollectionCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(projection=projection)

    def sort(self, sort: dict[str, Any] | None) -> AsyncCollectionCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(sort=sort)

    def limit(self, limit: int | None) -> AsyncCollectionCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(limit=limit)

    def include_similarity(
        self, include_similarity: bool | None
    ) -> AsyncCollectionCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(include_similarity=include_similarity)

    def include_sort_vector(
        self, include_sort_vector: bool | None
    ) -> AsyncCollectionCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(include_sort_vector=include_sort_vector)

    def skip(self, skip: int | None) -> AsyncCollectionCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(skip=skip)

    def map(self, mapper: Callable[[T], TNEW]) -> AsyncCollectionCursor[TRAW, TNEW]:
        # cannot happen once started consuming
        self._ensure_idle()
        if self._query_engine.async_collection is None:
            raise ValueError("Query engine has no async collection.")
        composite_mapper: Callable[[TRAW], TNEW]
        if self._mapper is not None:

            def _composite(document: TRAW) -> TNEW:
                return mapper(self._mapper(document))

            composite_mapper = _composite
        else:
            composite_mapper = mapper
        return AsyncCollectionCursor(
            collection=self._query_engine.async_collection,
            request_timeout_ms=self._request_timeout_ms,
            overall_timeout_ms=self._overall_timeout_ms,
            filter=self._filter,
            projection=self._projection,
            sort=self._sort,
            limit=self._limit,
            include_similarity=self._include_similarity,
            include_sort_vector=self._include_sort_vector,
            skip=self._skip,
            mapper=composite_mapper,
        )

    async def for_each(
        self,
        function: Callable[[T], Any],
        *,
        data_operation_timeout_ms: int | None = None,
    ) -> None:
        _cursor = self._copy(overall_timeout_ms=data_operation_timeout_ms)
        async for document in _cursor:
            function(document)
        self.close()

    async def to_list(
        self,
        *,
        data_operation_timeout_ms: int | None = None,
    ) -> list[T]:
        _cursor = self._copy(overall_timeout_ms=data_operation_timeout_ms)
        documents = [document async for document in _cursor]
        self.close()
        return documents

    async def has_next(self) -> bool:
        if self._state == CursorState.IDLE:
            raise CursorException(
                text="Cannot call has_next on an idle cursor.",
                cursor_state=self._state.value,
            )
        if self._state == CursorState.CLOSED:
            return False
        await self._try_ensure_fill_buffer()
        return len(self._buffer) > 0

    async def get_sort_vector(self) -> list[float] | None:
        """
        Return the vector used in this ANN search, if applicable.
        If this is not an ANN search, or it was invoked without the
        `include_sort_vector` parameter, return None.

        Invoking this method on a still-idle cursor will trigger an API call
        to get the first page of results.
        """

        await self._try_ensure_fill_buffer()
        if self._last_response_status:
            return self._last_response_status.get("sortVector")
        else:
            return None


class TableCursor(Generic[TRAW, T], _BufferedCursor[TRAW]):
    _query_engine: _TableQueryEngine[TRAW]
    _request_timeout_ms: int | None | None
    _overall_timeout_ms: int | None
    _timeout_manager: MultiCallTimeoutManager
    _filter: FilterType | None
    _projection: ProjectionType | None
    _sort: dict[str, Any] | None
    _limit: int | None
    _include_similarity: bool | None
    _include_sort_vector: bool | None
    _skip: int | None
    _mapper: Callable[[TRAW], T]

    def __init__(
        self,
        *,
        table: Table[TRAW],
        request_timeout_ms: int | None,
        overall_timeout_ms: int | None,
        filter: FilterType | None = None,
        projection: ProjectionType | None = None,
        sort: dict[str, Any] | None = None,
        limit: int | None = None,
        include_similarity: bool | None = None,
        include_sort_vector: bool | None = None,
        skip: int | None = None,
        mapper: Callable[[TRAW], T] | None = None,
    ) -> None:
        self._filter = deepcopy(filter)
        self._projection = projection
        self._sort = sort
        self._limit = limit
        self._include_similarity = include_similarity
        self._include_sort_vector = include_sort_vector
        self._skip = skip
        if mapper is None:

            def _identity(document: TRAW) -> T:
                return cast(T, document)

            self._mapper = _identity
        else:
            self._mapper = mapper
        self._request_timeout_ms = request_timeout_ms
        self._overall_timeout_ms = overall_timeout_ms
        self._query_engine = _TableQueryEngine(
            table=table,
            async_table=None,
            filter=self._filter,
            projection=self._projection,
            sort=self._sort,
            limit=self._limit,
            include_similarity=self._include_similarity,
            include_sort_vector=self._include_sort_vector,
            skip=self._skip,
        )
        _BufferedCursor.__init__(self)
        self._timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=self._overall_timeout_ms,
        )

    def _copy(
        self: TableCursor[TRAW, T],
        *,
        request_timeout_ms: int | None | UnsetType = _UNSET,
        overall_timeout_ms: int | None | UnsetType = _UNSET,
        filter: FilterType | None | UnsetType = _UNSET,
        projection: ProjectionType | None | UnsetType = _UNSET,
        sort: dict[str, Any] | None | UnsetType = _UNSET,
        limit: int | None | UnsetType = _UNSET,
        include_similarity: bool | None | UnsetType = _UNSET,
        include_sort_vector: bool | None | UnsetType = _UNSET,
        skip: int | None | UnsetType = _UNSET,
    ) -> TableCursor[TRAW, T]:
        if self._query_engine.table is None:
            raise ValueError("Query engine has no table.")
        return TableCursor(
            table=self._query_engine.table,
            request_timeout_ms=self._request_timeout_ms
            if isinstance(request_timeout_ms, UnsetType)
            else request_timeout_ms,
            overall_timeout_ms=self._overall_timeout_ms
            if isinstance(overall_timeout_ms, UnsetType)
            else overall_timeout_ms,
            filter=self._filter if isinstance(filter, UnsetType) else filter,
            projection=self._projection
            if isinstance(projection, UnsetType)
            else projection,
            sort=self._sort if isinstance(sort, UnsetType) else sort,
            limit=self._limit if isinstance(limit, UnsetType) else limit,
            include_similarity=self._include_similarity
            if isinstance(include_similarity, UnsetType)
            else include_similarity,
            include_sort_vector=self._include_sort_vector
            if isinstance(include_sort_vector, UnsetType)
            else include_sort_vector,
            skip=self._skip if isinstance(skip, UnsetType) else skip,
            mapper=self._mapper,
        )

    def _try_ensure_fill_buffer(self) -> None:
        """
        If buffer is empty, try to fill with next page, if applicable.
        If not possible, silently do nothing.
        """

        if self._state == CursorState.CLOSED:
            return
        if not self._buffer:
            if self._next_page_state is not None or self._state == CursorState.IDLE:
                new_buffer, next_page_state, resp_status = (
                    self._query_engine.fetch_page(
                        page_state=self._next_page_state,
                        request_timeout_ms=self._timeout_manager.remaining_timeout_ms(
                            cap_time_ms=self._request_timeout_ms,
                        ),
                    )
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

    def __iter__(self: TableCursor[TRAW, T]) -> TableCursor[TRAW, T]:
        self._ensure_alive()
        return self

    def __next__(self) -> T:
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
        return self._mapper(traw0)

    @property
    def data_source(self) -> Table[TRAW]:
        if self._query_engine.table is None:
            raise ValueError("Query engine has no table.")
        return self._query_engine.table

    @property
    def keyspace(self) -> str:
        return self.data_source.keyspace

    def clone(self) -> TableCursor[TRAW, TRAW]:
        """TODO. A new rewound cursor. Also: strips away any mapping."""
        if self._query_engine.table is None:
            raise ValueError("Query engine has no table.")
        return TableCursor(
            table=self._query_engine.table,
            request_timeout_ms=self._request_timeout_ms,
            overall_timeout_ms=self._overall_timeout_ms,
            filter=self._filter,
            projection=self._projection,
            sort=self._sort,
            limit=self._limit,
            include_similarity=self._include_similarity,
            include_sort_vector=self._include_sort_vector,
            skip=self._skip,
            mapper=None,
        )

    def filter(self, filter: FilterType | None) -> TableCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(filter=filter)

    def project(self, projection: ProjectionType | None) -> TableCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(projection=projection)

    def sort(self, sort: dict[str, Any] | None) -> TableCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(sort=sort)

    def limit(self, limit: int | None) -> TableCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(limit=limit)

    def include_similarity(
        self, include_similarity: bool | None
    ) -> TableCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(include_similarity=include_similarity)

    def include_sort_vector(
        self, include_sort_vector: bool | None
    ) -> TableCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(include_sort_vector=include_sort_vector)

    def skip(self, skip: int | None) -> TableCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(skip=skip)

    def map(self, mapper: Callable[[T], TNEW]) -> TableCursor[TRAW, TNEW]:
        # cannot happen once started consuming
        self._ensure_idle()
        if self._query_engine.table is None:
            raise ValueError("Query engine has no table.")
        composite_mapper: Callable[[TRAW], TNEW]
        if self._mapper is not None:

            def _composite(document: TRAW) -> TNEW:
                return mapper(self._mapper(document))

            composite_mapper = _composite
        else:
            composite_mapper = mapper
        return TableCursor(
            table=self._query_engine.table,
            request_timeout_ms=self._request_timeout_ms,
            overall_timeout_ms=self._overall_timeout_ms,
            filter=self._filter,
            projection=self._projection,
            sort=self._sort,
            limit=self._limit,
            include_similarity=self._include_similarity,
            include_sort_vector=self._include_sort_vector,
            skip=self._skip,
            mapper=composite_mapper,
        )

    def for_each(
        self,
        function: Callable[[T], Any],
        *,
        data_operation_timeout_ms: int | None = None,
    ) -> None:
        _cursor = self._copy(overall_timeout_ms=data_operation_timeout_ms)
        for document in _cursor:
            function(document)
        self.close()

    def to_list(
        self,
        *,
        data_operation_timeout_ms: int | None = None,
    ) -> list[T]:
        _cursor = self._copy(overall_timeout_ms=data_operation_timeout_ms)
        documents = [document for document in _cursor]
        self.close()
        return documents

    def has_next(self) -> bool:
        if self._state == CursorState.IDLE:
            raise CursorException(
                text="Cannot call has_next on an idle cursor.",
                cursor_state=self._state.value,
            )
        if self._state == CursorState.CLOSED:
            return False
        self._try_ensure_fill_buffer()
        return len(self._buffer) > 0

    def get_sort_vector(self) -> list[float] | None:
        """
        Return the vector used in this ANN search, if applicable.
        If this is not an ANN search, or it was invoked without the
        `include_sort_vector` parameter, return None.

        Invoking this method on a still-idle cursor will trigger an API call
        to get the first page of results.
        """

        self._try_ensure_fill_buffer()
        if self._last_response_status:
            return self._last_response_status.get("sortVector")
        else:
            return None


class AsyncTableCursor(Generic[TRAW, T], _BufferedCursor[TRAW]):
    _query_engine: _TableQueryEngine[TRAW]
    _request_timeout_ms: int | None
    _overall_timeout_ms: int | None
    _timeout_manager: MultiCallTimeoutManager
    _filter: FilterType | None
    _projection: ProjectionType | None
    _sort: dict[str, Any] | None
    _limit: int | None
    _include_similarity: bool | None
    _include_sort_vector: bool | None
    _skip: int | None
    _mapper: Callable[[TRAW], T]

    def __init__(
        self,
        *,
        table: AsyncTable[TRAW],
        request_timeout_ms: int | None,
        overall_timeout_ms: int | None,
        filter: FilterType | None = None,
        projection: ProjectionType | None = None,
        sort: dict[str, Any] | None = None,
        limit: int | None = None,
        include_similarity: bool | None = None,
        include_sort_vector: bool | None = None,
        skip: int | None = None,
        mapper: Callable[[TRAW], T] | None = None,
    ) -> None:
        self._filter = deepcopy(filter)
        self._projection = projection
        self._sort = sort
        self._limit = limit
        self._include_similarity = include_similarity
        self._include_sort_vector = include_sort_vector
        self._skip = skip
        if mapper is None:

            def _identity(document: TRAW) -> T:
                return cast(T, document)

            self._mapper = _identity
        else:
            self._mapper = mapper
        self._request_timeout_ms = request_timeout_ms
        self._overall_timeout_ms = overall_timeout_ms
        self._query_engine = _TableQueryEngine(
            table=None,
            async_table=table,
            filter=self._filter,
            projection=self._projection,
            sort=self._sort,
            limit=self._limit,
            include_similarity=self._include_similarity,
            include_sort_vector=self._include_sort_vector,
            skip=self._skip,
        )
        _BufferedCursor.__init__(self)
        self._timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=self._overall_timeout_ms,
        )

    def _copy(
        self: AsyncTableCursor[TRAW, T],
        *,
        request_timeout_ms: int | None | UnsetType = _UNSET,
        overall_timeout_ms: int | None | UnsetType = _UNSET,
        filter: FilterType | None | UnsetType = _UNSET,
        projection: ProjectionType | None | UnsetType = _UNSET,
        sort: dict[str, Any] | None | UnsetType = _UNSET,
        limit: int | None | UnsetType = _UNSET,
        include_similarity: bool | None | UnsetType = _UNSET,
        include_sort_vector: bool | None | UnsetType = _UNSET,
        skip: int | None | UnsetType = _UNSET,
    ) -> AsyncTableCursor[TRAW, T]:
        if self._query_engine.async_table is None:
            raise ValueError("Query engine has no async table.")
        return AsyncTableCursor(
            table=self._query_engine.async_table,
            request_timeout_ms=self._request_timeout_ms
            if isinstance(request_timeout_ms, UnsetType)
            else request_timeout_ms,
            overall_timeout_ms=self._overall_timeout_ms
            if isinstance(overall_timeout_ms, UnsetType)
            else overall_timeout_ms,
            filter=self._filter if isinstance(filter, UnsetType) else filter,
            projection=self._projection
            if isinstance(projection, UnsetType)
            else projection,
            sort=self._sort if isinstance(sort, UnsetType) else sort,
            limit=self._limit if isinstance(limit, UnsetType) else limit,
            include_similarity=self._include_similarity
            if isinstance(include_similarity, UnsetType)
            else include_similarity,
            include_sort_vector=self._include_sort_vector
            if isinstance(include_sort_vector, UnsetType)
            else include_sort_vector,
            skip=self._skip if isinstance(skip, UnsetType) else skip,
        )

    async def _try_ensure_fill_buffer(self) -> None:
        """
        If buffer is empty, try to fill with next page, if applicable.
        If not possible, silently do nothing.
        """

        if self._state == CursorState.CLOSED:
            return
        if not self._buffer:
            if self._next_page_state is not None or self._state == CursorState.IDLE:
                (
                    new_buffer,
                    next_page_state,
                    resp_status,
                ) = await self._query_engine.async_fetch_page(
                    page_state=self._next_page_state,
                    request_timeout_ms=self._timeout_manager.remaining_timeout_ms(
                        cap_time_ms=self._request_timeout_ms,
                    ),
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

    def __aiter__(
        self: AsyncTableCursor[TRAW, T],
    ) -> AsyncTableCursor[TRAW, T]:
        self._ensure_alive()
        return self

    async def __anext__(self) -> T:
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
        return self._mapper(traw0)

    @property
    def data_source(self) -> AsyncTable[TRAW]:
        if self._query_engine.async_table is None:
            raise ValueError("Query engine has no async table.")
        return self._query_engine.async_table

    @property
    def keyspace(self) -> str:
        return self.data_source.keyspace

    def clone(self) -> AsyncTableCursor[TRAW, TRAW]:
        """TODO. A new rewound cursor. Also: strips away any mapping."""
        if self._query_engine.async_table is None:
            raise ValueError("Query engine has no async table.")
        return AsyncTableCursor(
            table=self._query_engine.async_table,
            request_timeout_ms=self._request_timeout_ms,
            overall_timeout_ms=self._overall_timeout_ms,
            filter=self._filter,
            projection=self._projection,
            sort=self._sort,
            limit=self._limit,
            include_similarity=self._include_similarity,
            include_sort_vector=self._include_sort_vector,
            skip=self._skip,
            mapper=None,
        )

    def filter(self, filter: FilterType | None) -> AsyncTableCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(filter=filter)

    def project(self, projection: ProjectionType | None) -> AsyncTableCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(projection=projection)

    def sort(self, sort: dict[str, Any] | None) -> AsyncTableCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(sort=sort)

    def limit(self, limit: int | None) -> AsyncTableCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(limit=limit)

    def include_similarity(
        self, include_similarity: bool | None
    ) -> AsyncTableCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(include_similarity=include_similarity)

    def include_sort_vector(
        self, include_sort_vector: bool | None
    ) -> AsyncTableCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(include_sort_vector=include_sort_vector)

    def skip(self, skip: int | None) -> AsyncTableCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(skip=skip)

    def map(self, mapper: Callable[[T], TNEW]) -> AsyncTableCursor[TRAW, TNEW]:
        # cannot happen once started consuming
        self._ensure_idle()
        if self._query_engine.async_table is None:
            raise ValueError("Query engine has no async table.")
        composite_mapper: Callable[[TRAW], TNEW]
        if self._mapper is not None:

            def _composite(document: TRAW) -> TNEW:
                return mapper(self._mapper(document))

            composite_mapper = _composite
        else:
            composite_mapper = mapper
        return AsyncTableCursor(
            table=self._query_engine.async_table,
            request_timeout_ms=self._request_timeout_ms,
            overall_timeout_ms=self._overall_timeout_ms,
            filter=self._filter,
            projection=self._projection,
            sort=self._sort,
            limit=self._limit,
            include_similarity=self._include_similarity,
            include_sort_vector=self._include_sort_vector,
            skip=self._skip,
            mapper=composite_mapper,
        )

    async def for_each(
        self,
        function: Callable[[T], Any],
        *,
        data_operation_timeout_ms: int | None = None,
    ) -> None:
        _cursor = self._copy(overall_timeout_ms=data_operation_timeout_ms)
        async for document in _cursor:
            function(document)
        self.close()

    async def to_list(
        self,
        *,
        data_operation_timeout_ms: int | None = None,
    ) -> list[T]:
        _cursor = self._copy(overall_timeout_ms=data_operation_timeout_ms)
        documents = [document async for document in _cursor]
        self.close()
        return documents

    async def has_next(self) -> bool:
        if self._state == CursorState.IDLE:
            raise CursorException(
                text="Cannot call has_next on an idle cursor.",
                cursor_state=self._state.value,
            )
        if self._state == CursorState.CLOSED:
            return False
        await self._try_ensure_fill_buffer()
        return len(self._buffer) > 0

    async def get_sort_vector(self) -> list[float] | None:
        """
        Return the vector used in this ANN search, if applicable.
        If this is not an ANN search, or it was invoked without the
        `include_sort_vector` parameter, return None.

        Invoking this method on a still-idle cursor will trigger an API call
        to get the first page of results.
        """

        await self._try_ensure_fill_buffer()
        if self._last_response_status:
            return self._last_response_status.get("sortVector")
        else:
            return None