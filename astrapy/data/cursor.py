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
from abc import ABC, abstractmethod
from copy import deepcopy
from decimal import Decimal
from enum import Enum
from typing import Any, Callable, Generic, TypeVar, cast

from typing_extensions import override

from astrapy import AsyncCollection, AsyncTable, Collection, Table
from astrapy.constants import (
    FilterType,
    ProjectionType,
    normalize_optional_projection,
)
from astrapy.data.utils.collection_converters import (
    postprocess_collection_response,
    preprocess_collection_payload,
)
from astrapy.data_types import DataAPIVector
from astrapy.exceptions import (
    CursorException,
    MultiCallTimeoutManager,
    UnexpectedDataAPIResponseException,
    _TimeoutContext,
)
from astrapy.utils.api_options import FullSerdesOptions
from astrapy.utils.unset import _UNSET, UnsetType

# A cursor reads TRAW from DB and maps them to T if any mapping.
# A new cursor returned by .map will map to TNEW
TRAW = TypeVar("TRAW")
T = TypeVar("T")
TNEW = TypeVar("TNEW")


logger = logging.getLogger(__name__)


def _ensure_vector(
    fvector: list[float | Decimal] | None,
    options: FullSerdesOptions,
) -> list[float] | DataAPIVector | None:
    """
    For Tables and - depending on the JSON response parsing - collections alike,
    the sort vector included in the response from a find can be made into a list
    of Decimal instances. This utility makes it back to either a plain list of floats
    or a DataAPIVector, according the the preferences for the table/collection being
    queried.
    """
    if fvector is None:
        return None
    else:
        # this can be a list of Decimal instances (because it's from tables,
        # or from collections set to use decimals).
        f_list = [float(x) for x in fvector]
        if options.custom_datatypes_in_reading:
            return DataAPIVector(f_list)
        else:
            return f_list


def _revise_timeouts_for_cursor_copy(
    *,
    new_general_method_timeout_ms: int | None,
    new_timeout_ms: int | None,
    old_request_timeout_ms: int | None,
) -> tuple[int | None, int | None]:
    """
    This utility applies the desired logic to get the new timeout specification
    for a cursor copy done for the purpose of to_list or for_each.

    Namely, the original cursor would have an old request timeout (its overall
    timeout assumed empty); and the to_list method call may have a general timeout
    specified (and/or its alias, timeout_ms). This function returns the
        (request_timeout_ms, overall_timeout_ms)
    for use in the cursor copy.
    (1) the two new_* parameters put in their priority for the new 'overall"
    (2) the new per-request is either the old per-request or, if (1) is shorter,
        that takes precedence. This is done in a None-aware safe manner.
    """
    _general_method_timeout_ms = (
        new_timeout_ms if new_timeout_ms is not None else new_general_method_timeout_ms
    )
    # the per-request timeout, depending on what is specified, may have
    # to undergo a min(...) logic if overall timeout is surprisingly narrow:
    _new_request_timeout_ms: int | None
    if _general_method_timeout_ms is not None:
        if old_request_timeout_ms is not None:
            _new_request_timeout_ms = min(
                _general_method_timeout_ms,
                old_request_timeout_ms,
            )
        else:
            _new_request_timeout_ms = _general_method_timeout_ms
    else:
        if old_request_timeout_ms is not None:
            _new_request_timeout_ms = old_request_timeout_ms
        else:
            _new_request_timeout_ms = None
    return (_new_request_timeout_ms, _general_method_timeout_ms)


class CursorState(Enum):
    # Iteration over results has not started yet (alive=T, started=F)
    IDLE = "idle"
    # Iteration has started, *can* still yield results (alive=T, started=T)
    STARTED = "started"
    # Finished/forcibly stopped. Won't return more documents (alive=F)
    CLOSED = "closed"


class FindCursor(Generic[TRAW]):
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
        timeout_context: _TimeoutContext,
    ) -> tuple[list[TRAW], str | None, dict[str, Any] | None]:
        """Run a query for one page and return (entries, next-page-state, response.status)."""
        ...

    @abstractmethod
    async def async_fetch_page(
        self,
        *,
        page_state: str | None,
        timeout_context: _TimeoutContext,
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
        timeout_context: _TimeoutContext,
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
        converted_f_payload = preprocess_collection_payload(
            f_payload, options=self.collection.api_options.serdes_options
        )

        _page_str = page_state if page_state else "(empty page state)"
        _coll_name = self.collection.name if self.collection else "(none)"
        logger.info(f"cursor fetching a page: {_page_str} from {_coll_name}")
        raw_f_response = self.collection._api_commander.request(
            payload=converted_f_payload,
            timeout_context=timeout_context,
        )
        logger.info(f"cursor finished fetching a page: {_page_str} from {_coll_name}")

        f_response = postprocess_collection_response(
            raw_f_response, options=self.collection.api_options.serdes_options
        )
        if "documents" not in f_response.get("data", {}):
            raise UnexpectedDataAPIResponseException(
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
        timeout_context: _TimeoutContext,
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
        converted_f_payload = preprocess_collection_payload(
            f_payload, options=self.async_collection.api_options.serdes_options
        )

        _page_str = page_state if page_state else "(empty page state)"
        _coll_name = self.async_collection.name if self.async_collection else "(none)"
        logger.info(f"cursor fetching a page: {_page_str} from {_coll_name}, async")
        raw_f_response = await self.async_collection._api_commander.async_request(
            payload=converted_f_payload,
            timeout_context=timeout_context,
        )
        logger.info(
            f"cursor finished fetching a page: {_page_str} from {_coll_name}, async"
        )

        f_response = postprocess_collection_response(
            raw_f_response,
            options=self.async_collection.api_options.serdes_options,
        )
        if "documents" not in f_response.get("data", {}):
            raise UnexpectedDataAPIResponseException(
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
    include_similarity: bool | None
    include_sort_vector: bool | None
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
        self.include_similarity = include_similarity
        self.include_sort_vector = include_sort_vector
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
        timeout_context: _TimeoutContext,
    ) -> tuple[list[TRAW], str | None, dict[str, Any] | None]:
        if self.table is None:
            raise ValueError("Query engine has no sync table.")
        f_payload = self.table._converter_agent.preprocess_payload(
            {
                "find": {
                    **self.f_r_subpayload,
                    "options": {
                        **self.f_options0,
                        **({"pageState": page_state} if page_state else {}),
                    },
                },
            }
        )

        _page_str = page_state if page_state else "(empty page state)"
        _table_name = self.table.name if self.table else "(none)"
        logger.info(f"cursor fetching a page: {_page_str} from {_table_name}")
        f_response = self.table._api_commander.request(
            payload=f_payload,
            timeout_context=timeout_context,
        )
        logger.info(f"cursor finished fetching a page: {_page_str} from {_table_name}")

        if "documents" not in f_response.get("data", {}):
            raise UnexpectedDataAPIResponseException(
                text="Response from find API command missing 'documents'.",
                raw_response=f_response,
            )
        if "projectionSchema" not in f_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Response from find API command missing 'projectionSchema'.",
                raw_response=f_response,
            )
        p_documents = self.table._converter_agent.postprocess_rows(
            f_response["data"]["documents"],
            columns_dict=f_response["status"]["projectionSchema"],
            similarity_pseudocolumn="$similarity" if self.include_similarity else None,
        )
        n_p_state = f_response["data"]["nextPageState"]
        p_r_status = f_response.get("status")
        return (p_documents, n_p_state, p_r_status)

    @override
    async def async_fetch_page(
        self,
        *,
        page_state: str | None,
        timeout_context: _TimeoutContext,
    ) -> tuple[list[TRAW], str | None, dict[str, Any] | None]:
        if self.async_table is None:
            raise ValueError("Query engine has no async table.")
        f_payload = self.async_table._converter_agent.preprocess_payload(
            {
                "find": {
                    **self.f_r_subpayload,
                    "options": {
                        **self.f_options0,
                        **({"pageState": page_state} if page_state else {}),
                    },
                },
            }
        )

        _page_str = page_state if page_state else "(empty page state)"
        _table_name = self.async_table.name if self.async_table else "(none)"
        logger.info(f"cursor fetching a page: {_page_str} from {_table_name}")
        f_response = await self.async_table._api_commander.async_request(
            payload=f_payload,
            timeout_context=timeout_context,
        )
        logger.info(f"cursor finished fetching a page: {_page_str} from {_table_name}")

        if "documents" not in f_response.get("data", {}):
            raise UnexpectedDataAPIResponseException(
                text="Response from find API command missing 'documents'.",
                raw_response=f_response,
            )
        if "projectionSchema" not in f_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Response from find API command missing 'projectionSchema'.",
                raw_response=f_response,
            )
        p_documents = self.async_table._converter_agent.postprocess_rows(
            f_response["data"]["documents"],
            columns_dict=f_response["status"]["projectionSchema"],
            similarity_pseudocolumn="$similarity" if self.include_similarity else None,
        )
        n_p_state = f_response["data"]["nextPageState"]
        p_r_status = f_response.get("status")
        return (p_documents, n_p_state, p_r_status)


class CollectionFindCursor(Generic[TRAW, T], FindCursor[TRAW]):
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
    _mapper: Callable[[TRAW], T] | None

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
        FindCursor.__init__(self)
        self._timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=self._overall_timeout_ms,
        )

    def _copy(
        self: CollectionFindCursor[TRAW, T],
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
    ) -> CollectionFindCursor[TRAW, T]:
        if self._query_engine.collection is None:
            raise ValueError("Query engine has no collection.")
        return CollectionFindCursor(
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
                        timeout_context=self._timeout_manager.remaining_timeout(
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

    def __iter__(self: CollectionFindCursor[TRAW, T]) -> CollectionFindCursor[TRAW, T]:
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
        return cast(T, self._mapper(traw0) if self._mapper is not None else traw0)

    @property
    def data_source(self) -> Collection[TRAW]:
        if self._query_engine.collection is None:
            raise ValueError("Query engine has no collection.")
        return self._query_engine.collection

    def clone(self) -> CollectionFindCursor[TRAW, TRAW]:
        """TODO. A new rewound cursor. Also: strips away any mapping."""
        if self._query_engine.collection is None:
            raise ValueError("Query engine has no collection.")
        return CollectionFindCursor(
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

    def filter(self, filter: FilterType | None) -> CollectionFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(filter=filter)

    def project(
        self, projection: ProjectionType | None
    ) -> CollectionFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        if self._mapper is not None:
            raise CursorException(
                "Cannot set projection after map.",
                cursor_state=self._state.value,
            )
        return self._copy(projection=projection)

    def sort(self, sort: dict[str, Any] | None) -> CollectionFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(sort=sort)

    def limit(self, limit: int | None) -> CollectionFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(limit=limit)

    def include_similarity(
        self, include_similarity: bool | None
    ) -> CollectionFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(include_similarity=include_similarity)

    def include_sort_vector(
        self, include_sort_vector: bool | None
    ) -> CollectionFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(include_sort_vector=include_sort_vector)

    def skip(self, skip: int | None) -> CollectionFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(skip=skip)

    def map(self, mapper: Callable[[T], TNEW]) -> CollectionFindCursor[TRAW, TNEW]:
        # cannot happen once started consuming
        self._ensure_idle()
        if self._query_engine.collection is None:
            raise ValueError("Query engine has no collection.")
        composite_mapper: Callable[[TRAW], TNEW]
        if self._mapper is not None:

            def _composite(document: TRAW) -> TNEW:
                return mapper(self._mapper(document))  # type: ignore[misc]

            composite_mapper = _composite
        else:
            composite_mapper = cast(Callable[[TRAW], TNEW], mapper)
        return CollectionFindCursor(
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
        general_method_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        self._ensure_alive()
        copy_req_ms, copy_ovr_ms = _revise_timeouts_for_cursor_copy(
            new_general_method_timeout_ms=general_method_timeout_ms,
            new_timeout_ms=timeout_ms,
            old_request_timeout_ms=self._request_timeout_ms,
        )
        _cursor = self._copy(
            request_timeout_ms=copy_req_ms,
            overall_timeout_ms=copy_ovr_ms,
        )
        for document in _cursor:
            function(document)
        self.close()

    def to_list(
        self,
        *,
        general_method_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[T]:
        self._ensure_alive()
        copy_req_ms, copy_ovr_ms = _revise_timeouts_for_cursor_copy(
            new_general_method_timeout_ms=general_method_timeout_ms,
            new_timeout_ms=timeout_ms,
            old_request_timeout_ms=self._request_timeout_ms,
        )
        _cursor = self._copy(
            request_timeout_ms=copy_req_ms,
            overall_timeout_ms=copy_ovr_ms,
        )
        documents = [document for document in _cursor]
        self.close()
        return documents

    def has_next(self) -> bool:
        if self._state == CursorState.CLOSED:
            return False
        self._try_ensure_fill_buffer()
        return len(self._buffer) > 0

    def get_sort_vector(self) -> list[float] | DataAPIVector | None:
        """
        Return the vector used in this ANN search, if applicable.
        If this is not an ANN search, or it was invoked without the
        `include_sort_vector` parameter, return None.

        Invoking this method on a still-idle cursor will trigger an API call
        to get the first page of results.
        """

        self._try_ensure_fill_buffer()
        if self._last_response_status:
            return _ensure_vector(
                self._last_response_status.get("sortVector"),
                self.data_source.api_options.serdes_options,
            )
        else:
            return None


class AsyncCollectionFindCursor(Generic[TRAW, T], FindCursor[TRAW]):
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
    _mapper: Callable[[TRAW], T] | None

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
        FindCursor.__init__(self)
        self._timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=self._overall_timeout_ms,
        )

    def _copy(
        self: AsyncCollectionFindCursor[TRAW, T],
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
    ) -> AsyncCollectionFindCursor[TRAW, T]:
        if self._query_engine.async_collection is None:
            raise ValueError("Query engine has no async collection.")
        return AsyncCollectionFindCursor(
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
                    timeout_context=self._timeout_manager.remaining_timeout(
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
        self: AsyncCollectionFindCursor[TRAW, T],
    ) -> AsyncCollectionFindCursor[TRAW, T]:
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
        return cast(T, self._mapper(traw0) if self._mapper is not None else traw0)

    @property
    def data_source(self) -> AsyncCollection[TRAW]:
        if self._query_engine.async_collection is None:
            raise ValueError("Query engine has no async collection.")
        return self._query_engine.async_collection

    def clone(self) -> AsyncCollectionFindCursor[TRAW, TRAW]:
        """TODO. A new rewound cursor. Also: strips away any mapping."""
        if self._query_engine.async_collection is None:
            raise ValueError("Query engine has no async collection.")
        return AsyncCollectionFindCursor(
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

    def filter(self, filter: FilterType | None) -> AsyncCollectionFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(filter=filter)

    def project(
        self, projection: ProjectionType | None
    ) -> AsyncCollectionFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        if self._mapper is not None:
            raise CursorException(
                "Cannot set projection after map.",
                cursor_state=self._state.value,
            )
        return self._copy(projection=projection)

    def sort(self, sort: dict[str, Any] | None) -> AsyncCollectionFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(sort=sort)

    def limit(self, limit: int | None) -> AsyncCollectionFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(limit=limit)

    def include_similarity(
        self, include_similarity: bool | None
    ) -> AsyncCollectionFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(include_similarity=include_similarity)

    def include_sort_vector(
        self, include_sort_vector: bool | None
    ) -> AsyncCollectionFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(include_sort_vector=include_sort_vector)

    def skip(self, skip: int | None) -> AsyncCollectionFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(skip=skip)

    def map(self, mapper: Callable[[T], TNEW]) -> AsyncCollectionFindCursor[TRAW, TNEW]:
        # cannot happen once started consuming
        self._ensure_idle()
        if self._query_engine.async_collection is None:
            raise ValueError("Query engine has no async collection.")
        composite_mapper: Callable[[TRAW], TNEW]
        if self._mapper is not None:

            def _composite(document: TRAW) -> TNEW:
                return mapper(self._mapper(document))  # type: ignore[misc]

            composite_mapper = _composite
        else:
            composite_mapper = cast(Callable[[TRAW], TNEW], mapper)
        return AsyncCollectionFindCursor(
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
        general_method_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        self._ensure_alive()
        copy_req_ms, copy_ovr_ms = _revise_timeouts_for_cursor_copy(
            new_general_method_timeout_ms=general_method_timeout_ms,
            new_timeout_ms=timeout_ms,
            old_request_timeout_ms=self._request_timeout_ms,
        )
        _cursor = self._copy(
            request_timeout_ms=copy_req_ms,
            overall_timeout_ms=copy_ovr_ms,
        )
        async for document in _cursor:
            function(document)
        self.close()

    async def to_list(
        self,
        *,
        general_method_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[T]:
        self._ensure_alive()
        copy_req_ms, copy_ovr_ms = _revise_timeouts_for_cursor_copy(
            new_general_method_timeout_ms=general_method_timeout_ms,
            new_timeout_ms=timeout_ms,
            old_request_timeout_ms=self._request_timeout_ms,
        )
        _cursor = self._copy(
            request_timeout_ms=copy_req_ms,
            overall_timeout_ms=copy_ovr_ms,
        )
        documents = [document async for document in _cursor]
        self.close()
        return documents

    async def has_next(self) -> bool:
        if self._state == CursorState.CLOSED:
            return False
        await self._try_ensure_fill_buffer()
        return len(self._buffer) > 0

    async def get_sort_vector(self) -> list[float] | DataAPIVector | None:
        """
        Return the vector used in this ANN search, if applicable.
        If this is not an ANN search, or it was invoked without the
        `include_sort_vector` parameter, return None.

        Invoking this method on a still-idle cursor will trigger an API call
        to get the first page of results.
        """

        await self._try_ensure_fill_buffer()
        if self._last_response_status:
            return _ensure_vector(
                self._last_response_status.get("sortVector"),
                self.data_source.api_options.serdes_options,
            )
        else:
            return None


class TableFindCursor(Generic[TRAW, T], FindCursor[TRAW]):
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
    _mapper: Callable[[TRAW], T] | None

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
        FindCursor.__init__(self)
        self._timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=self._overall_timeout_ms,
        )

    def _copy(
        self: TableFindCursor[TRAW, T],
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
    ) -> TableFindCursor[TRAW, T]:
        if self._query_engine.table is None:
            raise ValueError("Query engine has no table.")
        return TableFindCursor(
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
                        timeout_context=self._timeout_manager.remaining_timeout(
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

    def __iter__(self: TableFindCursor[TRAW, T]) -> TableFindCursor[TRAW, T]:
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
        return cast(T, self._mapper(traw0) if self._mapper is not None else traw0)

    @property
    def data_source(self) -> Table[TRAW]:
        if self._query_engine.table is None:
            raise ValueError("Query engine has no table.")
        return self._query_engine.table

    def clone(self) -> TableFindCursor[TRAW, TRAW]:
        """TODO. A new rewound cursor. Also: strips away any mapping."""
        if self._query_engine.table is None:
            raise ValueError("Query engine has no table.")
        return TableFindCursor(
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

    def filter(self, filter: FilterType | None) -> TableFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(filter=filter)

    def project(self, projection: ProjectionType | None) -> TableFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        if self._mapper is not None:
            raise CursorException(
                "Cannot set projection after map.",
                cursor_state=self._state.value,
            )
        return self._copy(projection=projection)

    def sort(self, sort: dict[str, Any] | None) -> TableFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(sort=sort)

    def limit(self, limit: int | None) -> TableFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(limit=limit)

    def include_similarity(
        self, include_similarity: bool | None
    ) -> TableFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(include_similarity=include_similarity)

    def include_sort_vector(
        self, include_sort_vector: bool | None
    ) -> TableFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(include_sort_vector=include_sort_vector)

    def skip(self, skip: int | None) -> TableFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(skip=skip)

    def map(self, mapper: Callable[[T], TNEW]) -> TableFindCursor[TRAW, TNEW]:
        # cannot happen once started consuming
        self._ensure_idle()
        if self._query_engine.table is None:
            raise ValueError("Query engine has no table.")
        composite_mapper: Callable[[TRAW], TNEW]
        if self._mapper is not None:

            def _composite(document: TRAW) -> TNEW:
                return mapper(self._mapper(document))  # type: ignore[misc]

            composite_mapper = _composite
        else:
            composite_mapper = cast(Callable[[TRAW], TNEW], mapper)
        return TableFindCursor(
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
        general_method_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        self._ensure_alive()
        copy_req_ms, copy_ovr_ms = _revise_timeouts_for_cursor_copy(
            new_general_method_timeout_ms=general_method_timeout_ms,
            new_timeout_ms=timeout_ms,
            old_request_timeout_ms=self._request_timeout_ms,
        )
        _cursor = self._copy(
            request_timeout_ms=copy_req_ms,
            overall_timeout_ms=copy_ovr_ms,
        )
        for document in _cursor:
            function(document)
        self.close()

    def to_list(
        self,
        *,
        general_method_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[T]:
        self._ensure_alive()
        copy_req_ms, copy_ovr_ms = _revise_timeouts_for_cursor_copy(
            new_general_method_timeout_ms=general_method_timeout_ms,
            new_timeout_ms=timeout_ms,
            old_request_timeout_ms=self._request_timeout_ms,
        )
        _cursor = self._copy(
            request_timeout_ms=copy_req_ms,
            overall_timeout_ms=copy_ovr_ms,
        )
        documents = [document for document in _cursor]
        self.close()
        return documents

    def has_next(self) -> bool:
        if self._state == CursorState.CLOSED:
            return False
        self._try_ensure_fill_buffer()
        return len(self._buffer) > 0

    def get_sort_vector(self) -> list[float] | DataAPIVector | None:
        """
        Return the vector used in this ANN search, if applicable.
        If this is not an ANN search, or it was invoked without the
        `include_sort_vector` parameter, return None.

        Invoking this method on a still-idle cursor will trigger an API call
        to get the first page of results.
        """

        self._try_ensure_fill_buffer()
        if self._last_response_status:
            return _ensure_vector(
                self._last_response_status.get("sortVector"),
                self.data_source.api_options.serdes_options,
            )
        else:
            return None


class AsyncTableFindCursor(Generic[TRAW, T], FindCursor[TRAW]):
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
    _mapper: Callable[[TRAW], T] | None

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
        FindCursor.__init__(self)
        self._timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=self._overall_timeout_ms,
        )

    def _copy(
        self: AsyncTableFindCursor[TRAW, T],
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
    ) -> AsyncTableFindCursor[TRAW, T]:
        if self._query_engine.async_table is None:
            raise ValueError("Query engine has no async table.")
        return AsyncTableFindCursor(
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
                    timeout_context=self._timeout_manager.remaining_timeout(
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
        self: AsyncTableFindCursor[TRAW, T],
    ) -> AsyncTableFindCursor[TRAW, T]:
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
        return cast(T, self._mapper(traw0) if self._mapper is not None else traw0)

    @property
    def data_source(self) -> AsyncTable[TRAW]:
        if self._query_engine.async_table is None:
            raise ValueError("Query engine has no async table.")
        return self._query_engine.async_table

    def clone(self) -> AsyncTableFindCursor[TRAW, TRAW]:
        """TODO. A new rewound cursor. Also: strips away any mapping."""
        if self._query_engine.async_table is None:
            raise ValueError("Query engine has no async table.")
        return AsyncTableFindCursor(
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

    def filter(self, filter: FilterType | None) -> AsyncTableFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(filter=filter)

    def project(
        self, projection: ProjectionType | None
    ) -> AsyncTableFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        if self._mapper is not None:
            raise CursorException(
                "Cannot set projection after map.",
                cursor_state=self._state.value,
            )
        return self._copy(projection=projection)

    def sort(self, sort: dict[str, Any] | None) -> AsyncTableFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(sort=sort)

    def limit(self, limit: int | None) -> AsyncTableFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(limit=limit)

    def include_similarity(
        self, include_similarity: bool | None
    ) -> AsyncTableFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(include_similarity=include_similarity)

    def include_sort_vector(
        self, include_sort_vector: bool | None
    ) -> AsyncTableFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(include_sort_vector=include_sort_vector)

    def skip(self, skip: int | None) -> AsyncTableFindCursor[TRAW, T]:
        # cannot happen once started consuming
        self._ensure_idle()
        return self._copy(skip=skip)

    def map(self, mapper: Callable[[T], TNEW]) -> AsyncTableFindCursor[TRAW, TNEW]:
        # cannot happen once started consuming
        self._ensure_idle()
        if self._query_engine.async_table is None:
            raise ValueError("Query engine has no async table.")
        composite_mapper: Callable[[TRAW], TNEW]
        if self._mapper is not None:

            def _composite(document: TRAW) -> TNEW:
                return mapper(self._mapper(document))  # type: ignore[misc]

            composite_mapper = _composite
        else:
            composite_mapper = cast(Callable[[TRAW], TNEW], mapper)
        return AsyncTableFindCursor(
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
        general_method_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        self._ensure_alive()
        copy_req_ms, copy_ovr_ms = _revise_timeouts_for_cursor_copy(
            new_general_method_timeout_ms=general_method_timeout_ms,
            new_timeout_ms=timeout_ms,
            old_request_timeout_ms=self._request_timeout_ms,
        )
        _cursor = self._copy(
            request_timeout_ms=copy_req_ms,
            overall_timeout_ms=copy_ovr_ms,
        )
        async for document in _cursor:
            function(document)
        self.close()

    async def to_list(
        self,
        *,
        general_method_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[T]:
        self._ensure_alive()
        copy_req_ms, copy_ovr_ms = _revise_timeouts_for_cursor_copy(
            new_general_method_timeout_ms=general_method_timeout_ms,
            new_timeout_ms=timeout_ms,
            old_request_timeout_ms=self._request_timeout_ms,
        )
        _cursor = self._copy(
            request_timeout_ms=copy_req_ms,
            overall_timeout_ms=copy_ovr_ms,
        )
        documents = [document async for document in _cursor]
        self.close()
        return documents

    async def has_next(self) -> bool:
        if self._state == CursorState.CLOSED:
            return False
        await self._try_ensure_fill_buffer()
        return len(self._buffer) > 0

    async def get_sort_vector(self) -> list[float] | DataAPIVector | None:
        """
        Return the vector used in this ANN search, if applicable.
        If this is not an ANN search, or it was invoked without the
        `include_sort_vector` parameter, return None.

        Invoking this method on a still-idle cursor will trigger an API call
        to get the first page of results.
        """

        await self._try_ensure_fill_buffer()
        if self._last_response_status:
            return _ensure_vector(
                self._last_response_status.get("sortVector"),
                self.data_source.api_options.serdes_options,
            )
        else:
            return None
