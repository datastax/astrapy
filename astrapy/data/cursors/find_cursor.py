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

from copy import deepcopy
from inspect import iscoroutinefunction
from typing import Any, Awaitable, Callable, Generic, cast

from astrapy import AsyncCollection, AsyncTable, Collection, Table
from astrapy.constants import (
    FilterType,
    ProjectionType,
)
from astrapy.data.cursors.cursor import (
    TNEW,
    TRAW,
    AbstractCursor,
    CursorState,
    T,
    _ensure_vector,
    _revise_timeouts_for_cursor_copy,
)
from astrapy.data.cursors.query_engine import (
    _CollectionFindQueryEngine,
    _TableFindQueryEngine,
)
from astrapy.data_types import DataAPIVector
from astrapy.exceptions import (
    CursorException,
    MultiCallTimeoutManager,
)
from astrapy.utils.unset import _UNSET, UnsetType


class CollectionFindCursor(Generic[TRAW, T], AbstractCursor[TRAW]):
    """
    A synchronous cursor over documents, as returned by a `find` invocation on
    a Collection. A cursor can be iterated over, materialized into a list,
    and queried/manipulated in various ways.

    Some cursor operations mutate it in-place (such as consuming its documents),
    other return a new cursor without changing the original one. See the documentation
    for the various methods and the Collection `find` method for more details
    and usage patterns.

    A cursor has two type parameters: TRAW and T. The first is the type of the "raw"
    documents as they are obtained from the Data API, the second is the type of the
    items after the optional mapping function (see the `.map()` method). If there is
    no mapping, TRAW = T. In general, consuming a cursor returns items of type T,
    except for the `consume_buffer` primitive that draws directly from the buffer
    and always returns items of type TRAW.

    Example:
        >>> cursor = collection.find(
        ...     {},
        ...     projection={"seq": True, "_id": False},
        ...     limit=5,
        ... )
        >>> for document in cursor:
        ...     print(document)
        ...
        {'seq': 1}
        {'seq': 4}
        {'seq': 15}
        {'seq': 22}
        {'seq': 11}
    """

    _query_engine: _CollectionFindQueryEngine[TRAW]
    _request_timeout_ms: int | None
    _overall_timeout_ms: int | None
    _request_timeout_label: str | None
    _overall_timeout_label: str | None
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
        request_timeout_label: str | None = None,
        overall_timeout_label: str | None = None,
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
        self._sort = deepcopy(sort)
        self._limit = limit
        self._include_similarity = include_similarity
        self._include_sort_vector = include_sort_vector
        self._skip = skip
        self._mapper = mapper
        self._request_timeout_ms = request_timeout_ms
        self._overall_timeout_ms = overall_timeout_ms
        self._request_timeout_label = request_timeout_label
        self._overall_timeout_label = overall_timeout_label
        self._query_engine = _CollectionFindQueryEngine(
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
        AbstractCursor.__init__(self)
        self._timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=self._overall_timeout_ms,
            timeout_label=self._overall_timeout_label,
        )

    def _copy(
        self: CollectionFindCursor[TRAW, T],
        *,
        request_timeout_ms: int | None | UnsetType = _UNSET,
        overall_timeout_ms: int | None | UnsetType = _UNSET,
        request_timeout_label: str | None | UnsetType = _UNSET,
        overall_timeout_label: str | None | UnsetType = _UNSET,
        filter: FilterType | None | UnsetType = _UNSET,
        projection: ProjectionType | None | UnsetType = _UNSET,
        sort: dict[str, Any] | None | UnsetType = _UNSET,
        limit: int | None | UnsetType = _UNSET,
        include_similarity: bool | None | UnsetType = _UNSET,
        include_sort_vector: bool | None | UnsetType = _UNSET,
        skip: int | None | UnsetType = _UNSET,
    ) -> CollectionFindCursor[TRAW, T]:
        if self._query_engine.collection is None:
            raise RuntimeError("Query engine has no collection.")
        return CollectionFindCursor(
            collection=self._query_engine.collection,
            request_timeout_ms=self._request_timeout_ms
            if isinstance(request_timeout_ms, UnsetType)
            else request_timeout_ms,
            overall_timeout_ms=self._overall_timeout_ms
            if isinstance(overall_timeout_ms, UnsetType)
            else overall_timeout_ms,
            request_timeout_label=self._request_timeout_label
            if isinstance(request_timeout_label, UnsetType)
            else request_timeout_label,
            overall_timeout_label=self._overall_timeout_label
            if isinstance(overall_timeout_label, UnsetType)
            else overall_timeout_label,
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
        This method never changes the cursor state.
        """

        if self._state == CursorState.CLOSED:
            return
        if not self._buffer:
            if self._next_page_state is not None or self._state == CursorState.IDLE:
                new_buffer, next_page_state, resp_status = (
                    self._query_engine._fetch_page(
                        page_state=self._next_page_state,
                        timeout_context=self._timeout_manager.remaining_timeout(
                            cap_time_ms=self._request_timeout_ms,
                            cap_timeout_label=self._request_timeout_label,
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
        if self._state == CursorState.CLOSED:
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
        """
        The Collection object that originated this cursor through a `find` operation.

        Returns:
            a Collection instance.
        """

        if self._query_engine.collection is None:
            raise RuntimeError("Query engine has no collection.")
        return self._query_engine.collection

    def clone(self) -> CollectionFindCursor[TRAW, T]:
        """
        Create a copy of this cursor with:
        - the same parameters (timeouts, filter, projection, etc)
        - and the cursor is rewound to its pristine IDLE state.

        Returns:
            a new CollectionFindCursor, similar to this one but
            rewound to its initial state.

        Example:
            >>> cursor = collection.find(
            ...     {},
            ...     projection={"seq": True, "_id": False},
            ...     limit=2,
            ... ).map(lambda doc: doc["seq"])
            >>> for value in cursor:
            ...     print(value)
            ...
            1
            4
            >>> cloned_cursor = cursor.clone()
            >>> for document in cloned_cursor:
            ...     print(document)
            ...
            1
            4
        """

        if self._query_engine.collection is None:
            raise RuntimeError("Query engine has no collection.")
        return CollectionFindCursor(
            collection=self._query_engine.collection,
            request_timeout_ms=self._request_timeout_ms,
            overall_timeout_ms=self._overall_timeout_ms,
            request_timeout_label=self._request_timeout_label,
            overall_timeout_label=self._overall_timeout_label,
            filter=self._filter,
            projection=self._projection,
            sort=self._sort,
            limit=self._limit,
            include_similarity=self._include_similarity,
            include_sort_vector=self._include_sort_vector,
            skip=self._skip,
            mapper=self._mapper,
        )

    def filter(self, filter: FilterType | None) -> CollectionFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new filter setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the Collection `find` method.

        Args:
            filter: a new filter setting to apply to the returned new cursor.

        Returns:
            a new CollectionFindCursor with the same settings as this one,
                except for `filter` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(filter=filter)

    def project(
        self, projection: ProjectionType | None
    ) -> CollectionFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new projection setting.
        This operation is allowed only if the cursor state is still IDLE and if
        no mapping has been set on it.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the Collection `find` method.

        Args:
            projection: a new projection setting to apply to the returned new cursor.

        Returns:
            a new CollectionFindCursor with the same settings as this one,
                except for `projection` which is the provided value.
        """

        self._ensure_idle()
        if self._mapper is not None:
            raise CursorException(
                "Cannot set projection after map.",
                cursor_state=self._state.value,
            )
        return self._copy(projection=projection)

    def sort(self, sort: dict[str, Any] | None) -> CollectionFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new sort setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the Collection `find` method.

        Args:
            sort: a new sort setting to apply to the returned new cursor.

        Returns:
            a new CollectionFindCursor with the same settings as this one,
                except for `sort` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(sort=sort)

    def limit(self, limit: int | None) -> CollectionFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new limit setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the Collection `find` method.

        Args:
            limit: a new limit setting to apply to the returned new cursor.

        Returns:
            a new CollectionFindCursor with the same settings as this one,
                except for `limit` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(limit=limit)

    def include_similarity(
        self, include_similarity: bool | None
    ) -> CollectionFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new include_similarity setting.
        This operation is allowed only if the cursor state is still IDLE and if
        no mapping has been set on it.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the Collection `find` method.

        Args:
            include_similarity: a new include_similarity setting to apply
                to the returned new cursor.

        Returns:
            a new CollectionFindCursor with the same settings as this one,
                except for `include_similarity` which is the provided value.
        """

        self._ensure_idle()
        if self._mapper is not None:
            raise CursorException(
                "Cannot set include_similarity after map.",
                cursor_state=self._state.value,
            )
        return self._copy(include_similarity=include_similarity)

    def include_sort_vector(
        self, include_sort_vector: bool | None
    ) -> CollectionFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new include_sort_vector setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the Collection `find` method.

        Args:
            include_sort_vector: a new include_sort_vector setting to apply
                to the returned new cursor.

        Returns:
            a new CollectionFindCursor with the same settings as this one,
                except for `include_sort_vector` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(include_sort_vector=include_sort_vector)

    def skip(self, skip: int | None) -> CollectionFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new skip setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the Collection `find` method.

        Args:
            skip: a new skip setting to apply to the returned new cursor.

        Returns:
            a new CollectionFindCursor with the same settings as this one,
                except for `skip` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(skip=skip)

    def map(self, mapper: Callable[[T], TNEW]) -> CollectionFindCursor[TRAW, TNEW]:
        """
        Return a copy of this cursor with a mapping function to transform
        the returned items. Calling this method on a cursor with a mapping
        already set results in the mapping functions being composed.

        This operation is allowed only if the cursor state is still IDLE.

        Args:
            mapper: a function transforming the objects returned by the cursor
                into something else (i.e. a function T => TNEW).

        Returns:
            a new CollectionFindCursor with a new mapping function on the results,
                possibly composed with any pre-existing mapping function.

        Example:
            >>> cursor = collection.find(
            ...     {},
            ...     projection={"seq": True, "_id": False},
            ...     limit=2,
            ... )
            >>> for doc in cursor:
            ...     print(doc)
            ...
            {'seq': 1}
            {'seq': 4}
            >>> cursor_mapped = collection.find(
            ...     {},
            ...     projection={"seq": True, "_id": False},
            ...     limit=2,
            ... ).map(lambda doc: doc["seq"])
            >>> for value in cursor_mapped:
            ...     print(value)
            ...
            1
            4
            >>>
            >>> cursor_mapped_twice = collection.find(
            ...     {},
            ...     projection={"seq": True, "_id": False},
            ...     limit=2,
            ... ).map(lambda doc: doc["seq"]).map(lambda num: "x" * num)
            >>> for value in cursor_mapped_twice:
            ...     print(value)
            ...
            x
            xxxx
        """
        self._ensure_idle()
        if self._query_engine.collection is None:
            raise RuntimeError("Query engine has no collection.")
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
            request_timeout_label=self._request_timeout_label,
            overall_timeout_label=self._overall_timeout_label,
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
        function: Callable[[T], bool | None],
        *,
        general_method_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        """
        Consume the remaining documents in the cursor, invoking a provided callback
        function on each of them.

        Calling this method on a CLOSED cursor results in an error.

        The callback function can return any value. The return value is generally
        discarded, with the following exception: if the function returns the boolean
        `False`, it is taken to signify that the method should quit early, leaving the
        cursor half-consumed (ACTIVE state). If this does not occur, this method
        results in the cursor entering CLOSED state once it is exhausted.

        Args:
            function: a callback function whose only parameter is of the type returned
                by the cursor. This callback is invoked once per each document yielded
                by the cursor. If the callback returns a `False`, the `for_each`
                invocation stops early and returns without consuming further documents.
            general_method_timeout_ms: a timeout, in milliseconds, for the whole
                duration of this method. If not provided, there is no such timeout.
                Note that the per-request timeout set on the cursor still applies.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Example:
            >>> cursor = collection.find(
            ...     {},
            ...     projection={"seq": True, "_id": False},
            ...     limit=3,
            ... )
            >>> def printer(doc):
            ...     print(f"-> {doc['seq']}")
            ...
            >>> cursor.for_each(printer)
            -> 1
            -> 4
            -> 15
            >>>
            >>> if cursor.state != CursorState.CLOSED:
            ...     print(f"alive: {list(cursor)}")
            ... else:
            ...     print("(closed)")
            ...
            (closed)
            >>> cursor2 = collection.find(
            ...     {},
            ...     projection={"seq": True, "_id": False},
            ...     limit=3,
            ... )
            >>> def checker(doc):
            ...     print(f"-> {doc['seq']}")
            ...     return doc["seq"] != 4
            ...
            >>> cursor2.for_each(checker)
            -> 1
            -> 4
            >>>
            >>> if cursor2.state != CursorState.CLOSED:
            ...     print(f"alive: {list(cursor2)}")
            ... else:
            ...     print("(closed)")
            ...
            alive: [{'seq': 15}]
        """

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
        self._imprint_internal_state(_cursor)
        for document in _cursor:
            res = function(document)
            if res is False:
                break
        _cursor._imprint_internal_state(self)

    def to_list(
        self,
        *,
        general_method_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[T]:
        """
        Materialize all documents that remain to be consumed from a cursor into a list.

        Calling this method on a CLOSED cursor results in an error.

        If the cursor is IDLE, the result will be the whole set of documents returned
        by the `find` operation; otherwise, the documents already consumed by the cursor
        will not be in the resulting list.

        Calling this method is not recommended if a huge list of results is anticipated:
        it would involve a large number of data exchanges with the Data API and possibly
        a massive memory usage to construct the list. In such cases, a lazy pattern
        of iterating and consuming the documents is to be preferred.

        Args:
            general_method_timeout_ms: a timeout, in milliseconds, for the whole
                duration of this method. If not provided, there is no such timeout.
                Note that the per-request timeout set on the cursor still applies.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a list of documents (or other values depending on the mapping
                function, if one is set). These are all items that were left
                to be consumed on the cursor when `to_list` is called.

        Example:
            >>> collection.find(
            ...     {},
            ...     projection={"seq": True, "_id": False},
            ...     limit=3,
            ... ).to_list()
            [{'seq': 1}, {'seq': 4}, {'seq': 15}]
            >>>
            >>> cursor = collection.find(
            ...     {},
            ...     projection={"seq": True, "_id": False},
            ...     limit=5,
            ... ).map(lambda doc: doc["seq"])
            >>>
            >>> first_value = cursor.__next__()
            >>> cursor.to_list()
            [4, 15, 22, 11]
        """

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
        self._imprint_internal_state(_cursor)
        documents = [document for document in _cursor]
        _cursor._imprint_internal_state(self)
        return documents

    def has_next(self) -> bool:
        """
        Whether the cursor actually has more documents to return.

        `has_next` can be called on any cursor, but on a CLOSED cursor
        will always return False.

        This method can trigger the fetch operation of a new page, if the current
        buffer is empty.

        Calling `has_next` on an IDLE cursor triggers the first page fetch, but the
        cursor stays in the IDLE state until actual consumption starts.

        Returns:
            a boolean value of True if there is at least one further item
                available to consume; False otherwise (including the case of CLOSED
                cursor).
        """

        if self._state == CursorState.CLOSED:
            return False
        self._try_ensure_fill_buffer()
        return len(self._buffer) > 0

    def get_sort_vector(self) -> list[float] | DataAPIVector | None:
        """
        Return the query vector used in the vector (ANN) search that originated
        this cursor, if applicable. If this is not an ANN search, or it was invoked
        without the `include_sort_vector` flag, return None.

        Calling `get_sort_vector` on an IDLE cursor triggers the first page fetch,
        but the cursor stays in the IDLE state until actual consumption starts.

        The method can be invoked on a CLOSED cursor and will return either None
        or the sort vector used in the search.

        Returns:
            the query vector used in the search if this was a
                vector search (otherwise None). The vector is returned either
                as a DataAPIVector or a plain list of number depending on the
                `APIOptions.serdes_options` that apply. The query vector is available
                also for vectorize-based ANN searches.
        """

        self._try_ensure_fill_buffer()
        if self._last_response_status:
            return _ensure_vector(
                self._last_response_status.get("sortVector"),
                self.data_source.api_options.serdes_options,
            )
        else:
            return None


class AsyncCollectionFindCursor(Generic[TRAW, T], AbstractCursor[TRAW]):
    """
    An asynchronous cursor over documents, as returned by a `find` invocation on
    an AsyncCollection. A cursor can be iterated over, materialized into a list,
    and queried/manipulated in various ways.

    Some cursor operations mutate it in-place (such as consuming its documents),
    other return a new cursor without changing the original one. See the documentation
    for the various methods and the AsyncCollection `find` method for more details
    and usage patterns.

    A cursor has two type parameters: TRAW and T. The first is the type of the "raw"
    documents as they are obtained from the Data API, the second is the type of the
    items after the optional mapping function (see the `.map()` method). If there is
    no mapping, TRAW = T. In general, consuming a cursor returns items of type T,
    except for the `consume_buffer` primitive that draws directly from the buffer
    and always returns items of type TRAW.

    This class is the async counterpart of the CollectionFindCursor, for use with
    asyncio. Other than the async interface, its behavior is identical: please refer
    to the documentation for `CollectionFindCursor` for examples and details.
    """

    _query_engine: _CollectionFindQueryEngine[TRAW]
    _request_timeout_ms: int | None
    _overall_timeout_ms: int | None
    _request_timeout_label: str | None
    _overall_timeout_label: str | None
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
        request_timeout_label: str | None = None,
        overall_timeout_label: str | None = None,
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
        self._sort = deepcopy(sort)
        self._limit = limit
        self._include_similarity = include_similarity
        self._include_sort_vector = include_sort_vector
        self._skip = skip
        self._mapper = mapper
        self._request_timeout_ms = request_timeout_ms
        self._overall_timeout_ms = overall_timeout_ms
        self._request_timeout_label = request_timeout_label
        self._overall_timeout_label = overall_timeout_label
        self._query_engine = _CollectionFindQueryEngine(
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
        AbstractCursor.__init__(self)
        self._timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=self._overall_timeout_ms,
            timeout_label=self._overall_timeout_label,
        )

    def _copy(
        self: AsyncCollectionFindCursor[TRAW, T],
        *,
        request_timeout_ms: int | None | UnsetType = _UNSET,
        overall_timeout_ms: int | None | UnsetType = _UNSET,
        request_timeout_label: str | None | UnsetType = _UNSET,
        overall_timeout_label: str | None | UnsetType = _UNSET,
        filter: FilterType | None | UnsetType = _UNSET,
        projection: ProjectionType | None | UnsetType = _UNSET,
        sort: dict[str, Any] | None | UnsetType = _UNSET,
        limit: int | None | UnsetType = _UNSET,
        include_similarity: bool | None | UnsetType = _UNSET,
        include_sort_vector: bool | None | UnsetType = _UNSET,
        skip: int | None | UnsetType = _UNSET,
    ) -> AsyncCollectionFindCursor[TRAW, T]:
        if self._query_engine.async_collection is None:
            raise RuntimeError("Query engine has no async collection.")
        return AsyncCollectionFindCursor(
            collection=self._query_engine.async_collection,
            request_timeout_ms=self._request_timeout_ms
            if isinstance(request_timeout_ms, UnsetType)
            else request_timeout_ms,
            overall_timeout_ms=self._overall_timeout_ms
            if isinstance(overall_timeout_ms, UnsetType)
            else overall_timeout_ms,
            request_timeout_label=self._request_timeout_label
            if isinstance(request_timeout_label, UnsetType)
            else request_timeout_label,
            overall_timeout_label=self._overall_timeout_label
            if isinstance(overall_timeout_label, UnsetType)
            else overall_timeout_label,
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

    async def _try_ensure_fill_buffer(self) -> None:
        """
        If buffer is empty, try to fill with next page, if applicable.
        If not possible, silently do nothing.
        This method never changes the cursor state.
        """

        if self._state == CursorState.CLOSED:
            return
        if not self._buffer:
            if self._next_page_state is not None or self._state == CursorState.IDLE:
                (
                    new_buffer,
                    next_page_state,
                    resp_status,
                ) = await self._query_engine._async_fetch_page(
                    page_state=self._next_page_state,
                    timeout_context=self._timeout_manager.remaining_timeout(
                        cap_time_ms=self._request_timeout_ms,
                        cap_timeout_label=self._request_timeout_label,
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
        if self._state == CursorState.CLOSED:
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
        """
        The AsyncCollection object that originated this cursor through
        a `find` operation.

        Returns:
            an AsyncCollection instance.
        """

        if self._query_engine.async_collection is None:
            raise RuntimeError("Query engine has no async collection.")
        return self._query_engine.async_collection

    def clone(self) -> AsyncCollectionFindCursor[TRAW, T]:
        """
        Create a copy of this cursor with:
        - the same parameters (timeouts, filter, projection, etc)
        - and the cursor is rewound to its pristine IDLE state.

        For usage examples, please refer to the same method of the
        equivalent synchronous CollectionFindCursor class, and apply the necessary
        adaptations to the async interface.

        Returns:
            a new AsyncCollectionFindCursor, similar to this one but
            rewound to its initial state.
        """

        if self._query_engine.async_collection is None:
            raise RuntimeError("Query engine has no async collection.")
        return AsyncCollectionFindCursor(
            collection=self._query_engine.async_collection,
            request_timeout_ms=self._request_timeout_ms,
            overall_timeout_ms=self._overall_timeout_ms,
            request_timeout_label=self._request_timeout_label,
            overall_timeout_label=self._overall_timeout_label,
            filter=self._filter,
            projection=self._projection,
            sort=self._sort,
            limit=self._limit,
            include_similarity=self._include_similarity,
            include_sort_vector=self._include_sort_vector,
            skip=self._skip,
            mapper=self._mapper,
        )

    def filter(self, filter: FilterType | None) -> AsyncCollectionFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new filter setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the AsyncCollection `find` method.

        Args:
            filter: a new filter setting to apply to the returned new cursor.

        Returns:
            a new AsyncCollectionFindCursor with the same settings as this one,
                except for `filter` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(filter=filter)

    def project(
        self, projection: ProjectionType | None
    ) -> AsyncCollectionFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new projection setting.
        This operation is allowed only if the cursor state is still IDLE and if
        no mapping has been set on it.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the AsyncCollection `find` method.

        Args:
            projection: a new projection setting to apply to the returned new cursor.

        Returns:
            a new AsyncCollectionFindCursor with the same settings as this one,
                except for `projection` which is the provided value.
        """

        self._ensure_idle()
        if self._mapper is not None:
            raise CursorException(
                "Cannot set projection after map.",
                cursor_state=self._state.value,
            )
        return self._copy(projection=projection)

    def sort(self, sort: dict[str, Any] | None) -> AsyncCollectionFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new sort setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the AsyncCollection `find` method.

        Args:
            sort: a new sort setting to apply to the returned new cursor.

        Returns:
            a new AsyncCollectionFindCursor with the same settings as this one,
                except for `sort` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(sort=sort)

    def limit(self, limit: int | None) -> AsyncCollectionFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new limit setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the AsyncCollection `find` method.

        Args:
            limit: a new limit setting to apply to the returned new cursor.

        Returns:
            a new AsyncCollectionFindCursor with the same settings as this one,
                except for `limit` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(limit=limit)

    def include_similarity(
        self, include_similarity: bool | None
    ) -> AsyncCollectionFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new include_similarity setting.
        This operation is allowed only if the cursor state is still IDLE and if
        no mapping has been set on it.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the AsyncCollection `find` method.

        Args:
            include_similarity: a new include_similarity setting to apply
                to the returned new cursor.

        Returns:
            a new AsyncCollectionFindCursor with the same settings as this one,
                except for `include_similarity` which is the provided value.
        """

        self._ensure_idle()
        if self._mapper is not None:
            raise CursorException(
                "Cannot set include_similarity after map.",
                cursor_state=self._state.value,
            )
        return self._copy(include_similarity=include_similarity)

    def include_sort_vector(
        self, include_sort_vector: bool | None
    ) -> AsyncCollectionFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new include_sort_vector setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the AsyncCollection `find` method.

        Args:
            include_sort_vector: a new include_sort_vector setting to apply
                to the returned new cursor.

        Returns:
            a new AsyncCollectionFindCursor with the same settings as this one,
                except for `include_sort_vector` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(include_sort_vector=include_sort_vector)

    def skip(self, skip: int | None) -> AsyncCollectionFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new skip setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the AsyncCollection `find` method.

        Args:
            skip: a new skip setting to apply to the returned new cursor.

        Returns:
            a new AsyncCollectionFindCursor with the same settings as this one,
                except for `skip` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(skip=skip)

    def map(self, mapper: Callable[[T], TNEW]) -> AsyncCollectionFindCursor[TRAW, TNEW]:
        """
        Return a copy of this cursor with a mapping function to transform
        the returned items. Calling this method on a cursor with a mapping
        already set results in the mapping functions being composed.

        This operation is allowed only if the cursor state is still IDLE.

        For usage examples, please refer to the same method of the
        equivalent synchronous CollectionFindCursor class, and apply the necessary
        adaptations to the async interface.

        Args:
            mapper: a function transforming the objects returned by the cursor
                into something else (i.e. a function T => TNEW).

        Returns:
            a new AsyncCollectionFindCursor with a new mapping function on the results,
                possibly composed with any pre-existing mapping function.
        """

        self._ensure_idle()
        if self._query_engine.async_collection is None:
            raise RuntimeError("Query engine has no async collection.")
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
            request_timeout_label=self._request_timeout_label,
            overall_timeout_label=self._overall_timeout_label,
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
        function: Callable[[T], bool | None] | Callable[[T], Awaitable[bool | None]],
        *,
        general_method_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        """
        Consume the remaining documents in the cursor, invoking a provided callback
        function -- or coroutine -- on each of them.

        Calling this method on a CLOSED cursor results in an error.

        The callback function can return any value. The return value is generally
        discarded, with the following exception: if the function returns the boolean
        `False`, it is taken to signify that the method should quit early, leaving the
        cursor half-consumed (ACTIVE state). If this does not occur, this method
        results in the cursor entering CLOSED state once it is exhausted.

        For usage examples, please refer to the same method of the
        equivalent synchronous CollectionFindCursor class, and apply the necessary
        adaptations to the async interface.

        Args:
            function: a callback function, or a coroutine, whose only parameter is of
                the type returned by the cursor.
                This callback is invoked once per each document yielded
                by the cursor. If the callback returns a `False`, the `for_each`
                invocation stops early and returns without consuming further documents.
            general_method_timeout_ms: a timeout, in milliseconds, for the whole
                duration of this method. If not provided, there is no such timeout.
                Note that the per-request timeout set on the cursor still applies.
            timeout_ms: an alias for `general_method_timeout_ms`.
        """

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
        self._imprint_internal_state(_cursor)
        is_coro = iscoroutinefunction(function)
        async for document in _cursor:
            if is_coro:
                res = await function(document)  # type: ignore[misc]
            else:
                res = function(document)
            if res is False:
                break
        _cursor._imprint_internal_state(self)

    async def to_list(
        self,
        *,
        general_method_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[T]:
        """
        Materialize all documents that remain to be consumed from a cursor into a list.

        Calling this method on a CLOSED cursor results in an error.

        If the cursor is IDLE, the result will be the whole set of documents returned
        by the `find` operation; otherwise, the documents already consumed by the cursor
        will not be in the resulting list.

        Calling this method is not recommended if a huge list of results is anticipated:
        it would involve a large number of data exchanges with the Data API and possibly
        a massive memory usage to construct the list. In such cases, a lazy pattern
        of iterating and consuming the documents is to be preferred.

        For usage examples, please refer to the same method of the
        equivalent synchronous CollectionFindCursor class, and apply the necessary
        adaptations to the async interface.

        Args:
            general_method_timeout_ms: a timeout, in milliseconds, for the whole
                duration of this method. If not provided, there is no such timeout.
                Note that the per-request timeout set on the cursor still applies.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a list of documents (or other values depending on the mapping
                function, if one is set). These are all items that were left
                to be consumed on the cursor when `to_list` is called.
        """

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
        self._imprint_internal_state(_cursor)
        documents = [document async for document in _cursor]
        _cursor._imprint_internal_state(self)
        return documents

    async def has_next(self) -> bool:
        """
        Whether the cursor actually has more documents to return.

        `has_next` can be called on any cursor, but on a CLOSED cursor
        will always return False.

        This method can trigger the fetch operation of a new page, if the current
        buffer is empty.

        Calling `has_next` on an IDLE cursor triggers the first page fetch, but the
        cursor stays in the IDLE state until actual consumption starts.

        Returns:
            a boolean value of True if there is at least one further item
                available to consume; False otherwise (including the case of CLOSED
                cursor).
        """

        if self._state == CursorState.CLOSED:
            return False
        await self._try_ensure_fill_buffer()
        return len(self._buffer) > 0

    async def get_sort_vector(self) -> list[float] | DataAPIVector | None:
        """
        Return the query vector used in the vector (ANN) search that originated
        this cursor, if applicable. If this is not an ANN search, or it was invoked
        without the `include_sort_vector` flag, return None.

        Calling `get_sort_vector` on an IDLE cursor triggers the first page fetch,
        but the cursor stays in the IDLE state until actual consumption starts.

        The method can be invoked on a CLOSED cursor and will return either None
        or the sort vector used in the search.

        Returns:
            the query vector used in the search if this was a
                vector search (otherwise None). The vector is returned either
                as a DataAPIVector or a plain list of number depending on the
                `APIOptions.serdes_options` that apply. The query vector is available
                also for vectorize-based ANN searches.
        """

        await self._try_ensure_fill_buffer()
        if self._last_response_status:
            return _ensure_vector(
                self._last_response_status.get("sortVector"),
                self.data_source.api_options.serdes_options,
            )
        else:
            return None


class TableFindCursor(Generic[TRAW, T], AbstractCursor[TRAW]):
    """
    A synchronous cursor over rows, as returned by a `find` invocation on
    a Table. A cursor can be iterated over, materialized into a list,
    and queried/manipulated in various ways.

    Some cursor operations mutate it in-place (such as consuming its rows),
    other return a new cursor without changing the original one. See the documentation
    for the various methods and the Table `find` method for more details
    and usage patterns.

    A cursor has two type parameters: TRAW and T. The first is the type of the "raw"
    rows as they are obtained from the Data API, the second is the type of the
    items after the optional mapping function (see the `.map()` method). If there is
    no mapping, TRAW = T. In general, consuming a cursor returns items of type T,
    except for the `consume_buffer` primitive that draws directly from the buffer
    and always returns items of type TRAW.

    Example:
        >>> cursor = my_table.find(
        ...     {"match_id": "challenge6"},
        ...     projection={"winner": True},
        ...     limit=5,
        ... )
        >>> for row in cursor:
        ...     print(row)
        ...
        {'winner': 'Donna'}
        {'winner': 'Erick'}
        {'winner': 'Fiona'}
        {'winner': 'Georg'}
        {'winner': 'Helen'}
    """

    _query_engine: _TableFindQueryEngine[TRAW]
    _request_timeout_ms: int | None
    _overall_timeout_ms: int | None
    _request_timeout_label: str | None
    _overall_timeout_label: str | None
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
        request_timeout_label: str | None = None,
        overall_timeout_label: str | None = None,
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
        self._sort = deepcopy(sort)
        self._limit = limit
        self._include_similarity = include_similarity
        self._include_sort_vector = include_sort_vector
        self._skip = skip
        self._mapper = mapper
        self._request_timeout_ms = request_timeout_ms
        self._overall_timeout_ms = overall_timeout_ms
        self._request_timeout_label = request_timeout_label
        self._overall_timeout_label = overall_timeout_label
        self._query_engine = _TableFindQueryEngine(
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
        AbstractCursor.__init__(self)
        self._timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=self._overall_timeout_ms,
            timeout_label=self._overall_timeout_label,
        )

    def _copy(
        self: TableFindCursor[TRAW, T],
        *,
        request_timeout_ms: int | None | UnsetType = _UNSET,
        overall_timeout_ms: int | None | UnsetType = _UNSET,
        request_timeout_label: str | None | UnsetType = _UNSET,
        overall_timeout_label: str | None | UnsetType = _UNSET,
        filter: FilterType | None | UnsetType = _UNSET,
        projection: ProjectionType | None | UnsetType = _UNSET,
        sort: dict[str, Any] | None | UnsetType = _UNSET,
        limit: int | None | UnsetType = _UNSET,
        include_similarity: bool | None | UnsetType = _UNSET,
        include_sort_vector: bool | None | UnsetType = _UNSET,
        skip: int | None | UnsetType = _UNSET,
    ) -> TableFindCursor[TRAW, T]:
        if self._query_engine.table is None:
            raise RuntimeError("Query engine has no table.")
        return TableFindCursor(
            table=self._query_engine.table,
            request_timeout_ms=self._request_timeout_ms
            if isinstance(request_timeout_ms, UnsetType)
            else request_timeout_ms,
            overall_timeout_ms=self._overall_timeout_ms
            if isinstance(overall_timeout_ms, UnsetType)
            else overall_timeout_ms,
            request_timeout_label=self._request_timeout_label
            if isinstance(request_timeout_label, UnsetType)
            else request_timeout_label,
            overall_timeout_label=self._overall_timeout_label
            if isinstance(overall_timeout_label, UnsetType)
            else overall_timeout_label,
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
        This method never changes the cursor state.
        """

        if self._state == CursorState.CLOSED:
            return
        if not self._buffer:
            if self._next_page_state is not None or self._state == CursorState.IDLE:
                new_buffer, next_page_state, resp_status = (
                    self._query_engine._fetch_page(
                        page_state=self._next_page_state,
                        timeout_context=self._timeout_manager.remaining_timeout(
                            cap_time_ms=self._request_timeout_ms,
                            cap_timeout_label=self._request_timeout_label,
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
        if self._state == CursorState.CLOSED:
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
        """
        The Table object that originated this cursor through a `find` operation.

        Returns:
            a Table instance.
        """

        if self._query_engine.table is None:
            raise RuntimeError("Query engine has no table.")
        return self._query_engine.table

    def clone(self) -> TableFindCursor[TRAW, T]:
        """
        Create a copy of this cursor with:
        - the same parameters (timeouts, filter, projection, etc)
        - and the cursor is rewound to its pristine IDLE state.

        Returns:
            a new TableFindCursor, similar to this one but
            rewound to its initial state.

        Example:
            >>> cursor = my_table.find(
            ...     {"match_id": "challenge6"},
            ...     projection={"winner": True},
            ...     limit=2,
            ... ).map(lambda row: row["winner"])
            >>> for value in cursor:
            ...     print(value)
            ...
            Donna
            Erick
            >>> cloned_cursor = cursor.clone()
            >>> for row in cloned_cursor:
            ...     print(row)
            ...
            Donna
            Erick
        """

        if self._query_engine.table is None:
            raise RuntimeError("Query engine has no table.")
        return TableFindCursor(
            table=self._query_engine.table,
            request_timeout_ms=self._request_timeout_ms,
            overall_timeout_ms=self._overall_timeout_ms,
            request_timeout_label=self._request_timeout_label,
            overall_timeout_label=self._overall_timeout_label,
            filter=self._filter,
            projection=self._projection,
            sort=self._sort,
            limit=self._limit,
            include_similarity=self._include_similarity,
            include_sort_vector=self._include_sort_vector,
            skip=self._skip,
            mapper=self._mapper,
        )

    def filter(self, filter: FilterType | None) -> TableFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new filter setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the Table `find` method.

        Args:
            filter: a new filter setting to apply to the returned new cursor.

        Returns:
            a new TableFindCursor with the same settings as this one,
                except for `filter` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(filter=filter)

    def project(self, projection: ProjectionType | None) -> TableFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new projection setting.
        This operation is allowed only if the cursor state is still IDLE and if
        no mapping has been set on it.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the Table `find` method.

        Args:
            projection: a new projection setting to apply to the returned new cursor.

        Returns:
            a new TableFindCursor with the same settings as this one,
                except for `projection` which is the provided value.
        """

        self._ensure_idle()
        if self._mapper is not None:
            raise CursorException(
                "Cannot set projection after map.",
                cursor_state=self._state.value,
            )
        return self._copy(projection=projection)

    def sort(self, sort: dict[str, Any] | None) -> TableFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new sort setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the Table `find` method.

        Args:
            sort: a new sort setting to apply to the returned new cursor.

        Returns:
            a new TableFindCursor with the same settings as this one,
                except for `sort` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(sort=sort)

    def limit(self, limit: int | None) -> TableFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new limit setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the Table `find` method.

        Args:
            limit: a new limit setting to apply to the returned new cursor.

        Returns:
            a new TableFindCursor with the same settings as this one,
                except for `limit` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(limit=limit)

    def include_similarity(
        self, include_similarity: bool | None
    ) -> TableFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new include_similarity setting.
        This operation is allowed only if the cursor state is still IDLE and if
        no mapping has been set on it.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the Table `find` method.

        Args:
            include_similarity: a new include_similarity setting to apply
                to the returned new cursor.

        Returns:
            a new TableFindCursor with the same settings as this one,
                except for `include_similarity` which is the provided value.
        """

        self._ensure_idle()
        if self._mapper is not None:
            raise CursorException(
                "Cannot set include_similarity after map.",
                cursor_state=self._state.value,
            )
        return self._copy(include_similarity=include_similarity)

    def include_sort_vector(
        self, include_sort_vector: bool | None
    ) -> TableFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new include_sort_vector setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the Table `find` method.

        Args:
            include_sort_vector: a new include_sort_vector setting to apply
                to the returned new cursor.

        Returns:
            a new TableFindCursor with the same settings as this one,
                except for `include_sort_vector` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(include_sort_vector=include_sort_vector)

    def skip(self, skip: int | None) -> TableFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new skip setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the Table `find` method.

        Args:
            skip: a new skip setting to apply to the returned new cursor.

        Returns:
            a new TableFindCursor with the same settings as this one,
                except for `skip` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(skip=skip)

    def map(self, mapper: Callable[[T], TNEW]) -> TableFindCursor[TRAW, TNEW]:
        """
        Return a copy of this cursor with a mapping function to transform
        the returned items. Calling this method on a cursor with a mapping
        already set results in the mapping functions being composed.

        This operation is allowed only if the cursor state is still IDLE.

        Args:
            mapper: a function transforming the objects returned by the cursor
                into something else (i.e. a function T => TNEW).

        Returns:
            a new TableFindCursor with a new mapping function on the results,
                possibly composed with any pre-existing mapping function.

        Example:
            >>> cursor = my_table.find(
            ...     {"match_id": "challenge6"},
            ...     projection={"winner": True},
            ...     limit=2,
            ... )
            >>> for row in cursor:
            ...     print(row)
            ...
            {'winner': 'Donna'}
            {'winner': 'Erick'}
            >>> cursor_mapped = my_table.find(
            ...     {"match_id": "challenge6"},
            ...     projection={"winner": True},
            ...     limit=2,
            ... ).map(lambda row: row["winner"])
            >>> for value in cursor_mapped:
            ...     print(value)
            ...
            Donna
            Erick
            >>> cursor_mapped_twice = my_table.find(
            ...     {"match_id": "challenge6"},
            ...     projection={"winner": True},
            ...     limit=2,
            ... ).map(lambda row: row["winner"]).map(lambda w: w.upper())
            >>> for value in cursor_mapped_twice:
            ...     print(value)
            ...
            DONNA
            ERICK
        """

        self._ensure_idle()
        if self._query_engine.table is None:
            raise RuntimeError("Query engine has no table.")
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
            request_timeout_label=self._request_timeout_label,
            overall_timeout_label=self._overall_timeout_label,
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
        function: Callable[[T], bool | None],
        *,
        general_method_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        """
        Consume the remaining rows in the cursor, invoking a provided callback
        function on each of them.

        Calling this method on a CLOSED cursor results in an error.

        The callback function can return any value. The return value is generally
        discarded, with the following exception: if the function returns the boolean
        `False`, it is taken to signify that the method should quit early, leaving the
        cursor half-consumed (ACTIVE state). If this does not occur, this method
        results in the cursor entering CLOSED state once it is exhausted.

        Args:
            function: a callback function whose only parameter is of the type returned
                by the cursor. This callback is invoked once per each row yielded
                by the cursor. If the callback returns a `False`, the `for_each`
                invocation stops early and returns without consuming further rows.
            general_method_timeout_ms: a timeout, in milliseconds, for the whole
                duration of this method. If not provided, there is no such timeout.
                Note that the per-request timeout set on the cursor still applies.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Example:
            >>> cursor = my_table.find(
            ...     {"match_id": "challenge6"},
            ...     projection={"winner": True},
            ...     limit=3,
            ... )
            >>> def printer(row):
            ...     print(f"-> {row['winner']}")
            ...
            >>> cursor.for_each(printer)
            -> Donna
            -> Erick
            -> Fiona
            >>>
            >>> if cursor.state != CursorState.CLOSED:
            ...     print(f"alive: {list(cursor)}")
            ... else:
            ...     print("(closed)")
            ...
            (closed)
            >>> cursor2 = my_table.find(
            ...     {"match_id": "challenge6"},
            ...     projection={"winner": True},
            ...     limit=3,
            ... )
            >>> def checker(row):
            ...     print(f"-> {row['winner']}")
            ...     return row["winner"] != "Erick"
            ...
            >>> cursor2.for_each(checker)
            -> Donna
            -> Erick
            >>>
            >>> if cursor2.state != CursorState.CLOSED:
            ...     print(f"alive: {list(cursor2)}")
            ... else:
            ...     print("(closed)")
            ...
            alive: [{'winner': 'Fiona'}]
        """

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
        self._imprint_internal_state(_cursor)
        for row in _cursor:
            res = function(row)
            if res is False:
                break
        _cursor._imprint_internal_state(self)

    def to_list(
        self,
        *,
        general_method_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[T]:
        """
        Materialize all rows that remain to be consumed from a cursor into a list.

        Calling this method on a CLOSED cursor results in an error.

        If the cursor is IDLE, the result will be the whole set of rows returned
        by the `find` operation; otherwise, the rows already consumed by the cursor
        will not be in the resulting list.

        Calling this method is not recommended if a huge list of results is anticipated:
        it would involve a large number of data exchanges with the Data API and possibly
        a massive memory usage to construct the list. In such cases, a lazy pattern
        of iterating and consuming the rows is to be preferred.

        Args:
            general_method_timeout_ms: a timeout, in milliseconds, for the whole
                duration of this method. If not provided, there is no such timeout.
                Note that the per-request timeout set on the cursor still applies.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            list: a list of rows (or other values depending on the mapping
                function, if one is set). These are all items that were left
                to be consumed on the cursor when `to_list` is called.

        Example:
            >>> my_table.find(
            ...     {"match_id": "challenge6"},
            ...     projection={"winner": True},
            ...     limit=3,
            ... ).to_list()
            [{'winner': 'Donna'}, {'winner': 'Erick'}, {'winner': 'Fiona'}]
            >>>
            >>> cursor = my_table.find(
            ...     {"match_id": "challenge6"},
            ...     projection={"winner": True},
            ...     limit=5,
            ... ).map(lambda doc: doc["winner"])
            >>>
            >>> first_value = cursor.__next__()
            >>> cursor.to_list()
            ['Erick', 'Fiona', 'Georg', 'Helen']
        """

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
        self._imprint_internal_state(_cursor)
        documents = [document for document in _cursor]
        _cursor._imprint_internal_state(self)
        return documents

    def has_next(self) -> bool:
        """
        Whether the cursor actually has more documents to return.

        `has_next` can be called on any cursor, but on a CLOSED cursor
        will always return False.

        This method can trigger the fetch operation of a new page, if the current
        buffer is empty.

        Calling `has_next` on an IDLE cursor triggers the first page fetch, but the
        cursor stays in the IDLE state until actual consumption starts.

        Returns:
            a boolean value of True if there is at least one further item
                available to consume; False otherwise (including the case of CLOSED
                cursor).
        """

        if self._state == CursorState.CLOSED:
            return False
        self._try_ensure_fill_buffer()
        return len(self._buffer) > 0

    def get_sort_vector(self) -> list[float] | DataAPIVector | None:
        """
        Return the query vector used in the vector (ANN) search that originated
        this cursor, if applicable. If this is not an ANN search, or it was invoked
        without the `include_sort_vector` flag, return None.

        Calling `get_sort_vector` on an IDLE cursor triggers the first page fetch,
        but the cursor stays in the IDLE state until actual consumption starts.

        The method can be invoked on a CLOSED cursor and will return either None
        or the sort vector used in the search.

        Returns:
            the query vector used in the search if this was a
                vector search (otherwise None). The vector is returned either
                as a DataAPIVector or a plain list of number depending on the
                `APIOptions.serdes_options` that apply. The query vector is available
                also for vectorize-based ANN searches.
        """

        self._try_ensure_fill_buffer()
        if self._last_response_status:
            return _ensure_vector(
                self._last_response_status.get("sortVector"),
                self.data_source.api_options.serdes_options,
            )
        else:
            return None


class AsyncTableFindCursor(Generic[TRAW, T], AbstractCursor[TRAW]):
    """
    A synchronous cursor over rows, as returned by a `find` invocation on
    an AsyncTable. A cursor can be iterated over, materialized into a list,
    and queried/manipulated in various ways.

    Some cursor operations mutate it in-place (such as consuming its rows),
    other return a new cursor without changing the original one. See the documentation
    for the various methods and the AsyncTable `find` method for more details
    and usage patterns.

    A cursor has two type parameters: TRAW and T. The first is the type of the "raw"
    rows as they are obtained from the Data API, the second is the type of the
    items after the optional mapping function (see the `.map()` method). If there is
    no mapping, TRAW = T. In general, consuming a cursor returns items of type T,
    except for the `consume_buffer` primitive that draws directly from the buffer
    and always returns items of type TRAW.

    This class is the async counterpart of the TableFindCursor, for use with
    asyncio. Other than the async interface, its behavior is identical: please refer
    to the documentation for `TableFindCursor` for examples and details.
    """

    _query_engine: _TableFindQueryEngine[TRAW]
    _request_timeout_ms: int | None
    _overall_timeout_ms: int | None
    _request_timeout_label: str | None
    _overall_timeout_label: str | None
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
        request_timeout_label: str | None = None,
        overall_timeout_label: str | None = None,
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
        self._sort = deepcopy(sort)
        self._limit = limit
        self._include_similarity = include_similarity
        self._include_sort_vector = include_sort_vector
        self._skip = skip
        self._mapper = mapper
        self._request_timeout_ms = request_timeout_ms
        self._overall_timeout_ms = overall_timeout_ms
        self._request_timeout_label = request_timeout_label
        self._overall_timeout_label = overall_timeout_label
        self._query_engine = _TableFindQueryEngine(
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
        AbstractCursor.__init__(self)
        self._timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=self._overall_timeout_ms,
            timeout_label=self._overall_timeout_label,
        )

    def _copy(
        self: AsyncTableFindCursor[TRAW, T],
        *,
        request_timeout_ms: int | None | UnsetType = _UNSET,
        overall_timeout_ms: int | None | UnsetType = _UNSET,
        request_timeout_label: str | None | UnsetType = _UNSET,
        overall_timeout_label: str | None | UnsetType = _UNSET,
        filter: FilterType | None | UnsetType = _UNSET,
        projection: ProjectionType | None | UnsetType = _UNSET,
        sort: dict[str, Any] | None | UnsetType = _UNSET,
        limit: int | None | UnsetType = _UNSET,
        include_similarity: bool | None | UnsetType = _UNSET,
        include_sort_vector: bool | None | UnsetType = _UNSET,
        skip: int | None | UnsetType = _UNSET,
    ) -> AsyncTableFindCursor[TRAW, T]:
        if self._query_engine.async_table is None:
            raise RuntimeError("Query engine has no async table.")
        return AsyncTableFindCursor(
            table=self._query_engine.async_table,
            request_timeout_ms=self._request_timeout_ms
            if isinstance(request_timeout_ms, UnsetType)
            else request_timeout_ms,
            overall_timeout_ms=self._overall_timeout_ms
            if isinstance(overall_timeout_ms, UnsetType)
            else overall_timeout_ms,
            request_timeout_label=self._request_timeout_label
            if isinstance(request_timeout_label, UnsetType)
            else request_timeout_label,
            overall_timeout_label=self._overall_timeout_label
            if isinstance(overall_timeout_label, UnsetType)
            else overall_timeout_label,
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

    async def _try_ensure_fill_buffer(self) -> None:
        """
        If buffer is empty, try to fill with next page, if applicable.
        If not possible, silently do nothing.
        This method never changes the cursor state.
        """

        if self._state == CursorState.CLOSED:
            return
        if not self._buffer:
            if self._next_page_state is not None or self._state == CursorState.IDLE:
                (
                    new_buffer,
                    next_page_state,
                    resp_status,
                ) = await self._query_engine._async_fetch_page(
                    page_state=self._next_page_state,
                    timeout_context=self._timeout_manager.remaining_timeout(
                        cap_time_ms=self._request_timeout_ms,
                        cap_timeout_label=self._request_timeout_label,
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
        if self._state == CursorState.CLOSED:
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
        """
        The AsyncTable object that originated this cursor through a `find` operation.

        Returns:
            an AsyncTable instance.
        """

        if self._query_engine.async_table is None:
            raise RuntimeError("Query engine has no async table.")
        return self._query_engine.async_table

    def clone(self) -> AsyncTableFindCursor[TRAW, T]:
        """
        Create a copy of this cursor with:
        - the same parameters (timeouts, filter, projection, etc)
        - and the cursor is rewound to its pristine IDLE state.

        For usage examples, please refer to the same method of the
        equivalent synchronous TableFindCursor class, and apply the necessary
        adaptations to the async interface.

        Returns:
            a new AsyncTableFindCursor, similar to this one but
            rewound to its initial state.
        """

        if self._query_engine.async_table is None:
            raise RuntimeError("Query engine has no async table.")
        return AsyncTableFindCursor(
            table=self._query_engine.async_table,
            request_timeout_ms=self._request_timeout_ms,
            overall_timeout_ms=self._overall_timeout_ms,
            request_timeout_label=self._request_timeout_label,
            overall_timeout_label=self._overall_timeout_label,
            filter=self._filter,
            projection=self._projection,
            sort=self._sort,
            limit=self._limit,
            include_similarity=self._include_similarity,
            include_sort_vector=self._include_sort_vector,
            skip=self._skip,
            mapper=self._mapper,
        )

    def filter(self, filter: FilterType | None) -> AsyncTableFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new filter setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the AsyncTable `find` method.

        Args:
            filter: a new filter setting to apply to the returned new cursor.

        Returns:
            a new AsyncTableFindCursor with the same settings as this one,
                except for `filter` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(filter=filter)

    def project(
        self, projection: ProjectionType | None
    ) -> AsyncTableFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new projection setting.
        This operation is allowed only if the cursor state is still IDLE and if
        no mapping has been set on it.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the AsyncTable `find` method.

        Args:
            projection: a new projection setting to apply to the returned new cursor.

        Returns:
            a new AsyncTableFindCursor with the same settings as this one,
                except for `projection` which is the provided value.
        """

        self._ensure_idle()
        if self._mapper is not None:
            raise CursorException(
                "Cannot set projection after map.",
                cursor_state=self._state.value,
            )
        return self._copy(projection=projection)

    def sort(self, sort: dict[str, Any] | None) -> AsyncTableFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new sort setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the AsyncTable `find` method.

        Args:
            sort: a new sort setting to apply to the returned new cursor.

        Returns:
            a new AsyncTableFindCursor with the same settings as this one,
                except for `sort` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(sort=sort)

    def limit(self, limit: int | None) -> AsyncTableFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new limit setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the AsyncTable `find` method.

        Args:
            limit: a new limit setting to apply to the returned new cursor.

        Returns:
            a new AsyncTableFindCursor with the same settings as this one,
                except for `limit` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(limit=limit)

    def include_similarity(
        self, include_similarity: bool | None
    ) -> AsyncTableFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new include_similarity setting.
        This operation is allowed only if the cursor state is still IDLE and if
        no mapping has been set on it.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the AsyncTable `find` method.

        Args:
            include_similarity: a new include_similarity setting to apply
                to the returned new cursor.

        Returns:
            a new AsyncTableFindCursor with the same settings as this one,
                except for `include_similarity` which is the provided value.
        """

        self._ensure_idle()
        if self._mapper is not None:
            raise CursorException(
                "Cannot set include_similarity after map.",
                cursor_state=self._state.value,
            )
        return self._copy(include_similarity=include_similarity)

    def include_sort_vector(
        self, include_sort_vector: bool | None
    ) -> AsyncTableFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new include_sort_vector setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the AsyncTable `find` method.

        Args:
            include_sort_vector: a new include_sort_vector setting to apply
                to the returned new cursor.

        Returns:
            a new AsyncTableFindCursor with the same settings as this one,
                except for `include_sort_vector` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(include_sort_vector=include_sort_vector)

    def skip(self, skip: int | None) -> AsyncTableFindCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new skip setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the AsyncTable `find` method.

        Args:
            skip: a new skip setting to apply to the returned new cursor.

        Returns:
            a new AsyncTableFindCursor with the same settings as this one,
                except for `skip` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(skip=skip)

    def map(self, mapper: Callable[[T], TNEW]) -> AsyncTableFindCursor[TRAW, TNEW]:
        """
        Return a copy of this cursor with a mapping function to transform
        the returned items. Calling this method on a cursor with a mapping
        already set results in the mapping functions being composed.

        This operation is allowed only if the cursor state is still IDLE.

        For usage examples, please refer to the same method of the
        equivalent synchronous TableFindCursor class, and apply the necessary
        adaptations to the async interface.

        Args:
            mapper: a function transforming the objects returned by the cursor
                into something else (i.e. a function T => TNEW).

        Returns:
            a new AsyncTableFindCursor with a new mapping function on the results,
                possibly composed with any pre-existing mapping function.
        """

        self._ensure_idle()
        if self._query_engine.async_table is None:
            raise RuntimeError("Query engine has no async table.")
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
            request_timeout_label=self._request_timeout_label,
            overall_timeout_label=self._overall_timeout_label,
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
        function: Callable[[T], bool | None] | Callable[[T], Awaitable[bool | None]],
        *,
        general_method_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        """
        Consume the remaining rows in the cursor, invoking a provided callback
        function -- or coroutine -- on each of them.

        Calling this method on a CLOSED cursor results in an error.

        The callback function can return any value. The return value is generally
        discarded, with the following exception: if the function returns the boolean
        `False`, it is taken to signify that the method should quit early, leaving the
        cursor half-consumed (ACTIVE state). If this does not occur, this method
        results in the cursor entering CLOSED state once it is exhausted.

        For usage examples, please refer to the same method of the
        equivalent synchronous TableFindCursor class, and apply the necessary
        adaptations to the async interface.

        Args:
            function: a callback function, or a coroutine, whose only parameter is of
                the type returned by the cursor.
                This callback is invoked once per each row yielded
                by the cursor. If the callback returns a `False`, the `for_each`
                invocation stops early and returns without consuming further rows.
            general_method_timeout_ms: a timeout, in milliseconds, for the whole
                duration of this method. If not provided, there is no such timeout.
                Note that the per-request timeout set on the cursor still applies.
            timeout_ms: an alias for `general_method_timeout_ms`.
        """

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
        self._imprint_internal_state(_cursor)
        is_coro = iscoroutinefunction(function)
        async for row in _cursor:
            if is_coro:
                res = await function(row)  # type: ignore[misc]
            else:
                res = function(row)
            if res is False:
                break
        _cursor._imprint_internal_state(self)

    async def to_list(
        self,
        *,
        general_method_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[T]:
        """
        Materialize all rows that remain to be consumed from a cursor into a list.

        Calling this method on a CLOSED cursor results in an error.

        If the cursor is IDLE, the result will be the whole set of rows returned
        by the `find` operation; otherwise, the rows already consumed by the cursor
        will not be in the resulting list.

        Calling this method is not recommended if a huge list of results is anticipated:
        it would involve a large number of data exchanges with the Data API and possibly
        a massive memory usage to construct the list. In such cases, a lazy pattern
        of iterating and consuming the rows is to be preferred.

        For usage examples, please refer to the same method of the
        equivalent synchronous TableFindCursor class, and apply the necessary
        adaptations to the async interface.

        Args:
            general_method_timeout_ms: a timeout, in milliseconds, for the whole
                duration of this method. If not provided, there is no such timeout.
                Note that the per-request timeout set on the cursor still applies.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            list: a list of rows (or other values depending on the mapping
                function, if one is set). These are all items that were left
                to be consumed on the cursor when `to_list` is called.
        """

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
        self._imprint_internal_state(_cursor)
        documents = [document async for document in _cursor]
        _cursor._imprint_internal_state(self)
        return documents

    async def has_next(self) -> bool:
        """
        Whether the cursor actually has more documents to return.

        `has_next` can be called on any cursor, but on a CLOSED cursor
        will always return False.

        This method can trigger the fetch operation of a new page, if the current
        buffer is empty.

        Calling `has_next` on an IDLE cursor triggers the first page fetch, but the
        cursor stays in the IDLE state until actual consumption starts.

        Returns:
            a boolean value of True if there is at least one further item
                available to consume; False otherwise (including the case of CLOSED
                cursor).
        """

        if self._state == CursorState.CLOSED:
            return False
        await self._try_ensure_fill_buffer()
        return len(self._buffer) > 0

    async def get_sort_vector(self) -> list[float] | DataAPIVector | None:
        """
        Return the query vector used in the vector (ANN) search that originated
        this cursor, if applicable. If this is not an ANN search, or it was invoked
        without the `include_sort_vector` flag, return None.

        Calling `get_sort_vector` on an IDLE cursor triggers the first page fetch,
        but the cursor stays in the IDLE state until actual consumption starts.

        The method can be invoked on a CLOSED cursor and will return either None
        or the sort vector used in the search.

        Returns:
            the query vector used in the search if this was a
                vector search (otherwise None). The vector is returned either
                as a DataAPIVector or a plain list of number depending on the
                `APIOptions.serdes_options` that apply. The query vector is available
                also for vectorize-based ANN searches.
        """

        await self._try_ensure_fill_buffer()
        if self._last_response_status:
            return _ensure_vector(
                self._last_response_status.get("sortVector"),
                self.data_source.api_options.serdes_options,
            )
        else:
            return None
