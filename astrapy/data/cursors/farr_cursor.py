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

from astrapy import AsyncCollection, Collection
from astrapy.constants import (
    FilterType,
    HybridSortType,
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
from astrapy.data.cursors.query_engine import _CollectionFindAndRerankQueryEngine
from astrapy.data.cursors.reranked_result import RerankedResult
from astrapy.data_types import DataAPIVector
from astrapy.exceptions import (
    CursorException,
    MultiCallTimeoutManager,
)
from astrapy.utils.unset import _UNSET, UnsetType


class CollectionFindAndRerankCursor(
    Generic[TRAW, T], AbstractCursor[RerankedResult[TRAW]]
):
    """
    A synchronous cursor over documents, as returned by a `find_and_rerank` invocation
    on a Collection. A cursor can be iterated over, materialized into a list,
    and queried/manipulated in various ways.

    Some cursor operations mutate it in-place (such as consuming its documents),
    other return a new cursor without changing the original one. See the documentation
    for the various methods and the Collection `find_and_rerank` method for more details
    and usage patterns.

    This cursor has two type parameters: TRAW and T. The first is the type
    of the "raw" documents as they are found on the collection, the second
    is the type of the items after the optional mapping function (see the `.map()`
    method).
    If no mapping is specified, `T = RerankedResult[TRAW]`: the items yielded by
    the cursor are a `RerankedResult` wrapping the type (possibly after projection)
    of the documents found on the collection: in other words, such a cursor returns
    the documents, as they come back from the API, with their associated scores
    from the find-and-rerank operation.
    In general, consuming a cursor returns items of type T, except for the
    `consume_buffer` primitive that draws directly from the buffer and always
    returns items of type RerankedResult[TRAW].

    Example:
        >>> # (this assumes 'vectorize'. See `Collection.find_and_rerank` for more.)
        >>> cursor = collection.find_and_rerank(
        ...     sort={"$hybrid": "Weekdays?"},
        ...     projection={"wkd": True},
        ...     limit=5,
        ...     include_scores=True,
        ... )
        >>> for r_result in cursor:
        ...     print(f"{r_result.document['wkd']}: {r_result.scores['$rerank']}")
        ...
        Wed: -9.1015625
        Mon: -10.2421875
        Tue: -10.2421875
        Sun: -11.375
        Fri: -12.515625
    """

    _query_engine: _CollectionFindAndRerankQueryEngine[TRAW]
    _request_timeout_ms: int | None
    _overall_timeout_ms: int | None
    _request_timeout_label: str | None
    _overall_timeout_label: str | None
    _timeout_manager: MultiCallTimeoutManager
    _filter: FilterType | None
    _projection: ProjectionType | None
    _sort: HybridSortType | None
    _limit: int | None
    _hybrid_limits: int | dict[str, int] | None
    _include_scores: bool | None
    _include_sort_vector: bool | None
    _rerank_on: str | None
    _rerank_query: str | None
    _mapper: Callable[[RerankedResult[TRAW]], T] | None

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
        sort: HybridSortType | None = None,
        limit: int | None = None,
        hybrid_limits: int | dict[str, int] | None = None,
        include_scores: bool | None = None,
        include_sort_vector: bool | None = None,
        rerank_on: str | None = None,
        rerank_query: str | None = None,
        mapper: Callable[[RerankedResult[TRAW]], T] | None = None,
    ) -> None:
        self._filter = deepcopy(filter)
        self._projection = projection
        self._sort = deepcopy(sort)
        self._limit = limit
        self._hybrid_limits = deepcopy(hybrid_limits)
        self._include_scores = include_scores
        self._include_sort_vector = include_sort_vector
        self._rerank_on = rerank_on
        self._rerank_query = rerank_query
        self._mapper = mapper
        self._request_timeout_ms = request_timeout_ms
        self._overall_timeout_ms = overall_timeout_ms
        self._request_timeout_label = request_timeout_label
        self._overall_timeout_label = overall_timeout_label
        self._query_engine = _CollectionFindAndRerankQueryEngine(
            collection=collection,
            async_collection=None,
            filter=self._filter,
            projection=self._projection,
            sort=self._sort,
            limit=self._limit,
            hybrid_limits=self._hybrid_limits,
            include_scores=self._include_scores,
            include_sort_vector=self._include_sort_vector,
            rerank_on=self._rerank_on,
            rerank_query=self._rerank_query,
        )
        AbstractCursor.__init__(self)
        self._timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=self._overall_timeout_ms,
            timeout_label=self._overall_timeout_label,
        )

    def _copy(
        self: CollectionFindAndRerankCursor[TRAW, T],
        *,
        request_timeout_ms: int | None | UnsetType = _UNSET,
        overall_timeout_ms: int | None | UnsetType = _UNSET,
        request_timeout_label: str | None | UnsetType = _UNSET,
        overall_timeout_label: str | None | UnsetType = _UNSET,
        filter: FilterType | None | UnsetType = _UNSET,
        projection: ProjectionType | None | UnsetType = _UNSET,
        sort: dict[str, Any] | None | UnsetType = _UNSET,
        limit: int | None | UnsetType = _UNSET,
        hybrid_limits: int | dict[str, int] | None | UnsetType = _UNSET,
        include_scores: bool | None | UnsetType = _UNSET,
        include_sort_vector: bool | None | UnsetType = _UNSET,
        rerank_on: str | None | UnsetType = _UNSET,
        rerank_query: str | None | UnsetType = _UNSET,
    ) -> CollectionFindAndRerankCursor[TRAW, T]:
        if self._query_engine.collection is None:
            raise RuntimeError("Query engine has no collection.")
        return CollectionFindAndRerankCursor(
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
            hybrid_limits=self._hybrid_limits
            if isinstance(hybrid_limits, UnsetType)
            else hybrid_limits,
            include_scores=self._include_scores
            if isinstance(include_scores, UnsetType)
            else include_scores,
            include_sort_vector=self._include_sort_vector
            if isinstance(include_sort_vector, UnsetType)
            else include_sort_vector,
            rerank_on=self._rerank_on
            if isinstance(rerank_on, UnsetType)
            else rerank_on,
            rerank_query=self._rerank_query
            if isinstance(rerank_query, UnsetType)
            else rerank_query,
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

    def __iter__(
        self: CollectionFindAndRerankCursor[TRAW, T],
    ) -> CollectionFindAndRerankCursor[TRAW, T]:
        self._ensure_alive()
        return self

    def __next__(self) -> T:
        if self.state == CursorState.CLOSED:
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
        The Collection object that originated this cursor through a `find_and_rerank`
        operation.

        Returns:
            a Collection instance.
        """

        if self._query_engine.collection is None:
            raise RuntimeError("Query engine has no collection.")
        return self._query_engine.collection

    def clone(self) -> CollectionFindAndRerankCursor[TRAW, T]:
        """
        Create a copy of this cursor with:
        - the same parameters (timeouts, filter, projection, etc)
        - and the cursor is rewound to its pristine IDLE state.

        Returns:
            a new CollectionFindAndRerankCursor, similar to this one but without mapping
            and rewound to its initial state.

        Example:
            >>> # (this assumes 'vectorize'. See `Collection.find_and_rerank` for more.)
            >>> cursor = collection.find_and_rerank(
            ...     sort={"$hybrid": "Weekdays?"},
            ...     projection={"wkd": True},
            ...     limit=3,
            ... ).map(lambda r_result: r_result.document["wkd"].upper())
            >>> for idx, value in zip([0, 1], cursor):
            ...     print(f"{idx} ==> {value}")
            ...
            0 ==> MON
            1 ==> TUE
            >>> cloned_cursor = cursor.clone()
            >>> for value in cloned_cursor:
            ...     print(f"(cloned) {value}")
            ...
            (cloned) MON
            (cloned) TUE
            (cloned) SUN
            >>>
            >>> print(f"n ==> {next(cursor)}")
            n ==> SUN
        """

        if self._query_engine.collection is None:
            raise RuntimeError("Query engine has no collection.")
        return CollectionFindAndRerankCursor(
            collection=self._query_engine.collection,
            request_timeout_ms=self._request_timeout_ms,
            overall_timeout_ms=self._overall_timeout_ms,
            request_timeout_label=self._request_timeout_label,
            overall_timeout_label=self._overall_timeout_label,
            filter=self._filter,
            projection=self._projection,
            sort=self._sort,
            limit=self._limit,
            hybrid_limits=self._hybrid_limits,
            include_scores=self._include_scores,
            include_sort_vector=self._include_sort_vector,
            rerank_on=self._rerank_on,
            rerank_query=self._rerank_query,
            mapper=self._mapper,
        )

    def filter(
        self, filter: FilterType | None
    ) -> CollectionFindAndRerankCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new filter setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the Collection `find_and_rerank` method.

        Args:
            filter: a new filter setting to apply to the returned new cursor.

        Returns:
            a new CollectionFindAndRerankCursor with the same settings as this one,
                except for `filter` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(filter=filter)

    def project(
        self, projection: ProjectionType | None
    ) -> CollectionFindAndRerankCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new projection setting.
        This operation is allowed only if the cursor state is still IDLE and if
        no mapping has been set on it.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the Collection `find_and_rerank` method.

        Args:
            projection: a new projection setting to apply to the returned new cursor.

        Returns:
            a new CollectionFindAndRerankCursor with the same settings as this one,
                except for `projection` which is the provided value.
        """

        self._ensure_idle()
        if self._mapper is not None:
            raise CursorException(
                "Cannot set projection after map.",
                cursor_state=self._state.value,
            )
        return self._copy(projection=projection)

    def sort(
        self, sort: HybridSortType | None
    ) -> CollectionFindAndRerankCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new sort setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the Collection `find_and_rerank` method.

        Args:
            sort: a new sort setting to apply to the returned new cursor.

        Returns:
            a new CollectionFindAndRerankCursor with the same settings as this one,
                except for `sort` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(sort=sort)

    def limit(self, limit: int | None) -> CollectionFindAndRerankCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new limit setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the Collection `find_and_rerank` method.

        Args:
            limit: a new limit setting to apply to the returned new cursor.

        Returns:
            a new CollectionFindAndRerankCursor with the same settings as this one,
                except for `limit` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(limit=limit)

    def hybrid_limits(
        self, hybrid_limits: int | dict[str, int] | None
    ) -> CollectionFindAndRerankCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new hybrid_limits setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the Collection `find_and_rerank` method.

        Args:
            hybrid_limits: a new setting to apply to the returned new cursor.

        Returns:
            a new CollectionFindAndRerankCursor with the same settings as this one,
                except for `hybrid_limits` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(hybrid_limits=hybrid_limits)

    def include_scores(
        self, include_scores: bool | None
    ) -> CollectionFindAndRerankCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new include_scores setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the Collection `find_and_rerank` method.

        Args:
            include_scores: a new include_scores setting to apply
                to the returned new cursor.

        Returns:
            a new CollectionFindAndRerankCursor with the same settings as this one,
                except for `include_scores` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(include_scores=include_scores)

    def include_sort_vector(
        self, include_sort_vector: bool | None
    ) -> CollectionFindAndRerankCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new include_sort_vector setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the Collection `find_and_rerank` method.

        Args:
            include_sort_vector: a new include_sort_vector setting to apply
                to the returned new cursor.

        Returns:
            a new CollectionFindAndRerankCursor with the same settings as this one,
                except for `include_sort_vector` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(include_sort_vector=include_sort_vector)

    def rerank_on(
        self, rerank_on: str | None
    ) -> CollectionFindAndRerankCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new rerank_on setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the Collection `find_and_rerank` method.

        Args:
            rerank_on: a new setting to apply to the returned new cursor.

        Returns:
            a new CollectionFindAndRerankCursor with the same settings as this one,
                except for `rerank_on` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(rerank_on=rerank_on)

    def rerank_query(
        self, rerank_query: str | None
    ) -> CollectionFindAndRerankCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new rerank_query setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the Collection `find_and_rerank` method.

        Args:
            rerank_query: a new setting to apply to the returned new cursor.

        Returns:
            a new CollectionFindAndRerankCursor with the same settings as this one,
                except for `rerank_query` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(rerank_query=rerank_query)

    def map(
        self, mapper: Callable[[T], TNEW]
    ) -> CollectionFindAndRerankCursor[TRAW, TNEW]:
        """
        Return a copy of this cursor with a mapping function to transform
        the returned items. Calling this method on a cursor with a mapping
        already set results in the mapping functions being composed.

        This operation is allowed only if the cursor state is still IDLE.

        Args:
            mapper: a function transforming the objects returned by the cursor
                into something else (i.e. a function T => TNEW).
                If the map is imposed on a cursor without mapping yet, its input
                argument must be a `RerankedResult[TRAW]`, where TRAW
                stands for the type of the documents from the collection.

        Returns:
            a new CollectionFindAndRerankCursor with a new mapping function on the results,
                possibly composed with any pre-existing mapping function.

        Example:
            >>> # (this assumes 'vectorize'. See `Collection.find_and_rerank` for more.)
            >>> cursor = collection.find_and_rerank(
            ...     sort={"$hybrid": "Weekdays?"},
            ...     projection={"wkd": True},
            ...     limit=3,
            ... )
            >>> for r_result in cursor:
            ...     print(r_result.document)
            ...
            {'_id': 'A', 'wkd': 'Mon'}
            {'_id': 'B', 'wkd': 'Tue'}
            {'_id': 'G', 'wkd': 'Sun'}
            >>> cursor_mapped = cursor.clone().map(
            ...     lambda r_result: r_result.document["wkd"]
            ... )
            >>> for value in cursor_mapped:
            ...     print(value)
            ...
            Mon
            Tue
            Sun
            >>> cursor_mapped_twice = cursor_mapped.clone().map(
            ...     lambda wkd: f"<{wkd[:2].lower()}>"
            ... )
            >>> for value in cursor_mapped_twice:
            ...     print(value)
            ...
            <mo>
            <tu>
            <su>
        """
        self._ensure_idle()
        if self._query_engine.collection is None:
            raise RuntimeError("Query engine has no collection.")
        composite_mapper: Callable[[RerankedResult[TRAW]], TNEW]
        if self._mapper is not None:

            def _composite(document: RerankedResult[TRAW]) -> TNEW:
                return mapper(self._mapper(document))  # type: ignore[misc]

            composite_mapper = _composite
        else:
            composite_mapper = cast(Callable[[RerankedResult[TRAW]], TNEW], mapper)
        return CollectionFindAndRerankCursor(
            collection=self._query_engine.collection,
            request_timeout_ms=self._request_timeout_ms,
            overall_timeout_ms=self._overall_timeout_ms,
            request_timeout_label=self._request_timeout_label,
            overall_timeout_label=self._overall_timeout_label,
            filter=self._filter,
            projection=self._projection,
            sort=self._sort,
            limit=self._limit,
            hybrid_limits=self._hybrid_limits,
            include_scores=self._include_scores,
            include_sort_vector=self._include_sort_vector,
            rerank_on=self._rerank_on,
            rerank_query=self._rerank_query,
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
            >>> # (this assumes 'vectorize'. See `Collection.find_and_rerank` for more.)
            >>> from astrapy.cursors import CursorState, RerankedResult
            >>>
            >>> cursor = collection.find_and_rerank(
            ...     sort={"$hybrid": "Weekdays?"},
            ...     projection={"wkd": True},
            ...     limit=3,
            ... )
            >>> def printer(r_result: RerankedResult):
            ...     print(f"-> {r_result.document['wkd']}")
            ...
            >>> cursor.for_each(printer)
            -> Mon
            -> Tue
            -> Sun
            >>>
            >>> if cursor.state != CursorState.CLOSED:
            ...     print(f"alive: {cursor.to_list()}")
            ... else:
            ...     print("(closed)")
            ...
            (closed)
            >>> cursor2 = cursor.clone()
            >>> def checker(r_result: RerankedResult):
            ...     print(f"-> {r_result.document['wkd']}")
            ...     return r_result.document["wkd"] != "Tue"
            ...
            >>> cursor2.for_each(checker)
            -> Mon
            -> Tue
            >>>
            >>> if cursor2.state != CursorState.CLOSED:
            ...     print(f"alive: {list(cursor2)}")
            ... else:
            ...     print("(closed)")
            ...
            alive: [RerankedResult(document={'_id': 'G', 'wkd': 'Sun'}, scores={})]
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
        by the `find_and_rerank` operation; otherwise, the documents already consumed
        by the cursor will not be in the resulting list.

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
            >>> # (this assumes 'vectorize'. See `Collection.find_and_rerank` for more.)
            >>> collection.find_and_rerank(
            ...     sort={"$hybrid": "Weekdays?"},
            ...     projection={"wkd": True},
            ...     limit=4,
            ... ).map(
            ...     lambda r_result: r_result.document["wkd"]
            ... ).to_list()
            ['Wed', 'Mon', 'Tue', 'Sun']
            >>>
            >>> cursor = collection.find_and_rerank(
            ...     sort={"$hybrid": "Weekdays?"},
            ...     projection={"wkd": True},
            ...     limit=4,
            ... ).map(lambda r_result: r_result.document["wkd"])
            >>> print(f"First item: {cursor.__next__()}.")
            First item: Wed.
            >>> cursor.to_list()
            ['Mon', 'Tue', 'Sun']
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
        Return the query vector used in the vector (ANN) search that was run as
        part of the search expressed by this cursor, if applicable.

        Calling `get_sort_vector` on an IDLE cursor triggers the first page fetch,
        but the cursor stays in the IDLE state until actual consumption starts.

        The method can be invoked on a CLOSED cursor and will return either None
        or the sort vector used in the search.

        Returns:
            the query vector used in the search, if it was requested by passing
                `include_sort_vector=True` to the `find_and_rerank` call that originated
                the cursor.
                If the sort vector is not available, None is returned.
                Otherwise, the vector is returned as either a DataAPIVector
                or a plain list of number depending on the setting for
                `APIOptions.serdes_options`.
        """

        self._try_ensure_fill_buffer()
        if self._last_response_status:
            return _ensure_vector(
                self._last_response_status.get("sortVector"),
                self.data_source.api_options.serdes_options,
            )
        else:
            return None


class AsyncCollectionFindAndRerankCursor(
    Generic[TRAW, T], AbstractCursor[RerankedResult[TRAW]]
):
    """
    An asynchronous cursor over documents, as returned by a `find_and_rerank` invocation
    on an AsyncCollection. A cursor can be iterated over, materialized into a list,
    and queried/manipulated in various ways.

    Some cursor operations mutate it in-place (such as consuming its documents),
    other return a new cursor without changing the original one. See the documentation
    for the various methods and the AsyncCollection `find_and_rerank` method for more
    details and usage patterns.

    This cursor has two type parameters: TRAW and T. The first is the type
    of the "raw" documents as they are found on the collection, the second
    is the type of the items after the optional mapping function (see the `.map()`
    method).
    If no mapping is specified, `T = RerankedResult[TRAW]`: the items yielded by
    the cursor are a `RerankedResult` wrapping the type (possibly after projection)
    of the documents found on the collection: in other words, such a cursor returns
    the documents, as they come back from the API, with their associated scores
    from the find-and-rerank operation.
    In general, consuming a cursor returns items of type T, except for the
    `consume_buffer` primitive that draws directly from the buffer and always
    returns items of type RerankedResult[TRAW].

    This class is the async counterpart of the CollectionFindAndRerankCursor, for use
    with asyncio. Other than the async interface, its behavior is identical: please
    refer to the documentation for `CollectionFindAndRerankCursor` for examples
    and details.
    """

    _query_engine: _CollectionFindAndRerankQueryEngine[TRAW]
    _request_timeout_ms: int | None
    _overall_timeout_ms: int | None
    _request_timeout_label: str | None
    _overall_timeout_label: str | None
    _timeout_manager: MultiCallTimeoutManager
    _filter: FilterType | None
    _projection: ProjectionType | None
    _sort: HybridSortType | None
    _limit: int | None
    _hybrid_limits: int | dict[str, int] | None
    _include_scores: bool | None
    _include_sort_vector: bool | None
    _rerank_on: str | None
    _rerank_query: str | None
    _mapper: Callable[[RerankedResult[TRAW]], T] | None

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
        sort: HybridSortType | None = None,
        limit: int | None = None,
        hybrid_limits: int | dict[str, int] | None = None,
        include_scores: bool | None = None,
        include_sort_vector: bool | None = None,
        rerank_on: str | None = None,
        rerank_query: str | None = None,
        mapper: Callable[[RerankedResult[TRAW]], T] | None = None,
    ) -> None:
        self._filter = deepcopy(filter)
        self._projection = projection
        self._sort = deepcopy(sort)
        self._limit = limit
        self._hybrid_limits = deepcopy(hybrid_limits)
        self._include_scores = include_scores
        self._include_sort_vector = include_sort_vector
        self._rerank_on = rerank_on
        self._rerank_query = rerank_query
        self._mapper = mapper
        self._request_timeout_ms = request_timeout_ms
        self._overall_timeout_ms = overall_timeout_ms
        self._request_timeout_label = request_timeout_label
        self._overall_timeout_label = overall_timeout_label
        self._query_engine = _CollectionFindAndRerankQueryEngine(
            collection=None,
            async_collection=collection,
            filter=self._filter,
            projection=self._projection,
            sort=self._sort,
            limit=self._limit,
            hybrid_limits=self._hybrid_limits,
            include_scores=self._include_scores,
            include_sort_vector=self._include_sort_vector,
            rerank_on=self._rerank_on,
            rerank_query=self._rerank_query,
        )
        AbstractCursor.__init__(self)
        self._timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=self._overall_timeout_ms,
            timeout_label=self._overall_timeout_label,
        )

    def _copy(
        self: AsyncCollectionFindAndRerankCursor[TRAW, T],
        *,
        request_timeout_ms: int | None | UnsetType = _UNSET,
        overall_timeout_ms: int | None | UnsetType = _UNSET,
        request_timeout_label: str | None | UnsetType = _UNSET,
        overall_timeout_label: str | None | UnsetType = _UNSET,
        filter: FilterType | None | UnsetType = _UNSET,
        projection: ProjectionType | None | UnsetType = _UNSET,
        sort: dict[str, Any] | None | UnsetType = _UNSET,
        limit: int | None | UnsetType = _UNSET,
        hybrid_limits: int | dict[str, int] | None | UnsetType = _UNSET,
        include_scores: bool | None | UnsetType = _UNSET,
        include_sort_vector: bool | None | UnsetType = _UNSET,
        rerank_on: str | None | UnsetType = _UNSET,
        rerank_query: str | None | UnsetType = _UNSET,
    ) -> AsyncCollectionFindAndRerankCursor[TRAW, T]:
        if self._query_engine.async_collection is None:
            raise RuntimeError("Query engine has no async collection.")
        return AsyncCollectionFindAndRerankCursor(
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
            hybrid_limits=self._hybrid_limits
            if isinstance(hybrid_limits, UnsetType)
            else hybrid_limits,
            include_scores=self._include_scores
            if isinstance(include_scores, UnsetType)
            else include_scores,
            include_sort_vector=self._include_sort_vector
            if isinstance(include_sort_vector, UnsetType)
            else include_sort_vector,
            rerank_on=self._rerank_on
            if isinstance(rerank_on, UnsetType)
            else rerank_on,
            rerank_query=self._rerank_query
            if isinstance(rerank_query, UnsetType)
            else rerank_query,
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
        self: AsyncCollectionFindAndRerankCursor[TRAW, T],
    ) -> AsyncCollectionFindAndRerankCursor[TRAW, T]:
        self._ensure_alive()
        return self

    async def __anext__(self) -> T:
        if self.state == CursorState.CLOSED:
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
        The AsyncCollection object that originated this cursor through a
        `find_and_rerank` operation.

        Returns:
            an AsyncCollection instance.
        """

        if self._query_engine.async_collection is None:
            raise RuntimeError("Query engine has no async collection.")
        return self._query_engine.async_collection

    def clone(self) -> AsyncCollectionFindAndRerankCursor[TRAW, T]:
        """
        Create a copy of this cursor with:
        - the same parameters (timeouts, filter, projection, etc)
        - and the cursor is rewound to its pristine IDLE state.

        For usage examples, please refer to the same method of the
        equivalent synchronous CollectionFindCursor class, and apply the necessary
        adaptations to the async interface.

        Returns:
            a new AsyncCollectionFindAndRerankCursor, similar to this one but
            rewound to its initial state.
        """

        if self._query_engine.async_collection is None:
            raise RuntimeError("Query engine has no async collection.")
        return AsyncCollectionFindAndRerankCursor(
            collection=self._query_engine.async_collection,
            request_timeout_ms=self._request_timeout_ms,
            overall_timeout_ms=self._overall_timeout_ms,
            request_timeout_label=self._request_timeout_label,
            overall_timeout_label=self._overall_timeout_label,
            filter=self._filter,
            projection=self._projection,
            sort=self._sort,
            limit=self._limit,
            hybrid_limits=self._hybrid_limits,
            include_scores=self._include_scores,
            include_sort_vector=self._include_sort_vector,
            rerank_on=self._rerank_on,
            rerank_query=self._rerank_query,
            mapper=self._mapper,
        )

    def filter(
        self, filter: FilterType | None
    ) -> AsyncCollectionFindAndRerankCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new filter setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the AsyncCollection `find_and_rerank` method.

        Args:
            filter: a new filter setting to apply to the returned new cursor.

        Returns:
            a new AsyncCollectionFindAndRerankCursor with the same settings as this one,
                except for `filter` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(filter=filter)

    def project(
        self, projection: ProjectionType | None
    ) -> AsyncCollectionFindAndRerankCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new projection setting.
        This operation is allowed only if the cursor state is still IDLE and if
        no mapping has been set on it.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the AsyncCollection `find_and_rerank` method.

        Args:
            projection: a new projection setting to apply to the returned new cursor.

        Returns:
            a new AsyncCollectionFindAndRerankCursor with the same settings as this one,
                except for `projection` which is the provided value.
        """

        self._ensure_idle()
        if self._mapper is not None:
            raise CursorException(
                "Cannot set projection after map.",
                cursor_state=self._state.value,
            )
        return self._copy(projection=projection)

    def sort(
        self, sort: HybridSortType | None
    ) -> AsyncCollectionFindAndRerankCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new sort setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the AsyncCollection `find_and_rerank` method.

        Args:
            sort: a new sort setting to apply to the returned new cursor.

        Returns:
            a new AsyncCollectionFindAndRerankCursor with the same settings as this one,
                except for `sort` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(sort=sort)

    def limit(self, limit: int | None) -> AsyncCollectionFindAndRerankCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new limit setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the AsyncCollection `find_and_rerank` method.

        Args:
            limit: a new limit setting to apply to the returned new cursor.

        Returns:
            a new AsyncCollectionFindAndRerankCursor with the same settings as this one,
                except for `limit` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(limit=limit)

    def hybrid_limits(
        self, hybrid_limits: int | dict[str, int] | None
    ) -> AsyncCollectionFindAndRerankCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new hybrid_limits setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the AsyncCollection `find_and_rerank` method.

        Args:
            hybrid_limits: a new setting to apply to the returned new cursor.

        Returns:
            a new AsyncCollectionFindAndRerankCursor with the same settings as this one,
                except for `hybrid_limits` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(hybrid_limits=hybrid_limits)

    def include_scores(
        self, include_scores: bool | None
    ) -> AsyncCollectionFindAndRerankCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new include_scores setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the AsyncCollection `find_and_rerank` method.

        Args:
            include_scores: a new include_scores setting to apply
                to the returned new cursor.

        Returns:
            a new AsyncCollectionFindAndRerankCursor with the same settings as this one,
                except for `include_scores` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(include_scores=include_scores)

    def include_sort_vector(
        self, include_sort_vector: bool | None
    ) -> AsyncCollectionFindAndRerankCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new include_sort_vector setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the AsyncCollection `find_and_rerank` method.

        Args:
            include_sort_vector: a new include_sort_vector setting to apply
                to the returned new cursor.

        Returns:
            a new AsyncCollectionFindAndRerankCursor with the same settings as this one,
                except for `include_sort_vector` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(include_sort_vector=include_sort_vector)

    def rerank_on(
        self, rerank_on: str | None
    ) -> AsyncCollectionFindAndRerankCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new rerank_on setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the AsyncCollection `find_and_rerank` method.

        Args:
            rerank_on: a new setting to apply to the returned new cursor.

        Returns:
            a new AsyncCollectionFindAndRerankCursor with the same settings as this one,
                except for `rerank_on` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(rerank_on=rerank_on)

    def rerank_query(
        self, rerank_query: str | None
    ) -> AsyncCollectionFindAndRerankCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new rerank_query setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the AsyncCollection `find_and_rerank` method.

        Args:
            rerank_query: a new setting to apply to the returned new cursor.

        Returns:
            a new AsyncCollectionFindAndRerankCursor with the same settings as this one,
                except for `rerank_query` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(rerank_query=rerank_query)

    def map(
        self, mapper: Callable[[T], TNEW]
    ) -> AsyncCollectionFindAndRerankCursor[TRAW, TNEW]:
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
                If the map is imposed on a cursor without mapping yet, its input
                argument must be a `RerankedResult[TRAW]`, where TRAW
                stands for the type of the documents from the collection.

        Returns:
            a new AsyncCollectionFindAndRerankCursor with a new mapping function on the
                results, possibly composed with any pre-existing mapping function.
        """
        self._ensure_idle()
        if self._query_engine.async_collection is None:
            raise RuntimeError("Query engine has no async collection.")
        composite_mapper: Callable[[RerankedResult[TRAW]], TNEW]
        if self._mapper is not None:

            def _composite(document: RerankedResult[TRAW]) -> TNEW:
                return mapper(self._mapper(document))  # type: ignore[misc]

            composite_mapper = _composite
        else:
            composite_mapper = cast(Callable[[RerankedResult[TRAW]], TNEW], mapper)
        return AsyncCollectionFindAndRerankCursor(
            collection=self._query_engine.async_collection,
            request_timeout_ms=self._request_timeout_ms,
            overall_timeout_ms=self._overall_timeout_ms,
            request_timeout_label=self._request_timeout_label,
            overall_timeout_label=self._overall_timeout_label,
            filter=self._filter,
            projection=self._projection,
            sort=self._sort,
            limit=self._limit,
            hybrid_limits=self._hybrid_limits,
            include_scores=self._include_scores,
            include_sort_vector=self._include_sort_vector,
            rerank_on=self._rerank_on,
            rerank_query=self._rerank_query,
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
        by the `find_and_rerank` operation; otherwise, the documents already consumed
        by the cursor will not be in the resulting list.

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
        Return the query vector used in the vector (ANN) search that was run as
        part of the search expressed by this cursor, if applicable.

        Calling `get_sort_vector` on an IDLE cursor triggers the first page fetch,
        but the cursor stays in the IDLE state until actual consumption starts.

        The method can be invoked on a CLOSED cursor and will return either None
        or the sort vector used in the search.

        Returns:
            the query vector used in the search, if it was requested by passing
                `include_sort_vector=True` to the `find_and_rerank` call that originated
                the cursor.
                If the sort vector is not available, None is returned.
                Otherwise, the vector is returned as either a DataAPIVector
                or a plain list of number depending on the setting for
                `APIOptions.serdes_options`.
        """

        await self._try_ensure_fill_buffer()
        if self._last_response_status:
            return _ensure_vector(
                self._last_response_status.get("sortVector"),
                self.data_source.api_options.serdes_options,
            )
        else:
            return None
