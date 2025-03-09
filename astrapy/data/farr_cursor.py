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

from typing_extensions import override

from astrapy import AsyncCollection, Collection
from astrapy.constants import (
    FilterType,
    FindAndRerankSortType,
    ProjectionType,
    normalize_optional_projection,
)
from astrapy.data.cursor import (
    TNEW,
    TRAW,
    AbstractCursor,
    CursorState,
    T,
    _QueryEngine,
    _revise_timeouts_for_cursor_copy,
    logger,
)
from astrapy.data.utils.collection_converters import (
    postprocess_collection_response,
    preprocess_collection_payload,
)
from astrapy.exceptions import (
    CursorException,
    MultiCallTimeoutManager,
    UnexpectedDataAPIResponseException,
    _TimeoutContext,
)
from astrapy.utils.unset import _UNSET, UnsetType

# TODO: remove this (setting + function) once API testable
MOCK_FARR_API = True


def mock_farr_documents(pl: dict[str, Any] | None) -> list[dict[str, Any]]:
    return [
        {
            "_id": f"doc_{doc_i}",
            "pl": str(pl).replace(" ", ""),
            "$hybrid": {
                "passage": f"bla bla {doc_i}",
                "passageSource": "$vectorize",
                "scores": {
                    "$rerank": (6 - doc_i) / 8,
                    "$vector": (7 - doc_i) / 9,
                    "$lexical": (8 - doc_i) / 10,
                },
            },
        }
        for doc_i in range(5)
    ]


class _CollectionFindAndRerankQueryEngine(Generic[TRAW], _QueryEngine[TRAW]):
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
        sort: FindAndRerankSortType | None,
        limit: int | None,
        hybrid_limits: int | dict[str, int] | None,
        hybrid_projection: str | None,
        rerank_field: str | None,
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
                "hybridLimits": hybrid_limits if hybrid_limits != 0 else None,
                "rerankField": rerank_field,
                "hybridProjection": hybrid_projection,
            }.items()
            if v is not None
        }

    @override
    def _fetch_page(
        self,
        *,
        page_state: str | None,
        timeout_context: _TimeoutContext,
    ) -> tuple[list[TRAW], str | None, dict[str, Any] | None]:
        if self.collection is None:
            raise RuntimeError("Query engine has no sync collection.")
        f_payload = {
            "findAndRerank": {
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

        if MOCK_FARR_API:
            logger.info("MOCKING FARR API: '%s'", str(converted_f_payload))
            mock_p_documents = mock_farr_documents(converted_f_payload)
            mock_n_p_state = None
            mock_p_r_status = {"mocked": "OH_YEAH"}
            return (mock_p_documents, mock_n_p_state, mock_p_r_status)  # type: ignore[return-value]

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
                text="Faulty response from findAndRerank API command (no 'documents').",
                raw_response=f_response,
            )
        p_documents = f_response["data"]["documents"]
        n_p_state = f_response["data"]["nextPageState"]
        p_r_status = f_response.get("status")
        return (p_documents, n_p_state, p_r_status)

    @override
    async def _async_fetch_page(
        self,
        *,
        page_state: str | None,
        timeout_context: _TimeoutContext,
    ) -> tuple[list[TRAW], str | None, dict[str, Any] | None]:
        if self.async_collection is None:
            raise RuntimeError("Query engine has no async collection.")
        f_payload = {
            "findAndRerank": {
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

        if MOCK_FARR_API:
            logger.info("MOCKING FARR API: '%s'", str(converted_f_payload))
            mock_p_documents = mock_farr_documents(converted_f_payload)
            mock_n_p_state = None
            mock_p_r_status = {"mocked": "OH_YEAH"}
            return (mock_p_documents, mock_n_p_state, mock_p_r_status)  # type: ignore[return-value]

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
                text="Faulty response from findAndRerank API command (no 'documents').",
                raw_response=f_response,
            )
        p_documents = f_response["data"]["documents"]
        n_p_state = f_response["data"]["nextPageState"]
        p_r_status = f_response.get("status")
        return (p_documents, n_p_state, p_r_status)


class CollectionFindAndRerankCursor(Generic[TRAW, T], AbstractCursor[TRAW]):
    """
    A synchronous cursor over documents, as returned by a `find_and_rerank` invocation
    on a Collection. A cursor can be iterated over, materialized into a list,
    and queried/manipulated in various ways.

    Some cursor operations mutate it in-place (such as consuming its documents),
    other return a new cursor without changing the original one. See the documentation
    for the various methods and the Collection `find_and_rerank` method for more details
    and usage patterns.

    A cursor has two type parameters: TRAW and T. The first is the type of the "raw"
    documents as they are obtained from the Data API, the second is the type of the
    items after the optional mapping function (see the `.map()` method). If there is
    no mapping, TRAW = T. In general, consuming a cursor returns items of type T,
    except for the `consume_buffer` primitive that draws directly from the buffer
    and always returns items of type TRAW.

    Example:
        TODO DOCSTRING
        >>> cursor = collection.find_and_rerank(
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

    _query_engine: _CollectionFindAndRerankQueryEngine[TRAW]
    _request_timeout_ms: int | None
    _overall_timeout_ms: int | None
    _request_timeout_label: str | None
    _overall_timeout_label: str | None
    _timeout_manager: MultiCallTimeoutManager
    _filter: FilterType | None
    _projection: ProjectionType | None
    _sort: FindAndRerankSortType | None
    _limit: int | None
    _hybrid_limits: int | dict[str, int] | None
    _hybrid_projection: str | None
    _rerank_field: str | None
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
        sort: FindAndRerankSortType | None = None,
        limit: int | None = None,
        hybrid_limits: int | dict[str, int] | None = None,
        hybrid_projection: str | None = None,
        rerank_field: str | None = None,
        mapper: Callable[[TRAW], T] | None = None,
    ) -> None:
        self._filter = deepcopy(filter)
        self._projection = projection
        self._sort = sort
        self._limit = limit
        self._hybrid_limits = hybrid_limits
        self._hybrid_projection = hybrid_projection
        self._rerank_field = rerank_field
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
            hybrid_projection=self._hybrid_projection,
            rerank_field=self._rerank_field,
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
        hybrid_projection: str | None | UnsetType = _UNSET,
        rerank_field: str | None | UnsetType = _UNSET,
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
            hybrid_projection=self._hybrid_projection
            if isinstance(hybrid_projection, UnsetType)
            else hybrid_projection,
            rerank_field=self._rerank_field
            if isinstance(rerank_field, UnsetType)
            else rerank_field,
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
            TODO DOCSTRING TODO
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
            {'seq': 1}
            {'seq': 4}
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
            hybrid_projection=self._hybrid_projection,
            rerank_field=self._rerank_field,
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
        self, sort: FindAndRerankSortType | None
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

    def hybrid_projection(
        self, hybrid_projection: str | None
    ) -> CollectionFindAndRerankCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new hybrid_projection setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the Collection `find_and_rerank` method.

        Args:
            hybrid_projection: a new setting to apply to the returned new cursor.

        Returns:
            a new CollectionFindAndRerankCursor with the same settings as this one,
                except for `hybrid_projection` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(hybrid_projection=hybrid_projection)

    def rerank_field(
        self, rerank_field: str | None
    ) -> CollectionFindAndRerankCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new rerank_field setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the Collection `find_and_rerank` method.

        Args:
            rerank_field: a new setting to apply to the returned new cursor.

        Returns:
            a new CollectionFindAndRerankCursor with the same settings as this one,
                except for `rerank_field` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(rerank_field=rerank_field)

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

        Returns:
            a new CollectionFindAndRerankCursor with a new mapping function on the results,
                possibly composed with any pre-existing mapping function.

        Example:
            TODO DOCSTRING TODO
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
            hybrid_projection=self._hybrid_projection,
            rerank_field=self._rerank_field,
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
        results in the cursor entering CLOSED state.

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
            TODO DOCSTRING TODO
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
            ...     return doc['seq'] != 4
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
            list: a list of documents (or other values depending on the mapping
                function, if one is set). These are all items that were left
                to be consumed on the cursor when `to_list` is called.

        Example:
            TODO DOCSTRING TODO
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
            has_next: a boolean value of True if there is at least one further item
                available to consume; False otherwise (including the case of CLOSED
                cursor).
        """

        if self._state == CursorState.CLOSED:
            return False
        self._try_ensure_fill_buffer()
        return len(self._buffer) > 0


class AsyncCollectionFindAndRerankCursor(Generic[TRAW, T], AbstractCursor[TRAW]):
    """
    An asynchronous cursor over documents, as returned by a `find_and_rerank` invocation
    on an AsyncCollection. A cursor can be iterated over, materialized into a list,
    and queried/manipulated in various ways.

    Some cursor operations mutate it in-place (such as consuming its documents),
    other return a new cursor without changing the original one. See the documentation
    for the various methods and the AsyncCollection `find_and_rerank` method for more
    details and usage patterns.

    A cursor has two type parameters: TRAW and T. The first is the type of the "raw"
    documents as they are obtained from the Data API, the second is the type of the
    items after the optional mapping function (see the `.map()` method). If there is
    no mapping, TRAW = T. In general, consuming a cursor returns items of type T,
    except for the `consume_buffer` primitive that draws directly from the buffer
    and always returns items of type TRAW.

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
    _sort: FindAndRerankSortType | None
    _limit: int | None
    _hybrid_limits: int | dict[str, int] | None
    _hybrid_projection: str | None
    _rerank_field: str | None
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
        sort: FindAndRerankSortType | None = None,
        limit: int | None = None,
        hybrid_limits: int | dict[str, int] | None = None,
        hybrid_projection: str | None = None,
        rerank_field: str | None = None,
        mapper: Callable[[TRAW], T] | None = None,
    ) -> None:
        self._filter = deepcopy(filter)
        self._projection = projection
        self._sort = sort
        self._limit = limit
        self._hybrid_limits = hybrid_limits
        self._hybrid_projection = hybrid_projection
        self._rerank_field = rerank_field
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
            hybrid_projection=self._hybrid_projection,
            rerank_field=self._rerank_field,
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
        hybrid_projection: str | None | UnsetType = _UNSET,
        rerank_field: str | None | UnsetType = _UNSET,
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
            hybrid_projection=self._hybrid_projection
            if isinstance(hybrid_projection, UnsetType)
            else hybrid_projection,
            rerank_field=self._rerank_field
            if isinstance(rerank_field, UnsetType)
            else rerank_field,
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
            hybrid_projection=self._hybrid_projection,
            rerank_field=self._rerank_field,
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
        self, sort: FindAndRerankSortType | None
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

    def hybrid_projection(
        self, hybrid_projection: str | None
    ) -> AsyncCollectionFindAndRerankCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new hybrid_projection setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the AsyncCollection `find_and_rerank` method.

        Args:
            hybrid_projection: a new setting to apply to the returned new cursor.

        Returns:
            a new AsyncCollectionFindAndRerankCursor with the same settings as this one,
                except for `hybrid_projection` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(hybrid_projection=hybrid_projection)

    def rerank_field(
        self, rerank_field: str | None
    ) -> AsyncCollectionFindAndRerankCursor[TRAW, T]:
        """
        Return a copy of this cursor with a new rerank_field setting.
        This operation is allowed only if the cursor state is still IDLE.

        Instead of explicitly invoking this method, the typical usage consists
        in passing arguments to the AsyncCollection `find_and_rerank` method.

        Args:
            rerank_field: a new setting to apply to the returned new cursor.

        Returns:
            a new AsyncCollectionFindAndRerankCursor with the same settings as this one,
                except for `rerank_field` which is the provided value.
        """

        self._ensure_idle()
        return self._copy(rerank_field=rerank_field)

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

        Returns:
            a new AsyncCollectionFindAndRerankCursor with a new mapping function on the
                results, possibly composed with any pre-existing mapping function.
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
            hybrid_projection=self._hybrid_projection,
            rerank_field=self._rerank_field,
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
        results in the cursor entering CLOSED state.

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
            list: a list of documents (or other values depending on the mapping
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
            has_next: a boolean value of True if there is at least one further item
                available to consume; False otherwise (including the case of CLOSED
                cursor).
        """

        if self._state == CursorState.CLOSED:
            return False
        await self._try_ensure_fill_buffer()
        return len(self._buffer) > 0
