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
from abc import ABC
from decimal import Decimal
from enum import Enum
from typing import Any, Generic, TypeVar

from astrapy.data_types import DataAPIVector
from astrapy.exceptions import (
    CursorException,
)
from astrapy.utils.api_options import FullSerdesOptions

# A cursor reads TRAW from DB and maps them to T if any mapping.
# A new cursor returned by .map will map to TNEW
TRAW = TypeVar("TRAW")
T = TypeVar("T")
TNEW = TypeVar("TNEW")


logger = logging.getLogger(__name__)


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


def _ensure_vector(
    fvector: list[float | Decimal] | None,
    options: FullSerdesOptions,
) -> list[float] | DataAPIVector | None:
    """
    For Tables and - depending on the JSON response parsing - collections alike,
    the sort vector included in the response from a find-like could arrive as a list
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


class CursorState(Enum):
    """
    This enum expresses the possible states for a `Cursor`.

    Values:
        IDLE: Iteration over results has not started yet (alive=T, started=F)
        STARTED: Iteration has started, *can* still yield results (alive=T, started=T)
        CLOSED: Finished/forcibly stopped. Won't return more documents (alive=F)
    """

    # Iteration over results has not started yet (alive=T, started=F)
    IDLE = "idle"
    # Iteration has started, *can* still yield results (alive=T, started=T)
    STARTED = "started"
    # Finished/forcibly stopped. Won't return more documents (alive=F)
    CLOSED = "closed"


class AbstractCursor(ABC, Generic[TRAW]):
    """
    A cursor obtained from the invocation of a find-type method over a table or
    a collection.
    This is the main interface to scroll through the results (resp. rows or documents).

    This class is not meant to be directly instantiated by the user, rather it
    is a superclass capturing some basic mechanisms common to all find cursors.

    Cursors provide a seamless interface to the caller code, allowing iteration
    over results while chunks of new data (pages) are exchanged periodically with
    the API. For this reason, cursors internally manage a local buffer that is
    progressively emptied and re-filled with a new page in a manner hidden from the
    user -- except, some cursor methods allow to peek into this buffer should it
    be necessary.
    """

    _state: CursorState
    _buffer: list[TRAW]
    _pages_retrieved: int
    _consumed: int
    _next_page_state: str | None
    _last_response_status: dict[str, Any] | None

    def __init__(self) -> None:
        self.rewind()

    def _imprint_internal_state(self, other: AbstractCursor[TRAW]) -> None:
        """Mutably copy the internal state of this cursor onto another one."""
        other._state = self._state
        other._buffer = self._buffer
        other._pages_retrieved = self._pages_retrieved
        other._consumed = self._consumed
        other._next_page_state = self._next_page_state
        other._last_response_status = self._last_response_status

    def _ensure_alive(self) -> None:
        if self._state == CursorState.CLOSED:
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
        The current state of this cursor.

        Returns:
            a value in `astrapy.cursors.CursorState`.
        """

        return self._state

    @property
    def consumed(self) -> int:
        """
        The number of items the cursors has yielded, i.e. how many items
        have been already read by the code consuming the cursor.

        Returns:
            consumed: a non-negative integer, the count of items yielded so far.
        """

        return self._consumed

    @property
    def cursor_id(self) -> int:
        """
        An integer uniquely identifying this cursor.

        Returns:
            cursor_id: an integer number uniquely identifying the cursor.
        """

        return id(self)

    @property
    def buffered_count(self) -> int:
        """
        The number of items (documents, rows) currently stored in the client-side
        buffer of this cursor. Reading this property never triggers new API calls
        to re-fill the buffer.

        Returns:
            buffered_count: a non-negative integer, the amount of items currently
                stored in the local buffer.
        """

        return len(self._buffer)

    def close(self) -> None:
        """
        Close the cursor, regardless of its state. A cursor can be closed at any
        time, possibly discarding the portion of results that has not yet been
        consumed, if any.

        This is an in-place modification of the cursor.
        """

        self._state = CursorState.CLOSED
        self._buffer = []

    def rewind(self) -> None:
        """
        Rewind the cursor, bringing it back to its pristine state of no items
        retrieved/consumed yet, regardless of its current state.
        All cursor settings (filter, mapping, projection, etc) are retained.

        A cursor can be rewound at any time. Keep in mind that, subject to changes
        occurred on the table or collection the results may be different if a cursor
        is browsed a second time after rewinding it.

        This is an in-place modification of the cursor.
        """
        self._state = CursorState.IDLE
        self._buffer = []
        self._pages_retrieved = 0
        self._consumed = 0
        self._next_page_state = None
        self._last_response_status = None

    def consume_buffer(self, n: int | None = None) -> list[TRAW]:
        """
        Consume (return) up to the requested number of buffered items (rows/documents).
        The returned items are marked as consumed, meaning that subsequently consuming
        the cursor will start after those items.

        This method is an in-place modification of the cursor and only concerns
        the local buffer: it never triggers fetching of new pages from the Data API.

        This method can be called regardless of the cursor state without exceptions
        being raised.

        Args:
            n: amount of items to return. If omitted, the whole buffer is returned.

        Returns:
            list: a list of items (rows/document dictionaries). If there are fewer
                items than requested, the whole buffer is returned without errors:
                in particular, if it is empty (such as when the cursor is closed),
                an empty list is returned.
        """
        _n = n if n is not None else len(self._buffer)
        if _n < 0:
            raise ValueError("A negative amount of items was requested.")
        returned, remaining = self._buffer[:_n], self._buffer[_n:]
        self._buffer = remaining
        self._consumed += len(returned)
        return returned
