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

from collections.abc import Iterable, Iterator
from typing import Protocol, TypeVar

# TODO: once supporting 3.11+, this can be simplified.
# 'Self' is imported differently below 3.11, cf. below. Then:
# 1. To appease typecheckers on e.g. 3.9, we need the `attr-defined` exception
# 2. To appease typecheckers on 3.11+, we need the `unused-ignore` exception.
try:
    # Python 3.11 upward:
    from typing import Self  # type:ignore[attr-defined,unused-ignore]
except ImportError:
    # 3.10 and below:
    from typing_extensions import Self

from contextlib import contextmanager

from uuid6 import uuid7

from astrapy.api_options import APIOptions
from astrapy.event_observers.events import ObservableEvent, ObservableEventType
from astrapy.event_observers.observers import Observer
from astrapy.utils.unset import _UNSET, UnsetType


class OptionAwareDatabaseObject(Protocol):
    def with_options(
        self,
        *,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> Self: ...


DB_OBJ = TypeVar("DB_OBJ", bound=OptionAwareDatabaseObject)


@contextmanager
def event_collector(
    target: DB_OBJ,
    *,
    destination: list[ObservableEvent]
    | dict[ObservableEventType, list[ObservableEvent]],
    event_types: Iterable[ObservableEventType] | None = None,
) -> Iterator[DB_OBJ]:
    """
    Create a context manager wrapping a list (of `ObservableEvent` objects) or
    a dict (with `ObservableEventType` keys and `list[ObservableEvent]` values),
    for quickly instrumenting a client, database, table, collection or admin object.

    This utility is meant to be used in a `with` statement, so that events emitted
    by the instrumented classes within the with block are captured into the provided
    lists/dictionaries. Events emitted by spawned classes (e.g. when a Database
    creates a Collection in the `with` block) are also received this way.

    Once outside the `with` block, collection of events stops, but the provided
    destination can still be accessed according to its ordinary scoping.

    Args:
        target: an object that issues Data API / DevOps API requests. The target
            must have a `with_options` suitable method: meaning, it can be any of
            the following: `DataAPIClient`, `AsyncDatabase`, `Database`,
            `AsyncCollection`, `Collection`, `AsyncTable`, `Table`, `AstraDBAdmin`,
            `AstraDBDatabaseAdmin`, `DataAPIDatabaseAdmin`.
        destination: a list or a dictionary where the collected events will be stored.
            For dictionaries, events are grouped into lists, one per each event type,
            stored under the corresponding `ObservableEventType` value as dict key.
        event_types: if provided, it's a list of event types so that only
            events matching this filter are processed.

    Returns:
        Yields an instrumented version of the input target, with an added observer
        set to accumulate the received events into the provided destination. Any
        pre-existing observer is untouched.

    Example:
        >>> ev_lst: list[ObservableEvent] = []
        >>> with event_collector(db, destination=ev_lst) as instrumented_db:
        ...     _ = instrumented_db.list_table_names()
        ...     table = instrumented_db.get_table("my_table")
        ...     _ = table.find_one({"k": 101})
        ...
        >>> print(len(ev_lst))
        5
        >>> print(ev_lst[0].event_type)
        ObservableEventType.REQUEST
    """
    observer_id_ = f"observer_{str(uuid7())}"
    observer_: Observer
    if isinstance(destination, list):
        observer_ = Observer.from_event_list(destination, event_types=event_types)
    else:
        observer_ = Observer.from_event_dict(destination, event_types=event_types)
    api_options = APIOptions(event_observers={observer_id_: observer_})
    target_ = target.with_options(api_options=api_options)
    try:
        yield target_
    finally:
        observer_.enabled = False
