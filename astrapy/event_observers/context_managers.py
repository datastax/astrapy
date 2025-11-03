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

from typing import (
    Iterable,
    Iterator,
    Protocol,  # always use stdlib for Protocol on 3.8+
    TypeVar,
)

try:  # TODO: check python version removal from project
    # Python 3.11 upward
    from typing import Self
except ImportError:
    # 3.10 and below need this:
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
        del observer_
