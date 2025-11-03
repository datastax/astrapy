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
from typing import Any, Iterable

from astrapy.event_observers.events import ObservableEvent, ObservableEventType


class Observer(ABC):
    """
    An observer that can be attached to astrapy events through the API options.

    Users can subclass Observer and provide their implementation
    of the `receive` method. Request-issuing classes (such as Database or Table)
    will dispatch events to the observers registered in their API options.

    This class offers factory static methods for common use-cases:
    `from_event_list` and `from_event_dict`.
    """

    @abstractmethod
    def receive(
        self,
        event: ObservableEvent,
        sender: Any = None,
        function_name: str | None = None,
        request_id: str | None = None,
    ) -> None:
        """Receive and event.

        Args:
            event: the event that astrapy is dispatching to the observer.
            sender: the object directly responsible for generating the event.
            function_name: when applicable, the name of the function/method
                that triggered the event.
        """
        ...

    @staticmethod
    def from_event_list(
        event_list: list[ObservableEvent],
        *,
        event_types: Iterable[ObservableEventType] | None = None,
    ) -> Observer:
        """
        Create an Observer object wrapping a caller-provided list.

        The resulting observer will simply append the events it receives into
        the list.

        Args:
            event_list: the list where the caller will find the received events.
            event_types: if provided, it's a list of event types so that only
                events matching this filter are processed.
        """

        class _ObserverFromList(Observer):
            def __init__(
                self,
                _event_list: list[ObservableEvent],
                _event_types: Iterable[ObservableEventType] | None,
            ) -> None:
                self.event_list = _event_list
                self.event_types = (
                    set(ObservableEventType.__members__.values())
                    if _event_types is None
                    else set(_event_types)
                )

            def receive(
                self,
                event: ObservableEvent,
                sender: Any = None,
                function_name: str | None = None,
                request_id: str | None = None,
            ) -> None:
                if event.event_type in self.event_types:
                    self.event_list.append(event)

        return _ObserverFromList(event_list, event_types)

    @staticmethod
    def from_event_dict(
        event_dict: dict[ObservableEventType, list[ObservableEvent]],
        *,
        event_types: Iterable[ObservableEventType] | None = None,
    ) -> Observer:
        """
        Create an Observer object wrapping a caller-provided dictionary.

        The resulting observer will simply append the events it receives into
        the dictionary, grouped by event type. Dict values are lists of events.

        Args:
            event_dict: the dict where the caller will find the received events.
            event_types: if provided, it's a list of event types so that only
                events matching this filter are processed.
        """

        class _ObserverFromDict(Observer):
            def __init__(
                self,
                _event_dict: dict[ObservableEventType, list[ObservableEvent]],
                _event_types: Iterable[ObservableEventType] | None,
            ) -> None:
                self.event_dict = _event_dict
                self.event_types = (
                    set(ObservableEventType.__members__.values())
                    if _event_types is None
                    else set(_event_types)
                )

            def receive(
                self,
                event: ObservableEvent,
                sender: Any = None,
                function_name: str | None = None,
                request_id: str | None = None,
            ) -> None:
                if event.event_type in self.event_types:
                    self.event_dict[event.event_type] = self.event_dict.get(
                        event.event_type, []
                    ) + [event]

        return _ObserverFromDict(event_dict, event_types)
