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
from dataclasses import dataclass
from typing import Any, Iterable

from astrapy.exceptions.error_descriptors import (
    DataAPIErrorDescriptor,
    DataAPIWarningDescriptor,
)
from astrapy.utils.str_enum import StrEnum


class ObservableEventType(StrEnum):
    """
    Enum for the possible values of the event type for observable events
    """

    WARNING = "warning"
    ERROR = "error"
    REQUEST = "request"
    RESPONSE = "response"


@dataclass
class ObservableEvent(ABC):
    """
    Class that represents the most general 'event' that is sent to observers.

    Attributes:
        event_type: the type of the event, such as "log", "error", or "warning".
    """

    event_type: ObservableEventType


@dataclass
class ObservableError(ObservableEvent):
    """
    An event representing an error returned from the Data API in a response.

    These are dispatched unconditionally to the attached observers as the
    response is parsed. The actual raising of an exception does not always
    follow; moreover, further operations may take place before that occurs.

    Note:
        Only errors returned within the Data API response in the
        "errors" field are dispatched this way. The most general exception that
        can occur during a method call are not necessarily of this form.

    Attributes:
        event_type: it has value ObservableEventType.ERROR in this case.
        error: a descriptor of the error, as found in the Data API response.
    """

    error: DataAPIErrorDescriptor

    def __init__(self, error: DataAPIErrorDescriptor) -> None:
        self.event_type = ObservableEventType.ERROR
        self.error = error


@dataclass
class ObservableWarning(ObservableEvent):
    """
    An event representing a warning returned by a Data API command.

    These are dispatched to the attached observers as the response is parsed.

    Attributes:
        event_type: it has value ObservableEventType.WARNING in this case.
        warning: a descriptor of the warning, as found in the Data API response.
    """

    warning: DataAPIWarningDescriptor

    def __init__(self, warning: DataAPIWarningDescriptor) -> None:
        self.event_type = ObservableEventType.WARNING
        self.warning = warning


@dataclass
class ObservableRequest(ObservableEvent):
    """
    An event representing a request being sent, captured with its
    payload exactly as will be sent to the API.

    Attributes:
        event_type: it has value ObservableEventType.REQUEST in this case.
        payload: the payload as a string.
    """

    payload: str | None

    def __init__(self, payload: str | None) -> None:
        self.event_type = ObservableEventType.REQUEST
        self.payload = payload


@dataclass
class ObservableResponse(ObservableEvent):
    """
    An event representing a response received by the Data API, whose body
    is captured exactly as is sent by the Data API.

    Attributes:
        event_type: it has value ObservableEventType.RESPONSE in this case.
        body: a string expressing the response body.
    """

    body: str | None

    def __init__(self, body: str | None) -> None:
        self.event_type = ObservableEventType.RESPONSE
        self.body = body


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
            ) -> None:
                if event.event_type in self.event_types:
                    self.event_dict[event.event_type] = self.event_dict.get(
                        event.event_type, []
                    ) + [event]

        return _ObserverFromDict(event_dict, event_types)
