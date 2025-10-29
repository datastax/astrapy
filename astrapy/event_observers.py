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
from typing import Any

from astrapy.exceptions import DataAPIErrorDescriptor, DataAPIWarningDescriptor


@dataclass
class ObservableEvent(ABC):
    """
    Class that represents the most general 'event' that is sent to observers.

    Attributes:
        event_type: the type of the event, such as "log", "error", or "warning".
    """

    event_type: str


@dataclass
class ObservableLog(ObservableEvent):
    """
    A log entry event, with a severity level and a string message.

    Attributes:
        event_type: it has value "log" in this case.
        level: a number following the scale of `logging.INFO`, `DEBUG`, etc.
        message: a string containing the full log message.
    """

    level: int
    message: str

    def __init__(self, level: int, message: str) -> None:
        self.event_type = "log"
        self.level = level
        self.message = message


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
        event_type: it has value "error" in this case.
        error: a descriptor of the error, as found in the Data API response.
    """

    error: DataAPIErrorDescriptor

    def __init__(self, error: DataAPIErrorDescriptor) -> None:
        self.event_type = "error"
        self.error = error


@dataclass
class ObservableWarning(ObservableEvent):
    """
    An event representing a warning returned by a Data API command.

    These are dispatched to the attached observers as the response is parsed.

    Attributes:
        event_type: it has value "warning" in this case.
        warning: a descriptor of the warning, as found in the Data API response.
    """

    warning: DataAPIWarningDescriptor

    def __init__(self, warning: DataAPIWarningDescriptor) -> None:
        self.event_type = "warning"
        self.warning = warning


class Observer(ABC):
    """
    An observer that can be attached to astrapy events through the API options.

    Users are expected to subclass and provide their implementation
    of the `receive` method.
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
