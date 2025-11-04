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

from abc import ABC
from dataclasses import dataclass
from typing import Any

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
        http_method: one of `astrapy.utils.request_tools.HttpMethod`, e.g. "POST".
        url: the complete URL the request is targeted at.
        query_parameters: if present, all query parameters in dict form.
        redacted_headers: a dictionary of the non-sensitive headers being used
            for the request. Authentication credentials and API Keys are removed.
        dev_ops_api: true if and only if the request is aimed at the DevOps API.
    """

    payload: str | None
    http_method: str
    url: str
    query_parameters: dict[str, Any] | None
    redacted_headers: dict[str, Any] | None
    dev_ops_api: bool

    def __init__(
        self,
        payload: str | None,
        http_method: str,
        url: str,
        query_parameters: dict[str, Any] | None,
        redacted_headers: dict[str, Any] | None,
        dev_ops_api: bool,
    ) -> None:
        self.event_type = ObservableEventType.REQUEST
        self.payload = payload
        self.http_method = http_method
        self.url = url
        self.query_parameters = query_parameters
        self.redacted_headers = redacted_headers
        self.dev_ops_api = dev_ops_api


@dataclass
class ObservableResponse(ObservableEvent):
    """
    An event representing a response received by the Data API, whose body
    is captured exactly as is sent by the Data API.

    Attributes:
        event_type: it has value ObservableEventType.RESPONSE in this case.
        body: a string expressing the response body.
        status_code: the response HTTP status code.
    """

    body: str | None
    status_code: int

    def __init__(self, body: str | None, *, status_code: int) -> None:
        self.event_type = ObservableEventType.RESPONSE
        self.body = body
        self.status_code = status_code
