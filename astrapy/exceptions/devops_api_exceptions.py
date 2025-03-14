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

from dataclasses import dataclass
from typing import Any

import httpx


class DevOpsAPIException(Exception):
    """
    An exception specific to issuing requests to the DevOps API.
    """

    def __init__(self, text: str | None = None):
        Exception.__init__(self, text or "")


@dataclass
class DevOpsAPIErrorDescriptor:
    """
    An object representing a single error returned from the DevOps API,
    typically with an error code and a text message.

    A single response from the Devops API may return zero, one or more of these.

    Attributes:
        id: a numeric code as found in the API "ID" item.
        message: the text found in the API "error" item.
        attributes: a dict with any further key-value pairs returned by the API.
    """

    id: int | None
    message: str | None
    attributes: dict[str, Any]

    def __init__(self, error_dict: dict[str, Any]) -> None:
        self.id = error_dict.get("ID")
        self.message = error_dict.get("message")
        self.attributes = {
            k: v for k, v in error_dict.items() if k not in {"ID", "message"}
        }


@dataclass
class DevOpsAPIHttpException(DevOpsAPIException, httpx.HTTPStatusError):
    """
    A request to the DevOps API resulted in an HTTP 4xx or 5xx response.

    Though the DevOps API seldom enriches such errors with a response text,
    this class acts as the DevOps counterpart to DataAPIHttpException
    to facilitate a symmetryc handling of errors at application lebel.

    Attributes:
        text: a text message about the exception.
        error_descriptors: a list of all DevOpsAPIErrorDescriptor objects
            found in the response.
    """

    text: str | None
    error_descriptors: list[DevOpsAPIErrorDescriptor]

    def __init__(
        self,
        text: str | None,
        *,
        httpx_error: httpx.HTTPStatusError,
        error_descriptors: list[DevOpsAPIErrorDescriptor],
    ) -> None:
        DevOpsAPIException.__init__(self, text)
        httpx.HTTPStatusError.__init__(
            self,
            message=str(httpx_error),
            request=httpx_error.request,
            response=httpx_error.response,
        )
        self.text = text
        self.httpx_error = httpx_error
        self.error_descriptors = error_descriptors

    def __str__(self) -> str:
        return self.text or str(self.httpx_error)

    @classmethod
    def from_httpx_error(
        cls,
        httpx_error: httpx.HTTPStatusError,
        **kwargs: Any,
    ) -> DevOpsAPIHttpException:
        """Parse a httpx status error into this exception."""

        raw_response: dict[str, Any]
        # the attempt to extract a response structure cannot afford failure.
        try:
            raw_response = httpx_error.response.json()
        except Exception:
            raw_response = {}
        error_descriptors = [
            DevOpsAPIErrorDescriptor(error_dict)
            for error_dict in raw_response.get("errors") or []
        ]
        if error_descriptors:
            text = f"{error_descriptors[0].message}. {str(httpx_error)}"
        else:
            text = str(httpx_error)

        return cls(
            text=text,
            httpx_error=httpx_error,
            error_descriptors=error_descriptors,
            **kwargs,
        )


@dataclass
class DevOpsAPITimeoutException(DevOpsAPIException):
    """
    A DevOps API operation timed out.

    Attributes:
        text: a textual description of the error
        timeout_type: this denotes the phase of the HTTP request when the event
            occurred ("connect", "read", "write", "pool") or "generic" if there is
            not a specific request associated to the exception.
        endpoint: if the timeout is tied to a specific request, this is the
            URL that the request was targeting.
        raw_payload:  if the timeout is tied to a specific request, this is the
            associated payload (as a string).
    """

    text: str
    timeout_type: str
    endpoint: str | None
    raw_payload: str | None

    def __init__(
        self,
        text: str,
        *,
        timeout_type: str,
        endpoint: str | None,
        raw_payload: str | None,
    ) -> None:
        super().__init__(text)
        self.text = text
        self.timeout_type = timeout_type
        self.endpoint = endpoint
        self.raw_payload = raw_payload


@dataclass
class UnexpectedDevOpsAPIResponseException(DevOpsAPIException):
    """
    The DevOps API response is malformed in that it does not have
    expected field(s), or they are of the wrong type.

    Attributes:
        text: a text message about the exception.
        raw_response: the response returned by the API in the form of a dict.
    """

    text: str
    raw_response: dict[str, Any] | None

    def __init__(
        self,
        text: str,
        raw_response: dict[str, Any] | None,
    ) -> None:
        super().__init__(text)
        self.text = text
        self.raw_response = raw_response


class DevOpsAPIResponseException(DevOpsAPIException):
    """
    A request to the DevOps API returned with a non-success return code
    and one of more errors in the HTTP response.

    Attributes:
        text: a text message about the exception.
        command: the raw payload that was sent to the DevOps API.
        error_descriptors: a list of all DevOpsAPIErrorDescriptor objects
            returned by the API in the response.
    """

    text: str | None
    command: dict[str, Any] | None
    error_descriptors: list[DevOpsAPIErrorDescriptor]

    def __init__(
        self,
        text: str | None = None,
        *,
        command: dict[str, Any] | None = None,
        error_descriptors: list[DevOpsAPIErrorDescriptor] = [],
    ) -> None:
        super().__init__(text or self.__class__.__name__)
        self.text = text
        self.command = command
        self.error_descriptors = error_descriptors

    @staticmethod
    def from_response(
        command: dict[str, Any] | None,
        raw_response: dict[str, Any],
    ) -> DevOpsAPIResponseException:
        """Parse a raw response from the API into this exception."""

        error_descriptors = [
            DevOpsAPIErrorDescriptor(error_dict)
            for error_dict in raw_response.get("errors") or []
        ]
        if error_descriptors:
            _text = error_descriptors[0].message
        else:
            _text = None
        return DevOpsAPIResponseException(
            text=_text, command=command, error_descriptors=error_descriptors
        )
