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


class DataAPIException(Exception):
    """
    Any exception occurred while issuing requests to the Data API
    and specific to it, such as:
      - a collection is found not to exist when gettings its metadata,
      - the API return a response with an error,
    but not, for instance,
      - a network error while sending an HTTP request to the API.
    """

    pass


@dataclass
class DataAPIErrorDescriptor:
    """
    An object representing a single error, or warning, as returned from the Data API,
    typically with an error code, a text message and other properties.

    This object is used to describe errors/warnings received from the Data API,
    in the form of HTTP-200 ("success") responses containing errors
    (and possibly warnings).
    Depending on the API command semantics, responses may express partial successes
    with some errors (for instance, an insertMany command inserting most of the
    documents/rows, but failing on a couple of incompatible inputs).

    Attributes:
        error_code: a string code as found in the API "error" item.
        message: the text found in the API "error" item.
        title:  the text found in the API "title" item.
        family:  the text found in the API "family" item.
        scope:  the text found in the API "scope" item.
        id:  the text found in the API "id" item.
        attributes: a dict with any further key-value pairs returned by the API.
    """

    title: str | None
    error_code: str | None
    message: str | None
    family: str | None
    scope: str | None
    id: str | None
    attributes: dict[str, Any]

    _known_dict_fields = {
        "title",
        "errorCode",
        "message",
        "family",
        "scope",
        "id",
    }

    def __init__(self, error_dict: dict[str, str] | str) -> None:
        if isinstance(error_dict, str):
            self.message = error_dict
            self.title = None
            self.error_code = None
            self.family = None
            self.scope = None
            self.id = None
            self.attributes = {}
        else:
            self.title = error_dict.get("title")
            self.error_code = error_dict.get("errorCode")
            self.message = error_dict.get("message")
            self.family = error_dict.get("family")
            self.scope = error_dict.get("scope")
            self.id = error_dict.get("id")
            self.attributes = {
                k: v for k, v in error_dict.items() if k not in self._known_dict_fields
            }

    def __repr__(self) -> str:
        pieces = [
            f"{self.title.__repr__()}" if self.title else None,
            f"error_code={self.error_code.__repr__()}" if self.error_code else None,
            f"message={self.message.__repr__()}" if self.message else None,
            f"family={self.family.__repr__()}" if self.family else None,
            f"scope={self.scope.__repr__()}" if self.scope else None,
            f"id={self.id.__repr__()}" if self.id else None,
            f"attributes={self.attributes.__repr__()}" if self.attributes else None,
        ]
        return f"{self.__class__.__name__}({', '.join(pc for pc in pieces if pc)})"

    def __str__(self) -> str:
        return self.summary()

    def summary(self) -> str:
        """
        Determine a string succinct description of this error descriptor.

        The precise format of this summary is determined by which fields are set.
        """
        non_code_part: str | None
        if self.title:
            if self.message:
                non_code_part = f"{self.title}: {self.message}"
            else:
                non_code_part = f"{self.title}"
        else:
            if self.message:
                non_code_part = f"{self.message}"
            else:
                non_code_part = None
        if self.error_code:
            if non_code_part:
                return f"{non_code_part} ({self.error_code})"
            else:
                return f"{self.error_code}"
        else:
            if non_code_part:
                return non_code_part
            else:
                return ""


DataAPIWarningDescriptor = DataAPIErrorDescriptor


@dataclass
class DataAPIResponseException(DataAPIException):
    """
    The Data API returned an HTTP 200 ("success") response, which however
    reports API-specific error(s), possibly alongside partial successes.

    Attributes:
        text: a text message about the exception.
        command: the payload to the API that led to the response.
        raw_response: the full response from the API.
        error_descriptors: a list of DataAPIErrorDescriptor, one for each
            item in the API response's "errors" field.
        warning_descriptors: a list of DataAPIWarningDescriptor, one for each
            item in the API response's "warnings" field (if there are any).
    """

    text: str | None
    command: dict[str, Any] | None
    raw_response: dict[str, Any]
    error_descriptors: list[DataAPIErrorDescriptor]
    warning_descriptors: list[DataAPIWarningDescriptor]

    def __init__(
        self,
        text: str | None,
        *,
        command: dict[str, Any] | None,
        raw_response: dict[str, Any],
        error_descriptors: list[DataAPIErrorDescriptor],
        warning_descriptors: list[DataAPIWarningDescriptor],
    ) -> None:
        super().__init__(text)
        self.text = text
        self.command = command
        self.raw_response = raw_response
        self.error_descriptors = error_descriptors
        self.warning_descriptors = warning_descriptors

    @staticmethod
    def from_response(
        *,
        command: dict[str, Any] | None,
        raw_response: dict[str, Any],
        **kwargs: Any,
    ) -> DataAPIResponseException:
        """Parse a raw response from the API into this exception."""

        error_descriptors = [
            DataAPIErrorDescriptor(error_dict)
            for error_dict in raw_response.get("errors") or []
        ]
        warning_descriptors = [
            DataAPIWarningDescriptor(error_dict)
            for error_dict in raw_response.get("warnings") or []
        ]

        if error_descriptors:
            summaries = [e_d.summary() for e_d in error_descriptors]
            if len(summaries) == 1:
                text = summaries[0]
            else:
                _j_summaries = "; ".join(
                    f"[{summ_i + 1}] {summ_s}"
                    for summ_i, summ_s in enumerate(summaries)
                )
                text = f"[{len(summaries)} errors collected] {_j_summaries}"
        else:
            text = ""

        return DataAPIResponseException(
            text,
            command=command,
            raw_response=raw_response,
            error_descriptors=error_descriptors,
            warning_descriptors=warning_descriptors,
            **kwargs,
        )


@dataclass
class DataAPIHttpException(DataAPIException, httpx.HTTPStatusError):
    """
    A request to the Data API resulted in an HTTP 4xx or 5xx response.

    In most cases this comes with additional information: the purpose
    of this class is to present such information in a structured way,
    akin to what happens for the DataAPIResponseException, while
    still raising (a subclass of) `httpx.HTTPStatusError`.

    Attributes:
        text: a text message about the exception.
        error_descriptors: a list of all DataAPIErrorDescriptor objects
            found in the response.
    """

    text: str | None
    error_descriptors: list[DataAPIErrorDescriptor]

    def __init__(
        self,
        text: str | None,
        *,
        httpx_error: httpx.HTTPStatusError,
        error_descriptors: list[DataAPIErrorDescriptor],
    ) -> None:
        DataAPIException.__init__(self, text)
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
    ) -> DataAPIHttpException:
        """Parse a httpx status error into this exception."""

        raw_response: dict[str, Any]
        # the attempt to extract a response structure cannot afford failure.
        try:
            raw_response = httpx_error.response.json()
        except Exception:
            raw_response = {}
        error_descriptors = [
            DataAPIErrorDescriptor(error_dict)
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
class DataAPITimeoutException(DataAPIException):
    """
    A Data API operation timed out. This can be a request timeout occurring
    during a specific HTTP request, or can happen over the course of a method
    involving several requests in a row, such as a paginated find.

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
class CursorException(DataAPIException):
    """
    The cursor operation cannot be invoked if a cursor is not in its pristine
    state (i.e. is already being consumed or is exhausted altogether).

    Attributes:
        text: a text message about the exception.
        cursor_state: a string description of the current state
            of the cursor. See the documentation for Cursor.
    """

    text: str
    cursor_state: str

    def __init__(
        self,
        text: str,
        *,
        cursor_state: str,
    ) -> None:
        super().__init__(text)
        self.text = text
        self.cursor_state = cursor_state


@dataclass
class UnexpectedDataAPIResponseException(DataAPIException):
    """
    The Data API response is malformed in that it does not have
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
