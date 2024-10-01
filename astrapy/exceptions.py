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

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import httpx

if TYPE_CHECKING:
    from astrapy.request_tools import TimeoutInfo
    from astrapy.results import (
        BulkWriteResult,
        DeleteResult,
        InsertManyResult,
        OperationResult,
        UpdateResult,
    )


class DevOpsAPIException(ValueError):
    """
    An exception specific to issuing requests to the DevOps API.
    """

    def __init__(self, text: str = ""):
        super().__init__(text)


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
class DevOpsAPIFaultyResponseException(DevOpsAPIException):
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


@dataclass
class DataAPIErrorDescriptor:
    """
    An object representing a single error returned from the Data API,
    typically with an error code and a text message.
    An API request would return with an HTTP 200 success error code,
    but contain a nonzero amount of these.

    A single response from the Data API may return zero, one or more of these.
    Moreover, some operations, such as an insert_many, may partally succeed
    yet return these errors about the rest of the operation (such as,
    some of the input documents could not be inserted).

    Attributes:
        error_code: a string code as found in the API "error" item.
        message: the text found in the API "error" item.
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

    def __init__(self, error_dict: dict[str, str]) -> None:
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


@dataclass
class DataAPIDetailedErrorDescriptor:
    """
    An object representing an errorful response from the Data API.
    Errors specific to the Data API (as opposed to e.g. network failures)
    would result in an HTTP 200 success response code but coming with
    one or more DataAPIErrorDescriptor objects.

    This object corresponds to one response, and as such its attributes
    are a single request payload, a single response, but a list of
    DataAPIErrorDescriptor instances.

    Attributes:
        error_descriptors: a list of DataAPIErrorDescriptor objects.
        command: the raw payload of the API request.
        raw_response: the full API response in the form of a dict.
    """

    error_descriptors: list[DataAPIErrorDescriptor]
    command: dict[str, Any] | None
    raw_response: dict[str, Any]


class DataAPIException(ValueError):
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
class CursorIsStartedException(DataAPIException):
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
class CollectionNotFoundException(DataAPIException):
    """
    A collection is found non-existing and the requested operation
    cannot be performed.

    Attributes:
        text: a text message about the exception.
        keyspace: the keyspace where the collection was supposed to be.
        namespace: an alias for 'keyspace'. *DEPRECATED*, removal in 2.0
        collection_name: the name of the expected collection.
    """

    text: str
    keyspace: str
    namespace: str
    collection_name: str

    def __init__(
        self,
        text: str,
        *,
        keyspace: str,
        collection_name: str,
    ) -> None:
        super().__init__(text)
        self.text = text
        self.keyspace = keyspace
        self.namespace = keyspace
        self.collection_name = collection_name


@dataclass
class CollectionAlreadyExistsException(DataAPIException):
    """
    An operation expected a collection not to exist, yet it has
    been detected as pre-existing.

    Attributes:
        text: a text message about the exception.
        keyspace: the keyspace where the collection was expected not to exist.
        namespace: an alias for 'keyspace'. *DEPRECATED*, removal in 2.0
        collection_name: the name of the collection.
    """

    text: str
    keyspace: str
    namespace: str
    collection_name: str

    def __init__(
        self,
        text: str,
        *,
        keyspace: str,
        collection_name: str,
    ) -> None:
        super().__init__(text)
        self.text = text
        self.keyspace = keyspace
        self.namespace = keyspace
        self.collection_name = collection_name


@dataclass
class TooManyDocumentsToCountException(DataAPIException):
    """
    A `count_documents()` operation failed because the resulting number of documents
    exceeded either the upper bound set by the caller or the hard limit imposed
    by the Data API.

    Attributes:
        text: a text message about the exception.
        server_max_count_exceeded: True if the count limit imposed by the API
            is reached. In that case, increasing the upper bound in the method
            invocation is of no help.
    """

    text: str
    server_max_count_exceeded: bool

    def __init__(
        self,
        text: str,
        *,
        server_max_count_exceeded: bool,
    ) -> None:
        super().__init__(text)
        self.text = text
        self.server_max_count_exceeded = server_max_count_exceeded


@dataclass
class DataAPIFaultyResponseException(DataAPIException):
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


@dataclass
class DataAPIResponseException(DataAPIException):
    """
    The Data API returned an HTTP 200 success response, which however
    reports about API-specific error(s), possibly alongside partial successes.

    This exception is related to an operation that can have spanned several
    HTTP requests in sequence (e.g. a chunked insert_many). For this
    reason, it should be not thought as being in a 1:1 relation with
    actual API requests, rather with operations invoked by the user,
    such as the methods of the Collection object.

    Attributes:
        text: a text message about the exception.
        error_descriptors: a list of all DataAPIErrorDescriptor objects
            found across all requests involved in this exception, which are
            possibly more than one.
        detailed_error_descriptors: a list of DataAPIDetailedErrorDescriptor
            objects, one for each of the requests performed during this operation.
            For single-request methods, such as insert_one, this list always
            has a single element.
    """

    text: str | None
    error_descriptors: list[DataAPIErrorDescriptor]
    detailed_error_descriptors: list[DataAPIDetailedErrorDescriptor]

    def __init__(
        self,
        text: str | None,
        *,
        error_descriptors: list[DataAPIErrorDescriptor],
        detailed_error_descriptors: list[DataAPIDetailedErrorDescriptor],
    ) -> None:
        super().__init__(text)
        self.text = text
        self.error_descriptors = error_descriptors
        self.detailed_error_descriptors = detailed_error_descriptors

    @classmethod
    def from_response(
        cls,
        command: dict[str, Any] | None,
        raw_response: dict[str, Any],
        **kwargs: Any,
    ) -> DataAPIResponseException:
        """Parse a raw response from the API into this exception."""

        return cls.from_responses(
            commands=[command],
            raw_responses=[raw_response],
            **kwargs,
        )

    @classmethod
    def from_responses(
        cls,
        commands: list[dict[str, Any] | None],
        raw_responses: list[dict[str, Any]],
        **kwargs: Any,
    ) -> DataAPIResponseException:
        """Parse a list of raw responses from the API into this exception."""

        detailed_error_descriptors: list[DataAPIDetailedErrorDescriptor] = []
        for command, raw_response in zip(commands, raw_responses):
            if raw_response.get("errors", []):
                error_descriptors = [
                    DataAPIErrorDescriptor(error_dict)
                    for error_dict in raw_response["errors"]
                ]
                detailed_error_descriptor = DataAPIDetailedErrorDescriptor(
                    error_descriptors=error_descriptors,
                    command=command,
                    raw_response=raw_response,
                )
                detailed_error_descriptors.append(detailed_error_descriptor)

        # flatten
        error_descriptors = [
            error_descriptor
            for d_e_d in detailed_error_descriptors
            for error_descriptor in d_e_d.error_descriptors
        ]

        if error_descriptors:
            text = error_descriptors[0].message
        else:
            text = ""

        return cls(
            text,
            error_descriptors=error_descriptors,
            detailed_error_descriptors=detailed_error_descriptors,
            **kwargs,
        )

    def data_api_response_exception(self) -> DataAPIResponseException:
        """Cast the exception, whatever the subclass, into this parent superclass."""

        return DataAPIResponseException(
            text=self.text,
            error_descriptors=self.error_descriptors,
            detailed_error_descriptors=self.detailed_error_descriptors,
        )


class CumulativeOperationException(DataAPIResponseException):
    """
    An exception of type DataAPIResponseException (see) occurred
    during an operation that in general spans several requests.
    As such, besides information on the error, it may have accumulated
    a partial result from past successful Data API requests.

    Attributes:
        text: a text message about the exception.
        error_descriptors: a list of all DataAPIErrorDescriptor objects
            found across all requests involved in this exception, which are
            possibly more than one.
        detailed_error_descriptors: a list of DataAPIDetailedErrorDescriptor
            objects, one for each of the requests performed during this operation.
            For single-request methods, such as insert_one, this list always
            has a single element.
        partial_result: an OperationResult object, just like the one that would
            be the return value of the operation, had it succeeded completely.
    """

    partial_result: OperationResult


@dataclass
class InsertManyException(CumulativeOperationException):
    """
    An exception of type DataAPIResponseException (see) occurred
    during an insert_many (that in general spans several requests).
    As such, besides information on the error, it may have accumulated
    a partial result from past successful Data API requests.

    Attributes:
        text: a text message about the exception.
        error_descriptors: a list of all DataAPIErrorDescriptor objects
            found across all requests involved in this exception, which are
            possibly more than one.
        detailed_error_descriptors: a list of DataAPIDetailedErrorDescriptor
            objects, one for each of the requests performed during this operation.
            For single-request methods, such as insert_one, this list always
            has a single element.
        partial_result: an InsertManyResult object, just like the one that would
            be the return value of the operation, had it succeeded completely.
    """

    partial_result: InsertManyResult

    def __init__(
        self, text: str, partial_result: InsertManyResult, *pargs: Any, **kwargs: Any
    ) -> None:
        super().__init__(text, *pargs, **kwargs)
        self.partial_result = partial_result


@dataclass
class DeleteManyException(CumulativeOperationException):
    """
    An exception of type DataAPIResponseException (see) occurred
    during a delete_many (that in general spans several requests).
    As such, besides information on the error, it may have accumulated
    a partial result from past successful Data API requests.

    Attributes:
        text: a text message about the exception.
        error_descriptors: a list of all DataAPIErrorDescriptor objects
            found across all requests involved in this exception, which are
            possibly more than one.
        detailed_error_descriptors: a list of DataAPIDetailedErrorDescriptor
            objects, one for each of the requests performed during this operation.
            For single-request methods, such as insert_one, this list always
            has a single element.
        partial_result: a DeleteResult object, just like the one that would
            be the return value of the operation, had it succeeded completely.
    """

    partial_result: DeleteResult

    def __init__(
        self, text: str, partial_result: DeleteResult, *pargs: Any, **kwargs: Any
    ) -> None:
        super().__init__(text, *pargs, **kwargs)
        self.partial_result = partial_result


@dataclass
class UpdateManyException(CumulativeOperationException):
    """
    An exception of type DataAPIResponseException (see) occurred
    during an update_many (that in general spans several requests).
    As such, besides information on the error, it may have accumulated
    a partial result from past successful Data API requests.

    Attributes:
        text: a text message about the exception.
        error_descriptors: a list of all DataAPIErrorDescriptor objects
            found across all requests involved in this exception, which are
            possibly more than one.
        detailed_error_descriptors: a list of DataAPIDetailedErrorDescriptor
            objects, one for each of the requests performed during this operation.
            For single-request methods, such as insert_one, this list always
            has a single element.
        partial_result: an UpdateResult object, just like the one that would
            be the return value of the operation, had it succeeded completely.
    """

    partial_result: UpdateResult

    def __init__(
        self, text: str, partial_result: UpdateResult, *pargs: Any, **kwargs: Any
    ) -> None:
        super().__init__(text, *pargs, **kwargs)
        self.partial_result = partial_result


@dataclass
class BulkWriteException(DataAPIResponseException):
    """
    An exception of type DataAPIResponseException (see) occurred
    during a bulk_write of a list of operations.
    As such, besides information on the error, it may have accumulated
    a partial result from past successful operations.

    Attributes:
        text: a text message about the exception.
        error_descriptors: a list of all DataAPIErrorDescriptor objects
            found across all requests involved in the first
            operation that has failed.
        detailed_error_descriptors: a list of DataAPIDetailedErrorDescriptor
            objects, one for each of the requests performed during the first operation
            that has failed.
        partial_result: a BulkWriteResult object, just like the one that would
            be the return value of the operation, had it succeeded completely.
        exceptions: a list of DataAPIResponseException objects, one for each
            operation in the bulk that has failed. This information is made
            available here since the top-level fields of this error
            only surface the first such failure that is detected across the bulk.
            In case of bulk_writes with ordered=True, this trivially contains
            a single element, the same described by the top-level fields
            text, error_descriptors and detailed_error_descriptors.
    """

    partial_result: BulkWriteResult
    exceptions: list[DataAPIResponseException]

    def __init__(
        self,
        text: str | None,
        partial_result: BulkWriteResult,
        exceptions: list[DataAPIResponseException],
        *pargs: Any,
        **kwargs: Any,
    ) -> None:
        _text = text or "Bulk write exception"
        super().__init__(_text, *pargs, **kwargs)
        self.partial_result = partial_result
        self.exceptions = exceptions


def to_dataapi_timeout_exception(
    httpx_timeout: httpx.TimeoutException,
) -> DataAPITimeoutException:
    text = str(httpx_timeout)
    if isinstance(httpx_timeout, httpx.ConnectTimeout):
        timeout_type = "connect"
    elif isinstance(httpx_timeout, httpx.ReadTimeout):
        timeout_type = "read"
    elif isinstance(httpx_timeout, httpx.WriteTimeout):
        timeout_type = "write"
    elif isinstance(httpx_timeout, httpx.PoolTimeout):
        timeout_type = "pool"
    else:
        timeout_type = "generic"
    if httpx_timeout.request:
        endpoint = str(httpx_timeout.request.url)
        if isinstance(httpx_timeout.request.content, bytes):
            raw_payload = httpx_timeout.request.content.decode()
        else:
            raw_payload = None
    else:
        endpoint = None
        raw_payload = None
    return DataAPITimeoutException(
        text=text,
        timeout_type=timeout_type,
        endpoint=endpoint,
        raw_payload=raw_payload,
    )


def to_devopsapi_timeout_exception(
    httpx_timeout: httpx.TimeoutException,
) -> DevOpsAPITimeoutException:
    text = str(httpx_timeout) or "timed out"
    if isinstance(httpx_timeout, httpx.ConnectTimeout):
        timeout_type = "connect"
    elif isinstance(httpx_timeout, httpx.ReadTimeout):
        timeout_type = "read"
    elif isinstance(httpx_timeout, httpx.WriteTimeout):
        timeout_type = "write"
    elif isinstance(httpx_timeout, httpx.PoolTimeout):
        timeout_type = "pool"
    else:
        timeout_type = "generic"
    if httpx_timeout.request:
        endpoint = str(httpx_timeout.request.url)
        if isinstance(httpx_timeout.request.content, bytes):
            raw_payload = httpx_timeout.request.content.decode()
        else:
            raw_payload = None
    else:
        endpoint = None
        raw_payload = None
    return DevOpsAPITimeoutException(
        text=text,
        timeout_type=timeout_type,
        endpoint=endpoint,
        raw_payload=raw_payload,
    )


def base_timeout_info(max_time_ms: int | None) -> TimeoutInfo | None:
    if max_time_ms is not None:
        return {"base": max_time_ms / 1000.0}
    else:
        return None


class MultiCallTimeoutManager:
    """
    A helper class to keep track of timing and timeouts
    in a multi-call method context.

    Args:
        overall_max_time_ms: an optional max duration to track (milliseconds)

    Attributes:
        overall_max_time_ms: an optional max duration to track (milliseconds)
        started_ms: timestamp of the instance construction (milliseconds)
        deadline_ms: optional deadline in milliseconds (computed by the class).
    """

    overall_max_time_ms: int | None
    started_ms: int = -1
    deadline_ms: int | None

    def __init__(
        self, overall_max_time_ms: int | None, dev_ops_api: bool = False
    ) -> None:
        self.started_ms = int(time.time() * 1000)
        self.overall_max_time_ms = overall_max_time_ms
        self.dev_ops_api = dev_ops_api
        if self.overall_max_time_ms is not None:
            self.deadline_ms = self.started_ms + self.overall_max_time_ms
        else:
            self.deadline_ms = None

    def remaining_timeout_ms(self) -> int | None:
        """
        Ensure the deadline, if any, is not yet in the past.
        If it is, raise an appropriate timeout error.
        If not, return either None (if no timeout) or the remaining milliseconds.
        For use within the multi-call method.
        """
        now_ms = int(time.time() * 1000)
        if self.deadline_ms is not None:
            if now_ms < self.deadline_ms:
                return self.deadline_ms - now_ms
            else:
                if not self.dev_ops_api:
                    raise DataAPITimeoutException(
                        text="Operation timed out.",
                        timeout_type="generic",
                        endpoint=None,
                        raw_payload=None,
                    )
                else:
                    raise DevOpsAPITimeoutException(
                        text="Operation timed out.",
                        timeout_type="generic",
                        endpoint=None,
                        raw_payload=None,
                    )
        else:
            return None

    def remaining_timeout_info(self) -> TimeoutInfo | None:
        """
        Ensure the deadline, if any, is not yet in the past.
        If it is, raise an appropriate timeout error.
        It it is not, or there is no deadline, return a suitable TimeoutInfo
        for use within the multi-call method.
        """
        return base_timeout_info(max_time_ms=self.remaining_timeout_ms())


__pdoc__ = {
    "base_timeout_info": False,
    "to_dataapi_timeout_exception": False,
    "MultiCallTimeoutManager": False,
}
