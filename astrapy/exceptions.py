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

from astrapy.admin.devops_api_exceptions import (
    DevOpsAPIErrorDescriptor,
    DevOpsAPIException,
    DevOpsAPIHttpException,
    DevOpsAPIResponseException,
    DevOpsAPITimeoutException,
    UnexpectedDevOpsAPIResponseException,
)
from astrapy.data.info.collection_exceptions import CollectionNotFoundException
from astrapy.data.info.data_api_exceptions import (
    CursorException,
    DataAPIDetailedErrorDescriptor,
    DataAPIErrorDescriptor,
    DataAPIException,
    DataAPIHttpException,
    DataAPIResponseException,
    DataAPITimeoutException,
    UnexpectedDataAPIResponseException,
)
from astrapy.data.info.table_exceptions import TableNotFoundException

if TYPE_CHECKING:
    from astrapy.results import (
        CollectionDeleteResult,
        CollectionInsertManyResult,
        CollectionUpdateResult,
        OperationResult,
    )


class InvalidEnvironmentException(ValueError):
    """
    An operation was attempted, that is not available on the specified
    environment. For example, trying to get an AstraDBAdmin from a client
    set to a non-Astra environment.
    """

    pass


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
        partial_result: an CollectionInsertManyResult object, just like the one
            that would be the return value of the operation, had it succeeded
            completely.
    """

    partial_result: CollectionInsertManyResult

    def __init__(
        self,
        text: str,
        partial_result: CollectionInsertManyResult,
        *pargs: Any,
        **kwargs: Any,
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
        partial_result: a CollectionDeleteResult object, just like the one that would
            be the return value of the operation, had it succeeded completely.
    """

    partial_result: CollectionDeleteResult

    def __init__(
        self,
        text: str,
        partial_result: CollectionDeleteResult,
        *pargs: Any,
        **kwargs: Any,
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
        partial_result: an CollectionUpdateResult object, just like the one that
        would be the return value of the operation, had it succeeded completely.
    """

    partial_result: CollectionUpdateResult

    def __init__(
        self,
        text: str,
        partial_result: CollectionUpdateResult,
        *pargs: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(text, *pargs, **kwargs)
        self.partial_result = partial_result


def to_dataapi_timeout_exception(
    httpx_timeout: httpx.TimeoutException,
    timeout_context: _TimeoutContext,
) -> DataAPITimeoutException:
    text: str
    text_0 = str(httpx_timeout) or "timed out"
    timeout_ms = timeout_context.nominal_ms or timeout_context.request_ms
    if timeout_ms:
        text = f"{text_0} (timeout honoured: {timeout_ms} ms)"
    else:
        text = text_0
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
    timeout_context: _TimeoutContext,
) -> DevOpsAPITimeoutException:
    text: str
    text_0 = str(httpx_timeout) or "timed out"
    timeout_ms = timeout_context.nominal_ms or timeout_context.request_ms
    if timeout_ms:
        text = f"{text_0} (timeout honoured: {timeout_ms} ms)"
    else:
        text = text_0
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


@dataclass
class _TimeoutContext:
    """
    TODO
    """

    nominal_ms: int | None
    request_ms: int | None

    def __init__(
        self,
        *,
        request_ms: int | None,
        nominal_ms: int | None = None,
    ) -> None:
        self.nominal_ms = nominal_ms
        self.request_ms = request_ms

    def __bool__(self) -> bool:
        return self.nominal_ms is not None or self.request_ms is not None


class MultiCallTimeoutManager:
    """
    A helper class to keep track of timing and timeouts
    in a multi-call method context.

    Args:
        overall_timeout_ms: an optional max duration to track (milliseconds)

    Attributes:
        overall_timeout_ms: an optional max duration to track (milliseconds)
        started_ms: timestamp of the instance construction (milliseconds)
        deadline_ms: optional deadline in milliseconds (computed by the class).
    """

    overall_timeout_ms: int | None
    started_ms: int = -1
    deadline_ms: int | None

    def __init__(
        self, overall_timeout_ms: int | None, dev_ops_api: bool = False
    ) -> None:
        self.started_ms = int(time.time() * 1000)
        self.overall_timeout_ms = overall_timeout_ms
        self.dev_ops_api = dev_ops_api
        if self.overall_timeout_ms is not None:
            self.deadline_ms = self.started_ms + self.overall_timeout_ms
        else:
            self.deadline_ms = None

    def remaining_timeout(self, cap_time_ms: int | None = None) -> _TimeoutContext:
        """
        Ensure the deadline, if any, is not yet in the past.
        If it is, raise an appropriate timeout error.
        If not, return either None (if no timeout) or the remaining milliseconds.
        For use within the multi-call method.

        Args:
            cap_time_ms: an additional timeout constraint to cap the result of
                this method. If the remaining timeout from this manager exceeds
                the provided cap, the cap is returned.

        Returns:
            TODO
        """
        now_ms = int(time.time() * 1000)
        if self.deadline_ms is not None:
            if now_ms < self.deadline_ms:
                remaining = self.deadline_ms - now_ms
                if cap_time_ms is None:
                    return _TimeoutContext(
                        nominal_ms=self.overall_timeout_ms,
                        request_ms=remaining,
                    )
                else:
                    return _TimeoutContext(
                        nominal_ms=self.overall_timeout_ms,
                        request_ms=min(remaining, cap_time_ms),
                    )
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
            if cap_time_ms is None:
                return _TimeoutContext(
                    nominal_ms=self.overall_timeout_ms,
                    request_ms=None,
                )
            else:
                return _TimeoutContext(
                    nominal_ms=self.overall_timeout_ms,
                    request_ms=cap_time_ms,
                )


__all__ = [
    "CollectionNotFoundException",
    "TableNotFoundException",
    "DevOpsAPIException",
    "DevOpsAPIHttpException",
    "DevOpsAPITimeoutException",
    "DevOpsAPIErrorDescriptor",
    "UnexpectedDevOpsAPIResponseException",
    "DevOpsAPIResponseException",
    "DataAPIErrorDescriptor",
    "DataAPIDetailedErrorDescriptor",
    "DataAPIException",
    "DataAPIHttpException",
    "DataAPITimeoutException",
    "CursorException",
    "TooManyDocumentsToCountException",
    "UnexpectedDataAPIResponseException",
    "DataAPIResponseException",
    "CumulativeOperationException",
    "InsertManyException",
    "DeleteManyException",
    "UpdateManyException",
    "MultiCallTimeoutManager",
]

__pdoc__ = {
    "to_dataapi_timeout_exception": False,
    "MultiCallTimeoutManager": False,
}
