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

import httpx

from astrapy.exceptions.collection_exceptions import (
    CollectionDeleteManyException,
    CollectionInsertManyException,
    CollectionUpdateManyException,
    TooManyDocumentsToCountException,
)
from astrapy.exceptions.data_api_exceptions import (
    CumulativeOperationException,
    CursorException,
    DataAPIDetailedErrorDescriptor,
    DataAPIErrorDescriptor,
    DataAPIException,
    DataAPIHttpException,
    DataAPIResponseException,
    DataAPITimeoutException,
    UnexpectedDataAPIResponseException,
)
from astrapy.exceptions.devops_api_exceptions import (
    DevOpsAPIErrorDescriptor,
    DevOpsAPIException,
    DevOpsAPIHttpException,
    DevOpsAPIResponseException,
    DevOpsAPITimeoutException,
    UnexpectedDevOpsAPIResponseException,
)
from astrapy.exceptions.table_exceptions import (
    TableInsertManyException,
    TooManyRowsToCountException,
)


def first_valid_timeout(
    *items: tuple[int | None, str | None],
) -> tuple[int, str | None]:
    # items are: (timeout ms, label)
    not_nulls = [itm for itm in items if itm[0] is not None]
    if not_nulls:
        return not_nulls[0]  # type: ignore[return-value]
    else:
        # If no non-nulls provided, return zero
        # (a timeout of zero will stand for 'no timeout' later on to the request).
        return 0, None


class InvalidEnvironmentException(ValueError):
    """
    An operation was attempted, that is not available on the specified
    environment. For example, trying to get an AstraDBAdmin from a client
    set to a non-Astra environment.
    """

    pass


def to_dataapi_timeout_exception(
    httpx_timeout: httpx.TimeoutException,
    timeout_context: _TimeoutContext,
) -> DataAPITimeoutException:
    text: str
    text_0 = str(httpx_timeout) or "timed out"
    timeout_ms = timeout_context.nominal_ms or timeout_context.request_ms
    timeout_label = timeout_context.label
    if timeout_ms:
        if timeout_label:
            text = f"{text_0} (timeout honoured: {timeout_label} = {timeout_ms} ms)"
        else:
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
    timeout_label = timeout_context.label
    if timeout_ms:
        if timeout_label:
            text = f"{text_0} (timeout honoured: {timeout_label} = {timeout_ms} ms)"
        else:
            text = f"{text_0} (timeout honoured: {timeout_ms} ms)"
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
    label: str | None

    def __init__(
        self,
        *,
        request_ms: int | None,
        nominal_ms: int | None = None,
        label: str | None = None,
    ) -> None:
        self.nominal_ms = nominal_ms
        self.request_ms = request_ms
        self.label = label

    def __bool__(self) -> bool:
        return self.nominal_ms is not None or self.request_ms is not None


class MultiCallTimeoutManager:
    """
    A helper class to keep track of timing and timeouts
    in a multi-call method context.

    Args:
        overall_timeout_ms: an optional max duration to track (milliseconds)
        dev_ops_api: whether this manager is used in a DevOps context (a fact
            which affects which timeout exception is raised if needed).

    Attributes:
        overall_timeout_ms: an optional max duration to track (milliseconds)
        started_ms: timestamp of the instance construction (milliseconds)
        deadline_ms: optional deadline in milliseconds (computed by the class).
        timeout_label: TODO
    """

    overall_timeout_ms: int | None
    started_ms: int = -1
    deadline_ms: int | None
    timeout_label: str | None

    def __init__(
        self,
        overall_timeout_ms: int | None,
        dev_ops_api: bool = False,
        timeout_label: str | None = None,
    ) -> None:
        self.started_ms = int(time.time() * 1000)
        self.timeout_label = timeout_label
        # zero timeouts provided internally are mapped to None for deadline mgmt:
        self.overall_timeout_ms = overall_timeout_ms or None
        self.dev_ops_api = dev_ops_api
        if self.overall_timeout_ms is not None:
            self.deadline_ms = self.started_ms + self.overall_timeout_ms
        else:
            self.deadline_ms = None

    def remaining_timeout(
        self, cap_time_ms: int | None = None, cap_timeout_label: str | None = None
    ) -> _TimeoutContext:
        """
        Ensure the deadline, if any, is not yet in the past.
        If it is, raise an appropriate timeout error.
        If not, return either None (if no timeout) or the remaining milliseconds.
        For use within the multi-call method.

        Args:
            cap_time_ms: an additional timeout constraint to cap the result of
                this method. If the remaining timeout from this manager exceeds
                the provided cap, the cap is returned.
            cap_timeout_label: TODO

        Returns:
            TODO
        """

        # a zero 'cap' must be treated as None:
        _cap_time_ms = cap_time_ms or None
        now_ms = int(time.time() * 1000)
        if self.deadline_ms is not None:
            if now_ms < self.deadline_ms:
                remaining = self.deadline_ms - now_ms
                if _cap_time_ms is None:
                    return _TimeoutContext(
                        nominal_ms=self.overall_timeout_ms,
                        request_ms=remaining,
                        label=self.timeout_label,
                    )
                else:
                    if remaining > _cap_time_ms:
                        return _TimeoutContext(
                            nominal_ms=_cap_time_ms,
                            request_ms=_cap_time_ms,
                            label=cap_timeout_label,
                        )
                    else:
                        return _TimeoutContext(
                            nominal_ms=self.overall_timeout_ms,
                            request_ms=remaining,
                            label=self.timeout_label,
                        )
            else:
                err_msg: str
                if self.timeout_label:
                    err_msg = (
                        f"Operation timed out (timeout honoured: {self.timeout_label} "
                        f"= {self.overall_timeout_ms} ms)."
                    )
                else:
                    err_msg = f"Operation timed out (timeout honoured: {self.overall_timeout_ms} ms)."
                if not self.dev_ops_api:
                    raise DataAPITimeoutException(
                        text=err_msg,
                        timeout_type="generic",
                        endpoint=None,
                        raw_payload=None,
                    )
                else:
                    raise DevOpsAPITimeoutException(
                        text=err_msg,
                        timeout_type="generic",
                        endpoint=None,
                        raw_payload=None,
                    )
        else:
            if _cap_time_ms is None:
                return _TimeoutContext(
                    nominal_ms=self.overall_timeout_ms,
                    request_ms=None,
                    label=self.timeout_label,
                )
            else:
                return _TimeoutContext(
                    nominal_ms=_cap_time_ms,
                    request_ms=_cap_time_ms,
                    label=cap_timeout_label,
                )


__all__ = [
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
    "CollectionInsertManyException",
    "CollectionDeleteManyException",
    "CollectionUpdateManyException",
    "CumulativeOperationException",
    "MultiCallTimeoutManager",
    "TooManyRowsToCountException",
    "TableInsertManyException",
]

__pdoc__ = {
    "to_dataapi_timeout_exception": False,
    "MultiCallTimeoutManager": False,
}
