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
    CursorException,
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
from astrapy.utils.api_options import FullTimeoutOptions


def _min_labeled_timeout(
    *timeouts: tuple[int | None, str | None],
) -> tuple[int, str | None]:
    _non_null: list[tuple[int, str | None]] = [
        to  # type: ignore[misc]
        for to in timeouts
        if to[0] is not None
    ]
    if _non_null:
        min_to, min_lb = min(_non_null, key=lambda p: p[0])
        # min_to is never None, this is for added robustness
        return (min_to or 0, min_lb)
    else:
        return (0, None)


def _select_singlereq_timeout_gm(
    *,
    timeout_options: FullTimeoutOptions,
    general_method_timeout_ms: int | None,
    request_timeout_ms: int | None = None,
    timeout_ms: int | None = None,
) -> tuple[int, str | None]:
    """
    Apply the logic for determining and labeling the timeout for
    (non-admin) single-request methods.

    If no int args are passed, pick (and correctly label) the least of the
    involved parameters.
    If any of the int args are passed, pick (and correctly labeled) the least
    of them, disregarding the options altogether.
    """
    if all(
        iarg is None
        for iarg in (general_method_timeout_ms, request_timeout_ms, timeout_ms)
    ):
        ao_r = timeout_options.request_timeout_ms
        ao_gm = timeout_options.general_method_timeout_ms
        if ao_r < ao_gm:
            return (ao_r, "request_timeout_ms")
        else:
            return (ao_gm, "general_method_timeout_ms")
    else:
        return _min_labeled_timeout(
            (general_method_timeout_ms, "general_method_timeout_ms"),
            (request_timeout_ms, "request_timeout_ms"),
            (timeout_ms, "timeout_ms"),
        )


def _select_singlereq_timeout_ca(
    *,
    timeout_options: FullTimeoutOptions,
    collection_admin_timeout_ms: int | None,
    request_timeout_ms: int | None = None,
    timeout_ms: int | None = None,
) -> tuple[int, str | None]:
    """
    Apply the logic for determining and labeling the timeout for
    (collection-admin) single-request methods.

    If no int args are passed, pick (and correctly label) the least of the
    involved parameters.
    If any of the int args are passed, pick (and correctly labeled) the least
    of them, disregarding the options altogether.
    """
    if all(
        iarg is None
        for iarg in (collection_admin_timeout_ms, request_timeout_ms, timeout_ms)
    ):
        ao_r = timeout_options.request_timeout_ms
        ao_gm = timeout_options.collection_admin_timeout_ms
        if ao_r < ao_gm:
            return (ao_r, "request_timeout_ms")
        else:
            return (ao_gm, "collection_admin_timeout_ms")
    else:
        return _min_labeled_timeout(
            (collection_admin_timeout_ms, "collection_admin_timeout_ms"),
            (request_timeout_ms, "request_timeout_ms"),
            (timeout_ms, "timeout_ms"),
        )


def _select_singlereq_timeout_ta(
    *,
    timeout_options: FullTimeoutOptions,
    table_admin_timeout_ms: int | None,
    request_timeout_ms: int | None = None,
    timeout_ms: int | None = None,
) -> tuple[int, str | None]:
    """
    Apply the logic for determining and labeling the timeout for
    (table-admin) single-request methods.

    If no int args are passed, pick (and correctly label) the least of the
    involved parameters.
    If any of the int args are passed, pick (and correctly labeled) the least
    of them, disregarding the options altogether.
    """
    if all(
        iarg is None
        for iarg in (table_admin_timeout_ms, request_timeout_ms, timeout_ms)
    ):
        ao_r = timeout_options.request_timeout_ms
        ao_gm = timeout_options.table_admin_timeout_ms
        if ao_r < ao_gm:
            return (ao_r, "request_timeout_ms")
        else:
            return (ao_gm, "table_admin_timeout_ms")
    else:
        return _min_labeled_timeout(
            (table_admin_timeout_ms, "table_admin_timeout_ms"),
            (request_timeout_ms, "request_timeout_ms"),
            (timeout_ms, "timeout_ms"),
        )


def _select_singlereq_timeout_da(
    *,
    timeout_options: FullTimeoutOptions,
    database_admin_timeout_ms: int | None,
    request_timeout_ms: int | None = None,
    timeout_ms: int | None = None,
) -> tuple[int, str | None]:
    """
    Apply the logic for determining and labeling the timeout for
    (database-admin) single-request methods.

    If no int args are passed, pick (and correctly label) the least of the
    involved parameters.
    If any of the int args are passed, pick (and correctly labeled) the least
    of them, disregarding the options altogether.
    """
    if all(
        iarg is None
        for iarg in (database_admin_timeout_ms, request_timeout_ms, timeout_ms)
    ):
        ao_r = timeout_options.request_timeout_ms
        ao_gm = timeout_options.database_admin_timeout_ms
        if ao_r < ao_gm:
            return (ao_r, "request_timeout_ms")
        else:
            return (ao_gm, "database_admin_timeout_ms")
    else:
        return _min_labeled_timeout(
            (database_admin_timeout_ms, "database_admin_timeout_ms"),
            (request_timeout_ms, "request_timeout_ms"),
            (timeout_ms, "timeout_ms"),
        )


def _select_singlereq_timeout_ka(
    *,
    timeout_options: FullTimeoutOptions,
    keyspace_admin_timeout_ms: int | None,
    request_timeout_ms: int | None = None,
    timeout_ms: int | None = None,
) -> tuple[int, str | None]:
    """
    Apply the logic for determining and labeling the timeout for
    (keyspace-admin) single-request methods.

    If no int args are passed, pick (and correctly label) the least of the
    involved parameters.
    If any of the int args are passed, pick (and correctly labeled) the least
    of them, disregarding the options altogether.
    """
    if all(
        iarg is None
        for iarg in (keyspace_admin_timeout_ms, request_timeout_ms, timeout_ms)
    ):
        ao_r = timeout_options.request_timeout_ms
        ao_gm = timeout_options.keyspace_admin_timeout_ms
        if ao_r < ao_gm:
            return (ao_r, "request_timeout_ms")
        else:
            return (ao_gm, "keyspace_admin_timeout_ms")
    else:
        return _min_labeled_timeout(
            (keyspace_admin_timeout_ms, "keyspace_admin_timeout_ms"),
            (request_timeout_ms, "request_timeout_ms"),
            (timeout_ms, "timeout_ms"),
        )


def _first_valid_timeout(
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


class InvalidEnvironmentException(Exception):
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
    This class encodes standardized "enriched information" attached to a timeout
    value to obey. This makes it possible, in case the timeout is raised, to present
    the user with a better error message detailing the name of the setting responsible
    for the timeout and the "nominal" value (which may not always coincide with the
    actual elapsed number of milliseconds because of cumulative timeouts spanning
    several HTTP requests).

    Args:
        nominal_ms: the original timeout in milliseconds that was ultimately set by
            the user.
        request_ms: the actual number of millisecond a given HTTP request was allowed
            to last. This may be smaller than `nominal_ms` because of timeouts imposed
            on a succession of requests.
        label: a string, providing the name of the timeout setting as known by the user.
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
        timeout_label: a string label identifying the `overall_timeout_ms` in a way
            that is understood by the user who can set timeouts.
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
            cap_timeout_label: the label identifying the "cap timeout" if one is
                set. This is for the purpose of tracing the "lineage" of timeout
                settings in a way that is transparent to the user.

        Returns:
            A _TimeoutContext appropriately detailing the residual time an overall
            operation is allowed to last. Alternatively, the method may not return
            and raise a DataAPITimeoutException/DevOpsAPITimeoutException directly.
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
    "MultiCallTimeoutManager",
    "TooManyRowsToCountException",
    "TableInsertManyException",
]

__pdoc__ = {
    "to_dataapi_timeout_exception": False,
    "MultiCallTimeoutManager": False,
}
