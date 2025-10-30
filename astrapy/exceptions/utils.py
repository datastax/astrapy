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

from astrapy.utils.api_options import FullTimeoutOptions


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
