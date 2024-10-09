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

import warnings
from typing import Sequence

from deprecation import DeprecatedWarning

from astrapy.constants import CallerType
from astrapy.defaults import (
    CALLER_NAME_VERSION_DEPRECATION_NOTICE_DETAILS,
    NAMESPACE_DEPRECATION_NOTICE_NS_DETAILS,
    NAMESPACE_DEPRECATION_NOTICE_NS_SUBJECT,
    NAMESPACE_DEPRECATION_NOTICE_UPDATEDBNS_DETAILS,
    NAMESPACE_DEPRECATION_NOTICE_UPDATEDBNS_SUBJECT,
)


def check_caller_parameters(
    callers: Sequence[CallerType],
    caller_name: str | None,
    caller_version: str | None,
) -> Sequence[CallerType]:
    if caller_name is None and caller_version is None:
        return callers
    else:
        # issue a deprecation warning
        the_warning = DeprecatedWarning(
            "Use of parameters `caller_name` and `caller_version`",
            deprecated_in="1.5.1",
            removed_in="2.0.0",
            details=CALLER_NAME_VERSION_DEPRECATION_NOTICE_DETAILS,
        )
        warnings.warn(
            the_warning,
            stacklevel=3,
        )
        if not callers:
            return [(caller_name, caller_version)]
        else:
            msg = (
                "None of the deprecated `caller_name` and `caller_version` "
                "parameters can be passed if the newer `callers` parameter is "
                "also provided."
            )
            raise ValueError(msg)


def check_deprecated_id_region(
    id: str | None,
    region: str | None,
) -> None:
    # issue a deprecation warning if a database "id" is passed,
    # possibly accompanied by a "region".

    if id is not None:
        if region is None:
            deprecation_subject = "Passing an `id` parameter"
        else:
            deprecation_subject = "Passing an `id` parameter with a `region`"
        # issue a deprecation warning
        the_warning = DeprecatedWarning(
            deprecation_subject,
            deprecated_in="1.5.1",
            removed_in="2.0.0",
            details="Please switch to using the API Endpoint.",
        )
        warnings.warn(
            the_warning,
            stacklevel=3,
        )


def check_namespace_keyspace(
    keyspace: str | None,
    namespace: str | None,
) -> str | None:
    # normalize the two aliased parameter names, raising deprecation
    # when needed and an error if both parameter supplied.
    # The returned value is the final one for the parameter.

    if namespace is None:
        # no need for deprecation nor exceptions
        return keyspace
    else:
        # issue a deprecation warning
        the_warning = DeprecatedWarning(
            NAMESPACE_DEPRECATION_NOTICE_NS_SUBJECT,
            deprecated_in="1.5.0",
            removed_in="2.0.0",
            details=NAMESPACE_DEPRECATION_NOTICE_NS_DETAILS,
        )
        warnings.warn(
            the_warning,
            stacklevel=3,
        )

        if keyspace is None:
            return namespace
        else:
            msg = (
                "Parameters `keyspace` and `namespace` "
                "(a deprecated alias for the former) cannot be passed at the same time."
            )
            raise ValueError(msg)


def check_update_db_namespace_keyspace(
    update_db_keyspace: bool | None,
    update_db_namespace: bool | None,
) -> bool | None:
    # normalize the two aliased parameter names, raising deprecation
    # when needed and an error if both parameter supplied.
    # The returned value is the final one for the parameter.

    if update_db_namespace is None:
        # no need for deprecation nor exceptions
        return update_db_keyspace
    else:
        # issue a deprecation warning
        the_warning = DeprecatedWarning(
            NAMESPACE_DEPRECATION_NOTICE_UPDATEDBNS_SUBJECT,
            deprecated_in="1.5.0",
            removed_in="2.0.0",
            details=NAMESPACE_DEPRECATION_NOTICE_UPDATEDBNS_DETAILS,
        )
        warnings.warn(
            the_warning,
            stacklevel=3,
        )

        if update_db_keyspace is None:
            return update_db_namespace
        else:
            msg = (
                "Parameters `update_db_keyspace` and `update_db_namespace` "
                "(a deprecated alias for the former) cannot be passed at the same time."
            )
            raise ValueError(msg)
