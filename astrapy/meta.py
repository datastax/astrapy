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

from deprecation import DeprecatedWarning

from astrapy.defaults import (
    NAMESPACE_DEPRECATION_NOTICE_NS_DETAILS,
    NAMESPACE_DEPRECATION_NOTICE_NS_SUBJECT,
    NAMESPACE_DEPRECATION_NOTICE_UPDATEDBNS_DETAILS,
    NAMESPACE_DEPRECATION_NOTICE_UPDATEDBNS_SUBJECT,
)


def check_deprecated_alias(
    new_name: str | None,
    deprecated_name: str | None,
) -> str | None:
    """Generic blueprint utility for deprecating parameters through an alias.

    Normalize the two aliased parameter names, raising deprecation
    when needed and an error if both parameter supplied.
    The returned value is the final one for the parameter.
    """

    if deprecated_name is None:
        # no need for deprecation nor exceptions
        return new_name
    else:
        # issue a deprecation warning
        the_warning = DeprecatedWarning(
            "Parameter 'deprecated_name'",
            deprecated_in="2.0.0",
            removed_in="3.0.0",
            details="Please use 'new_name' instead.",
        )
        warnings.warn(
            the_warning,
            stacklevel=3,
        )

        if new_name is None:
            return deprecated_name
        else:
            msg = (
                "Parameters `new_name` and `deprecated_name` "
                "(a deprecated alias for the former) cannot be passed at the same time."
            )
            raise ValueError(msg)


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
