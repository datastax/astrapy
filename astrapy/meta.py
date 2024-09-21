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
from typing import Any, Optional

from deprecation import DeprecatedWarning

from astrapy.defaults import (
    NAMESPACE_DEPRECATION_NOTICE_NS_DETAILS,
    NAMESPACE_DEPRECATION_NOTICE_NS_SUBJECT,
    NAMESPACE_DEPRECATION_NOTICE_UPDATEDBNS_DETAILS,
    NAMESPACE_DEPRECATION_NOTICE_UPDATEDBNS_SUBJECT,
)


def check_deprecated_vector_ize(
    vector: Any,
    vectors: Any,
    vectorize: Any,
    kind: str,
) -> None:
    # Version check is not done at all - it will be a manual handling once this
    # deprecation becomes a removal.
    passeds = [
        item
        for item in (
            "'vector'" if vector is not None else None,
            "'vectors'" if vectors is not None else None,
            "'vectorize'" if vectorize is not None else None,
        )
        if item is not None
    ]
    if passeds:
        passed_desc = (
            f"Usage of {', '.join(passeds)} parameter{'s' if len(passeds) > 1 else ''}"
            " in collection methods"
        )
        message: str
        if kind == "insert":
            message = "Use $vector or $vectorize fields in the documents instead."
        elif kind == "find":
            message = "Use $vector or $vectorize fields in the sort clause instead."
        else:
            message = "Replace with $vector or $vectorize appropriately."

        the_warning = DeprecatedWarning(
            passed_desc,
            deprecated_in="1.3.0",
            removed_in="2.0.0",
            details=message,
        )
        warnings.warn(
            the_warning,
            stacklevel=3,
        )


def check_namespace_keyspace(
    keyspace: Optional[str],
    namespace: Optional[str],
) -> Optional[str]:
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
    update_db_keyspace: Optional[bool],
    update_db_namespace: Optional[bool],
) -> Optional[bool]:
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
