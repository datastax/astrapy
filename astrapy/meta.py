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
