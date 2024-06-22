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
from typing import Any

from deprecation import DeprecatedWarning


def check_deprecated_vector_ize(
    vector: Any,
    vectors: Any,
    vectorize: Any,
    kind: str,
    from_async_method: bool = False,
    from_operation_class: bool = False,
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
        #
        the_warning = DeprecatedWarning(
            passed_desc,
            deprecated_in="1.3.0",
            removed_in="2.0.0",
            details=message,
        )
        # trying to surface the warning at the rick stack level to best inform user:
        # (considering both the error-recast wrap decorator and the internals)
        if from_async_method:
            if from_operation_class:
                s_level = 3
            else:
                s_level = 2
        else:
            if from_operation_class:
                s_level = 3
            else:
                s_level = 4
        warnings.warn(
            the_warning,
            stacklevel=s_level,
        )
