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

from enum import Enum
from typing import Any, Dict, Iterable, Optional, Union


DocumentType = Dict[str, Any]
ProjectionType = Union[Iterable[str], Dict[str, bool]]


class ReturnDocument(Enum):
    """
    Admitted values for the `return_document` parameter in 
    `find_one_and_replace` and `find_one_and_update` collection
    methods.
    """
    BEFORE = "before"
    AFTER = "after"


def normalize_optional_projection(
    projection: Optional[ProjectionType],
    ensure_fields: Iterable[str] = set(),
) -> Optional[Dict[str, bool]]:
    _ensure_fields = set(ensure_fields)
    if projection:
        if isinstance(projection, dict):
            if any(bool(v) for v in projection.values()):
                # positive projection: {a: True, b: True ...}
                return {
                    k: projection.get(k, True)
                    for k in list(projection.keys()) + list(_ensure_fields)
                }
            else:
                # negative projection: {x: False, y: False, ...}
                return {k: v for k, v in projection.items() if k not in _ensure_fields}
        else:
            # an iterable over strings
            return {field: True for field in list(projection) + list(_ensure_fields)}
    else:
        return None
