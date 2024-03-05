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
    BEFORE = "before"
    AFTER = "after"


def normalize_optional_projection(
    projection: Optional[ProjectionType],
) -> Optional[Dict[str, bool]]:
    if projection:
        if isinstance(projection, dict):
            return projection
        else:
            # an iterable over strings
            return {field: True for field in projection}
    else:
        return None
