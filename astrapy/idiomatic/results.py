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

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class DeleteResult:
    deleted_count: Optional[int]
    raw_result: Dict[str, Any]
    acknowledged: bool = True


@dataclass
class InsertOneResult:
    inserted_id: Any
    acknowledged: bool = True


@dataclass
class InsertManyResult:
    inserted_ids: List[Any]
    acknowledged: bool = True