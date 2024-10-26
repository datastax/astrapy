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

import datetime
from dataclasses import dataclass
from typing import Any


@dataclass
class APITimestamp:
    """
    TODO also for methods
    """

    timestamp_ms: int

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(timestamp_ms={self.timestamp_ms})"

    def __reduce__(self) -> tuple[type, tuple[int]]:
        return self.__class__, (self.timestamp_ms,)

    def __le__(self, other: Any) -> bool:
        if isinstance(other, APITimestamp):
            return self.timestamp_ms <= other.timestamp_ms
        else:
            return NotImplemented

    def __lt__(self, other: Any) -> bool:
        if isinstance(other, APITimestamp):
            return self.timestamp_ms < other.timestamp_ms
        else:
            return NotImplemented

    def __ge__(self, other: Any) -> bool:
        if isinstance(other, APITimestamp):
            return self.timestamp_ms >= other.timestamp_ms
        else:
            return NotImplemented

    def __gt__(self, other: Any) -> bool:
        if isinstance(other, APITimestamp):
            return self.timestamp_ms > other.timestamp_ms
        else:
            return NotImplemented

    def __sub__(self, other: Any) -> int:
        if isinstance(other, APITimestamp):
            return self.timestamp_ms - other.timestamp_ms
        else:
            return NotImplemented

    def to_datetime(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(self.timestamp_ms / 1000.0)

    @staticmethod
    def from_datetime(dt: datetime.datetime) -> APITimestamp:
        return APITimestamp(timestamp_ms=int(dt.timestamp() * 1000.0))
