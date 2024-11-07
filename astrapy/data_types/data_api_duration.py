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

from astrapy.utils.duration_c_utils import (
    _build_c_duration_string,
    _parse_c_duration_string,
)


@dataclass
class DataAPIDuration:
    """
    TODO

    TODO: note reasons why != timedelta
    """

    months: int
    days: int
    nanoseconds: int

    def __repr__(self) -> str:
        inner_desc = (
            f"months={self.months}, days={self.days}, nanoseconds={self.nanoseconds}"
        )
        return f"{self.__class__.__name__}({inner_desc})"

    def __str__(self) -> str:
        return self.to_string()

    def __reduce__(self) -> tuple[type, tuple[int, int, int]]:
        return self.__class__, (self.months, self.days, self.nanoseconds)

    def __hash__(self) -> int:
        return hash((self.months, self.days, self.nanoseconds))

    @staticmethod
    def from_string(duration_string: str) -> DataAPIDuration:
        mo, da, ns = _parse_c_duration_string(duration_string)
        return DataAPIDuration(
            months=mo,
            days=da,
            nanoseconds=ns,
        )

    def to_string(self) -> str:
        return _build_c_duration_string(
            months=self.months,
            days=self.days,
            nanoseconds=self.nanoseconds,
        )

    @staticmethod
    def from_timedelta(td: datetime.timedelta) -> DataAPIDuration:
        return DataAPIDuration.from_string(
            f"{td.days}d{td.seconds}s{td.microseconds}us"
        )

    def to_timedelta(self) -> datetime.timedelta:
        if self.months != 0:
            raise ValueError(
                "Cannot convert a DataAPIDuration with nonzero months into a timedelta."
            )
        return datetime.timedelta(
            days=self.days,
            microseconds=self.nanoseconds // 1000,
        )
