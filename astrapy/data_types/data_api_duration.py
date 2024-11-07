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
from astrapy.utils.duration_std_utils import (
    _build_std_duration_string,
    _parse_std_duration_string,
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
    signum: int

    def __init__(
        self,
        signum: int,
        months: int,
        days: int,
        nanoseconds: int,
    ) -> None:
        if months < 0 or days < 0 or nanoseconds < 0:
            raise ValueError(
                "months, days, nanoseconds cannot be negative. Use overall 'signum'."
            )
        if signum not in {+1, -1}:
            raise ValueError("signum must be either +1 or -1.")
        self.months = months
        self.days = days
        self.nanoseconds = nanoseconds
        self.signum = signum

    def __repr__(self) -> str:
        def irepr(val: int) -> str:
            if val != 0 and self.signum < 0:
                return f"-{val}"
            else:
                return f"{val}"

        inner_desc = (
            f"months={irepr(self.months)}, days={irepr(self.days)}, "
            f"nanoseconds={irepr(self.nanoseconds)}"
        )
        return f"{self.__class__.__name__}({inner_desc})"

    def __str__(self) -> str:
        return self.to_string()

    def __reduce__(self) -> tuple[type, tuple[int, int, int, int]]:
        return self.__class__, (self.signum, self.months, self.days, self.nanoseconds)

    def __hash__(self) -> int:
        return hash((self.signum, self.months, self.days, self.nanoseconds))

    @staticmethod
    def from_string(duration_string: str) -> DataAPIDuration:
        si, mo, da, ns = _parse_std_duration_string(duration_string)
        return DataAPIDuration(
            signum=si,
            months=mo,
            days=da,
            nanoseconds=ns,
        )

    def to_string(self) -> str:
        return _build_std_duration_string(
            signum=self.signum,
            months=self.months,
            days=self.days,
            nanoseconds=self.nanoseconds,
        )

    @staticmethod
    def from_c_string(duration_string: str) -> DataAPIDuration:
        si, mo, da, ns = _parse_c_duration_string(duration_string)
        return DataAPIDuration(
            signum=si,
            months=mo,
            days=da,
            nanoseconds=ns,
        )

    def to_c_string(self) -> str:
        return _build_c_duration_string(
            signum=self.signum,
            months=self.months,
            days=self.days,
            nanoseconds=self.nanoseconds,
        )

    @staticmethod
    def from_timedelta(td: datetime.timedelta) -> DataAPIDuration:
        # this conversion expresses a duration with sub-days component only,
        # since a 'timedelta' is a precise time span (as opposed to durations).
        total_nanoseconds = int(td.total_seconds() * 1000000000)
        if total_nanoseconds >= 0:
            return DataAPIDuration(
                signum=+1,
                months=0,
                days=0,
                nanoseconds=total_nanoseconds,
            )
        else:
            return DataAPIDuration(
                signum=-1,
                months=0,
                days=0,
                nanoseconds=-total_nanoseconds,
            )

    def to_timedelta(self) -> datetime.timedelta:
        if self.months != 0:
            raise ValueError(
                "Cannot convert a DataAPIDuration with nonzero months into a timedelta."
            )
        return datetime.timedelta(
            days=self.signum * self.days,
            microseconds=self.signum * self.nanoseconds // 1000,
        )
