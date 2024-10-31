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
import re
from dataclasses import dataclass
from typing import Any

from astrapy.utils.date_utils import _validate_date

DATE_PARSE_PATTERN = re.compile(r"^(-?\d+)-(\d+)-(\d+)$")
MAX_DAY_PER_MONTH_LEAP = {2: 29}
MAX_DAY_PER_MONTH = {
    1: 31,
    2: 28,
    3: 31,
    4: 30,
    5: 31,
    6: 30,
    7: 31,
    8: 31,
    9: 30,
    10: 31,
    11: 30,
    12: 31,
}

DATE_FORMAT_DESC = (
    "Dates must be '<year>-<month>-<day>', with: year an integer with optional "
    "leading minus sign; month a 1-12 integer; day an integer from 1 to the maximum "
    "value allowed by the month (and year) choice."
)


@dataclass
class TableDate:
    """
    TODO also for methods
    """

    year: int
    month: int
    day: int

    def __init__(self, year: int, month: int, day: int):
        _fail_reason = _validate_date(
            year=year,
            month=month,
            day=day,
        )
        if _fail_reason:
            raise ValueError(f"Invalid date arguments: {_fail_reason}.")
        self.year = year
        self.month = month
        self.day = day

    def __hash__(self) -> int:
        return hash((self.year, self.month, self.day))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.year}, {self.month}, {self.day})"

    def __str__(self) -> str:
        return self.to_string()

    def __reduce__(self) -> tuple[type, tuple[int, int, int]]:
        return self.__class__, (self.year, self.month, self.day)

    def __le__(self, other: Any) -> bool:
        if isinstance(other, TableDate):
            return self._to_tuple() <= other._to_tuple()
        elif isinstance(other, datetime.date):
            return self.__le__(TableDate.from_date(other))
        else:
            return NotImplemented

    def __lt__(self, other: Any) -> bool:
        if isinstance(other, TableDate):
            return self._to_tuple() < other._to_tuple()
        elif isinstance(other, datetime.date):
            return self.__lt__(TableDate.from_date(other))
        else:
            return NotImplemented

    def __ge__(self, other: Any) -> bool:
        if isinstance(other, TableDate):
            return self._to_tuple() >= other._to_tuple()
        elif isinstance(other, datetime.date):
            return self.__ge__(TableDate.from_date(other))
        else:
            return NotImplemented

    def __gt__(self, other: Any) -> bool:
        if isinstance(other, TableDate):
            return self._to_tuple() > other._to_tuple()
        elif isinstance(other, datetime.date):
            return self.__gt__(TableDate.from_date(other))
        else:
            return NotImplemented

    def _to_tuple(self) -> tuple[int, int, int]:
        return (self.year, self.month, self.day)

    def to_string(self) -> str:
        if self.year < 0:
            return f"{self.year:05}-{self.month:02}-{self.day:02}"
        return f"{self.year:04}-{self.month:02}-{self.day:02}"

    def to_date(self) -> datetime.date:
        return datetime.date(*self._to_tuple())

    @staticmethod
    def from_date(dt: datetime.date) -> TableDate:
        return TableDate(
            year=dt.year,
            month=dt.month,
            day=dt.day,
        )

    @staticmethod
    def from_string(date_string: str) -> TableDate:
        match = DATE_PARSE_PATTERN.match(date_string)
        if match:
            year = int(match[1])
            month = int(match[2])
            day = int(match[3])
            _fail_reason = _validate_date(year=year, month=month, day=day)
            if _fail_reason:
                raise ValueError(
                    f"Cannot parse '{date_string}' into a valid date: "
                    f"{_fail_reason}. {DATE_FORMAT_DESC}"
                )
            return TableDate(year=year, month=month, day=day)
        else:
            raise ValueError(
                f"Cannot parse '{date_string}' into a valid date "
                f"(unrecognized format). {DATE_FORMAT_DESC}"
            )
