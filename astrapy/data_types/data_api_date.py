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

DATE_PARSE_PATTERN = re.compile(r"^([-\+]?\d*[\d]{4})-(\d+)-(\d+)$")
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
    "Dates must be '<year>-<month>-<day>', with: year a four-or-more-digit integer "
    "with optional leading minus sign (a plus sign if and only if positive year "
    "with more than 4 digits); "
    "month a 1-12 integer; day an integer from 1 to the maximum value allowed "
    "by the month (and year) choice."
)


@dataclass
class DataAPIDate:
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
        if isinstance(other, DataAPIDate):
            return self._to_tuple() <= other._to_tuple()
        elif isinstance(other, datetime.date):
            return self.__le__(DataAPIDate.from_date(other))
        else:
            return NotImplemented

    def __lt__(self, other: Any) -> bool:
        if isinstance(other, DataAPIDate):
            return self._to_tuple() < other._to_tuple()
        elif isinstance(other, datetime.date):
            return self.__lt__(DataAPIDate.from_date(other))
        else:
            return NotImplemented

    def __ge__(self, other: Any) -> bool:
        if isinstance(other, DataAPIDate):
            return self._to_tuple() >= other._to_tuple()
        elif isinstance(other, datetime.date):
            return self.__ge__(DataAPIDate.from_date(other))
        else:
            return NotImplemented

    def __gt__(self, other: Any) -> bool:
        if isinstance(other, DataAPIDate):
            return self._to_tuple() > other._to_tuple()
        elif isinstance(other, datetime.date):
            return self.__gt__(DataAPIDate.from_date(other))
        else:
            return NotImplemented

    def _to_tuple(self) -> tuple[int, int, int]:
        return (self.year, self.month, self.day)

    def to_string(self) -> str:
        # the year part requires care around the sign and number of digits
        y = self.year
        year_str: str
        if y > 9999:
            year_str = f"{y:+}"
        elif y >= 0:
            year_str = f"{y:04}"
        else:
            year_str = f"{y:+05}"
        return f"{year_str}-{self.month:02}-{self.day:02}"

    def to_date(self) -> datetime.date:
        return datetime.date(*self._to_tuple())

    @staticmethod
    def from_date(dt: datetime.date) -> DataAPIDate:
        return DataAPIDate(
            year=dt.year,
            month=dt.month,
            day=dt.day,
        )

    @staticmethod
    def from_string(date_string: str) -> DataAPIDate:
        match = DATE_PARSE_PATTERN.match(date_string)
        if match:
            # the year string has additional constraints besides the regexp:
            year_str = match[1]
            if year_str and year_str[0] == "+":
                if len(year_str[1:]) <= 4:
                    raise ValueError(
                        f"Cannot parse '{date_string}' into a valid timestamp: "
                        "four-digit positive year should bear no plus sign. "
                        f"{DATE_FORMAT_DESC}"
                    )
            if len(year_str) > 4 and year_str[0] not in {"+", "-"}:
                raise ValueError(
                    f"Cannot parse '{date_string}' into a valid timestamp: "
                    "years with more than four digits should bear a leading sign. "
                    f"{DATE_FORMAT_DESC}"
                )
            year = int(year_str)
            if year == 0 and year_str[0] == "-":
                raise ValueError(
                    f"Cannot parse '{date_string}' into a valid timestamp: "
                    "year zero should be provided as '0000' without leading sign. "
                    f"{DATE_FORMAT_DESC}"
                )
            month = int(match[2])
            day = int(match[3])
            _fail_reason = _validate_date(year=year, month=month, day=day)
            if _fail_reason:
                raise ValueError(
                    f"Cannot parse '{date_string}' into a valid date: "
                    f"{_fail_reason}. {DATE_FORMAT_DESC}"
                )
            return DataAPIDate(year=year, month=month, day=day)
        else:
            raise ValueError(
                f"Cannot parse '{date_string}' into a valid date "
                f"(unrecognized format). {DATE_FORMAT_DESC}"
            )
