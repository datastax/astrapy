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
    "month a 01-12 integer; day an integer from 01 to the maximum value allowed "
    "by the month (and year) choice."
)


@dataclass
class DataAPIDate:
    """
    A value expressing a date, composed of a year, a month and a day, suitable
    for working with the "date" table column type.

    This class is designed to losslessly express the full date range the Data API
    supports, overcoming the year range limitation of Python's standard-library
    date (i.e. 1AD to 9999AD).

    DataAPIDate objects are meant to easily work in the context of the Data API,
    hence its conversion methods from/to a string assumed the particular format
    employed by the API.

    The class also offers conversion methods from/to the regular
    Python `datetime.date`; however these may fail if the year falls outside
    of the range supported by the latter.

    Args:
        year: the year for the date. Any integer is accepted.
        month: an integer number in the 1-12 range.
        day: an integer number in a range between 1 and the number of days
            in the chosen month (whose value depends on the month and, in the case
            of February, on whether the year is a leap year).

    Example:
        >>> from astrapy.data_types import DataAPIDate
        >>> date1 = DataAPIDate(2024, 12, 31)
        >>> date2 = DataAPIDate(-44, 3, 15)
        >>> date2
        DataAPIDate(-44, 3, 15)
        >>> date1.year
        2024
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
        """
        Express the date as a string according to the Data API convention,
        including the presence of a sign, and the number of digits, for the year.

        Returns:
            a string, such as "2024-12-31", formatted in a way suitable to be
            in a Data API payload.

        Example:
            >>> from astrapy.data_types import DataAPIDate
            >>> date1 = DataAPIDate(2024, 12, 31)
            >>> date2 = DataAPIDate(-44, 3, 15)
            >>> date1.to_string()
            '2024-12-31'
            >>> date2.to_string()
            '-0044-03-15'
        """

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
        """
        Attempt to convert the date into a Python standard-library `datetime.date`.
        This operation may fail with a ValueError if the DataAPIDate's year falls
        outside of the range supported by the standard library.

        Returns:
            a `datetime.date` object if the conversion is successful.

        Example:
            >>> from astrapy.data_types import DataAPIDate
            >>> date1 = DataAPIDate(2024, 12, 31)
            >>> date2 = DataAPIDate(-44, 3, 15)
            >>> date1.to_date()
            datetime.date(2024, 12, 31)
            >>> date2.to_date()
            Traceback (most recent call last):
              [...]
            ValueError: year -44 is out of range
        """

        return datetime.date(*self._to_tuple())

    @staticmethod
    def from_date(dt: datetime.date) -> DataAPIDate:
        """
        Convert a Python standard-library date into a DataAPIDate.

        Args:
            dt: a `datetime.date` object.

        Returns:
            a DataAPIDate, corresponding to the provided input.

        Example:
            >>> from datetime import date
            >>>
            >>> from astrapy.data_types import DataAPIDate
            >>>
            >>> std_date = date(2024, 12, 31)
            >>> DataAPIDate.from_date(std_date)
            DataAPIDate(2024, 12, 31)
        """

        return DataAPIDate(
            year=dt.year,
            month=dt.month,
            day=dt.day,
        )

    @staticmethod
    def from_string(date_string: str) -> DataAPIDate:
        """
        Convert a string into a DataAPIDate, provided the string represents one
        according to the Data API format conventions. If the format is unrecognized,
        a ValueError is raised.

        Args:
            date_string: a valid string expressing a date as per Data API conventions.

        Returns:
            a DataAPIDate corresponding to the provided input.

        Example:
            >>> from astrapy.data_types import DataAPIDate
            >>>
            >>> DataAPIDate.from_string("2024-12-31")
            DataAPIDate(2024, 12, 31)
            >>> DataAPIDate.from_string("-0044-03-15")
            DataAPIDate(-44, 3, 15)
            >>> DataAPIDate.from_string("1905-13-15")
            Traceback (most recent call last):
                [...]
            ValueError: Cannot parse '1905-13-15' into a valid date: illegal month [...]
        """

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
