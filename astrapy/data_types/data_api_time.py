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

from astrapy.utils.date_utils import (
    _validate_time,
)

TIME_PARSE_PATTERN = re.compile(r"^(\d+):(\d+)(:(\d+)(\.\d+)?)?$")
SECONDS_PARSE_PATTERN = re.compile(r"^:(\d+)(\.\d+)?$")

TIME_FORMAT_DESC = (
    "Times must be '<hour>:<minute>[:<second>[.<fractional-seconds>]]', with: "
    "hour in 0..23; minute in 0..59; second in 0..59; and optionally a decimal "
    "point followed by a fraction of second up to nanosecond precision (9 digits)."
)


@dataclass
class DataAPITime:
    """
    A value expressing a time, composed of hours, minutes, seconds and nanoseconds,
    suitable for working with the "time" table column type.

    This class is designed to losslessly express the time values the Data API
    supports, overcoming the precision limitation of Python's standard-library
    time (whose sub-second quantity only has microsecond precision).

    DataAPITime objects are meant to easily work in the context of the Data API,
    hence its conversion methods from/to a string assumed the particular format
    employed by the API.

    The class also offers conversion methods from/to the regular
    Python `datetime.time`; however these can entail a lossy conversion because
    of the missing support for nanoseconds by the latter.

    Args:
        hour: an integer in 0-23, the hour value of the time.
        minute: an integer in 0-59, the minute value of the time.
        second: an integer in 0-59, the second value of the time.
        nanosecond: an integer in 0-999999999, the nanosecond value of the time.

    Example:
        >>> from astrapy.data_types import DataAPITime
        >>>
        >>> DataAPITime(12, 34, 56)
        DataAPITime(12, 34, 56, 0)
        >>> t1 = DataAPITime(21, 43, 56, 789012345)
        >>> t1
        DataAPITime(21, 43, 56, 789012345)
        >>> t1.second
        56
    """

    hour: int
    minute: int
    second: int
    nanosecond: int

    def __init__(
        self, hour: int, minute: int = 0, second: int = 0, nanosecond: int = 0
    ):
        _fail_reason = _validate_time(
            hour=hour,
            minute=minute,
            second=second,
            nanosecond=nanosecond,
        )
        if _fail_reason:
            raise ValueError(f"Invalid time arguments: {_fail_reason}.")
        self.hour = hour
        self.minute = minute
        self.second = second
        self.nanosecond = nanosecond

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}({self.hour}, {self.minute}, "
            f"{self.second}, {self.nanosecond})"
        )

    def __hash__(self) -> int:
        return hash((self.hour, self.minute, self.second, self.nanosecond))

    def __str__(self) -> str:
        return self.to_string()

    def __reduce__(self) -> tuple[type, tuple[int, int, int, int]]:
        return self.__class__, (self.hour, self.minute, self.second, self.nanosecond)

    def __le__(self, other: Any) -> bool:
        if isinstance(other, DataAPITime):
            return self._to_tuple() <= other._to_tuple()
        elif isinstance(other, datetime.time):
            return self.__le__(DataAPITime.from_time(other))
        else:
            return NotImplemented

    def __lt__(self, other: Any) -> bool:
        if isinstance(other, DataAPITime):
            return self._to_tuple() < other._to_tuple()
        elif isinstance(other, datetime.time):
            return self.__lt__(DataAPITime.from_time(other))
        else:
            return NotImplemented

    def __ge__(self, other: Any) -> bool:
        if isinstance(other, DataAPITime):
            return self._to_tuple() >= other._to_tuple()
        elif isinstance(other, datetime.time):
            return self.__ge__(DataAPITime.from_time(other))
        else:
            return NotImplemented

    def __gt__(self, other: Any) -> bool:
        if isinstance(other, DataAPITime):
            return self._to_tuple() > other._to_tuple()
        elif isinstance(other, datetime.time):
            return self.__gt__(DataAPITime.from_time(other))
        else:
            return NotImplemented

    def _to_tuple(self) -> tuple[int, int, int, int]:
        return (self.hour, self.minute, self.second, self.nanosecond)

    def to_string(self) -> str:
        """
        Convert the DataAPITime to a string according to the Data API specification.

        Returns:
            a string expressing the time value in a way compatible with the Data API
            conventions.

        Example:
            >>> from astrapy.data_types import DataAPITime
            >>>
            >>> DataAPITime(12, 34, 56).to_string()
            '12:34:56'
            >>> DataAPITime(21, 43, 56, 789012345).to_string()
            '21:43:56.789012345'
        """

        hm_part = f"{self.hour:02}:{self.minute:02}"
        s_part: str
        if self.nanosecond:
            nano_div: int
            nano_digits: int
            if self.nanosecond % 1000000 == 0:
                nano_div = 1000000
                nano_digits = 3
            elif self.nanosecond % 1000 == 0:
                nano_div = 1000
                nano_digits = 6
            else:
                nano_div = 1
                nano_digits = 9
            ns_format_string = f"%0{nano_digits}i"
            s_part = (
                f"{self.second:02}.{ns_format_string % (self.nanosecond // nano_div)}"
            )
        else:
            s_part = f"{self.second:02}"
        return f"{hm_part}:{s_part}"

    def to_time(self) -> datetime.time:
        """
        Convert the DataAPITime into a Python standard-library `datetime.time` object.
        This may involve a loss of precision since the latter cannot express
        nanoseconds, only microseconds.

        Returns:
            a `datetime.time` object, corresponding (possibly in a lossy way)
            to the original DataAPITime.

        Example:
            >>> from astrapy.data_types import DataAPITime
            >>>
            >>> DataAPITime(12, 34, 56).to_time()
            datetime.time(12, 34, 56)
            >>> DataAPITime(21, 43, 56, 789012345).to_time()
            datetime.time(21, 43, 56, 789012)
        """

        return datetime.time(
            hour=self.hour,
            minute=self.minute,
            second=self.second,
            microsecond=int(self.nanosecond / 1000.0),
        )

    @staticmethod
    def from_time(dt: datetime.time) -> DataAPITime:
        """
        Create a DataAPITime from a Python standard-library `datetime.time` object.

        Args:
            dt: a `datetime.time` value

        Returns:
            a DataAPITime object, corresponding exactly to the provided input.

        Example:
            >>> from datetime import time
            >>>
            >>> from astrapy.data_types import DataAPITime
            >>>
            >>> DataAPITime.from_time(time(12, 34, 56))
            DataAPITime(12, 34, 56, 0)
            >>> DataAPITime.from_time(time(12, 34, 56, 789012))
            DataAPITime(12, 34, 56, 789012000)
        """
        return DataAPITime(
            hour=dt.hour,
            minute=dt.minute,
            second=dt.second,
            nanosecond=dt.microsecond * 1000,
        )

    @staticmethod
    def from_string(time_string: str) -> DataAPITime:
        """
        Parse a string, expressed according to the Data API format, into a DataAPITime.
        If the format is not recognized, a ValueError is raised.

        Args:
            time_string: a string compliant to the Data API specification for times.

        Returns:
            a DataAPITime corresponding to the provided input.

        Example:
            >>> from astrapy.data_types import DataAPITime
            >>>
            >>> DataAPITime.from_string("12:34:56")
            DataAPITime(12, 34, 56, 0)
            >>> DataAPITime.from_string("21:43:56.789012345")
            DataAPITime(21, 43, 56, 789012345)
            >>> DataAPITime.from_string("34:11:22.123")
            Traceback (most recent call last):
              [...]
            ValueError: Cannot parse '34:11:22.123' into a valid time: illegal hour. ...
        """

        match = TIME_PARSE_PATTERN.match(time_string)
        if match:
            hour = int(match[1])
            minute = int(match[2])
            second: int
            nanosecond: int
            if match[3]:
                second_match = SECONDS_PARSE_PATTERN.match(match[3])
                if second_match:
                    second = int(second_match[1])
                    if second_match[2]:
                        nanosecond = int(float(second_match[2]) * 1000000000)
                    else:
                        nanosecond = 0
                else:
                    raise ValueError(
                        f"Cannot parse '{time_string}' into a valid time "
                        f"(unrecognized format). {TIME_FORMAT_DESC}"
                    )
            else:
                second = 0
                nanosecond = 0
            _fail_reason = _validate_time(
                hour=hour,
                minute=minute,
                second=second,
                nanosecond=nanosecond,
            )
            if _fail_reason:
                raise ValueError(
                    f"Cannot parse '{time_string}' into a valid time: "
                    f"{_fail_reason}. {TIME_FORMAT_DESC}"
                )
            return DataAPITime(
                hour=hour,
                minute=minute,
                second=second,
                nanosecond=nanosecond,
            )
        else:
            raise ValueError(
                f"Cannot parse '{time_string}' into a valid time "
                f"(unrecognized format). {TIME_FORMAT_DESC}"
            )
