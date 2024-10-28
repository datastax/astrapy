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

TIME_PARSE_PATTERN = re.compile(r"^(\d+):(\d+):(\d+)(\.\d+)?$")

TIME_FORMAT_DESC = (
    "Times must be '<hour>:<minute>:<second>[.<fractional-seconds>]', with: "
    "hour in 0..23; minute in 0..59; second in 0..59; and optionally a decimal "
    "point followed by a fraction of second up to nanosecond precision (9 digits)."
)


@dataclass
class TableTime:
    """
    TODO also for methods
    """

    hour: int
    minute: int
    second: int
    nanosecond: int

    def __init__(
        self, hour: int, minute: int = 0, second: int = 0, nanosecond: int = 0
    ):
        _fail_reason = self._validate_time(
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

    @staticmethod
    def _validate_time(
        *, hour: int, minute: int, second: int, nanosecond: int
    ) -> str | None:
        # None if valid date, reason otherwise
        if hour < 0 or hour > 23:
            return "illegal hour"
        if minute < 0 or minute > 59:
            return "illegal minute"
        if second < 0 or second > 59:
            return "illegal second"
        if nanosecond < 0 or nanosecond >= 1000000000:
            return "illegal nanosecond"
        return None

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}({self.hour}, {self.minute}, "
            f"{self.second}, {self.nanosecond})"
        )

    def __str__(self) -> str:
        return self.to_string()

    def __reduce__(self) -> tuple[type, tuple[int, int, int, int]]:
        return self.__class__, (self.hour, self.minute, self.second, self.nanosecond)

    def __le__(self, other: Any) -> bool:
        if isinstance(other, TableTime):
            return self._to_tuple() <= other._to_tuple()
        elif isinstance(other, datetime.time):
            return self.__le__(TableTime.from_time(other))
        else:
            return NotImplemented

    def __lt__(self, other: Any) -> bool:
        if isinstance(other, TableTime):
            return self._to_tuple() < other._to_tuple()
        elif isinstance(other, datetime.time):
            return self.__lt__(TableTime.from_time(other))
        else:
            return NotImplemented

    def __ge__(self, other: Any) -> bool:
        if isinstance(other, TableTime):
            return self._to_tuple() >= other._to_tuple()
        elif isinstance(other, datetime.time):
            return self.__ge__(TableTime.from_time(other))
        else:
            return NotImplemented

    def __gt__(self, other: Any) -> bool:
        if isinstance(other, TableTime):
            return self._to_tuple() > other._to_tuple()
        elif isinstance(other, datetime.time):
            return self.__gt__(TableTime.from_time(other))
        else:
            return NotImplemented

    def _to_tuple(self) -> tuple[int, int, int, int]:
        return (self.hour, self.minute, self.second, self.nanosecond)

    def to_string(self) -> str:
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
        return datetime.time(
            hour=self.hour,
            minute=self.minute,
            second=self.second,
            microsecond=int(self.nanosecond / 1000.0),
        )

    @staticmethod
    def from_time(dt: datetime.time) -> TableTime:
        return TableTime(
            hour=dt.hour,
            minute=dt.minute,
            second=dt.second,
            nanosecond=dt.microsecond * 1000,
        )

    @staticmethod
    def from_string(time_string: str) -> TableTime:
        match = TIME_PARSE_PATTERN.match(time_string)
        if match:
            hour = int(match[1])
            minute = int(match[2])
            second = int(match[3])
            if match[4]:
                nanosecond = int(float(match[4]) * 1000000000)
            else:
                nanosecond = 0
            _fail_reason = TableTime._validate_time(
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
            return TableTime(
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
