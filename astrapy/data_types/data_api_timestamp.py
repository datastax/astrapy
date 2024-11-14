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
    _is_leap_year,
    _unix_timestamp_ms_to_timetuple,
    _validate_date,
    _validate_time,
    _year_to_unix_timestamp_ms,
)

TIMESTAMP_PARSE_PATTERN = re.compile(
    r"^([-\+]?\d*[\d]{4})-(\d+)-(\d+)T(\d+):(\d+):(\d+)(\.\d+)?([+-]\d+):(\d+)$"
)

TIMESTAMP_FORMAT_DESC = (
    "Timestamp strings must adhere to the following specific syntax of RFC 3339 "
    "'<year>-<month>-<day>T<hour>:<minute>:<second>[.<fractional-seconds>]<offset>', "
    "with: year a four-or-more-digit integer with optional leading minus sign "
    "(a plus sign if and only if positive year with more than 4 digits); "
    "month a 1-12 integer; day "
    "an integer from 1 to the maximum value allowed by the month (and year) choice; "
    "hour in 0..23; minute in 0..59; second in 0..59; and optionally a decimal "
    "point followed by a fraction of second up to millisecond precision (3 digits);"
    "offset can either be in the form '[sign]hh:mm' or 'Z' (shorthand for '+00:00'). "
    "Examples: '2024-10-28T21:19:57Z', '1993-02-12T05:13:44.347-01:30'."
)


@dataclass
class DataAPITimestamp:
    """
    TODO also for methods
    """

    timestamp_ms: int

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(timestamp_ms={self.timestamp_ms}"
            f" [{self.to_string()}])"
        )

    def __str__(self) -> str:
        return self.to_string()

    def __hash__(self) -> int:
        return self.timestamp_ms

    def __reduce__(self) -> tuple[type, tuple[int]]:
        return self.__class__, (self.timestamp_ms,)

    def __le__(self, other: Any) -> bool:
        if isinstance(other, DataAPITimestamp):
            return self.timestamp_ms <= other.timestamp_ms
        else:
            return NotImplemented

    def __lt__(self, other: Any) -> bool:
        if isinstance(other, DataAPITimestamp):
            return self.timestamp_ms < other.timestamp_ms
        else:
            return NotImplemented

    def __ge__(self, other: Any) -> bool:
        if isinstance(other, DataAPITimestamp):
            return self.timestamp_ms >= other.timestamp_ms
        else:
            return NotImplemented

    def __gt__(self, other: Any) -> bool:
        if isinstance(other, DataAPITimestamp):
            return self.timestamp_ms > other.timestamp_ms
        else:
            return NotImplemented

    def __sub__(self, other: Any) -> int:
        if isinstance(other, DataAPITimestamp):
            return self.timestamp_ms - other.timestamp_ms
        else:
            return NotImplemented

    def to_datetime(
        self, *, tz: datetime.timezone = datetime.timezone.utc
    ) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(self.timestamp_ms / 1000.0, tz=tz)

    @staticmethod
    def from_datetime(dt: datetime.datetime) -> DataAPITimestamp:
        return DataAPITimestamp(timestamp_ms=int(dt.timestamp() * 1000.0))

    @staticmethod
    def from_string(datetime_string: str) -> DataAPITimestamp:
        """
        parse a RFC3339 datetime string, in the particular formats
        "yyyy-mm-ddThh:mm:ss[.nnnnnn](Z|+hh:mm)", i.e. for instance
            "2024-12-30T12:34:56Z"
            "2024-12-30T12:34:56+01:30"
        into a DataAPITimestamp. The custom logic allows to cover a broader
        range than the Python standard datetime library.
        """
        _datetime_string = datetime_string.upper().replace("Z", "+00:00")
        match = TIMESTAMP_PARSE_PATTERN.match(_datetime_string)
        if match:
            # the year string has additional constraints besides the regexp:
            year_str = match[1]
            if year_str and year_str[0] == "+":
                if len(year_str[1:]) <= 4:
                    raise ValueError(
                        f"Cannot parse '{datetime_string}' into a valid timestamp: "
                        "four-digit positive year should bear no plus sign. "
                        f"{TIMESTAMP_FORMAT_DESC}"
                    )
            if len(year_str) > 4 and year_str[0] not in {"+", "-"}:
                raise ValueError(
                    f"Cannot parse '{datetime_string}' into a valid timestamp: "
                    "years with more than four digits should bear a leading sign. "
                    f"{TIMESTAMP_FORMAT_DESC}"
                )
            year = int(year_str)
            if year == 0 and year_str[0] == "-":
                raise ValueError(
                    f"Cannot parse '{datetime_string}' into a valid timestamp: "
                    "year zero should be provided as '0000' without leading sign. "
                    f"{TIMESTAMP_FORMAT_DESC}"
                )
            month = int(match[2])
            day = int(match[3])
            hour = int(match[4])
            minute = int(match[5])
            second = int(match[6])
            millisecond: int
            if match[7]:
                millisecond = int(float(match[7]) * 1000)
            else:
                millisecond = 0
            offset_hour = int(match[8])
            offset_minute = int(match[9])

            # validations
            _d_f_reason = _validate_date(
                year=year,
                month=month,
                day=day,
            )
            if _d_f_reason:
                raise ValueError(
                    f"Cannot parse '{datetime_string}' into a valid timestamp: "
                    f"{_d_f_reason}. {TIMESTAMP_FORMAT_DESC}"
                )
            _t_f_reason = _validate_time(
                hour=hour,
                minute=minute,
                second=second,
                nanosecond=millisecond * 1000000,
            )
            if _t_f_reason:
                raise ValueError(
                    f"Cannot parse '{datetime_string}' into a valid timestamp: "
                    f"{_t_f_reason}. {TIMESTAMP_FORMAT_DESC}"
                )
            # validate offset
            if offset_hour < -23 or offset_hour > 23:
                raise ValueError(
                    f"Cannot parse '{datetime_string}' into a valid timestamp: "
                    f"illegal offset hours. {TIMESTAMP_FORMAT_DESC}"
                )
            if offset_minute < 0 or offset_hour > 59:
                raise ValueError(
                    f"Cannot parse '{datetime_string}' into a valid timestamp: "
                    f"illegal offset minutes. {TIMESTAMP_FORMAT_DESC}"
                )

            # convert into a timestamp, part 1: year
            year_timestamp_ms = _year_to_unix_timestamp_ms(year)

            # convert into a timestamp, part 2: the rest (taking care of offset as well)
            ref_year = 1972 if _is_leap_year(year) else 1971
            year_start_date = datetime.datetime(ref_year, 1, 1, 0, 0, 0, 0).replace(
                tzinfo=datetime.timezone.utc
            )
            offset_delta = datetime.timedelta(hours=offset_hour, minutes=offset_minute)
            year_reset_date = datetime.datetime(
                ref_year, month, day, hour, minute, second, millisecond * 1000
            ).replace(tzinfo=datetime.timezone.utc)
            in_year_timestamp_ms = int(
                (year_reset_date - offset_delta - year_start_date).total_seconds()
                * 1000
            )

            return DataAPITimestamp(
                timestamp_ms=year_timestamp_ms + in_year_timestamp_ms
            )
        else:
            raise ValueError(
                f"Cannot parse '{datetime_string}' into a valid timestamp "
                f"(unrecognized format). {TIMESTAMP_FORMAT_DESC}"
            )

    def timetuple(self) -> tuple[int, int, int, int, int, int, int]:
        """
        Return (UTC, i.e. offset +00:00) a tuple
            (year, month, day, hour, minute, second, millisecond)
        Note the last entry is millisecond for internal consistency.
        """
        return _unix_timestamp_ms_to_timetuple(self.timestamp_ms)

    def to_string(self) -> str:
        """
        The output format is fixed to:
            "<y>-<mo>-<d>T<h>:<m>:<s>.<ms>Z"
        Use `.timetuple()` for achieving custom string formatting.
        """
        y, mo, d, h, m, s, ms = self.timetuple()
        # the year part requires care around the sign and number of digits
        year_str: str
        if y > 9999:
            year_str = f"{y:+}"
        elif y >= 0:
            year_str = f"{y:04}"
        else:
            year_str = f"{y:+05}"
        return f"{year_str}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}.{ms:03}Z"
