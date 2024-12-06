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
    "month a 01-12 integer; day "
    "an integer from 01 to the maximum value allowed by the month (and year) choice; "
    "hour in 00..23; minute in 00..59; second in 00..59; and optionally a decimal "
    "point followed by a fraction of second up to millisecond precision (3 digits);"
    "offset can either be in the form '[sign]hh:mm' or 'Z' (shorthand for '+00:00'). "
    "Examples: '2024-10-28T21:19:57Z', '1993-02-12T05:13:44.347-01:30'."
)


@dataclass
class DataAPITimestamp:
    """
    A value expressing a unambiguous timestamp, accurate to down millisecond precision,
    suitable for working with the "timestamp" table column type.

    A DataAPITimestamp can be thought of as an integer signed number of milliseconds
    elapsed since (or before) the epoch (that is, 1970-01-01 00:00:00 GMT+0).
    An alternative representation is that of a "date + time + offset", such as
    the (year, month, day; hour, minute, second, millisecond; offset), where offset
    is a quantity of type "hours:minutes" essentially canceling the timezone ambiguity
    of the remaining terms. This latter representation thus is not a bijection to the
    former, as different representation map to one and the same timestamp integer value.

    The fact that DataAPITimestamp values are only identified by their timestamp value
    is one of the key differences between this class and the Python standard-library
    `datetime.datetime`; another important practical difference is the available year
    range, which for the latter spans the 1AD-9999AD time period while for the
    DataAPITimestamp is unlimited for most practical purposes. In particular,
    this class is designed to losslessly express the full date range the Data API
    supports.

    DataAPITimestamp objects are meant to easily work in the context of the Data API,
    hence its conversion methods from/to a string assumed the particular format
    employed by the API.

    The class also offers conversion methods from/to the regular
    Python `datetime.datetime`; however these may fail if the year falls outside
    of the range supported by the latter.

    Args:
        timestamp_ms: an integer number of milliseconds elapsed since the epoch.
            Negative numbers signify timestamp occurring before the epoch.

    Example:
        >>> from astrapy.data_types import DataAPITimestamp
        >>>
        >>> ds1 = DataAPITimestamp(2000000000321)
        >>> ds1
        DataAPITimestamp(timestamp_ms=2000000000321 [2033-05-18T03:33:20.321Z])
        >>> ds1.timestamp_ms
        2000000000321
        >>> DataAPITimestamp(-1000000000321)
        DataAPITimestamp(timestamp_ms=-1000000000321 [1938-04-24T22:13:19.679Z])
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

    def to_datetime(self, *, tz: datetime.timezone | None) -> datetime.datetime:
        """
        Convert the DataAPITimestamp into a standard-library `datetime.datetime` value.

        The conversion may fail if the target year range is outside of the capabilities
        of `datetime.datetime`, in which case a ValueError will be raised.

        Args:
            tz: a `datetime.timezone` setting for providing offset information to the
                result, thus making it an "aware" datetime. If a "naive" datetime is
                desired (which however may lead to inconsistent timestamp handling
                throughout the application), it is possible to pass `tz=None`.

        Returns:
            A `datetime.datetime` value, set to the desired timezone - or naive, in case
            a null timezone is explicitly provided.

        Example:
            >>> import datetime
            >>>
            >>> from astrapy.data_types import DataAPITimestamp
            >>>
            >>> ds1 = DataAPITimestamp(2000000000321)
            >>> ds1.to_datetime(tz=datetime.timezone.utc)
            datetime.datetime(2033, 5, 18, 3, 33, 20, 321000, tzinfo=datetime.timezone.utc)
            >>> ds1.to_datetime(tz=None)
            datetime.datetime(2033, 5, 18, 5, 33, 20, 321000)
            >>>
            >>> ds2 = DataAPITimestamp(300000000000000)
            >>> ds2.to_datetime(tz=datetime.timezone.utc)
            Traceback (most recent call last):
              [...]
            ValueError: year 11476 is out of range
        """

        return datetime.datetime.fromtimestamp(self.timestamp_ms / 1000.0, tz=tz)

    def to_naive_datetime(self) -> datetime.datetime:
        """
        Convert the DataAPITimestamp into a standard-library  naive
        `datetime.datetime` value, i.e. one without attached timezone/offset info.

        The conversion may fail if the target year range is outside of the capabilities
        of `datetime.datetime`, in which case a ValueError will be raised.

        Returns:
            A naive `datetime.datetime` value. The ambiguity stemming from the lack of
            timezone information is handed off to the standard-library `.fromtimestamp`
            method, which works in the timezone set by the system locale.

        Example:
            >>> import datetime
            >>>
            >>> from astrapy.data_types import DataAPITimestamp
            >>>
            >>> ds1 = DataAPITimestamp(300000000321)
            >>> ds1
            DataAPITimestamp(timestamp_ms=300000000321 [1979-07-05T05:20:00.321Z])
            >>> # running in wintertime, in the Paris/Berlin timezone:
            >>> ds1.to_naive_datetime()
            datetime.datetime(1979, 7, 5, 7, 20, 0, 321000)
        """

        return datetime.datetime.fromtimestamp(self.timestamp_ms / 1000.0, tz=None)

    @staticmethod
    def from_datetime(dt: datetime.datetime) -> DataAPITimestamp:
        """
        Convert a standard-library `datetime.datetime` into a DataAPITimestamp.

        The conversion correctly takes timezone information into account, if provided.
        If it is absent (naive datetime), the ambiguity is resolved by the stdlib
        `.timestamp()` method, which assumes the timezone set by the system locale.

        Args:
            dt: the `datetime.datetime` to convert into a DataAPITimestamp.

        Returns:
            A DataAPITimestamp, corresponding to the provided datetime.

        Example (running in the Paris/Berlin timezone):
            >>> import datetime
            >>>
            >>> from astrapy.data_types import DataAPITimestamp
            >>>
            >>> ds1 = DataAPITimestamp(300000000321)
            >>> ds1
            DataAPITimestamp(timestamp_ms=300000000321 [1979-07-05T05:20:00.321Z])
            >>> ds1.to_naive_datetime()
            datetime.datetime(1979, 7, 5, 7, 20, 0, 321000)
        """

        return DataAPITimestamp(timestamp_ms=int(dt.timestamp() * 1000.0))

    @staticmethod
    def from_string(datetime_string: str) -> DataAPITimestamp:
        """
        Convert a string into a DataAPITimestamp, provided the string represents one
        according to the Data API RFC3339 format conventions. If the format is
        unrecognized, a ValueError is raised.

        Args:
            date_string: a string expressing a timestamp as per Data API conventions.

        Returns:
            a DataAPITimestamp corresponding to the provided input.

        Example:
            >>> from astrapy.data_types import DataAPITimestamp
            >>>
            >>> DataAPITimestamp.from_string("2021-07-18T14:56:23.987Z")
            DataAPITimestamp(timestamp_ms=1626620183987 [2021-07-18T14:56:23.987Z])
            >>> DataAPITimestamp.from_string("-0044-03-15T11:22:33+01:00")
            DataAPITimestamp(timestamp_ms=-63549322647000 [-0044-03-15T10:22:33.000Z])
            >>> # missing trailing offset information:
            >>> DataAPITimestamp.from_string("1991-11-22T01:23:45.678")
            Traceback (most recent call last):
              [...]
            ValueError: Cannot parse '1991-11-22T01:23:45.678' into a valid timestamp...
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
        Convert the DataAPITimestamp into a 7-item tuple expressing
        the corresponding datetime in UTC, i.e. with implied "+00:00" offset.

        Returns:
            a (year, month, day, hour, minute, second, millisecond) of integers.
            Note the last entry is millisecond.

        Example:
            >>> from astrapy.data_types import DataAPITimestamp
            >>>
            >>> dt1 = DataAPITimestamp(300000000321)
            >>> dt1
            DataAPITimestamp(timestamp_ms=300000000321 [1979-07-05T05:20:00.321Z])
            >>> dt1.timetuple()
            (1979, 7, 5, 5, 20, 0, 321)
        """
        return _unix_timestamp_ms_to_timetuple(self.timestamp_ms)

    def to_string(self) -> str:
        """
        Express the timestamp as a string according to the Data API RFC3339 syntax,
        including the presence of a sign, and the number of digits, for the year.

        The string returned from this method can be directly used in the appropriate
        parts of a payload to the Data API.

        Note that there is no parameter to control formatting (as there would be
        for `datetime.strftime`). To customize the formatting, one should invoke
        `DataAPITimestamp.timetuple` and use its output subsequently.

        Returns:
            a string, such as "2024-12-31T12:34:56.543Z", formatted in a way suitable
            to be used in a Data API payload.

        Example:
            >>> from astrapy.data_types import DataAPITimestamp
            >>>
            >>> dt1 = DataAPITimestamp(300000000321)
            >>> dt1
            DataAPITimestamp(timestamp_ms=300000000321 [1979-07-05T05:20:00.321Z])
            >>> dt1.to_string()
            '1979-07-05T05:20:00.321Z'
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
