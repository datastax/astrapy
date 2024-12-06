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
    A "duration" value, suitable for working with the "duration" table column type.

    Durations are an abstract notion: as such, they cannot be mapped into a precise,
    well-defined time span. In other words, their components cannot be referred
    one to another. Indeed, a 'month' is not a fixed amount of days, and likewise
    a 'day' is not a fixed amount of hours (due to daylight saving changes, there are
    days made of 23 or 25 hours).

    For this reason, the DataAPIDuration class cannot really be mapped to a Python
    standard-library `datetime.timedelta`, unless in a restricted set of cases and/or
    with some approximations involved.

    Args:
        months: an integer non-negative amount of months.
        days: an integer non-negative amount of days.
        nanoseconds: an integer non-negative amount of nanoseconds.
            This quantity encodes the whole sub-day component of the
            duration: for instance, a duration of 36 hours is expressed as
            36 * 3600 * 1000000000 = 129600000000000 nanoseconds.
        signum: an overall plus or minus sign, represented as either +1 or -1.
            This allows the encoding of negative durations.
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
        """
        Parse a string, expressed according to the Data API ISO-8601 format, into a
        DataAPIDuration. If the format is not recognized, a ValueError is raised.

        Args:
            duration_string: a string compliant to the Data API ISO-8601 specification
            for durations.

        Returns:
            a DataAPIDuration corresponding to the provided input.

        Example:
            >>> from astrapy.data_types import DataAPIDuration
            >>>
            >>> DataAPIDuration.from_string("P3Y6M4DT12H30M5S")
            DataAPIDuration(months=42, days=4, nanoseconds=45005000000000)
            >>> DataAPIDuration.from_string("PT12H")
            DataAPIDuration(months=0, days=0, nanoseconds=43200000000000)
            >>> DataAPIDuration.from_string("PT12H11X")
            Traceback (most recent call last):
              [...]
            ValueError: Invalid fraction-of-day component for a duration string ...
        """

        si, mo, da, ns = _parse_std_duration_string(duration_string)
        return DataAPIDuration(
            signum=si,
            months=mo,
            days=da,
            nanoseconds=ns,
        )

    def to_string(self) -> str:
        """
        Convert the DataAPIDuration to a string according to the Data API
        ISO-8601 standard.

        Returns:
            a string expressing the time value in a way compatible with the Data API
            conventions.

        Example:
            >>> from astrapy.data_types import DataAPIDuration
            >>>
            >>> dd1 = DataAPIDuration(1, months=42, days=4, nanoseconds=45005000000000)
            >>> dd1.to_string()
            'P3Y6M4DT12H30M5S'
            >>> dd2 = DataAPIDuration(1, months=0, days=0, nanoseconds=43200000000000)
            >>> dd2.to_string()
            'PT12H'
        """

        return _build_std_duration_string(
            signum=self.signum,
            months=self.months,
            days=self.days,
            nanoseconds=self.nanoseconds,
        )

    @staticmethod
    def from_c_string(duration_string: str) -> DataAPIDuration:
        """
        Parse a string, expressed according to the Data API "Apache Cassandra(R)
        notation", into a DataAPIDuration.
        If the format is not recognized, a ValueError is raised.

        Args:
            duration_string: a string compliant to the Data API "Apache Cassandra(R)
            notation" for durations.

        Returns:
            a DataAPIDuration corresponding to the provided input.

        Example:
            >>> from astrapy.data_types import DataAPIDuration
            >>>
            >>> DataAPIDuration.from_c_string("12y3mo1d")
            DataAPIDuration(months=147, days=1, nanoseconds=0)
            >>> DataAPIDuration.from_c_string("12y3mo1d12h30m5s")
            DataAPIDuration(months=147, days=1, nanoseconds=45005000000000)
            >>> DataAPIDuration.from_c_string("-4h1978us")
            DataAPIDuration(months=0, days=0, nanoseconds=-14400001978000)
            >>> DataAPIDuration.from_c_string("0h0m")
            DataAPIDuration(months=0, days=0, nanoseconds=0)
            >>> DataAPIDuration.from_c_string("1h1y")
            Traceback (most recent call last):
              [...]
            ValueError: Unit 'y' cannot follow smaller units in literal for ...
        """

        si, mo, da, ns = _parse_c_duration_string(duration_string)
        return DataAPIDuration(
            signum=si,
            months=mo,
            days=da,
            nanoseconds=ns,
        )

    def to_c_string(self) -> str:
        """
        Convert the DataAPIDuration to a string according to the Data API
        "Apache Cassandra(R) notation".

        Returns:
            a string expressing the time value in a way compatible with the Data API
            conventions.

        Example:
            >>> from astrapy.data_types import DataAPIDuration
            >>>
            >>> dd1 = DataAPIDuration(1, months=147, days=1, nanoseconds=0)
            >>> dd1.to_c_string()
            '12y3mo1d'
            >>> dd2 = DataAPIDuration(1, months=147, days=1, nanoseconds=45005000000000)
            >>> dd2.to_c_string()
            '12y3mo1d12h30m5s'
            >>> dd3 = DataAPIDuration(-1, months=0, days=0, nanoseconds=14400001978000)
            >>> dd3.to_c_string()
            '-4h1ms978us'
            >>> dd4 = DataAPIDuration(1, months=0, days=0, nanoseconds=0)
            >>> dd4.to_c_string()
            '0s'
        """

        return _build_c_duration_string(
            signum=self.signum,
            months=self.months,
            days=self.days,
            nanoseconds=self.nanoseconds,
        )

    @staticmethod
    def from_timedelta(td: datetime.timedelta) -> DataAPIDuration:
        """
        Construct a DataAPIDuration from a Python standard-library `datetime.timedelta`.

        Due to the intrinsic difference between the notions of a DataAPIDuration and
        the timedelta, the latter - a definite time span - is only ever resulting
        in a duration with a nonzero "nanoseconds" component.

        Args:
            dt: a `datetime.timedelta` value.

        Returns:
            A DataAPIDuration corresponding value, with null month and day components
            by construction.

        Example:
            >>> from datetime import timedelta
            >>>
            >>> from astrapy.data_types import DataAPIDuration
            >>>
            >>> DataAPIDuration.from_timedelta(
            ...     timedelta(days=1, hours=2, seconds=10.987)
            ... )
            DataAPIDuration(months=0, days=0, nanoseconds=93610987000000)
            >>> DataAPIDuration.from_timedelta(timedelta(hours=-4))
            DataAPIDuration(months=0, days=0, nanoseconds=-14400000000000)
        """

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
        """
        Convert a DataAPIDuration into a Python standard library `datetime.timedelta`.

        Due to the intrinsic difference between the notions of a DataAPIDuration and
        the timedelta, the conversion attempt raises an error if the starting
        DataAPIDuration has a nonzero number of months. For the same reason, a somewhat
        lossy conversion occurs for a nonzero number of days since the formal notion
        of a day is lost in favor of that of "a span of exactly 24 hours".

        Returns:
            a `datetime.timedelta` value corresponding to the origin DataAPIDuration
            in the best possible way.

        Example:
            >>> from astrapy.data_types import DataAPIDuration
            >>>
            >>> dd1 = DataAPIDuration(
            ...     signum=-1, months=0, days=0, nanoseconds=93610987000000
            ... )
            >>> dd1.to_timedelta()
            datetime.timedelta(days=-2, seconds=79189, microseconds=13000)
            >>> dd2 = DataAPIDuration(
            ...     signum=1, months=0, days=0, nanoseconds=14400000000000
            ... )
            >>> dd2.to_timedelta()
            datetime.timedelta(seconds=14400)
            >>> dd3 = DataAPIDuration(signum=1, months=19, days=0, nanoseconds=0)
            >>> dd3.to_timedelta()
            Traceback (most recent call last):
                [...]
            ValueError: Cannot convert a DataAPIDuration with nonzero months into ...
        """

        if self.months != 0:
            raise ValueError(
                "Cannot convert a DataAPIDuration with nonzero months into a timedelta."
            )
        return datetime.timedelta(
            days=self.signum * self.days,
            microseconds=self.signum * self.nanoseconds // 1000,
        )
