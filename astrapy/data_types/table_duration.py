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

DURATION_UNIT_TO_POSITION = {
    "y": 0,
    "mo": 1,
    "w": 2,
    "d": 3,
    "h": 4,
    "m": 5,
    "s": 6,
    "ms": 7,
    "us": 8,
    "ns": 9,
}

MONTH_MULTIPLIER = {
    "y": 12,
    "mo": 1,
}
DAY_MULTIPLIER = {
    "w": 7,
    "d": 1,
}
NANOSECOND_MULTIPLIER = {
    "h": 3600000000000,
    "m": 60000000000,
    "s": 1000000000,
    "ms": 1000000,
    "us": 1000,
    "ns": 1,
}

# Note: "ms" and "mo" MUST come before "m" here (otherwise a wrong "m" is found first).
DURATION_V_PATTERN = re.compile(r"^(-?\d+(y|ms|mo|w|d|h|m|s|us|ns))+$")
DURATION_I_PATTERN = re.compile(r"(-?\d+)(y|ms|mo|w|d|h|m|s|us|ns)")

DURATION_FORMAT_DESC = (
    "Durations must be a non-empty sequence of '<quantity><unit>', without "
    "repetitions and in strict decreasing order. Quantities are integers, possibly "
    "with a leading minus sign, and units take value among: 'y', 'mo', 'w', 'd', "
    "'h', 'm', 's', 'ms', 'us' (or 'µs'), 'ns', in this order."
)


def _parse_duration(duration_string: str) -> TableDuration:
    _norm_string = duration_string.lower().replace("µs", "us")
    if DURATION_V_PATTERN.fullmatch(_norm_string):
        qunits = [
            (match.group(1), match.group(2))
            for match in DURATION_I_PATTERN.finditer(_norm_string)
        ]
        # validate sorting and duplicates
        if qunits:
            last_uindex = DURATION_UNIT_TO_POSITION[qunits[0][1]]
            for _, this_unit in qunits[1:]:
                this_uindex = DURATION_UNIT_TO_POSITION[this_unit]
                if this_uindex < last_uindex:
                    raise ValueError(
                        f"Unit '{this_unit}' cannot follow smaller units in literal "
                        f"for a TableDuration: '{duration_string}'. {DURATION_FORMAT_DESC}"
                    )
                elif this_uindex == last_uindex:
                    raise ValueError(
                        f"Unit '{this_unit}' cannot be repeated in literal for a "
                        f"TableDuration: '{duration_string}'. {DURATION_FORMAT_DESC}"
                    )
                last_uindex = this_uindex
            # reconstruct the final value
            parsed_uvals = {unit: int(valstr) for valstr, unit in qunits}
            #
            months = sum(
                umult * parsed_uvals.get(unit, 0)
                for unit, umult in MONTH_MULTIPLIER.items()
            )
            days = sum(
                umult * parsed_uvals.get(unit, 0)
                for unit, umult in DAY_MULTIPLIER.items()
            )
            nanoseconds = sum(
                umult * parsed_uvals.get(unit, 0)
                for unit, umult in NANOSECOND_MULTIPLIER.items()
            )
            return TableDuration(months=months, days=days, nanoseconds=nanoseconds)
        else:
            raise ValueError(
                "No quantity+unit groups in literal for a "
                f"TableDuration: '{duration_string}'. {DURATION_FORMAT_DESC}"
            )
    else:
        # TODO more verbose error (here and above)
        raise ValueError(
            "Invalid literal for a TableDuration: "
            f"'{duration_string}'. {DURATION_FORMAT_DESC}"
        )


@dataclass
class TableDuration:
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
    def from_string(duration_string: str) -> TableDuration:
        if duration_string == "":
            return TableDuration(
                months=0,
                days=0,
                nanoseconds=0,
            )
        return _parse_duration(duration_string)

    def to_string(self) -> str:
        _month_string: str | None = None
        _day_string: str | None = None
        _nanosecond_string: str | None = None
        #
        if self.months:
            _month_string = ""
            _residual_months = self.months
            for u, div in MONTH_MULTIPLIER.items():
                u_qty = _residual_months // div
                if u_qty >= 1:
                    _month_string += f"{u_qty}{u}"
                    _residual_months -= u_qty * div
        _day_string = f"{self.days}d" if self.days > 0 else None  # use no 'weeks' here
        if self.nanoseconds:
            _nanosecond_string = ""
            _residual_nanoseconds = self.nanoseconds
            for u, div in NANOSECOND_MULTIPLIER.items():
                u_qty = _residual_nanoseconds // div
                if u_qty >= 1:
                    _nanosecond_string += f"{u_qty}{u}"
                    _residual_nanoseconds -= u_qty * div
        return "".join(
            b for b in (_month_string, _day_string, _nanosecond_string) if b is not None
        )

    @staticmethod
    def from_timedelta(td: datetime.timedelta) -> TableDuration:
        return TableDuration.from_string(f"{td.days}d{td.seconds}s{td.microseconds}us")

    def to_timedelta(self) -> datetime.timedelta:
        if self.months != 0:
            raise ValueError(
                "Cannot convert a TableDuration with nonzero months into a timedelta."
            )
        return datetime.timedelta(
            days=self.days,
            microseconds=self.nanoseconds // 1000,
        )
