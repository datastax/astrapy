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

import re

DURATION_C_UNIT_TO_POSITION = {
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

DURATION_C_MONTH_MULTIPLIER = {
    "y": 12,
    "mo": 1,
}
DURATION_C_DAY_MULTIPLIER = {
    "w": 7,
    "d": 1,
}
DURATION_C_NANOSECOND_MULTIPLIER = {
    "h": 3600000000000,
    "m": 60000000000,
    "s": 1000000000,
    "ms": 1000000,
    "us": 1000,
    "ns": 1,
}

# Note: "ms" and "mo" MUST come before "m" here (otherwise a wrong "m" is found first).
DURATION_C_V_PATTERN = re.compile(r"^(-?)(\d+(y|ms|mo|w|d|h|m|s|us|ns))+$")
DURATION_C_I_PATTERN = re.compile(r"(\d+)(y|ms|mo|w|d|h|m|s|us|ns)")

DURATION_C_FORMAT_DESC = (
    "Durations must be a non-empty sequence of '<quantity><unit>', without "
    "repetitions and in strict decreasing order. Quantities are integers, possibly "
    "with a leading minus sign, and units take value among: 'y', 'mo', 'w', 'd', "
    "'h', 'm', 's', 'ms', 'us' (or 'µs'), 'ns', in this order."
)


def _parse_c_duration_string(duration_string: str) -> tuple[int, int, int, int]:
    # signum, months, days, nanoseconds
    if duration_string == "":
        return (1, 0, 0, 0)
    _norm_string0 = duration_string.lower().replace("µs", "us")
    _norm_string: str
    signum0: int
    if _norm_string0[0] == "-":
        signum0 = -1
        _norm_string = _norm_string0[1:]
    else:
        signum0 = +1
        _norm_string = _norm_string0
    if DURATION_C_V_PATTERN.fullmatch(_norm_string):
        qunits = [
            (match.group(1), match.group(2))
            for match in DURATION_C_I_PATTERN.finditer(_norm_string)
        ]
        # validate sorting and duplicates
        if qunits:
            last_uindex = DURATION_C_UNIT_TO_POSITION[qunits[0][1]]
            for _, this_unit in qunits[1:]:
                this_uindex = DURATION_C_UNIT_TO_POSITION[this_unit]
                if this_uindex < last_uindex:
                    raise ValueError(
                        f"Unit '{this_unit}' cannot follow smaller units in literal "
                        f"for a duration: '{duration_string}'. {DURATION_C_FORMAT_DESC}"
                    )
                elif this_uindex == last_uindex:
                    raise ValueError(
                        f"Unit '{this_unit}' cannot be repeated in literal for a "
                        f"duration: '{duration_string}'. {DURATION_C_FORMAT_DESC}"
                    )
                last_uindex = this_uindex
            # reconstruct the final value
            parsed_uvals = {unit: int(valstr) for valstr, unit in qunits}
            #
            months = sum(
                umult * parsed_uvals.get(unit, 0)
                for unit, umult in DURATION_C_MONTH_MULTIPLIER.items()
            )
            days = sum(
                umult * parsed_uvals.get(unit, 0)
                for unit, umult in DURATION_C_DAY_MULTIPLIER.items()
            )
            nanoseconds = sum(
                umult * parsed_uvals.get(unit, 0)
                for unit, umult in DURATION_C_NANOSECOND_MULTIPLIER.items()
            )

            # resolve signum ambiguity if null duration
            signum: int
            if months != 0 or days != 0 or nanoseconds != 0:
                signum = signum0
            else:
                signum = +1

            return (signum, months, days, nanoseconds)
        else:
            raise ValueError(
                "No quantity+unit groups in literal for a "
                f"duration: '{duration_string}'. {DURATION_C_FORMAT_DESC}"
            )
    else:
        raise ValueError(
            "Invalid literal for a duration: "
            f"'{duration_string}'. {DURATION_C_FORMAT_DESC}"
        )


def _build_c_duration_string(
    signum: int,
    months: int,
    days: int,
    nanoseconds: int,
) -> str:
    _signum_string: str | None = None
    _month_string: str | None = None
    _day_string: str | None = None
    _nanosecond_string: str | None = None
    #
    if months == 0 and days == 0 and nanoseconds == 0:
        # a special case
        return "0s"
    if signum < 0:
        _signum_string = "-"
    if months:
        _month_string = ""
        _residual_months = months
        for u, div in DURATION_C_MONTH_MULTIPLIER.items():
            u_qty = _residual_months // div
            if u_qty >= 1:
                _month_string += f"{u_qty}{u}"
                _residual_months -= u_qty * div
    _day_string = f"{days}d" if days > 0 else None  # use no 'weeks' here
    if nanoseconds:
        _nanosecond_string = ""
        _residual_nanoseconds = nanoseconds
        for u, div in DURATION_C_NANOSECOND_MULTIPLIER.items():
            u_qty = _residual_nanoseconds // div
            if u_qty >= 1:
                _nanosecond_string += f"{u_qty}{u}"
                _residual_nanoseconds -= u_qty * div
    return "".join(
        b
        for b in (_signum_string, _month_string, _day_string, _nanosecond_string)
        if b is not None
    )
