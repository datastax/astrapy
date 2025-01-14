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

DURATION_STD_MONTH_MULTIPLIER = {
    "Y": 12,
    "M": 1,
}
DURATION_STD_DAY_MULTIPLIER = {
    "D": 1,
}
DURATION_STD_NANOSECOND_MULTIPLIER = {
    "H": 3600000000000,
    "M": 60000000000,
    "S": 1000000000,
}
DAYS_PER_WEEK = 7

PRE_T_VALIDATOR = re.compile(r"^(\d+Y)?(\d+M)?(\d+D)?$")
PRE_T_PARSER = re.compile(r"(\d+)(Y|M|D)")
POST_T_VALIDATOR = re.compile(r"^(\d+H)?(\d+M)?(\d*\.?\d*S)?$")
POST_T_PARSER = re.compile(r"(\d*\.?\d*)(H|M|S)")
FULL_W_VALIDATOR = re.compile(r"^(\d+)W$")
FULL_W_PARSER = re.compile(r"(\d+)(W)")

DURATION_STD_FORMAT_DESC = (
    "Durations must be expressed according to the '[-]P[nY][nM][nD][T[nH][nM][fS]]' "
    "or '[-]P[nW]' "
    "pattern, with: an optional leading minus sign; a literal P; a sequence of at "
    "least one '<quantity><unit>' specification. These, if appearing, must be in "
    "strict order (resp. years, months, days, hours, minutes, seconds; or just weeks). "
    "If sub-day "
    "specifications are present, a literal 'T' must separate them from the whole-day "
    "part. Quantities are non-negative integers ('n') except for the seconds ('S') "
    "specification, which can be a non-negative decimal number. If using 'W', no other "
    'units are allowed. Examples: "P1M10D", "P1Y3MT4H15.43S", "-PT2M55S", "P3W".'
)


def _parse_std_duration_string(duration_string: str) -> tuple[int, int, int, int]:
    # format: "[-]P[nY][nM][nD][T[nH][nM][fS]]" (n=non-neg int, f=non-neg float)
    if duration_string == "":
        raise ValueError(
            f"Empty strings are not valid durations. {DURATION_STD_FORMAT_DESC}"
        )

    signum0: int
    # without the '[-]P' preamble:
    _stripped0: str
    if duration_string[:2] == "-P":
        signum0 = -1
        _stripped0 = duration_string[2:]
    elif duration_string[:1] == "P":
        signum0 = +1
        _stripped0 = duration_string[1:]
    else:
        raise ValueError(
            "A string not starting with '-P' or 'P' is not a valid "
            f"duration (received: {duration_string}. {DURATION_STD_FORMAT_DESC}"
        )

    # chunks before and after the "T" (even if absent, filled here)
    if _stripped0 == "" or _stripped0 == "T":
        return (+1, 0, 0, 0)
    t_blocks = _stripped0.split("T")

    months: int
    days: int
    nanoseconds: int

    if len(t_blocks) == 0:
        raise ValueError(
            "A string without quantity-unit specifications is not a valid "
            f"duration (received: {duration_string}. {DURATION_STD_FORMAT_DESC}"
        )
    elif len(t_blocks) == 1 and FULL_W_VALIDATOR.fullmatch(_stripped0):
        wnumss = [int(match[1]) for match in FULL_W_PARSER.finditer(_stripped0)]
        if len(wnumss) != 1:
            raise ValueError(
                "Multiple 'W' units encountered in a duration "
                f"(received: {duration_string}. {DURATION_STD_FORMAT_DESC}"
            )
        months = 0
        days = DAYS_PER_WEEK * wnumss[0]
        nanoseconds = 0
    elif len(t_blocks) <= 2:
        _pre_t = t_blocks[0]
        _post_t = (t_blocks + [""])[1]
        # parsing the pre-T block
        if PRE_T_VALIDATOR.fullmatch(_pre_t):
            pre_t_qunits = {
                match.group(2): match.group(1)
                for match in PRE_T_PARSER.finditer(_pre_t)
            }
            parsed_pre_t_vals = {
                unit: int(valstr) for unit, valstr in pre_t_qunits.items()
            }
            months = sum(
                umult * parsed_pre_t_vals.get(unit, 0)
                for unit, umult in DURATION_STD_MONTH_MULTIPLIER.items()
            )
            days = sum(
                umult * parsed_pre_t_vals.get(unit, 0)
                for unit, umult in DURATION_STD_DAY_MULTIPLIER.items()
            )
        else:
            raise ValueError(
                "Invalid whole-day component for a duration string "
                f"(received: {duration_string}. {DURATION_STD_FORMAT_DESC}"
            )

        # parsing the post-T block. Here only the last one is allowed to be float
        if POST_T_VALIDATOR.fullmatch(_post_t):
            post_t_qunits = {
                match.group(2): match.group(1)
                for match in POST_T_PARSER.finditer(_post_t)
            }
            try:
                parsed_post_t_vals = {
                    unit: int(valstr) if unit != "S" else float(valstr)
                    for unit, valstr in post_t_qunits.items()
                }
            except ValueError:
                raise ValueError(
                    "Float values are accepted only for the seconds duration specification "
                    f"(received: {duration_string}. {DURATION_STD_FORMAT_DESC}"
                )
            nanoseconds = sum(
                int(
                    umult * parsed_post_t_vals.get(unit, 0)
                    + (0.5 if unit == "S" else 0.0)
                )
                for unit, umult in DURATION_STD_NANOSECOND_MULTIPLIER.items()
            )
        else:
            raise ValueError(
                "Invalid fraction-of-day component for a duration string "
                f"(received: {duration_string}. {DURATION_STD_FORMAT_DESC}"
            )
    else:
        raise ValueError(
            "A duration string must containt at most one single 'T' separator "
            f"(received: {duration_string}. {DURATION_STD_FORMAT_DESC}"
        )

    # resolve signum ambiguity if null duration
    signum: int
    if months != 0 or days != 0 or nanoseconds != 0:
        signum = signum0
    else:
        signum = +1

    return (signum, months, days, nanoseconds)


def _build_std_duration_string(
    signum: int,
    months: int,
    days: int,
    nanoseconds: int,
) -> str:
    _signum_string: str | None = None
    _preamble_string = "P"
    _month_string: str | None = None
    _day_string: str | None = None
    _t_block: str | None = None
    _nanosecond_string: str | None = None

    if months == 0 and days == 0 and nanoseconds == 0:
        # a special case
        return "PT0S"

    if signum < 0:
        _signum_string = "-"
    if months:
        _month_string = ""
        _residual_months = months
        for u, div in DURATION_STD_MONTH_MULTIPLIER.items():
            u_qty = _residual_months // div
            if u_qty >= 1:
                _month_string += f"{u_qty}{u}"
                _residual_months -= u_qty * div
    _day_string = f"{days}D" if days > 0 else None
    if nanoseconds:
        _nanosecond_string = ""
        _residual_nanoseconds = nanoseconds
        for u, div in DURATION_STD_NANOSECOND_MULTIPLIER.items():
            if u != "S":
                u_qty = _residual_nanoseconds // div
                if u_qty >= 1:
                    _nanosecond_string += f"{u_qty}{u}"
                    _residual_nanoseconds -= u_qty * div
        if _residual_nanoseconds != 0:
            _seconds_part = f"{_residual_nanoseconds / 1000000000:.9f}".rstrip(
                "0"
            ).rstrip(".")
            _nanosecond_string += f"{_seconds_part}S"
    if _nanosecond_string is not None:
        _t_block = "T"
    return "".join(
        b
        for b in (
            _signum_string,
            _preamble_string,
            _month_string,
            _day_string,
            _t_block,
            _nanosecond_string,
        )
        if b is not None
    )
