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

EPOCH_YEAR = 1970
DAY_MS = 24 * 3600 * 1000
BASE_YEAR_MS = 365 * DAY_MS
# a 400-year span has exactly 97 leap years
AVERAGE_YEAR_MS = int((365 + 97 / 400) * DAY_MS)


def _is_leap_year(year: int) -> bool:
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)


def _validate_date(*, year: int, month: int, day: int) -> str | None:
    # None if valid date, reason otherwise
    if month <= 0 or month > 12:
        return "illegal month"
    is_leap = _is_leap_year(year)
    if month == 2 and is_leap:
        if day <= 0 or day > MAX_DAY_PER_MONTH_LEAP[month]:
            return "illegal monthday, leap-year case"
    if month == 2 and not is_leap:
        if day <= 0 or day > MAX_DAY_PER_MONTH[month]:
            return "illegal monthday, nonleap-year case"
    if month != 2:
        if day <= 0 or day > MAX_DAY_PER_MONTH[month]:
            return "illegal monthday"
    return None


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
        return "illegal fractional seconds"
    return None


def _year_to_unix_timestamp_ms_forward(year: int) -> int:
    # leap years 1970 to 'year'
    num_leap_years = sum(
        [1 if _is_leap_year(y) else 0 for y in range(EPOCH_YEAR, year)]
    )
    # total milliseconds from epoch
    y_since_epoch = year - EPOCH_YEAR
    elapsed_ms = y_since_epoch * BASE_YEAR_MS + num_leap_years * DAY_MS
    return elapsed_ms


def _year_to_unix_timestamp_ms_backward(year: int) -> int:
    # leap years 'year' to 1970
    num_leap_years = sum(
        [1 if _is_leap_year(y) else 0 for y in range(year, EPOCH_YEAR)]
    )
    # total milliseconds to epoch
    y_until_epoch = EPOCH_YEAR - year
    elapsed_ms = y_until_epoch * BASE_YEAR_MS + num_leap_years * DAY_MS
    return -elapsed_ms


def _year_to_unix_timestamp_ms(year: int) -> int:
    if year >= EPOCH_YEAR:
        return _year_to_unix_timestamp_ms_forward(year)
    return _year_to_unix_timestamp_ms_backward(year)


def _unix_timestamp_ms_to_timetuple(
    timestamp_ms: int,
) -> tuple[int, int, int, int, int, int, int]:
    # return (year, month, day, hour, minute, second, millisecond). UTC time tuples.
    year_guess = int(EPOCH_YEAR + (timestamp_ms / AVERAGE_YEAR_MS))
    # this is mostly correct but not necessarily. Walk by +/-1 if needed
    year_start_ms = _year_to_unix_timestamp_ms(year_guess)
    next_year_start_ms = _year_to_unix_timestamp_ms(year_guess + 1)
    while (timestamp_ms < year_start_ms) or (timestamp_ms >= next_year_start_ms):
        # shift and recalc
        if timestamp_ms < year_start_ms:
            year_guess -= 1
            next_year_start_ms = year_start_ms
            year_start_ms = _year_to_unix_timestamp_ms(year_guess)
        else:
            year_guess += 1
            year_start_ms = next_year_start_ms
            next_year_start_ms = _year_to_unix_timestamp_ms(year_guess + 1)
    # now, year_guess is correct. Deal with the remainder part
    year = year_guess
    y_remainder_ms = timestamp_ms - year_start_ms
    ref_year = 1972 if _is_leap_year(year) else 1971
    ref_dt = datetime.datetime(ref_year, 1, 1) + datetime.timedelta(
        milliseconds=y_remainder_ms
    )
    if ref_dt.year != ref_year:
        raise RuntimeError(f"Could not map timestamp ({timestamp_ms} ms) to a date.")
    month = ref_dt.month
    day = ref_dt.day
    hour = ref_dt.hour
    minute = ref_dt.minute
    second = ref_dt.second
    millisecond = int(ref_dt.microsecond / 1000.0)
    return (year, month, day, hour, minute, second, millisecond)


def _get_datetime_offset(dt: datetime.datetime) -> tuple[int, int] | None:
    # if not naive, return the (possibly) nontrivial offset as (hours, minutes)
    offset = dt.utcoffset()
    if offset is None:
        return offset
    offset_hours = offset.seconds // 3600 + offset.days * 24
    offset_minutes = (offset.seconds // 60) % 60
    return (offset_hours, offset_minutes)
