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

DAY_MS = 24 * 3600 * 1000
BASE_YEAR_MS = 365 * DAY_MS


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
    num_leap_years = sum([1 if _is_leap_year(y) else 0 for y in range(1970, year)])
    # total milliseconds from epoch
    y_since_epoch = year - 1970
    elapsed_ms = y_since_epoch * BASE_YEAR_MS + num_leap_years * DAY_MS
    return elapsed_ms


def _year_to_unix_timestamp_ms_backward(year: int) -> int:
    # leap years 'year' to 1970
    num_leap_years = sum([1 if _is_leap_year(y) else 0 for y in range(year, 1970)])
    # total milliseconds to epoch
    y_until_epoch = 1970 - year
    elapsed_ms = y_until_epoch * BASE_YEAR_MS + num_leap_years * DAY_MS
    return -elapsed_ms


def _year_to_unix_timestamp_ms(year: int) -> int:
    if year >= 1970:
        return _year_to_unix_timestamp_ms_forward(year)
    return _year_to_unix_timestamp_ms_backward(year)
