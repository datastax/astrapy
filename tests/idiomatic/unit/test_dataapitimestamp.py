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

import pytest

from astrapy.data_types import DataAPITimestamp
from astrapy.utils.date_utils import (
    _year_to_unix_timestamp_ms,
    _year_to_unix_timestamp_ms_backward,
    _year_to_unix_timestamp_ms_forward,
)


class TestDataAPITimestamp:
    @pytest.mark.describe("test of DataAPITimestamp class")
    def test_dataapitimestamp_lifecycle(self) -> None:
        # in-range use (far enough from valid range for tz-dependent conversions)
        Y1_1_1_MILLIS = -62135596800000
        Y10K_12_31_MILLIS = 253402300800000
        ONE_DAY_AND_EPSILON_MILLIS = 86400000 + 555
        TEN_YEARS_MILLIS = 10 * 365 * 86400000
        ts_y00001 = DataAPITimestamp(Y1_1_1_MILLIS + ONE_DAY_AND_EPSILON_MILLIS)
        ts_y00001_2 = DataAPITimestamp(Y1_1_1_MILLIS + ONE_DAY_AND_EPSILON_MILLIS)
        ts_y10000 = DataAPITimestamp(Y10K_12_31_MILLIS - ONE_DAY_AND_EPSILON_MILLIS)

        assert ts_y00001 == ts_y00001_2
        assert ts_y00001 != ts_y10000
        assert DataAPITimestamp.from_datetime(ts_y00001.to_datetime()) == ts_y00001
        assert DataAPITimestamp.from_datetime(ts_y10000.to_datetime()) == ts_y10000

        # (not: DataAPITimestamp is not supposed to respect sub-millisecond precision.)
        dt1 = datetime.datetime(2024, 10, 27, 11, 22, 33, 831000)
        assert dt1 == DataAPITimestamp.from_datetime(dt1).to_datetime()

        # out-of-ranges
        ts_y00009_bc = DataAPITimestamp(Y1_1_1_MILLIS - TEN_YEARS_MILLIS)
        ts_y10010_ad = DataAPITimestamp(Y10K_12_31_MILLIS + TEN_YEARS_MILLIS)
        with pytest.raises(ValueError, match="is out of range"):
            ts_y00009_bc.to_datetime()
        with pytest.raises(ValueError, match="is out of range"):
            ts_y10010_ad.to_datetime()

        # algebra
        assert ts_y00001 < ts_y10000
        assert ts_y00001 <= ts_y10000
        assert ts_y00001 <= ts_y00001_2
        assert ts_y10000 >= ts_y00001
        assert ts_y10000 >= ts_y00001
        assert ts_y00001 >= ts_y00001_2
        assert ts_y00001 - ts_y00001 == 0
        assert ts_y10000 - ts_y00001 > 0
        assert ts_y00001 - ts_y10000 < 0

    @pytest.mark.describe("test of DataAPITimestamp year-to-epoch utilities")
    def test_dataapitimestamp_year_timestamp(self) -> None:
        assert _year_to_unix_timestamp_ms_forward(1970) == 0
        assert _year_to_unix_timestamp_ms_backward(1970) == 0
        assert _year_to_unix_timestamp_ms(1970) == 0
        for y in range(1, 10000, 15):
            py_y = datetime.datetime(y, 1, 1, 0, 0, 0).replace(
                tzinfo=datetime.timezone.utc
            )
            py_ts_s = py_y.timestamp()
            util_ts_ms = _year_to_unix_timestamp_ms(y)
            assert py_ts_s * 1000 == util_ts_ms

    @pytest.mark.describe("test of DataAPITimestamp parsing, failures")
    def test_dataapitimestamp_parsing_failures(self) -> None:
        with pytest.raises(ValueError):
            DataAPITimestamp.from_string("")
        with pytest.raises(ValueError):
            DataAPITimestamp.from_string("boo!")

        DataAPITimestamp.from_string("2024-10-29T01:25:37.123Z")
        DataAPITimestamp.from_string("2024-10-29T01:25:37Z")
        DataAPITimestamp.from_string("2024-10-29T01:25:37.123+01:23")
        DataAPITimestamp.from_string("2024-10-29T01:25:37.123-23:12")
        DataAPITimestamp.from_string("2024-10-29T01:25:37+01:23")
        DataAPITimestamp.from_string("2024-10-29T01:25:37-23:12")
        DataAPITimestamp.from_string("-123456-10-29T01:25:37.123Z")
        DataAPITimestamp.from_string("123456-10-29T01:25:37.123Z")

        DataAPITimestamp.from_string("2024-2-3T4:5:6Z")
        DataAPITimestamp.from_string("2024-02-03T04:05:06Z")

        DataAPITimestamp.from_string("1972-02-29T04:05:06Z")
        DataAPITimestamp.from_string("1972-02-29T04:05:06+23:59")
        DataAPITimestamp.from_string("1972-02-29T04:05:06-23:59")
        with pytest.raises(ValueError):
            DataAPITimestamp.from_string("1971-02-29T04:05:06Z")
        with pytest.raises(ValueError):
            DataAPITimestamp.from_string("1971-02-29T04:05:06+23:59")
        with pytest.raises(ValueError):
            DataAPITimestamp.from_string("1971-02-29T04:05:06-23:59")

        with pytest.raises(ValueError):
            DataAPITimestamp.from_string("a2024-10-29T01:25:37.123Z")
        with pytest.raises(ValueError):
            DataAPITimestamp.from_string("2024-10-q29T01:25:37.123Z")
        with pytest.raises(ValueError):
            DataAPITimestamp.from_string("2024-10-29T01:25:37.123Zb")
        with pytest.raises(ValueError):
            DataAPITimestamp.from_string("2024-13-29T01:25:37.123Z")
        with pytest.raises(ValueError):
            DataAPITimestamp.from_string("2024-10-32T01:25:37.123Z")
        with pytest.raises(ValueError):
            DataAPITimestamp.from_string("2024-10-29T25:25:37.123Z")
        with pytest.raises(ValueError):
            DataAPITimestamp.from_string("2024-10-29T01:61:37.123Z")
        with pytest.raises(ValueError):
            DataAPITimestamp.from_string("2024-10-29T01:25:61.123Z")
        with pytest.raises(ValueError):
            DataAPITimestamp.from_string("2024-10-29T01:25:37.123+25:43")
        with pytest.raises(ValueError):
            DataAPITimestamp.from_string("2024-10-29T01:25:37.123-27:91")
        with pytest.raises(ValueError):
            DataAPITimestamp.from_string("2024-00-29T01:25:37.123Z")
        with pytest.raises(ValueError):
            DataAPITimestamp.from_string("2024-10-00T01:25:37.123Z")

    @pytest.mark.describe("test of DataAPITimestamp parsing, consistency")
    def test_dataapitimestamp_parsing_consistency(self) -> None:
        for year in range(2, 9999, 50):
            for offset_hour in [-20, -2, 0, 2, 20]:
                for offset_minute in [0, 25, 45]:
                    for millisecond in [0, 789]:
                        py_dt_0 = datetime.datetime(
                            year, 4, 27, 12, 34, 56, millisecond * 1000
                        ).replace(tzinfo=datetime.timezone.utc)
                        py_dt = py_dt_0 - datetime.timedelta(
                            hours=offset_hour,
                            minutes=offset_minute,
                        )
                        _fraction = "" if millisecond == 0 else f".{millisecond:03}"
                        _offset = (
                            "Z"
                            if offset_hour == 0 and offset_minute == 0
                            else f"{offset_hour:+03}:{offset_minute:02}"
                        )
                        ts_string = f"{year}-04-27T12:34:56{_fraction}{_offset}"
                        py_ts_ms = int(py_dt.timestamp() * 1000)
                        util_ts_ms = DataAPITimestamp.from_string(
                            ts_string
                        ).timestamp_ms
                        assert py_ts_ms == util_ts_ms
