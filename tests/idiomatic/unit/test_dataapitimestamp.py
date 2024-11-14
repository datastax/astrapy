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
    AVERAGE_YEAR_MS,
    EPOCH_YEAR,
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
        dt1 = datetime.datetime(
            2024, 10, 27, 11, 22, 33, 831000, tzinfo=datetime.timezone.utc
        )
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

        # signs for various lengths of the 'year digits' portion
        # valid cases
        DataAPITimestamp.from_string("0000-10-29T01:25:37.123Z")
        DataAPITimestamp.from_string("-2024-10-29T01:25:37.123Z")
        DataAPITimestamp.from_string("+112024-10-29T01:25:37.123Z")
        DataAPITimestamp.from_string("-112024-10-29T01:25:37.123Z")
        DataAPITimestamp.from_string("0124-10-29T01:25:37.123Z")
        DataAPITimestamp.from_string("-0124-10-29T01:25:37.123Z")
        # incorrect cases for sign of year
        with pytest.raises(ValueError):
            DataAPITimestamp.from_string("+0000-10-29T01:25:37.123Z")
        with pytest.raises(ValueError):
            DataAPITimestamp.from_string("-0000-10-29T01:25:37.123Z")
        with pytest.raises(ValueError):
            DataAPITimestamp.from_string("+124-10-29T01:25:37.123Z")
        with pytest.raises(ValueError):
            DataAPITimestamp.from_string("+2024-10-29T01:25:37.123Z")
        with pytest.raises(ValueError):
            DataAPITimestamp.from_string("+-2024-10-29T01:25:37.123Z")
        with pytest.raises(ValueError):
            DataAPITimestamp.from_string("-+2024-10-29T01:25:37.123Z")
        with pytest.raises(ValueError):
            DataAPITimestamp.from_string("124-10-29T01:25:37.123Z")
        with pytest.raises(ValueError):
            DataAPITimestamp.from_string("+124-10-29T01:25:37.123Z")
        with pytest.raises(ValueError):
            DataAPITimestamp.from_string("-124-10-29T01:25:37.123Z")
        with pytest.raises(ValueError):
            DataAPITimestamp.from_string("123456-10-29T01:25:37.123Z")

        DataAPITimestamp.from_string("2024-10-29T01:25:37.123Z")
        DataAPITimestamp.from_string("2024-10-29T01:25:37Z")
        DataAPITimestamp.from_string("2024-10-29T01:25:37.123+01:23")
        DataAPITimestamp.from_string("2024-10-29T01:25:37.123-23:12")
        DataAPITimestamp.from_string("2024-10-29T01:25:37+01:23")
        DataAPITimestamp.from_string("2024-10-29T01:25:37-23:12")
        DataAPITimestamp.from_string("-123456-10-29T01:25:37.123Z")
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

    @pytest.mark.describe(
        "test of DataAPITimestamp sign-of-year parse and stringify consistency"
    )
    def test_dataapitimestamp_sign_of_year_consistency(self) -> None:
        def parse_and_back_year_str(year_str: str) -> None:
            # just testing that years come out as they get in with numdigits, sign
            ts_string = f"{year_str}-01-12T12:00:00.000Z"
            ts = DataAPITimestamp.from_string(ts_string)
            ts_string_1 = ts.to_string()
            assert ts_string == ts_string_1

        parse_and_back_year_str("2024")
        parse_and_back_year_str("0000")
        parse_and_back_year_str("-2024")
        parse_and_back_year_str("+112024")
        parse_and_back_year_str("-112024")
        parse_and_back_year_str("0124")
        parse_and_back_year_str("-0124")
        parse_and_back_year_str("-123456")
        parse_and_back_year_str("1972")

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
                        ts_string = f"{year:04}-04-27T12:34:56{_fraction}{_offset}"
                        py_ts_ms = int(py_dt.timestamp() * 1000)
                        util_ts_ms = DataAPITimestamp.from_string(
                            ts_string
                        ).timestamp_ms
                        assert py_ts_ms == util_ts_ms

    @pytest.mark.describe(
        "test of DataAPITimestamp, back to tuple and string, with datetimes"
    )
    def test_dataapitimestamp_back_to_tuple_string_wdatetimes(self) -> None:
        """
        Test of various string/tuple-powered conversions and their consistency.
        In range covered by 'datetime'.

        Plan:
                                                    --> datetime[4]
                                                   /
        datetime[0] --> timestamp --> DataAPITS[1] ---> tuple[2]
                                                   \
                                                    --> string --> DataAPITS[3]
                
        followed by checks that:
            [0] == [2]
            [1] == [3]
            [0] == [4]
        """

        for test_y in [
            1,
            8,
            100,
            1604,
            1969,
            1970,
            1971,
            1972,
            2024,
            2100,
            2777,
            5432,
            7776,
            9999,
        ]:
            for test_mo in [1, 2, 7, 10, 12]:
                for test_h in [0, 1, 17, 23]:
                    for test_m in [0, 59]:
                        for test_s in [0, 59]:
                            for test_us in [0, 654000, 999000]:
                                test_dt = datetime.datetime(
                                    test_y,
                                    test_mo,
                                    28,
                                    test_h,
                                    test_m,
                                    test_s,
                                    test_us,
                                    tzinfo=datetime.timezone.utc,
                                )
                                dapi_ts = DataAPITimestamp.from_datetime(test_dt)
                                dapi_tuple = dapi_ts.timetuple()
                                # [0] == [2]
                                assert test_dt.timetuple()[:6] == dapi_tuple[:6]
                                # allow for 1 ms fluctuations (from rounding and the like):
                                assert (
                                    abs(test_dt.microsecond // 1000 - dapi_tuple[6])
                                    <= 1
                                )

                                # [1] == [3]
                                dapi_str = dapi_ts.to_string()
                                assert dapi_ts == DataAPITimestamp.from_string(dapi_str)

                                # [0] == [4]
                                # allow for 1 ms fluctuations (from rounding and the like):
                                assert (
                                    abs(
                                        (
                                            dapi_ts.to_datetime() - test_dt
                                        ).total_seconds()
                                    )
                                    <= 0.001
                                )

    @pytest.mark.describe(
        "test of DataAPITimestamp, back to tuple and string, no datetimes"
    )
    def test_dataapitimestamp_back_to_tuple_string_nodatetimes(self) -> None:
        """
        Test of various string/tuple-powered conversions and their consistency.

        In range NOT covered by 'datetime':
        spanning a wider year range, loop by timestamp

            timestamp -> DataAPITS[0] -> strin -> DataAPITS[1]

        checking that [0] == [1] between 50000 BC and 50000 AD.
        """

        min_ts = AVERAGE_YEAR_MS * (-50000 - EPOCH_YEAR)
        max_ts = AVERAGE_YEAR_MS * (50000 - EPOCH_YEAR)
        num_steps = 200
        step_ms = (max_ts - min_ts) // num_steps
        for test_ms in range(min_ts, max_ts, step_ms):
            dapi_ts = DataAPITimestamp(test_ms)
            dapi_ts2 = DataAPITimestamp.from_string(dapi_ts.to_string())
            assert dapi_ts == dapi_ts2
