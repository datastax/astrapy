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

from astrapy.data_types import DataAPIDuration

# constants lifted from https://github.com/apache/cassandra/blob/
#   198df38860d1575b60fbcaf9a8a506d2efd1bb31/test/unit/org/apache/
#   cassandra/cql3/DurationTest.java#L37
NANOS_PER_MICRO = 1000
NANOS_PER_MILLI = 1000 * NANOS_PER_MICRO
NANOS_PER_SECOND = 1000 * NANOS_PER_MILLI
NANOS_PER_MINUTE = 60 * NANOS_PER_SECOND
NANOS_PER_HOUR = 60 * NANOS_PER_MINUTE


class TestDataAPIDuration:
    @pytest.mark.describe("test of duration type, errors in parsing from string")
    def test_dataapiduration_parse_errors(self) -> None:
        # baselines
        DataAPIDuration.from_string("P1Y")
        DataAPIDuration.from_string("P3W")
        # spurious begin
        with pytest.raises(ValueError):
            DataAPIDuration.from_string("?P1Y")
        # spurious end
        with pytest.raises(ValueError):
            DataAPIDuration.from_string("P1YR")
        # spurious in the middle
        with pytest.raises(ValueError):
            DataAPIDuration.from_string("P1YG1M")
        # fake unit
        with pytest.raises(ValueError):
            DataAPIDuration.from_string("P1Y1B")
        # repeated unit
        with pytest.raises(ValueError):
            DataAPIDuration.from_string("P1Y2Y1M")
        # out-of-order unit
        with pytest.raises(ValueError):
            DataAPIDuration.from_string("P1Y1M4Y")
        # quantity with '+'
        with pytest.raises(ValueError):
            DataAPIDuration.from_string("P1Y+1M")
        # signs in the middle
        with pytest.raises(ValueError):
            DataAPIDuration.from_string("P1Y-1M")
        with pytest.raises(ValueError):
            DataAPIDuration.from_string("-P1Y-1M")

        # W-format with other things pre-T
        with pytest.raises(ValueError):
            DataAPIDuration.from_string("P3W1D")
        # W-format with other things post-T
        with pytest.raises(ValueError):
            DataAPIDuration.from_string("P3WT12H")
        # W-format with trailing T
        with pytest.raises(ValueError):
            DataAPIDuration.from_string("P3WT")

        # repeated unit, subday part
        with pytest.raises(ValueError):
            DataAPIDuration.from_string("P1Y1MT1H1M4M1.123S")
        # out-of-order unit, subday part
        with pytest.raises(ValueError):
            DataAPIDuration.from_string("P1Y1MT1H1M1.123S3H")
        # quantity with '+', subday part
        with pytest.raises(ValueError):
            DataAPIDuration.from_string("P1Y1MT1H1M+1.123S")
        # signs in the middle, subday part
        with pytest.raises(ValueError):
            DataAPIDuration.from_string("P1Y1MT1H-1M1.123S")
        with pytest.raises(ValueError):
            DataAPIDuration.from_string("-P1Y1MT1H-1M1.123S")

    @pytest.mark.describe("test of duration type, null-duration forms parsing")
    def test_dataapiduration_parse_nulldurations(self) -> None:
        zerod = DataAPIDuration(
            signum=+1,
            months=0,
            days=0,
            nanoseconds=0,
        )
        assert zerod == DataAPIDuration.from_string("P")
        assert zerod == DataAPIDuration.from_string("PT")
        assert zerod == DataAPIDuration.from_string("-P")
        assert zerod == DataAPIDuration.from_string("-PT")

    @pytest.mark.describe("test of duration type, lifecycle")
    def test_dataapiduration_lifecycle(self) -> None:
        # base
        dfull = DataAPIDuration.from_string("P1Y1M1DT1H1M1.001001001S")
        dfull_exp = DataAPIDuration(
            signum=+1, months=13, days=1, nanoseconds=3661001001001
        )
        assert dfull == dfull_exp
        # holey cases
        dhole1 = DataAPIDuration.from_string("P1M1DT1M")
        dhole1_exp = DataAPIDuration(
            signum=+1, months=1, days=1, nanoseconds=60000000000
        )
        assert dhole1 == dhole1_exp
        dhole2 = DataAPIDuration.from_string("-P1YT1H1.000000123S")
        dhole2_exp = DataAPIDuration(
            signum=-1, months=12, days=0, nanoseconds=3601000000123
        )
        assert dhole2 == dhole2_exp
        assert DataAPIDuration.from_string("PT1M1S") == DataAPIDuration.from_string(
            "PT61S"
        )
        # equivalent-formulation identity
        deq1 = DataAPIDuration.from_string("P12MT60M")
        deq2 = DataAPIDuration.from_string("P1YT1H")
        assert deq1 == deq2
        # null duration
        zd0 = DataAPIDuration(signum=+1, months=0, days=0, nanoseconds=0)
        zd1 = DataAPIDuration.from_string("P0Y")
        zd2 = DataAPIDuration.from_string("P0M0D")
        zd3 = DataAPIDuration.from_string("-P0DT0S")
        assert zd0 == zd1
        assert zd0 == zd2
        assert zd0 == zd3
        # stringy functions
        dfull.to_string()
        repr(dfull)
        zd0.to_string()
        repr(zd0)
        # trailing 'T' test
        one_y_0 = DataAPIDuration.from_string("P1YT")
        one_y_t = DataAPIDuration.from_string("P1Y")
        m_one_y_0 = DataAPIDuration.from_string("-P1YT")
        m_one_y_t = DataAPIDuration.from_string("-P1Y")
        assert one_y_0 == one_y_t
        assert m_one_y_0 == m_one_y_t

    @pytest.mark.describe("test of duration type, errors in c-parsing from string")
    def test_dataapiduration_c_parse_errors(self) -> None:
        # baseline
        DataAPIDuration.from_c_string("1mo")
        # spurious begin
        with pytest.raises(ValueError):
            DataAPIDuration.from_c_string("X1mo")
        # spurious end
        with pytest.raises(ValueError):
            DataAPIDuration.from_c_string("1moY")
        # spurious in the middle
        with pytest.raises(ValueError):
            DataAPIDuration.from_c_string("1moX1s")
        # fake unit
        with pytest.raises(ValueError):
            DataAPIDuration.from_c_string("1mo6b1s")
        # repeated unit
        with pytest.raises(ValueError):
            DataAPIDuration.from_c_string("1mo1d1d1s")
        # out-of-order unit
        with pytest.raises(ValueError):
            DataAPIDuration.from_c_string("1d1mo1s")
        # quantity with '+'
        with pytest.raises(ValueError):
            DataAPIDuration.from_c_string("1mo+3s")
        # signs in the middle
        with pytest.raises(ValueError):
            DataAPIDuration.from_c_string("-1d-1s")
        with pytest.raises(ValueError):
            DataAPIDuration.from_c_string("1d-1s")

    @pytest.mark.describe("test of duration type, c-lifecycle")
    def test_dataapiduration_c_lifecycle(self) -> None:
        # base
        dfull = DataAPIDuration.from_c_string("1y1mo1w1d1h1m1s1ms1us1ns")
        dfull_exp = DataAPIDuration(
            signum=+1, months=13, days=8, nanoseconds=3661001001001
        )
        assert dfull == dfull_exp
        # mu, signed quantity and case-insensitivity
        assert DataAPIDuration.from_c_string("-123US") == DataAPIDuration.from_c_string(
            "-123µs"
        )
        # holey cases
        dhole1 = DataAPIDuration.from_c_string("1mo1d1m1ms1ns")
        dhole1_exp = DataAPIDuration(
            signum=+1, months=1, days=1, nanoseconds=60001000001
        )
        assert dhole1 == dhole1_exp
        dhole2 = DataAPIDuration.from_c_string("-1y1w1h1s1us")
        dhole2_exp = DataAPIDuration(
            signum=-1, months=12, days=7, nanoseconds=3601000001000
        )
        assert dhole2 == dhole2_exp
        assert DataAPIDuration.from_c_string("1m1s") == DataAPIDuration.from_c_string(
            "61s"
        )
        # equivalent-formulation identity
        deq1 = DataAPIDuration.from_c_string("13mo2w1h1s1us")
        deq2 = DataAPIDuration.from_c_string("1y1mo14d60m1000ms1000ns")
        assert deq1 == deq2
        # null duration
        zd0 = DataAPIDuration(signum=+1, months=0, days=0, nanoseconds=0)
        zd1 = DataAPIDuration.from_c_string("0y")
        zd2 = DataAPIDuration.from_c_string("0mo0d")
        zd3 = DataAPIDuration.from_c_string("0w0ns")
        zd4 = DataAPIDuration.from_c_string("")
        assert zd0 == zd1
        assert zd0 == zd2
        assert zd0 == zd3
        assert zd0 == zd4
        # stringy functions
        dfull.to_c_string()
        repr(dfull)
        zd0.to_c_string()
        repr(zd0)

    @pytest.mark.describe("test of duration type, c-parsing and parsing equivalence")
    def test_dataapiduration_c_parsing_parsing_equivalence(self) -> None:
        c_td_ok_0 = DataAPIDuration.from_c_string("")
        c_td_ok_1 = DataAPIDuration.from_c_string("-1h1s333ms")
        c_td_ok_2 = DataAPIDuration.from_c_string("-191h1s")
        c_td_ok_3 = DataAPIDuration.from_c_string("1h44m3s777000ns")
        c_td_no = DataAPIDuration.from_c_string("-1y1s")

        td_ok_0 = DataAPIDuration.from_string("PT0S")
        td_ok_1 = DataAPIDuration.from_string("-PT1H1.333S")
        td_ok_2 = DataAPIDuration.from_string("-PT191H1S")
        td_ok_3 = DataAPIDuration.from_string("PT1H44M3.000777S")
        td_no = DataAPIDuration.from_string("-P1YT1S")

        assert c_td_ok_0 == td_ok_0
        assert c_td_ok_1 == td_ok_1
        assert c_td_ok_2 == td_ok_2
        assert c_td_ok_3 == td_ok_3
        assert c_td_no == td_no

    @pytest.mark.describe("test of duration type, timedelta conversions")
    def test_dataapiduration_timedelta_conversions(self) -> None:
        td_ok_0 = DataAPIDuration.from_string("PT0S")
        td_ok_1 = DataAPIDuration.from_string("-PT1H1.333S")
        td_ok_2 = DataAPIDuration.from_string("-PT191H1S")
        td_ok_3 = DataAPIDuration.from_string("PT1H44M3.000777S")
        td_no = DataAPIDuration.from_string("-P1YT1S")

        # due to lossy conversions and the month-day-subday math being different,
        # other (more challenging) values will fail here (limitations of timedelta)
        assert DataAPIDuration.from_timedelta(td_ok_0.to_timedelta()) == td_ok_0
        assert DataAPIDuration.from_timedelta(td_ok_1.to_timedelta()) == td_ok_1
        assert DataAPIDuration.from_timedelta(td_ok_2.to_timedelta()) == td_ok_2
        assert DataAPIDuration.from_timedelta(td_ok_3.to_timedelta()) == td_ok_3
        with pytest.raises(ValueError):
            td_no.to_timedelta()

        tdelta_0 = datetime.timedelta()
        tdelta_1 = datetime.timedelta(days=1, seconds=2, microseconds=345678)
        tdelta_2 = datetime.timedelta(seconds=10, milliseconds=-456)
        assert DataAPIDuration.from_timedelta(tdelta_0).to_timedelta() == tdelta_0
        assert DataAPIDuration.from_timedelta(tdelta_1).to_timedelta() == tdelta_1
        assert DataAPIDuration.from_timedelta(tdelta_2).to_timedelta() == tdelta_2

    @pytest.mark.describe("test of duration parsing, lifted from Cassandra codebase")
    def test_dataapiduration_parsing_cassandra_testcases(self) -> None:
        assert DataAPIDuration(+1, 12, 2, 0) == DataAPIDuration.from_string("P1Y2D")
        assert DataAPIDuration(+1, 14, 0, 0) == DataAPIDuration.from_string("P1Y2M")
        assert DataAPIDuration(+1, 0, 14, 0) == DataAPIDuration.from_string("P2W")
        assert DataAPIDuration(
            +1, 12, 0, 2 * NANOS_PER_HOUR
        ) == DataAPIDuration.from_string("P1YT2H")
        assert DataAPIDuration(-1, 14, 0, 0) == DataAPIDuration.from_string("-P1Y2M")
        assert DataAPIDuration(+1, 0, 2, 0) == DataAPIDuration.from_string("P2D")
        assert DataAPIDuration(
            +1, 0, 0, 30 * NANOS_PER_HOUR
        ) == DataAPIDuration.from_string("PT30H")
        assert DataAPIDuration(
            +1, 0, 0, 30 * NANOS_PER_HOUR + 20 * NANOS_PER_MINUTE
        ) == DataAPIDuration.from_string("PT30H20M")
        assert DataAPIDuration(
            +1, 0, 0, 20 * NANOS_PER_MINUTE
        ) == DataAPIDuration.from_string("PT20M")
        assert DataAPIDuration(
            +1, 0, 0, 56 * NANOS_PER_SECOND
        ) == DataAPIDuration.from_string("PT56S")
        assert DataAPIDuration(
            +1, 15, 0, 130 * NANOS_PER_MINUTE
        ) == DataAPIDuration.from_string("P1Y3MT2H10M")

    @pytest.mark.describe("test of duration c-parsing, lifted from Cassandra codebase")
    def test_dataapiduration_c_parsing_cassandra_testcases(self) -> None:
        assert DataAPIDuration(+1, 14, 0, 0) == DataAPIDuration.from_c_string("1y2mo")
        assert DataAPIDuration(-1, 14, 0, 0) == DataAPIDuration.from_c_string("-1y2mo")
        assert DataAPIDuration(+1, 14, 0, 0) == DataAPIDuration.from_c_string("1Y2MO")
        assert DataAPIDuration(+1, 0, 14, 0) == DataAPIDuration.from_c_string("2w")
        assert DataAPIDuration(
            +1, 0, 2, 10 * NANOS_PER_HOUR
        ) == DataAPIDuration.from_c_string("2d10h")
        assert DataAPIDuration(+1, 0, 2, 0) == DataAPIDuration.from_c_string("2d")
        assert DataAPIDuration(
            +1, 0, 0, 30 * NANOS_PER_HOUR
        ) == DataAPIDuration.from_c_string("30h")
        assert DataAPIDuration(
            +1, 0, 0, 30 * NANOS_PER_HOUR + 20 * NANOS_PER_MINUTE
        ) == DataAPIDuration.from_c_string("30h20m")
        assert DataAPIDuration(
            +1, 0, 0, 20 * NANOS_PER_MINUTE
        ) == DataAPIDuration.from_c_string("20m")
        assert DataAPIDuration(
            +1, 0, 0, 56 * NANOS_PER_SECOND
        ) == DataAPIDuration.from_c_string("56s")
        assert DataAPIDuration(
            +1, 0, 0, 567 * NANOS_PER_MILLI
        ) == DataAPIDuration.from_c_string("567ms")
        assert DataAPIDuration(
            +1, 0, 0, 1950 * NANOS_PER_MICRO
        ) == DataAPIDuration.from_c_string("1950us")
        assert DataAPIDuration(
            +1, 0, 0, 1950 * NANOS_PER_MICRO
        ) == DataAPIDuration.from_c_string("1950µs")
        assert DataAPIDuration(+1, 0, 0, 1950000) == DataAPIDuration.from_c_string(
            "1950000ns"
        )
        assert DataAPIDuration(+1, 0, 0, 1950000) == DataAPIDuration.from_c_string(
            "1950000NS"
        )
        assert DataAPIDuration(-1, 0, 0, 1950000) == DataAPIDuration.from_c_string(
            "-1950000ns"
        )
        assert DataAPIDuration(
            +1, 15, 0, 130 * NANOS_PER_MINUTE
        ) == DataAPIDuration.from_c_string("1y3mo2h10m")
