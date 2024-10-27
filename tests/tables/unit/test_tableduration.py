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

import pytest

from astrapy.data_types import TableDuration


class TestTableDuration:
    @pytest.mark.describe("test of duration type, errors in parsing from string")
    def test_tableduration_parse_errors(self) -> None:
        # empty
        with pytest.raises(ValueError):
            TableDuration.from_string("")
        # spurious begin
        with pytest.raises(ValueError):
            TableDuration.from_string("X1mo")
        # spurious end
        with pytest.raises(ValueError):
            TableDuration.from_string("1moY")
        # spurious in the middle
        with pytest.raises(ValueError):
            TableDuration.from_string("1moX1s")
        # fake unit
        with pytest.raises(ValueError):
            TableDuration.from_string("1mo6b1s")
        # repeated unit
        with pytest.raises(ValueError):
            TableDuration.from_string("1mo1d1d1s")
        # out-of-order unit
        with pytest.raises(ValueError):
            TableDuration.from_string("1d1mo1s")
        # quantity with '-'
        with pytest.raises(ValueError):
            TableDuration.from_string("1mo-3s")
        # quantity with '+'
        with pytest.raises(ValueError):
            TableDuration.from_string("1mo+3s")

    @pytest.mark.describe("test of duration type, lifecycle")
    def test_tableduration_lifecycle(self) -> None:
        # base
        dfull = TableDuration.from_string("1y1mo1w1d1h1m1s1ms1us1ns")
        dfull_exp = TableDuration(months=13, days=8, nanoseconds=3661001001001)
        assert dfull == dfull_exp
        # mu and case-insensitivity
        assert TableDuration.from_string("123US") == TableDuration.from_string("123µs")
        # holey cases
        dhole1 = TableDuration.from_string("1mo1d1m1ms1ns")
        dhole1_exp = TableDuration(months=1, days=1, nanoseconds=60001000001)
        assert dhole1 == dhole1_exp
        dhole2 = TableDuration.from_string("1y1w1h1s1us")
        dhole2_exp = TableDuration(months=12, days=7, nanoseconds=3601000001000)
        assert dhole2 == dhole2_exp
        # equivalent-formulation identity
        deq1 = TableDuration.from_string("13mo2w1h1s1us")
        deq2 = TableDuration.from_string("1y1mo14d60m1000ms1000ns")
        assert deq1 == deq2
        # null duration
        zd0 = TableDuration(months=0, days=0, nanoseconds=0)
        zd1 = TableDuration.from_string("0y")
        zd2 = TableDuration.from_string("0mo0d")
        zd3 = TableDuration.from_string("0w0ns")
        assert zd0 == zd1
        assert zd0 == zd2
        assert zd0 == zd3
        # stringy functions
        str(dfull)
        repr(dfull)
        str(zd0)
        repr(zd0)
