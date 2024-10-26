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

from astrapy.data_types import APITimestamp


class TestAPITimestamp:
    @pytest.mark.describe("test of APITimestamp class")
    def test_apitimestamp_lifecycle(self) -> None:
        # in-range use (far enough from valid range for tz-dependent conversions)
        Y1_1_1_MILLIS = -62135596800000
        Y10K_12_31_MILLIS = 253402300800000
        ONE_DAY_AND_EPSILON_MILLIS = 86400000 + 555
        TEN_YEARS_MILLIS = 10 * 365 * 86400000
        ts_y00001 = APITimestamp(Y1_1_1_MILLIS + ONE_DAY_AND_EPSILON_MILLIS)
        ts_y00001_2 = APITimestamp(Y1_1_1_MILLIS + ONE_DAY_AND_EPSILON_MILLIS)
        ts_y10000 = APITimestamp(Y10K_12_31_MILLIS - ONE_DAY_AND_EPSILON_MILLIS)

        assert ts_y00001 == ts_y00001_2
        assert ts_y00001 != ts_y10000
        assert APITimestamp.from_datetime(ts_y00001.to_datetime()) == ts_y00001
        assert APITimestamp.from_datetime(ts_y10000.to_datetime()) == ts_y10000

        # (not: APITimestamp is not supposed to respect sub-millisecond precision.)
        dt1 = datetime.datetime(2024, 10, 27, 11, 22, 33, 831000)
        assert dt1 == APITimestamp.from_datetime(dt1).to_datetime()

        # out-of-ranges
        ts_y00009_bc = APITimestamp(Y1_1_1_MILLIS - TEN_YEARS_MILLIS)
        ts_y10010_ad = APITimestamp(Y10K_12_31_MILLIS + TEN_YEARS_MILLIS)
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
