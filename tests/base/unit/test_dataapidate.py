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

from astrapy.data_types import DataAPIDate


class TestDataAPIDate:
    @pytest.mark.describe("test of date type, errors in parsing from string")
    def test_dataapidate_parse_errors(self) -> None:
        # empty, faulty, misformatted
        with pytest.raises(ValueError):
            DataAPIDate.from_string("")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("boom")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("1-1-1-1")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("1-1")

        # signs for various lengths of the 'year digits' portion
        # valid cases
        DataAPIDate.from_string("0000-10-29")
        DataAPIDate.from_string("-2024-10-29")
        DataAPIDate.from_string("+112024-10-29")
        DataAPIDate.from_string("+000024-10-29")
        DataAPIDate.from_string("-112024-10-29")
        DataAPIDate.from_string("0124-10-29")
        DataAPIDate.from_string("-0124-10-29")
        # incorrect cases for sign of year
        with pytest.raises(ValueError):
            DataAPIDate.from_string("+0000-10-29")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("-0000-10-29")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("+124-10-29")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("+2024-10-29")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("+-2024-10-29")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("-+2024-10-29")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("124-10-29")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("+124-10-29")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("-124-10-29")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("123456-10-29")

        with pytest.raises(ValueError):
            DataAPIDate.from_string("+1999-1-1")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("2024-10-10X")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("X2024-10-10")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("X2024-X10-10")

        with pytest.raises(ValueError):
            DataAPIDate.from_string("2024-0-1")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("2024-1-0")

        with pytest.raises(ValueError):
            DataAPIDate.from_string("2024-13-20")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("2024-04-31")

        DataAPIDate.from_string("2024-01-31")
        DataAPIDate.from_string("2023-02-28")
        DataAPIDate.from_string("2024-02-29")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("2023-02-29")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("2024-02-30")

        DataAPIDate.from_string("2024-03-31")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("2024-03-32")
        DataAPIDate.from_string("2024-04-30")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("2024-04-31")
        DataAPIDate.from_string("2024-05-31")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("2024-05-32")
        DataAPIDate.from_string("2024-06-30")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("2024-06-31")
        DataAPIDate.from_string("2024-07-31")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("2024-07-32")
        DataAPIDate.from_string("2024-08-31")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("2024-08-32")
        DataAPIDate.from_string("2024-09-30")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("2024-09-31")
        DataAPIDate.from_string("2024-10-31")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("2024-10-32")
        DataAPIDate.from_string("2024-11-30")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("2024-11-31")
        DataAPIDate.from_string("2024-12-31")
        with pytest.raises(ValueError):
            DataAPIDate.from_string("2024-12-32")

    @pytest.mark.describe(
        "test of DataAPIData sign-of-year parse and stringify consistency"
    )
    def test_dataapidate_sign_of_year_consistency(self) -> None:
        def parse_and_back_year_str(year_str: str) -> None:
            # just testing that years come out as they get in with numdigits, sign
            dt_string = f"{year_str}-01-12"
            dt = DataAPIDate.from_string(dt_string)
            dt_string_1 = dt.to_string()
            assert dt_string == dt_string_1

        parse_and_back_year_str("2024")
        parse_and_back_year_str("0000")
        parse_and_back_year_str("-2024")
        parse_and_back_year_str("+112024")
        parse_and_back_year_str("-112024")
        parse_and_back_year_str("0124")
        parse_and_back_year_str("-0124")
        parse_and_back_year_str("-123456")
        parse_and_back_year_str("1972")

    @pytest.mark.describe("test of date type, lifecycle")
    def test_dataapidate_lifecycle(self) -> None:
        dfull = DataAPIDate.from_string("1995-02-09")
        dfull_exp = DataAPIDate(year=1995, month=2, day=9)
        assert dfull == dfull_exp
        assert dfull == DataAPIDate.from_string("1995-2-9")
        assert dfull == DataAPIDate.from_string("+00001995-0002-0009")

        py_dfull = datetime.date(1995, 2, 9)
        assert DataAPIDate.from_date(py_dfull) == dfull
        assert dfull.to_date() == py_dfull

        DataAPIDate.from_string("-55000-03-30")
        DataAPIDate.from_string("+1234567-12-31")

        repr(dfull)
        str(dfull)
        dfull.to_string()

        end_pleistocene = DataAPIDate.from_string("-11700-12-13")
        end_pleistocene.to_string()
        with pytest.raises(ValueError):
            end_pleistocene.to_date()

        far_future = DataAPIDate.from_string("+252525-01-24")
        far_future.to_string()
        with pytest.raises(ValueError):
            far_future.to_date()

        assert far_future > end_pleistocene
        assert far_future >= end_pleistocene
        assert end_pleistocene < far_future
        assert end_pleistocene <= far_future

        modernity = datetime.date(2024, 10, 28)
        assert far_future > modernity
        assert far_future >= modernity
        assert modernity < far_future
        assert modernity <= far_future

        assert end_pleistocene < modernity
        assert end_pleistocene <= modernity
        assert modernity > end_pleistocene
        assert modernity >= end_pleistocene
