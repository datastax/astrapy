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

from astrapy.data_types import TableTime


class TestTableTime:
    @pytest.mark.describe("test of time type, errors in parsing from string")
    def test_tabletime_parse_errors(self) -> None:
        # empty, faulty, misformatted
        with pytest.raises(ValueError):
            TableTime.from_string("")
        with pytest.raises(ValueError):
            TableTime.from_string("boom")

        with pytest.raises(ValueError):
            TableTime.from_string("12:34:56:21")
        with pytest.raises(ValueError):
            TableTime.from_string("12:23")
        with pytest.raises(ValueError):
            TableTime.from_string("+12:34:56")
        with pytest.raises(ValueError):
            TableTime.from_string("12:+34:56")
        with pytest.raises(ValueError):
            TableTime.from_string("12:34:56.")
        with pytest.raises(ValueError):
            TableTime.from_string("12:34:56X")
        with pytest.raises(ValueError):
            TableTime.from_string("X12:34:56")
        with pytest.raises(ValueError):
            TableTime.from_string("12:34X:56")

        with pytest.raises(ValueError):
            TableTime.from_string("24:00:00")
        with pytest.raises(ValueError):
            TableTime.from_string("00:60:00")
        with pytest.raises(ValueError):
            TableTime.from_string("00:00:60")

        TableTime.from_string("00:00:00")
        TableTime.from_string("23:00:00")
        TableTime.from_string("00:59:00")
        TableTime.from_string("00:00:00.123456789")
        TableTime.from_string("00:00:00.123")
        TableTime.from_string("01:02:03.123456789")
        TableTime.from_string("01:02:03.123")

    @pytest.mark.describe("test of time type, lifecycle")
    def test_tabletime_lifecycle(self) -> None:
        tint = TableTime.from_string("02:03:04")
        tint_exp = TableTime(hour=2, minute=3, second=4)
        assert tint == tint_exp
        assert tint == TableTime.from_string("2:3:4")
        assert tint == TableTime.from_string("0002:003:000004")
        py_tint = datetime.time(2, 3, 4)
        assert TableTime.from_time(py_tint) == tint
        assert tint.to_time() == py_tint
        repr(tint)
        str(tint)
        tint.to_string()

        tfra = TableTime.from_string("02:03:04.9876")
        tfra_exp = TableTime(hour=2, minute=3, second=4, nanosecond=987600000)
        assert tfra == tfra_exp
        assert tfra == TableTime.from_string("2:3:4.9876")
        assert tfra == TableTime.from_string("0002:003:000004.987600000")
        py_tfra = datetime.time(2, 3, 4, 987600)
        assert TableTime.from_time(py_tfra) == tfra
        assert tfra.to_time() == py_tfra
        repr(tfra)
        str(tfra)
        tfra.to_string()

        tfra1 = TableTime(1, 2, 3, 123)
        tfra1p = TableTime(1, 2, 3, 12)
        tfra2p = TableTime(1, 2, 3, 12345)
        tfra2 = TableTime(1, 2, 3, 123456)
        tfra3p = TableTime(1, 2, 3, 12345678)
        tfra3 = TableTime(1, 2, 3, 123456789)
        assert TableTime.from_string(tfra1.to_string()) == tfra1
        assert TableTime.from_string(tfra1p.to_string()) == tfra1p
        assert TableTime.from_string(tfra2p.to_string()) == tfra2p
        assert TableTime.from_string(tfra2.to_string()) == tfra2
        assert TableTime.from_string(tfra3p.to_string()) == tfra3p
        assert TableTime.from_string(tfra3.to_string()) == tfra3

        t1 = TableTime(1, 2, 3, 30000)
        t2 = TableTime(1, 2, 3, 45000)
        py_t1 = t1.to_time()
        py_t2 = t2.to_time()
        assert t1 < t2
        assert t1 <= t2
        assert t2 > t1
        assert t2 >= t1
        assert py_t1 < t2
        assert py_t1 <= t2
        assert py_t2 > t1
        assert py_t2 >= t1
        assert t1 < py_t2
        assert t1 <= py_t2
        assert t2 > py_t1
        assert t2 >= py_t1