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

from typing import Any, Iterable

import pytest

from astrapy.data_types import DataAPIDictUDT


class TestDataAPIDictUDT:
    @pytest.mark.describe("test of DataAPIDictUDT instance checks")
    def test_dataapidictudt_instancechecks(self) -> None:
        test_dict = {"name": "John", "age": 40}
        test_udt = DataAPIDictUDT(test_dict)

        # equality
        assert test_dict == test_udt
        assert test_udt == test_dict

        # instance checks
        assert isinstance(test_udt, dict)
        assert isinstance(test_udt, DataAPIDictUDT)
        assert not isinstance(test_dict, DataAPIDictUDT)

        # construction methods
        test_udt_2 = DataAPIDictUDT(name="John", age=40)
        test_udt_3 = DataAPIDictUDT([("name", "John"), ("age", 40)])
        assert test_udt_2 == test_udt
        assert test_udt_3 == test_udt

        # dict behaviour
        def _t_eq(ite1: Iterable[Any], ite2: Iterable[Any]) -> bool:
            lst1 = list(ite1)
            lst2 = list(ite2)
            return len(lst1) == len(lst2) and all(itm1 in lst2 for itm1 in lst1)

        assert "name" in test_udt
        assert "xyz" not in test_udt
        assert _t_eq(test_udt.items(), test_dict.items())
        assert _t_eq(test_udt.keys(), test_dict.keys())
        assert _t_eq(test_udt.values(), test_dict.values())
        assert len(test_udt) == len(test_dict)
