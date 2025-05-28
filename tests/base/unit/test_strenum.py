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

from astrapy.utils.str_enum import StrEnum


class TestEnum(StrEnum):
    VALUE = "value"
    VALUE_DASH = "value-dash"


class TestStrEnum:
    def test_strenum_contains(self) -> None:
        assert "value" in TestEnum
        assert "value_dash" in TestEnum
        assert "value-dash" in TestEnum
        assert "VALUE-DASH" in TestEnum
        assert "pippo" not in TestEnum
        assert {6: 12} not in TestEnum

    def test_strenum_coerce(self) -> None:
        TestEnum.coerce("value")
        TestEnum.coerce("value_dash")
        TestEnum.coerce("value-dash")
        TestEnum.coerce("VALUE-DASH")
        TestEnum.coerce(TestEnum.VALUE)
        with pytest.raises(ValueError):
            TestEnum.coerce("pippo")
