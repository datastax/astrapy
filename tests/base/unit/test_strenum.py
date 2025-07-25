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


class MyTestEnum(StrEnum):
    VALUE = "value"
    VALUE_DASH = "value-dash"


class TestStrEnum:
    def test_strenum_contains(self) -> None:
        assert "value" in MyTestEnum
        assert "value_dash" in MyTestEnum
        assert "value-dash" in MyTestEnum
        assert "VALUE-DASH" in MyTestEnum
        assert "pippo" not in MyTestEnum
        assert {6: 12} not in MyTestEnum

    def test_strenum_coerce(self) -> None:
        MyTestEnum.coerce("value")
        MyTestEnum.coerce("value_dash")
        MyTestEnum.coerce("value-dash")
        MyTestEnum.coerce("VALUE-DASH")
        MyTestEnum.coerce(MyTestEnum.VALUE)
        with pytest.raises(ValueError):
            MyTestEnum.coerce("pippo")
