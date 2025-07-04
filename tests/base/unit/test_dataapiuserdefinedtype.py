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

from astrapy.data_types import DataAPIUDT

from ..table_udt_assets import (
    PLAYER_TYPE_DEFINITION,
    NullablePlayer,
    NullablePlayerUDTWrapper,
    Player,
    PlayerExplicitDataAPIUDT,
    PlayerUDTWrapper,
)


class TestDataAPIUserDefinedType:
    @pytest.mark.describe("test of dict-backed data api UDT")
    def test_dict_dataapiudt(self) -> None:
        test_dict = {"name": "John", "age": 40}

        wrapped = DataAPIUDT(test_dict)

        assert wrapped.value == test_dict
        assert wrapped.as_dict() == test_dict

        wrapped2 = DataAPIUDT.from_dict(test_dict, definition=PLAYER_TYPE_DEFINITION)
        assert wrapped == wrapped2

    @pytest.mark.describe("test of dataclass-backed data api UDT")
    def test_dataclass_dataapiudt(self) -> None:
        test_dict = {"name": "John", "age": 40}
        test_player = Player(name="John", age=40)

        wrapped = PlayerExplicitDataAPIUDT(test_player)

        assert wrapped.value == test_player
        assert wrapped.as_dict() == test_dict

        wrapped2 = PlayerExplicitDataAPIUDT.from_dict(
            test_dict, definition=PLAYER_TYPE_DEFINITION
        )
        assert wrapped == wrapped2

    @pytest.mark.describe("test of dataclass-backed data api UDT factory")
    def test_dataclassfactory_dataapiudt(self) -> None:
        test_dict = {"name": "John", "age": 40}
        test_player = Player(name="John", age=40)

        wrapped = PlayerUDTWrapper(test_player)

        assert wrapped.value == test_player
        assert wrapped.as_dict() == test_dict

        wrapped2 = PlayerUDTWrapper.from_dict(
            test_dict, definition=PLAYER_TYPE_DEFINITION
        )
        assert wrapped == wrapped2

    @pytest.mark.describe("test of nullable dataclass-backed data api UDT factory")
    def test_nullabledataclassfactory_dataapiudt(self) -> None:
        test_dict = {"name": "John"}
        test_player = NullablePlayer(name="John")

        wrapped = NullablePlayerUDTWrapper(test_player)

        assert wrapped.value == test_player
        assert wrapped.as_dict() == test_dict

        wrapped2 = NullablePlayerUDTWrapper.from_dict(
            test_dict, definition=PLAYER_TYPE_DEFINITION
        )
        assert wrapped == wrapped2
