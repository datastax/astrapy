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

from dataclasses import dataclass
from typing import Any

import pytest

from astrapy.data_types import (
    DataAPIUserDefinedType,
    DictDataAPIUserDefinedType,
    create_dataclass_userdefinedtype,
)
from astrapy.info import CreateTypeDefinition


@dataclass
class Player:
    """
    An example dataclass which may be used to represent a user-defined type (UDT)
    such as one would define, and create on the database, with this code:

    .. code-block:: python

        from astrapy.info import CreateTypeDefinition, ColumnType

        player_udt_def = CreateTypeDefinition(fields={
            "name": ColumnType.TEXT,
            "age": ColumnType.INT,
        })

        database.create_type("player_udt", definition=player_udt_def)
    """

    name: str
    age: int


class PlayerDataAPIUDT(DataAPIUserDefinedType[Player]):
    """
    A concrete Data API type wrapper for the `Player` dataclass
    (defined in this module), exemplifying the usage for writes and reads through
    the Data API.

    See the test functions in this module for actual usage examples.
    """

    def as_dict(self) -> dict[str, Any]:
        return self.value.__dict__

    @classmethod
    def from_dict(
        cls: type[PlayerDataAPIUDT],
        raw_dict: dict[str, Any],
        *,
        definition: CreateTypeDefinition,
    ) -> PlayerDataAPIUDT:
        return PlayerDataAPIUDT(Player(**raw_dict))


class TestDataAPIUserDefinedType:
    @pytest.mark.describe("test of dict-backed data api UDT")
    def test_dict_dataapiudt(self) -> None:
        test_dict = {"name": "John", "age": 40}

        wrapped = DictDataAPIUserDefinedType(test_dict)

        assert wrapped.value == test_dict
        assert wrapped.as_dict() == test_dict

        # TODO: definition?
        wrapped2 = DictDataAPIUserDefinedType.from_dict(
            test_dict, definition=CreateTypeDefinition(fields={})
        )
        assert wrapped == wrapped2

    @pytest.mark.describe("test of dataclass-backed data api UDT")
    def test_dataclass_dataapiudt(self) -> None:
        test_dict = {"name": "John", "age": 40}
        test_player = Player(name="John", age=40)

        wrapped = PlayerDataAPIUDT(test_player)

        assert wrapped.value == test_player
        assert wrapped.as_dict() == test_dict

        # TODO: definition?
        wrapped2 = PlayerDataAPIUDT.from_dict(
            test_dict, definition=CreateTypeDefinition(fields={})
        )
        assert wrapped == wrapped2

    @pytest.mark.describe("test of dataclass-backed data api UDT factory")
    def test_dataclassfactory_dataapiudt(self) -> None:
        test_dict = {"name": "John", "age": 40}
        test_player = Player(name="John", age=40)

        PlayerWrapper = create_dataclass_userdefinedtype(Player)

        wrapped = PlayerWrapper(test_player)

        assert wrapped.value == test_player
        assert wrapped.as_dict() == test_dict

        # TODO: definition?
        wrapped2 = PlayerWrapper.from_dict(
            test_dict, definition=CreateTypeDefinition(fields={})
        )
        assert wrapped == wrapped2
