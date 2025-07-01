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
from dataclasses import dataclass
from typing import Any

from astrapy.data_types import (
    DataAPITimestamp,
    DataAPIUserDefinedType,
    create_dataclass_userdefinedtype,
)
from astrapy.info import CreateTypeDefinition

PLAYER_TYPE_DEFINITION = CreateTypeDefinition(
    fields={
        "name": "text",
        "age": "int",
    },
)


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


class PlayerExplicitDataAPIUDT(DataAPIUserDefinedType[Player]):
    """
    An example dataclass which may be used to represent a user-defined type (UDT)
    such as one would define, and create on the database, with this code:

    .. code-block:: python

        from astrapy.info import CreateTypeDefinition, ColumnType

        xplayer_udt_def = CreateTypeDefinition(fields={
            "name": ColumnType.TEXT,
            "age": ColumnType.INT,
            "blb": ColumnType.BLOB,
            "ts": ColumnType.TIMESTAMP,
        })

        database.create_type("xplayer_udt", definition=xplayer_udt_def)

    See the test functions using this resource for actual usage examples.
    """

    def as_dict(self) -> dict[str, Any]:
        return self.value.__dict__

    @classmethod
    def from_dict(
        cls: type[PlayerExplicitDataAPIUDT],
        raw_dict: dict[str, Any],
        *,
        definition: CreateTypeDefinition,
    ) -> PlayerExplicitDataAPIUDT:
        return PlayerExplicitDataAPIUDT(Player(**raw_dict))


@dataclass
class ExtendedPlayer:
    """
    An example dataclass which may be used to represent a user-defined type (UDT)
    such as one would define, and create on the database, with this code:

    .. code-block:: python

        from astrapy.info import CreateTypeDefinition, ColumnType

        xplayer_udt_def = CreateTypeDefinition(fields={
            "name": ColumnType.TEXT,
            "age": ColumnType.INT,
            "blb": ColumnType.BLOB,
            "ts": ColumnType.TIMESTAMP,
        })

        database.create_type("xplayer_udt", definition=xplayer_udt_def)

    See the test functions using this resource for actual usage examples.
    """

    name: str
    age: int
    blb: bytes
    ts: DataAPITimestamp | datetime.datetime


@dataclass
class NullablePlayer:
    """
    A counterpart of the Player model class (see), but whose each field
    can be omitted and defaults to None.
    """

    name: str | None = None
    age: int | None = None


PlayerUDTWrapper = create_dataclass_userdefinedtype(Player)
ExtendedPlayerUDTWrapper = create_dataclass_userdefinedtype(ExtendedPlayer)
NullablePlayerUDTWrapper = create_dataclass_userdefinedtype(NullablePlayer)
