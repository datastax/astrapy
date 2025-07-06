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

from astrapy.data.utils.extended_json_converters import convert_to_ejson_bytes
from astrapy.data_types import DataAPITimestamp
from astrapy.info import CreateTableDefinition, CreateTypeDefinition

PLAYER_TYPE_NAME = "udt_player"
PLAYER_TYPE_DEFINITION = CreateTypeDefinition(
    fields={
        "name": "text",
        "age": "int",
    },
)
PLAYER_TABLE_NAME = "test_table_udt_player"
PLAYER_TABLE_DEFINITION = CreateTableDefinition.coerce(
    {
        "columns": {
            "id": "text",
            "scalar_udt": {"type": "userDefined", "udtName": PLAYER_TYPE_NAME},
            "list_udt": {
                "type": "list",
                "valueType": {
                    "type": "userDefined",
                    "udtName": PLAYER_TYPE_NAME,
                },
            },
            "set_udt": {
                "type": "set",
                "valueType": {
                    "type": "userDefined",
                    "udtName": PLAYER_TYPE_NAME,
                },
            },
            "map_udt": {
                "type": "map",
                "keyType": "text",
                "valueType": {
                    "type": "userDefined",
                    "udtName": PLAYER_TYPE_NAME,
                },
            },
        },
        "primaryKey": {
            "partitionBy": ["id"],
            "partitionSort": {},
        },
    }
)

EXTENDED_PLAYER_TYPE_NAME = "udt_extended_player"
EXTENDED_PLAYER_TYPE_DEFINITION = CreateTypeDefinition(
    fields={
        "name": "text",
        "age": "int",
        "ts": "timestamp",
    },
)
#        TODO NOBLOBINUDT "blb": "blob",
EXTENDED_PLAYER_TABLE_NAME = "test_table_udt_extended_player"
EXTENDED_PLAYER_TABLE_DEFINITION = CreateTableDefinition.coerce(
    {
        "columns": {
            "id": "text",
            "scalar_udt": {"type": "userDefined", "udtName": EXTENDED_PLAYER_TYPE_NAME},
            "list_udt": {
                "type": "list",
                "valueType": {
                    "type": "userDefined",
                    "udtName": EXTENDED_PLAYER_TYPE_NAME,
                },
            },
            "set_udt": {
                "type": "set",
                "valueType": {
                    "type": "userDefined",
                    "udtName": EXTENDED_PLAYER_TYPE_NAME,
                },
            },
            "map_udt": {
                "type": "map",
                "keyType": "text",
                "valueType": {
                    "type": "userDefined",
                    "udtName": EXTENDED_PLAYER_TYPE_NAME,
                },
            },
        },
        "primaryKey": {
            "partitionBy": ["id"],
            "partitionSort": {},
        },
    }
)

UNIT_EXTENDED_PLAYER_TYPE_NAME = "unit_udt_extended_player"
UNIT_EXTENDED_PLAYER_TYPE_DEFINITION = CreateTypeDefinition(
    fields={
        "name": "text",
        "age": "int",
        "blb": "blob",
        "ts": "timestamp",
    },
)

THE_BYTES = b"\xa6"
THE_SERIALIZED_BYTES = convert_to_ejson_bytes(THE_BYTES)
THE_TIMESTAMP = DataAPITimestamp.from_string("2025-10-29T01:25:37.123Z")
THE_SERIALIZED_TIMESTAMP = THE_TIMESTAMP.to_string()
THE_TIMEZONE = datetime.timezone(datetime.timedelta(hours=2, minutes=45))
THE_DATETIME = THE_TIMESTAMP.to_datetime(tz=THE_TIMEZONE)


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
    # TODO NOBLOBINUDT blb: bytes
    ts: DataAPITimestamp | datetime.datetime


# TODO NOBLOBINUDT reabsorb this (+usage in Unit Tests) once reinstating blb above
@dataclass
class UnitExtendedPlayer:
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


def _unit_extended_player_serializer(uexp: UnitExtendedPlayer) -> dict[str, Any]:
    return {k: v for k, v in uexp.__dict__.items() if v is not None}


def _unit_extended_player_from_dict(
    udict: dict[str, Any],
    definition: CreateTypeDefinition | None,
) -> UnitExtendedPlayer:
    return UnitExtendedPlayer(**udict)


def _nullable_player_serializer(np: NullablePlayer) -> dict[str, Any]:
    return {k: v for k, v in np.__dict__.items() if v is not None}
