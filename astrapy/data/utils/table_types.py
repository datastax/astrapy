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

from astrapy.utils.str_enum import StrEnum


class ColumnType(StrEnum):
    """
    Enum to describe the scalar column types for Tables.

    A 'scalar' type is a non-composite type: that means, no sets, lists, maps
    and other non-primitive data types.
    """

    ASCII = "ascii"
    BIGINT = "bigint"
    BLOB = "blob"
    BOOLEAN = "boolean"
    COUNTER = "counter"
    DATE = "date"
    DECIMAL = "decimal"
    DOUBLE = "double"
    DURATION = "duration"
    FLOAT = "float"
    INET = "inet"
    INT = "int"
    SMALLINT = "smallint"
    TEXT = "text"
    TIME = "time"
    TIMESTAMP = "timestamp"
    TIMEUUID = "timeuuid"
    TINYINT = "tinyint"
    UUID = "uuid"
    VARINT = "varint"


class TableValuedColumnType(StrEnum):
    """
    An enum to describe the types of column with "values".
    """

    LIST = "list"
    SET = "set"


class TableKeyValuedColumnType(StrEnum):
    """
    An enum to describe the types of column with "keys and values".
    """

    MAP = "map"


class TableVectorColumnType(StrEnum):
    """
    An enum to describe the types of 'vector-like' column.
    """

    VECTOR = "vector"


class TableUnsupportedColumnType(StrEnum):
    """
    An enum to describe the types of column falling into the 'unsupported' group.
    """

    UNSUPPORTED = "UNSUPPORTED"
