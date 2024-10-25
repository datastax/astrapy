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


class TableScalarColumnType(StrEnum):
    ASCII = "ascii"
    BIGINT = "bigint"
    BLOB = "blob"
    BOOLEAN = "boolean"
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
    # TIMEUUID = "timeuuid"  # TODO confirm gone?
    TINYINT = "tinyint"
    UUID = "uuid"
    # VARCHAR = "varchar" TODO confirm gone?
    VARINT = "varint"


class TableValuedColumnType(StrEnum):
    LIST = "list"
    SET = "set"


class TableKeyValuedColumnType(StrEnum):
    MAP = "map"


class TableVectorColumnType(StrEnum):
    VECTOR = "vector"


class TableUnsupportedColumnType(StrEnum):
    UNSUPPORTED = "UNSUPPORTED"
