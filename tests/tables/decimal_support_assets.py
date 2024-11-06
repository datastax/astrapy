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

from decimal import Decimal

from astrapy.data_types import DataAPIMap, DataAPISet

_LD = "1.229999999999999982236431605997495353221893310546875"

BASELINE_OBJ = {
    "f": 1.23,
    "fn": float("NaN"),
    "fp": float("Infinity"),
    "fm": float("-Infinity"),
    "i": 123,
    "i0": 0,
    "t": "T",
}
BASELINE_KEY_STR = '[1.23,"Nan","Infinity","-Infinity",123,0,"T"]'
BASELINE_COLUMNS = {
    "f": {"type": "float"},
    "fn": {"type": "float"},
    "fp": {"type": "float"},
    "fm": {"type": "float"},
    "i": {"type": "int"},
    "i0": {"type": "int"},
    "t": {"type": "text"},
}
WDECS_OBJ = {
    "f": 1.23,
    "fn": float("NaN"),
    "fp": float("Infinity"),
    "fm": float("-Infinity"),
    "i": 123,
    "i0": 0,
    "t": "T",
    "d": Decimal("1.23"),
    "df": Decimal(1.23),
    "dl": Decimal(_LD),
    "dn": Decimal("NaN"),
    "dp": Decimal("Infinity"),
    "dm": Decimal("-Infinity"),
}
WDECS_KEY_STR = f'[1.23,"Nan","Infinity","-Infinity",123,0,"T",1.23,{_LD},{_LD},"Nan","Infinity","-Infinity"]'
WDECS_OBJ_COLUMNS = {
    "f": {"type": "float"},
    "fn": {"type": "float"},
    "fp": {"type": "float"},
    "fm": {"type": "float"},
    "i": {"type": "int"},
    "i0": {"type": "int"},
    "t": {"type": "text"},
    "d": {"type": "decimal"},
    "df": {"type": "decimal"},
    "dl": {"type": "decimal"},
    "dn": {"type": "decimal"},
    "dp": {"type": "decimal"},
    "dm": {"type": "decimal"},
}

COLLTYPES_CUSTOM_OBJ_SCHEMA_TRIPLES = [
    (
        False,
        {
            "f": {
                "f": 1.23,
                "fn": float("NaN"),
                "fp": float("Infinity"),
                "fm": float("-Infinity"),
            },
        },
        {
            "f": {
                "type": "map",
                "keyType": "text",
                "valueType": "float",
            },
        },
    ),
    (
        True,
        {
            "f": DataAPIMap(
                {
                    "f": 1.23,
                    "fn": float("NaN"),
                    "fp": float("Infinity"),
                    "fm": float("-Infinity"),
                }
            ),
        },
        {
            "f": {
                "type": "map",
                "keyType": "text",
                "valueType": "float",
            },
        },
    ),
    (
        False,
        {
            "f": [
                1.23,
                float("NaN"),
                float("Infinity"),
                float("-Infinity"),
            ],
        },
        {
            "f": {
                "type": "list",
                "valueType": "float",
            },
        },
    ),
    (
        False,
        {
            "f": {
                1.23,
                float("NaN"),
                float("Infinity"),
                float("-Infinity"),
            },
        },
        {
            "f": {
                "type": "set",
                "valueType": "float",
            },
        },
    ),
    (
        True,
        {
            "f": DataAPISet(
                {
                    1.23,
                    float("NaN"),
                    float("Infinity"),
                    float("-Infinity"),
                }
            ),
        },
        {
            "f": {
                "type": "set",
                "valueType": "float",
            },
        },
    ),
    (
        False,
        {
            "f": {
                "i0": 0,
                "im": -19,
                "ip": 384,
            },
        },
        {
            "f": {
                "type": "map",
                "keyType": "text",
                "valueType": "int",
            },
        },
    ),
    (
        True,
        {
            "f": DataAPIMap(
                {
                    "i0": 0,
                    "im": -19,
                    "ip": 384,
                }
            ),
        },
        {
            "f": {
                "type": "map",
                "keyType": "text",
                "valueType": "int",
            },
        },
    ),
    (
        False,
        {
            "f": [
                0,
                -19,
                384,
            ],
        },
        {
            "f": {
                "type": "list",
                "valueType": "int",
            },
        },
    ),
    (
        False,
        {
            "f": {
                0,
                -19,
                384,
            },
        },
        {
            "f": {
                "type": "set",
                "valueType": "int",
            },
        },
    ),
    (
        True,
        {
            "f": DataAPISet(
                {
                    0,
                    -19,
                    384,
                }
            ),
        },
        {
            "f": {
                "type": "set",
                "valueType": "int",
            },
        },
    ),
    (
        False,
        {
            "f": {
                "d": Decimal("1.23"),
                "df": Decimal(1.23),
                "dl": Decimal(_LD),
                "dn": Decimal("NaN"),
                "dp": Decimal("Infinity"),
                "dm": Decimal("-Infinity"),
            },
        },
        {
            "f": {
                "type": "map",
                "keyType": "text",
                "valueType": "decimal",
            },
        },
    ),
    (
        True,
        {
            "f": DataAPIMap(
                {
                    "d": Decimal("1.23"),
                    "df": Decimal(1.23),
                    "dl": Decimal(_LD),
                    "dn": Decimal("NaN"),
                    "dp": Decimal("Infinity"),
                    "dm": Decimal("-Infinity"),
                }
            ),
        },
        {
            "f": {
                "type": "map",
                "keyType": "text",
                "valueType": "decimal",
            },
        },
    ),
    (
        False,
        {
            "f": [
                Decimal("1.23"),
                Decimal(1.23),
                Decimal(_LD),
                Decimal("NaN"),
                Decimal("Infinity"),
                Decimal("-Infinity"),
            ],
        },
        {
            "f": {
                "type": "list",
                "valueType": "decimal",
            },
        },
    ),
    (
        False,
        {
            "f": {
                Decimal("1.23"),
                Decimal(1.23),
                Decimal(_LD),
                Decimal("NaN"),
                Decimal("Infinity"),
                Decimal("-Infinity"),
            },
        },
        {
            "f": {
                "type": "set",
                "valueType": "decimal",
            },
        },
    ),
    (
        True,
        {
            "f": DataAPISet(
                {
                    Decimal("1.23"),
                    Decimal(1.23),
                    Decimal(_LD),
                    Decimal("NaN"),
                    Decimal("Infinity"),
                    Decimal("-Infinity"),
                }
            ),
        },
        {
            "f": {
                "type": "set",
                "valueType": "decimal",
            },
        },
    ),
]
COLLTYPES_TRIPLE_IDS = [
    "map/float",
    "DataAPIMap/float",
    "list/float",
    "set/float",
    "DataAPISet/float",
    "map/int",
    "DataAPIMap/int",
    "list/int",
    "set/int",
    "DataAPISet/int",
    "map/Decimal",
    "DataAPIMap/Decimal",
    "list/Decimal",
    "set/Decimal",
    "DataAPISet/Decimal",
]
