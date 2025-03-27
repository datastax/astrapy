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
import ipaddress
from decimal import Decimal
from typing import Any

from astrapy.data_types import (
    DataAPIDate,
    DataAPIDuration,
    DataAPIMap,
    DataAPISet,
    DataAPITime,
    DataAPITimestamp,
    DataAPIVector,
)
from astrapy.ids import UUID

AR_ROW_PK_0 = {
    "p_ascii": "abc",
    "p_bigint": 10000,
    "p_int": 987,
    "p_boolean": False,
}
AR_ROW_PK_0_TUPLE = ("abc", 10000, 987, False)
AR_ROW_0 = {
    "p_text": "Ålesund",
    **AR_ROW_PK_0,
}
AR_ROW_0_B = {
    "p_text": "Overwritten™",
    **AR_ROW_PK_0,
}

DISTINCT_AR_ROWS = [
    {
        "p_ascii": "A",
        "p_bigint": 1,
        "p_int": 1,
        "p_boolean": True,
        #
        "p_float": 0.1,
        "p_text": "a",
        "p_timestamp": DataAPITimestamp.from_string("1111-01-01T01:01:01Z"),
        "p_list_int": [1, 1, 2],
        "p_map_text_text": {"a": "va", "b": "vb"},
        "p_set_int": {100, 200},
    },
    {
        "p_ascii": "A",
        "p_bigint": 1,
        "p_int": 2,
        "p_boolean": True,
        #
        "p_float": 0.1,
        "p_text": "a",
        "p_timestamp": DataAPITimestamp.from_string("1111-01-01T01:01:01Z"),
        "p_list_int": [2, 1],
        "p_map_text_text": {"a": "va", "b": "vb"},
        "p_set_int": {200},
    },
    {
        "p_ascii": "A",
        "p_bigint": 1,
        "p_int": 3,
        "p_boolean": True,
        #
        "p_float": float("NaN"),
        "p_text": "a",
        "p_list_int": [9999],  # FIXME back to [] once issue #1906 resolved
        "p_map_text_text": {"b": "VB"},
        "p_set_int": set([9999]),  # FIXME back to set() once issue #1906 resolved
    },
    {
        "p_ascii": "A",
        "p_bigint": 1,
        "p_int": 4,
        "p_boolean": True,
        #
        "p_float": float("NaN"),
    },
    {
        "p_ascii": "A",
        "p_bigint": 1,
        "p_int": 5,
        "p_boolean": True,
        #
        "p_float": 0.2,
        "p_text": "b",
        "p_timestamp": DataAPITimestamp.from_string("1221-01-01T01:01:01Z"),
        "p_list_int": [3, 1],
        "p_map_text_text": {"a": "VA", "b": "VB"},
        "p_set_int": {200, 300},
    },
]

INSMANY_AR_ROW_HALFN = 250
INSMANY_AR_ROWS = [
    {
        "p_ascii": "A",
        "p_bigint": (PART_I + 1) * 100,
        "p_int": i,
        "p_boolean": i % 2 == 0,
        #
        "p_vector": None,
        "p_float": 0.2 + i,
        "p_text": f"b{i}",
        "p_timestamp": DataAPITimestamp.from_string(f"1{i:03}-01-01T01:01:01Z"),
        "p_list_int": [3 + i, 1, i],
        "p_map_text_text": {"a": f"V{i}A", "b": f"V{i}B"},
        "p_set_int": {200 + i, 300, i},
    }
    for PART_I in range(2)
    for i in range(INSMANY_AR_ROW_HALFN)
]
INSMANY_AR_ROWS_PKS = [
    {
        "p_ascii": ar_row["p_ascii"],
        "p_bigint": ar_row["p_bigint"],
        "p_int": ar_row["p_int"],
        "p_boolean": ar_row["p_boolean"],
    }
    for ar_row in INSMANY_AR_ROWS
]
INSMANY_AR_ROWS_PK_TUPLES = [
    (ar_row["p_ascii"], ar_row["p_bigint"], ar_row["p_int"], ar_row["p_boolean"])
    for ar_row in INSMANY_AR_ROWS
]

SIMPLE_FULL_ROWS = [
    {"p_text": "A1", "p_int": 1, "p_vector": [1.1, 1.1, 1.1]},
    {"p_text": "A2", "p_int": 2, "p_vector": [2.2, 2.2, 2.2]},
    {"p_text": "A3", "p_int": 3, "p_vector": [3.3, 3.3, 3.3]},
]

SIMPLE_SEVEN_ROWS_OK: list[dict[str, Any]] = [
    {"p_text": "p1", "p_int": 1},
    {"p_text": "p2", "p_int": 2},
    {"p_text": "p3", "p_int": 3},
    {"p_text": "p4", "p_int": 4},
    {"p_text": "p5", "p_int": 5},
    {"p_text": "p6", "p_int": 6},
    {"p_text": "p7", "p_int": 7},
]

SIMPLE_SEVEN_ROWS_F2: list[dict[str, Any]] = [
    {"p_text": "p1", "p_int": 1},
    {"p_text": "p2", "p_int": "boo"},
    {"p_text": "p3", "p_int": 3},
    {"p_text": "p4", "p_int": 4},
    {"p_text": "p5", "p_int": 5},
    {"p_text": "p6", "p_int": 6},
    {"p_text": "p7", "p_int": 7},
]

SIMPLE_SEVEN_ROWS_F4: list[dict[str, Any]] = [
    {"p_text": "p1", "p_int": 1},
    {"p_text": "p2", "p_int": 2},
    {"p_text": "p3", "p_int": 3},
    {"p_text": "p4", "p_int": "boo"},
    {"p_text": "p5", "p_int": 5},
    {"p_text": "p6", "p_int": 6},
    {"p_text": "p7", "p_int": 7},
]

FULL_AR_ROW_CUSTOMTYPED = {
    "p_ascii": "A",
    "p_bigint": 1230000,
    "p_blob": b"xyz",
    "p_boolean": True,
    "p_date": DataAPIDate.from_string("9876-01-12"),
    "p_decimal": Decimal("123.456"),
    "p_double": 123.5555,
    "p_duration": DataAPIDuration.from_c_string("4d3h55ns"),
    "p_float": 1.345,
    "p_inet": ipaddress.ip_address("127.0.0.1"),
    "p_int": 1012,
    "p_smallint": 12,
    "p_text": "Àäxxyxy",
    "p_text_nulled": None,
    "p_text_omitted": None,
    "p_time": DataAPITime.from_string("12:34:56.789"),
    "p_timestamp": DataAPITimestamp.from_string("1998-07-14T12:34:56.789Z"),
    "p_tinyint": 7,
    "p_varint": 10293,
    "p_uuid": UUID("01932c57-b8b7-8310-a84e-6d3fada3c525"),
    "p_vector": DataAPIVector([-0.1, 0.2, -0.3]),
    "p_list_int": [10, 11, -12],
    "p_map_text_text": DataAPIMap({"x": "YYY", "p": "QQQ"}),
    "p_set_int": DataAPISet({-43, 111, 109, 0}),
    "p_double_minf": float("-Infinity"),
    "p_double_pinf": float("Infinity"),
    "p_float_nan": float("NaN"),
}
FULL_AR_ROW_NONCUSTOMTYPED = {
    "p_ascii": "A",
    "p_bigint": 1230000,
    "p_blob": b"xyz",
    "p_boolean": True,
    "p_date": datetime.date(2013, 7, 24),
    "p_decimal": Decimal("123.456"),
    "p_double": 123.5555,
    "p_duration": datetime.timedelta(hours=12, minutes=33, seconds=1.234),
    "p_float": 1.345,
    "p_inet": ipaddress.ip_address("127.0.0.1"),
    "p_int": 1012,
    "p_smallint": 12,
    "p_text": "Àäxxyxy",
    "p_text_nulled": None,
    "p_text_omitted": None,
    "p_time": datetime.time(11, 22, 33, 456),
    # datetime in Python's stdlib is lossy after milliseconds:
    "p_timestamp": datetime.datetime(
        2021, 11, 21, 15, 34, 44, 123000, tzinfo=datetime.timezone.utc
    ),
    "p_tinyint": 7,
    "p_varint": 10293,
    "p_uuid": UUID("01932c57-b8b7-8310-a84e-6d3fada3c525"),
    "p_vector": [-0.1, 0.2, -0.3],
    "p_list_int": [10, 11, -12],
    "p_map_text_text": {"x": "YYY", "p": "QQQ"},
    "p_set_int": {-43, 111, 109, 0},
    "p_double_minf": float("-Infinity"),
    "p_double_pinf": float("Infinity"),
    "p_float_nan": float("NaN"),
}

COMPOSITE_VECTOR_ROWS_N = 3
COMPOSITE_VECTOR_ROWS = [
    {
        "p_text": p_t,
        "p_int": p_i,
        "p_boolean": p_i % 2 == 0,
        "p_vector": DataAPIVector([p_i + {"A": 0.0, "B": 0.1}[p_t], 1, 0]),
    }
    for p_t in {"A", "B"}
    for p_i in range(COMPOSITE_VECTOR_ROWS_N)
]

ALLMAPS_STDLIB_ROW = simplemap_row = {
    "id": "sl_row",
    "text_map": {
        "text key": "text value",
    },
    "ascii_map": {
        "ascii key": "ascii value",
    },
    "inet_map": {
        ipaddress.IPv4Address("192.168.1.1"): ipaddress.IPv4Address("10.0.0.1"),
    },
    "date_map": {
        datetime.date(2025, 1, 1): datetime.date(2026, 1, 1),
    },
    "duration_map": {
        "duration text key": datetime.timedelta(seconds=4800),
    },
    "time_map": {
        datetime.time(12, 0): datetime.time(23, 59, 59, 999999),
    },
    "timestamp_map": {
        datetime.datetime(
            2025, 1, 1, 12, 0, tzinfo=datetime.timezone.utc
        ): datetime.datetime(2026, 1, 1, 12, 0, tzinfo=datetime.timezone.utc),
    },
    "uuid_map": {
        UUID("123e4567-e89b-12d3-a456-426614174000"): UUID(
            "550e8400-e29b-41d4-a716-446655440000"
        ),
    },
    "tinyint_map": {
        5: 10,
    },
    "varint_map": {
        100000000: 200000000000,
    },
    "decimal_map": {
        Decimal("1.23"): Decimal("4.56"),
    },
    "double_map": {
        1.1: 2.2,
    },
    "int_map": {
        42: 99,
    },
    "smallint_map": {
        12: 34,
    },
    "bigint_map": {
        1: 1,
    },
    "boolean_map": {
        True: False,
    },
    "float_map": {
        3.14: 6.28,
    },
    "blob_map": {
        b"Hello World": b"Hello World",
    },
}
ALLMAPS_CUSTOMTYPES_ROW = {
    "id": "ct_row",
    "text_map": DataAPIMap(
        [
            ("text key", "text value"),
        ]
    ),
    "ascii_map": DataAPIMap(
        [
            ("ascii key", "ascii value"),
        ]
    ),
    "inet_map": DataAPIMap(
        [
            (ipaddress.ip_address("192.168.1.1"), ipaddress.ip_address("10.0.0.1")),
        ]
    ),
    "date_map": DataAPIMap(
        [
            (
                DataAPIDate.from_string("2025-01-01"),
                DataAPIDate.from_string("2026-01-01"),
            ),
        ]
    ),
    "duration_map": DataAPIMap(
        [
            ("duration text key", DataAPIDuration.from_c_string("1h20m")),
        ]
    ),
    "time_map": DataAPIMap(
        [
            (
                DataAPITime.from_string("12:00:00.000000000"),
                DataAPITime.from_string("23:59:59.999999999"),
            ),
        ]
    ),
    "timestamp_map": DataAPIMap(
        [
            (
                DataAPITimestamp.from_string("2025-01-01T12:00:00.000Z"),
                DataAPITimestamp.from_string("2026-01-01T12:00:00.000Z"),
            ),
        ]
    ),
    "uuid_map": DataAPIMap(
        [
            (
                UUID("123e4567-e89b-12d3-a456-426614174000"),
                UUID("550e8400-e29b-41d4-a716-446655440000"),
            ),
        ]
    ),
    "tinyint_map": DataAPIMap(
        [
            (5, 10),
        ]
    ),
    "varint_map": DataAPIMap(
        [
            (100000000, 200000000000),
        ]
    ),
    "decimal_map": DataAPIMap(
        [
            (Decimal("1.23"), Decimal("4.56")),
        ]
    ),
    "double_map": DataAPIMap(
        [
            (1.1, 2.2),
        ]
    ),
    "int_map": DataAPIMap(
        [
            (42, 99),
        ]
    ),
    "smallint_map": DataAPIMap(
        [
            (12, 34),
        ]
    ),
    "bigint_map": DataAPIMap(
        [
            (1, 1),
        ]
    ),
    "boolean_map": DataAPIMap(
        [
            (True, False),
        ]
    ),
    "float_map": DataAPIMap(
        [
            (3.14, 6.28),
        ]
    ),
    "blob_map": DataAPIMap(
        [
            (b"Hello World", b"Hello World"),
        ]
    ),
}
