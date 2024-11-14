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

from typing import Any

from astrapy.data_types import DataAPITimestamp

AR_DOC_PK_0 = {
    "p_ascii": "abc",
    "p_bigint": 10000,
    "p_int": 987,
    "p_boolean": False,
}
AR_DOC_PK_0_TUPLE = ("abc", 10000, 987, False)
AR_DOC_0 = {
    "p_text": "Ålesund",
    **AR_DOC_PK_0,
}
AR_DOC_0_B = {
    "p_text": "Overwritten™",
    **AR_DOC_PK_0,
}

DISTINCT_AR_DOCS = [
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
        "p_list_int": [],
        "p_map_text_text": {"b": "VB"},
        "p_set_int": set(),
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
DISTINCT_AR_DOCS_PKS = [
    {
        "p_ascii": ar_doc["p_ascii"],
        "p_bigint": ar_doc["p_bigint"],
        "p_int": ar_doc["p_int"],
        "p_boolean": ar_doc["p_boolean"],
    }
    for ar_doc in DISTINCT_AR_DOCS
]
DISTINCT_AR_DOCS_PK_TUPLES = [
    (ar_doc["p_ascii"], ar_doc["p_bigint"], ar_doc["p_int"], ar_doc["p_boolean"])
    for ar_doc in DISTINCT_AR_DOCS
]

SIMPLE_FULL_DOCS = [
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
