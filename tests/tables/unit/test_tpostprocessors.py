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

import decimal
import ipaddress
import math
from typing import Any

import pytest

from astrapy.data.utils.table_converters import create_row_tpostprocessor
from astrapy.data_types import (
    DataAPITimestamp,
    TableDate,
    TableDuration,
    TableMap,
    TableSet,
    TableTime,
)
from astrapy.ids import UUID
from astrapy.info import TableDescriptor

TABLE_DESCRIPTION = {
    "name": "table_simple",
    "definition": {
        "columns": {
            "p_text": {"type": "text"},
            "p_boolean": {"type": "boolean"},
            "p_int": {"type": "int"},
            "p_float": {"type": "float"},
            "p_float_nan": {"type": "float"},
            "p_float_pinf": {"type": "float"},
            "p_float_minf": {"type": "float"},
            "p_blob": {"type": "blob"},
            "p_uuid": {"type": "uuid"},
            "p_decimal": {"type": "decimal"},
            "p_date": {"type": "date"},
            "p_duration": {"type": "duration"},
            "p_inet": {"type": "inet"},
            "p_time": {"type": "time"},
            "p_timestamp": {"type": "timestamp"},
            "p_timestamp_out_offset": {"type": "timestamp"},
            "p_list_int": {"type": "list", "valueType": "int"},
            "p_list_double": {"type": "list", "valueType": "double"},
            "p_set_ascii": {"type": "set", "valueType": "ascii"},
            "p_set_float": {"type": "set", "valueType": "float"},
            "p_map_text_float": {
                "type": "map",
                "valueType": "float",
                "keyType": "text",
            },
            "p_map_float_text": {
                "type": "map",
                "valueType": "text",
                "keyType": "float",
            },
            "somevector": {"type": "vector", "dimension": 3},
            "embeddings": {
                "type": "vector",
                "dimension": 2,
                "service": {
                    "provider": "openai",
                    "modelName": "text-embedding-3-small",
                },
            },
            "p_counter": {
                "type": "UNSUPPORTED",
                "apiSupport": {
                    "createTable": False,
                    "insert": False,
                    "read": False,
                    "cqlDefinition": "counter",
                },
            },
            "p_varchar": {
                "type": "UNSUPPORTED",
                "apiSupport": {
                    "createTable": False,
                    "insert": False,
                    "read": False,
                    "cqlDefinition": "varchar",
                },
            },
            "p_timeuuid": {
                "type": "UNSUPPORTED",
                "apiSupport": {
                    "createTable": False,
                    "insert": False,
                    "read": False,
                    "cqlDefinition": "timeuuid",
                },
            },
        },
        "primaryKey": {"partitionBy": [], "partitionSort": {}},
    },
}

FULL_RESPONSE_ROW = {
    "p_text": "italy",
    "p_boolean": True,
    "p_int": 123,
    "p_float": 1.2,
    "p_float_nan": "NaN",
    "p_float_pinf": "Infinity",
    "p_float_minf": "-Infinity",
    "p_blob": {"$binary": "YWJjMTIz"},  # b"abc123",
    "p_uuid": "9c5b94b1-35ad-49bb-b118-8e8fc24abf80",
    "p_decimal": 123.456,
    "p_date": "11111-09-30",
    "p_duration": "1mo1d1m1ns",
    "p_inet": "10.1.1.2",
    "p_time": "12:34:56.78912",
    "p_timestamp": "2015-05-03T13:30:54.234Z",
    "p_timestamp_out_offset": "-123-04-03T13:13:04.123+1:00",
    "p_list_int": [99, 100, 101],
    "p_list_double": [1.1, 2.2, "NaN", "Infinity", "-Infinity", 9.9],
    "p_set_ascii": ["a", "b", "c"],
    "p_set_float": [1.1, "NaN", "-Infinity", 9.9],
    "p_map_text_float": {"a": 0.1, "b": 0.2},
    "p_map_float_text": {1.1: "1-1", "NaN": "NANNN!"},
    "somevector": [0.1, -0.2, 0.3],
    "embeddings": [1.2, 3.4],
    "p_counter": 100,
    "p_varchar": "the_varchar",
    "p_timeuuid": "0de779c0-92e3-11ef-96a4-a745ae2c0a0b",
}

FULL_EXPECTED_ROW = {
    "p_text": "italy",
    "p_boolean": True,
    "p_int": 123,
    "p_float": 1.2,
    "p_float_nan": float("NaN"),
    "p_float_pinf": float("Infinity"),
    "p_float_minf": float("-Infinity"),
    "p_blob": b"abc123",
    "p_uuid": UUID("9c5b94b1-35ad-49bb-b118-8e8fc24abf80"),
    "p_decimal": decimal.Decimal("123.456"),
    "p_date": TableDate.from_string("11111-09-30"),
    "p_duration": TableDuration(months=1, days=1, nanoseconds=60000000001),
    "p_inet": ipaddress.ip_address("10.1.1.2"),
    "p_time": TableTime(12, 34, 56, 789120000),
    "p_timestamp": DataAPITimestamp.from_string("2015-05-03T13:30:54.234Z"),
    "p_timestamp_out_offset": DataAPITimestamp.from_string(
        "-123-04-03T13:13:04.123+1:00"
    ),
    "p_list_int": [99, 100, 101],
    "p_list_double": [
        1.1,
        2.2,
        float("NaN"),
        float("Infinity"),
        float("-Infinity"),
        9.9,
    ],
    "p_set_ascii": TableSet(["a", "b", "c"]),
    "p_set_float": TableSet([1.1, float("NaN"), float("-Infinity"), 9.9]),
    "p_map_text_float": TableMap({"a": 0.1, "b": 0.2}),
    "p_map_float_text": TableMap({1.1: "1-1", float("NaN"): "NANNN!"}),
    "somevector": [0.1, -0.2, 0.3],
    "embeddings": [1.2, 3.4],
    "p_counter": 100,
    "p_varchar": "the_varchar",
    "p_timeuuid": UUID("0de779c0-92e3-11ef-96a4-a745ae2c0a0b"),
}


_NaN = object()


def _repaint_NaNs(val: Any) -> Any:
    if isinstance(val, float) and math.isnan(val):
        return _NaN
    elif isinstance(val, dict):
        return {_repaint_NaNs(k): _repaint_NaNs(v) for k, v in val.items()}
    elif isinstance(val, list):
        return [_repaint_NaNs(x) for x in val]
    elif isinstance(val, TableSet):
        return TableSet(_repaint_NaNs(v) for v in val)
    elif isinstance(val, TableMap):
        return TableMap((_repaint_NaNs(k), _repaint_NaNs(v)) for k, v in val.items())
    else:
        return val


class TestTableConverters:
    @pytest.mark.describe("test of table converters based on schema")
    def test_tableconverters_schema_based(self) -> None:
        col_desc = TableDescriptor.coerce(TABLE_DESCRIPTION)
        tpostprocessor = create_row_tpostprocessor(col_desc.definition.columns)

        converted_column = tpostprocessor(FULL_RESPONSE_ROW)
        assert _repaint_NaNs(converted_column) == _repaint_NaNs(FULL_EXPECTED_ROW)

        all_nulls = {k: None for k in FULL_RESPONSE_ROW.keys()}
        converted_empty = tpostprocessor({})
        assert converted_empty == all_nulls

        converted_nulls = tpostprocessor(all_nulls)
        assert converted_nulls == all_nulls
