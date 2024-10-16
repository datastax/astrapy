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

from astrapy.info import TableDescriptor

TABLE_DICTS = [
    {
        "name": "table_composite_pk",
        "definition": {
            "columns": {
                "name": {"type": "text"},
                "id": {"type": "text"},
                "age": {"type": "int"},
            },
            "primaryKey": {"partitionBy": ["id", "name"], "partitionSort": {}},
        },
    },
    {
        "name": "table_simple",
        "definition": {
            "columns": {
                "country": {"type": "text"},
                "name": {"type": "text"},
                "human": {"type": "boolean"},
                "email": {"type": "text"},
                "age": {"type": "int"},
            },
            "primaryKey": {"partitionBy": ["email"], "partitionSort": {}},
        },
    },
    {
        "name": "table_clustering",
        "definition": {
            "columns": {
                "f1": {"type": "text"},
                "f2": {"type": "int"},
                "f3": {"type": "text"},
            },
            "primaryKey": {"partitionBy": ["f1"], "partitionSort": {"f2": -1}},
        },
    },
    {
        "name": "table_types",
        "definition": {
            "columns": {
                "p_ascii": {"type": "ascii"},
                "p_list": {"type": "list", "valueType": "text"},
                "p_boolean": {"type": "boolean"},
                "p_set": {"type": "set", "valueType": "int"},
                "p_tinyint": {"type": "tinyint"},
                "p_smallint": {"type": "smallint"},
                "p_duration": {"type": "duration"},
                "p_inet": {"type": "inet"},
                "p_blob": {"type": "blob"},
                "p_double": {"type": "double"},
                "p_float": {"type": "float"},
                "p_varint": {"type": "varint"},
                "p_decimal": {"type": "decimal"},
                "p_text": {"type": "text"},
                "p_time": {"type": "time"},
                "p_date": {"type": "date"},
                "p_int": {"type": "int"},
                "p_bigint": {"type": "bigint"},
                "p_uuid": {"type": "uuid"},
                "p_map": {"type": "map", "keyType": "text", "valueType": "text"},
                "p_timestamp": {"type": "timestamp"},
            },
            "primaryKey": {
                "partitionBy": ["p_uuid"],
                "partitionSort": {"p_text": 1, "p_int": -1},
            },
        },
    },
]


class TestTableDescriptors:
    @pytest.mark.describe("test of parsing into and from table descriptors")
    def test_tabledescriptor_parsing(self) -> None:
        table_descs = [
            TableDescriptor.from_dict(table_dict) for table_dict in TABLE_DICTS
        ]
        assert all(
            table_desc.as_dict() == table_dict
            for table_desc, table_dict in zip(table_descs, TABLE_DICTS)
        )
