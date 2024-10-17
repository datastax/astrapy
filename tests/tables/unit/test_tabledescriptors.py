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
        "name": "with_a_vector",
        "definition": {
            "columns": {
                "city": {"type": "text"},
                "name": {"type": "text"},
                "id": {"type": "text"},
                "age": {"type": "int"},
                "somevector": {"type": "vector", "dimension": 123},
            },
            "primaryKey": {"partitionBy": ["id"], "partitionSort": {}},
        },
    },
    {
        "name": "with_vectorize",
        "definition": {
            "columns": {
                "embeddings": {
                    "type": "vector",
                    "dimension": 123,
                    "service": {
                        "provider": "openai",
                        "modelName": "text-embedding-3-small",
                    },
                },
                "city": {"type": "text"},
                "name": {"type": "text"},
                "id": {"type": "text"},
                "age": {"type": "int"},
            },
            "primaryKey": {"partitionBy": ["id"], "partitionSort": {}},
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
