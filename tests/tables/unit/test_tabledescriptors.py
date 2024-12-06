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

from astrapy.data.info.table_descriptor.table_altering import AlterTableOperation
from astrapy.data.utils.table_types import ColumnType
from astrapy.info import (
    AlterTableAddColumns,
    AlterTableAddVectorize,
    AlterTableDropColumns,
    AlterTableDropVectorize,
    CreateTableDefinition,
    ListTableDefinition,
    ListTableDescriptor,
    TableAPIIndexSupportDescriptor,
    TableIndexDefinition,
    TableIndexOptions,
    TablePrimaryKeyDescriptor,
    TableScalarColumnTypeDescriptor,
    TableUnsupportedIndexDefinition,
    TableVectorIndexDefinition,
    TableVectorIndexOptions,
    VectorServiceOptions,
)

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
    {
        "name": "uns_counter",
        "definition": {
            "columns": {
                "p_text": {"type": "text"},
                "p_counter": {
                    "type": "UNSUPPORTED",
                    "apiSupport": {
                        "createTable": False,
                        "insert": False,
                        "read": False,
                        "cqlDefinition": "counter",
                    },
                },
            },
            "primaryKey": {"partitionBy": ["p_text"], "partitionSort": {}},
        },
    },
]

DICT_DEFINITION = {
    "columns": {
        "country": {"type": "text"},
        "name": {"type": "text"},
        "human": {"type": "boolean"},
        "email": {"type": "text"},
        "age": {"type": "int"},
    },
    "primaryKey": {"partitionBy": ["email"], "partitionSort": {}},
}
HYBRID_DEFINITION = {
    "columns": {
        "country": TableScalarColumnTypeDescriptor(column_type="text"),
        "name": {"type": "text"},
        "human": TableScalarColumnTypeDescriptor(column_type="boolean"),
        "email": {"type": "text"},
        "age": TableScalarColumnTypeDescriptor(column_type="int"),
    },
    "primaryKey": TablePrimaryKeyDescriptor(
        partition_by=["email"],
        partition_sort={},
    ),
}

SHORT_FORM_COLUMN_TYPE = "int"
LONG_FORM_COLUMN_TYPE = {"type": "int"}

SHORT_FORM_PRIMARY_KEY = "column"
LONG_FORM_PRIMARY_KEY = {"partitionBy": ["column"], "partitionSort": {}}

INDEX_DEFINITION_FULL = TableIndexDefinition(
    column="the_column",
    options=TableIndexOptions(
        ascii=True,
        normalize=False,
        case_sensitive=True,
    ),
)
INDEX_DEFINITION_DICT_FULL = {
    "column": "the_column",
    "options": {
        "ascii": True,
        "normalize": False,
        "caseSensitive": True,
    },
}
INDEX_DEFINITION_DICT_PARTIAL = {
    "column": "the_column",
    "options": {
        "normalize": True,
    },
}
INDEX_DEFINITION_DICT_MINIMAL = {
    "column": "the_column",
    "options": {},
}
INDEX_DEFINITION_DICT_COERCEABLE_MINIMAL = {
    "column": "the_column",
}

VECTOR_INDEX_DEFINITION_FULL = TableVectorIndexDefinition(
    column="the_v_column",
    options=TableVectorIndexOptions(
        metric="the_metric",
        source_model="the_source_model",
    ),
)
VECTOR_INDEX_DEFINITION_DICT_FULL = {
    "column": "the_v_column",
    "options": {
        "metric": "the_metric",
        "sourceModel": "the_source_model",
    },
}
VECTOR_INDEX_DEFINITION_DICT_PARTIAL = {
    "column": "the_v_column",
    "options": {
        "sourceModel": "the_source_model",
    },
}
VECTOR_INDEX_DEFINITION_DICT_MINIMAL = {
    "column": "the_v_column",
    "options": {},
}
VECTOR_INDEX_DEFINITION_DICT_COERCEABLE_MINIMAL = {
    "column": "the_v_column",
}
UNSUPPORTED_INDEX_DEFINITION = TableUnsupportedIndexDefinition(
    column="UNKNOWN",
    api_support=TableAPIIndexSupportDescriptor(
        create_index=False,
        filter=True,
        cql_definition="CREATE INDEX SO-AND-SO!",
    ),
)
UNSUPPORTED_INDEX_DEFINITION_DICT = {
    "column": "UNKNOWN",
    "apiSupport": {
        "createIndex": False,
        "filter": True,
        "cqlDefinition": "CREATE INDEX SO-AND-SO!",
    },
}


class TestListTableDescriptors:
    @pytest.mark.describe("test of parsing list-table descriptors, fully dict")
    def test_listtabledescriptor_parsing_fulldict(self) -> None:
        table_descs = [
            ListTableDescriptor.coerce(table_dict) for table_dict in TABLE_DICTS
        ]
        assert all(
            table_desc.as_dict() == table_dict
            for table_desc, table_dict in zip(table_descs, TABLE_DICTS)
        )

    @pytest.mark.describe("test of parsing list-table descriptors with hybrid inputs")
    def test_listtabledescriptor_hybrid_parsing(self) -> None:
        from_dict_def = ListTableDefinition.coerce(DICT_DEFINITION)
        from_hyb_def = ListTableDefinition.coerce(HYBRID_DEFINITION)
        assert from_dict_def == from_hyb_def

    @pytest.mark.describe("test of parsing short forms for column types")
    def test_columntype_short_form(self) -> None:
        long_col = TableScalarColumnTypeDescriptor.coerce(LONG_FORM_COLUMN_TYPE)
        short_col = TableScalarColumnTypeDescriptor.coerce(SHORT_FORM_COLUMN_TYPE)
        assert long_col == short_col

    @pytest.mark.describe("test of parsing short forms for primary-key descriptor")
    def test_primarykey_short_form(self) -> None:
        long_pk = TablePrimaryKeyDescriptor.coerce(LONG_FORM_PRIMARY_KEY)
        short_pk = TablePrimaryKeyDescriptor.coerce(SHORT_FORM_PRIMARY_KEY)
        assert long_pk == short_pk

    @pytest.mark.describe("test of fluent interface for table definition")
    def test_tabledefinition_fluent(self) -> None:
        pb0 = (
            CreateTableDefinition.builder()
            .add_partition_by(["p_text", "p_int"])
            .build()
        )
        pb1 = (
            CreateTableDefinition.builder()
            .add_partition_by("p_text")
            .add_partition_by("p_int")
            .build()
        )
        pb2 = (
            CreateTableDefinition.builder()
            .add_partition_by(["p_text"])
            .add_partition_by(["p_int"])
            .build()
        )
        pb3 = (
            CreateTableDefinition.builder()
            .add_partition_by("p_text")
            .add_partition_by(["p_int"])
            .build()
        )
        pbz = (
            CreateTableDefinition.builder()
            .add_partition_by(["p_int"])
            .add_partition_by("p_text")
            .build()
        )
        assert pb0 == pb1
        assert pb0 == pb2
        assert pb0 == pb3
        assert pb0 != pbz

        def0 = (
            CreateTableDefinition.builder()
            .add_column("p_text", "text")
            .add_column("p_int", "int")
            .add_column("p_boolean", "boolean")
            .add_scalar_column("p_float", "float")
            .add_set_column("p_set", "int")
            .add_map_column("p_map", "text", "int")
            .add_vector_column("p_vector", dimension=191)
            .add_vector_column(
                "p_vectorize",
                dimension=1024,
                service=VectorServiceOptions(
                    provider="mistral",
                    model_name="mistral-embed",
                ),
            )
            .add_partition_by(["p_text", "p_int"])
            .add_partition_sort({"p_boolean": -1, "p_float": 1})
            .build()
        )
        def1 = CreateTableDefinition.coerce(
            {
                "columns": {
                    "p_text": {"type": "text"},
                    "p_int": {"type": "int"},
                    "p_boolean": {"type": "boolean"},
                    "p_float": {"type": "float"},
                    "p_set": {"type": "set", "valueType": "int"},
                    "p_map": {"type": "map", "keyType": "text", "valueType": "int"},
                    "p_vector": {"type": "vector", "dimension": 191},
                    "p_vectorize": {
                        "type": "vector",
                        "dimension": 1024,
                        "service": {
                            "provider": "mistral",
                            "modelName": "mistral-embed",
                        },
                    },
                },
                "primaryKey": {
                    "partitionBy": [
                        "p_text",
                        "p_int",
                    ],
                    "partitionSort": {"p_boolean": -1, "p_float": 1},
                },
            }
        )
        assert def0 == def1

        adef0 = (
            CreateTableDefinition.builder()
            .add_list_column("p_list", "tinyint")
            .add_vector_column(
                "p_vectorize",
                dimension=333,
                service={
                    "provider": "mistral",
                    "modelName": "mistral-embed",
                },
            )
            .build()
        )
        adef1 = CreateTableDefinition.coerce(
            {
                "columns": {
                    "p_list": {"type": "list", "valueType": "tinyint"},
                    "p_vectorize": {
                        "type": "vector",
                        "dimension": 333,
                        "service": {
                            "provider": "mistral",
                            "modelName": "mistral-embed",
                        },
                    },
                },
                "primaryKey": {"partitionBy": [], "partitionSort": {}},
            }
        )
        assert adef0 == adef1

    @pytest.mark.describe("test of parsing of index definitions")
    def test_indexdefinition_parsing(self) -> None:
        ti_full = TableIndexDefinition.coerce(INDEX_DEFINITION_DICT_FULL)
        assert INDEX_DEFINITION_DICT_FULL == ti_full.as_dict()
        assert ti_full == INDEX_DEFINITION_FULL

        ti_partial = TableIndexDefinition.coerce(INDEX_DEFINITION_DICT_PARTIAL)
        assert INDEX_DEFINITION_DICT_PARTIAL == ti_partial.as_dict()

        ti_minimal = TableIndexDefinition.coerce(INDEX_DEFINITION_DICT_MINIMAL)
        assert INDEX_DEFINITION_DICT_MINIMAL == ti_minimal.as_dict()

        ti_coerceable_minimal = TableIndexDefinition.coerce(
            INDEX_DEFINITION_DICT_COERCEABLE_MINIMAL
        )
        assert INDEX_DEFINITION_DICT_MINIMAL == ti_coerceable_minimal.as_dict()

    @pytest.mark.describe("test of parsing of vector index definitions")
    def test_vectorindexdefinition_parsing(self) -> None:
        tvi_full = TableVectorIndexDefinition.coerce(VECTOR_INDEX_DEFINITION_DICT_FULL)
        assert VECTOR_INDEX_DEFINITION_DICT_FULL == tvi_full.as_dict()
        assert tvi_full == VECTOR_INDEX_DEFINITION_FULL

        tvi_partial = TableVectorIndexDefinition.coerce(
            VECTOR_INDEX_DEFINITION_DICT_PARTIAL
        )
        assert VECTOR_INDEX_DEFINITION_DICT_PARTIAL == tvi_partial.as_dict()
        tvi_minimal = TableVectorIndexDefinition.coerce(
            VECTOR_INDEX_DEFINITION_DICT_MINIMAL
        )
        assert VECTOR_INDEX_DEFINITION_DICT_MINIMAL == tvi_minimal.as_dict()

        tvi_coerceable_minimal = TableVectorIndexDefinition.coerce(
            VECTOR_INDEX_DEFINITION_DICT_COERCEABLE_MINIMAL
        )
        assert VECTOR_INDEX_DEFINITION_DICT_MINIMAL == tvi_coerceable_minimal.as_dict()

    @pytest.mark.describe("test of parsing of unsupported index definitions")
    def test_unsupportedindexdefinition_parsing(self) -> None:
        tui_full = TableUnsupportedIndexDefinition.coerce(
            UNSUPPORTED_INDEX_DEFINITION_DICT
        )
        assert UNSUPPORTED_INDEX_DEFINITION_DICT == tui_full.as_dict()
        assert tui_full == UNSUPPORTED_INDEX_DEFINITION

    @pytest.mark.describe("test of parsing AlterTableOperation classes")
    def test_altertableoperation_parsing(self) -> None:
        addc_o = AlterTableOperation.from_full_dict(
            {
                "add": {
                    "columns": {
                        "p_int": {"type": "int"},
                        "p_text": "text",
                    }
                },
            },
        )
        addc = AlterTableAddColumns(
            columns={
                "p_int": TableScalarColumnTypeDescriptor(column_type=ColumnType.INT),
                "p_text": TableScalarColumnTypeDescriptor(column_type=ColumnType.TEXT),
            }
        )
        assert addc_o == addc

        addv_o = AlterTableOperation.from_full_dict(
            {
                "addVectorize": {
                    "columns": {"col_v": {"provider": "p", "modelName": "mn"}}
                }
            }
        )
        addv = AlterTableAddVectorize(
            columns={
                "col_v": VectorServiceOptions(
                    provider="p",
                    model_name="mn",
                )
            }
        )
        assert addv_o == addv

        dropc_o = AlterTableOperation.from_full_dict(
            {"drop": {"columns": ["col1", "col2"]}},
        )
        dropc = AlterTableDropColumns(columns=["col1", "col2"])
        assert dropc_o == dropc

        dropv_o = AlterTableOperation.from_full_dict(
            {"dropVectorize": {"columns": ["col1", "col2"]}},
        )
        dropv = AlterTableDropVectorize(columns=["col1", "col2"])
        assert dropv_o == dropv
