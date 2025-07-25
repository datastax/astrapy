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

import pytest

from astrapy.data.info.table_descriptor.table_altering import AlterTableOperation
from astrapy.data.info.table_descriptor.type_altering import AlterTypeOperation
from astrapy.data.utils.table_types import ColumnType
from astrapy.info import (
    AlterTableAddColumns,
    AlterTableAddVectorize,
    AlterTableDropColumns,
    AlterTableDropVectorize,
    AlterTypeAddFields,
    AlterTypeRenameFields,
    CreateTableDefinition,
    CreateTypeDefinition,
    ListTableDefinition,
    ListTableDescriptor,
    TableAPIIndexSupportDescriptor,
    TableIndexDefinition,
    TableIndexOptions,
    TablePrimaryKeyDescriptor,
    TableScalarColumnTypeDescriptor,
    TableTextIndexDefinition,
    TableTextIndexOptions,
    TableUDTColumnDescriptor,
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
        "name": "with_uncreatable_columns",
        "definition": {
            "columns": {
                "p_text": {"type": "text"},
                "p_counter": {
                    "type": "counter",
                    "apiSupport": {
                        "createTable": False,
                        "insert": False,
                        "read": True,
                        "filter": True,
                        "cqlDefinition": "counter",
                    },
                },
                "p_timeuuid": {
                    "type": "timeuuid",
                    "apiSupport": {
                        "createTable": False,
                        "insert": True,
                        "read": True,
                        "filter": True,
                        "cqlDefinition": "timeuuid",
                    },
                },
            },
            "primaryKey": {"partitionBy": ["p_text"], "partitionSort": {}},
        },
    },
    {
        "name": "with_unsupported",
        "definition": {
            "columns": {
                "p_text": {"type": "text"},
                "my_nested_frozen": {
                    "type": "UNSUPPORTED",
                    "apiSupport": {
                        "createTable": False,
                        "insert": False,
                        "read": False,
                        "filter": False,
                        "cqlDefinition": "map<text, frozen<list<int>>>",
                    },
                },
            },
            "primaryKey": {"partitionBy": ["p_text"], "partitionSort": {}},
        },
    },
    {
        "name": "without_apisupports",
        "definition": {
            "columns": {
                "p_scalar": {
                    "type": "text",
                },
                "p_list": {
                    "type": "list",
                    "valueType": {"type": "text"},
                },
                "p_map": {
                    "type": "map",
                    "keyType": {"type": "text"},
                    "valueType": {"type": "text"},
                },
                "p_vector": {
                    "type": "vector",
                    "dimension": 999,
                },
                "p_unsupported": {
                    "type": "UNSUPPORTED",
                    "apiSupport": {
                        "createTable": False,
                        "insert": False,
                        "read": False,
                        "filter": False,
                        "cqlDefinition": "real unsupported cannot omit apiSupport!",
                    },
                },
            },
            "primaryKey": {"partitionBy": ["id"], "partitionSort": {}},
        },
    },
    {
        "name": "with_all_apisupports",
        "definition": {
            "columns": {
                "p_scalar": {
                    "type": "text",
                    "apiSupport": {
                        "createTable": True,
                        "insert": True,
                        "read": True,
                        "filter": True,
                        "cqlDefinition": "for scalar, not seen in API yet",
                    },
                },
                "p_list": {
                    "type": "list",
                    "valueType": {"type": "text"},
                    "apiSupport": {
                        "createTable": True,
                        "insert": True,
                        "read": True,
                        "filter": True,
                        "cqlDefinition": "the API returns apisupport for this type",
                    },
                },
                "p_map": {
                    "type": "map",
                    "keyType": {"type": "text"},
                    "valueType": {"type": "text"},
                    "apiSupport": {
                        "createTable": True,
                        "insert": True,
                        "read": True,
                        "filter": True,
                        "cqlDefinition": "the API returns apisupport for this type",
                    },
                },
                "p_vector": {
                    "type": "vector",
                    "dimension": 999,
                    "apiSupport": {
                        "createTable": True,
                        "insert": True,
                        "read": True,
                        "filter": True,
                        "cqlDefinition": "the API returns apisupport for this type",
                    },
                },
                "p_unsupported": {
                    "type": "UNSUPPORTED",
                    "apiSupport": {
                        "createTable": False,
                        "insert": False,
                        "read": False,
                        "filter": False,
                        "cqlDefinition": "real unsupported cannot omit apiSupport!",
                    },
                },
            },
            "primaryKey": {"partitionBy": ["id"], "partitionSort": {}},
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
INDEX_DEFINITION_DICT_MINIMAL = {"column": "the_column"}
INDEX_DEFINITION_DICT_COERCEABLE_MINIMAL = {"column": "the_column"}

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
VECTOR_INDEX_DEFINITION_DICT_MINIMAL = {"column": "the_v_column"}
VECTOR_INDEX_DEFINITION_DICT_COERCEABLE_MINIMAL = {"column": "the_v_column"}

TEXT_INDEX_DEFINITION_SHORT_DICT = {
    "column": "the_analyz_column1",
    "options": {
        "analyzer": "STANDARD",
    },
}
TEXT_INDEX_DEFINITION_SHORT = TableTextIndexDefinition(
    column="the_analyz_column1",
    options=TableTextIndexOptions(analyzer="STANDARD"),
)
TEXT_INDEX_DEFINITION_LONG_DICT = {
    "column": "the_analyz_column1",
    "options": {"analyzer": {"tokenizer": {"name": "whitespace"}}},
}
TEXT_INDEX_DEFINITION_LONG = TableTextIndexDefinition(
    column="the_analyz_column1",
    options=TableTextIndexOptions(
        analyzer={
            "tokenizer": {
                "name": "whitespace",
            },
        }
    ),
)

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
            .add_set_column("p_set", ColumnType.INT)
            .add_list_column("p_list", ColumnType.FLOAT)
            .add_map_column(
                "p_map",
                {"type": "text"},
                TableScalarColumnTypeDescriptor(column_type=ColumnType.INT),
            )
            .add_vector_column("p_vector", dimension=191)
            .add_vector_column(
                "p_vectorize",
                dimension=1024,
                service=VectorServiceOptions(
                    provider="mistral",
                    model_name="mistral-embed",
                ),
            )
            .add_userdefinedtype_column("p_udt", "the_udt")
            .add_set_column(
                "p_set_udt", {"type": "userDefined", "udtName": "the_udt_s"}
            )
            .add_list_column(
                "p_list_udt",
                TableUDTColumnDescriptor(udt_name="the_udt_l"),
            )
            .add_map_column(
                "p_map_udt",
                {"type": "text"},
                {"type": "userDefined", "udtName": "the_udt_m"},
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
                    "p_list": {"type": "list", "valueType": "float"},
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
                    "p_udt": {
                        "type": "userDefined",
                        "udtName": "the_udt",
                    },
                    "p_set_udt": {
                        "type": "set",
                        "valueType": {
                            "type": "userDefined",
                            "udtName": "the_udt_s",
                        },
                    },
                    "p_list_udt": {
                        "type": "list",
                        "valueType": {
                            "type": "userDefined",
                            "udtName": "the_udt_l",
                        },
                    },
                    "p_map_udt": {
                        "type": "map",
                        "keyType": "text",
                        "valueType": {
                            "type": "userDefined",
                            "udtName": "the_udt_m",
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
        ti_full = TableIndexDefinition.coerce(
            INDEX_DEFINITION_DICT_FULL,
        )
        assert INDEX_DEFINITION_DICT_FULL == ti_full.as_dict()
        assert ti_full == INDEX_DEFINITION_FULL

        ti_partial = TableIndexDefinition.coerce(
            INDEX_DEFINITION_DICT_PARTIAL,
        )
        assert INDEX_DEFINITION_DICT_PARTIAL == ti_partial.as_dict()

        ti_minimal = TableIndexDefinition.coerce(
            INDEX_DEFINITION_DICT_MINIMAL,
        )
        assert INDEX_DEFINITION_DICT_MINIMAL == ti_minimal.as_dict()

        ti_coerceable_minimal = TableIndexDefinition.coerce(
            INDEX_DEFINITION_DICT_COERCEABLE_MINIMAL,
        )
        assert INDEX_DEFINITION_DICT_MINIMAL == ti_coerceable_minimal.as_dict()

    @pytest.mark.describe("test of parsing of map index definitions")
    def test_map_indexdefinition_parsing(self) -> None:
        assert TableIndexDefinition.coerce(
            {"column": "the_text"}
        ) == TableIndexDefinition(column="the_text", options=TableIndexOptions())

        assert TableIndexDefinition.coerce(
            {"column": {"the_map": "$keys"}}
        ) == TableIndexDefinition(
            column={"the_map": "$keys"}, options=TableIndexOptions()
        )

        assert TableIndexDefinition.coerce(
            {"column": {"the_map": "$values"}}
        ) == TableIndexDefinition(
            column={"the_map": "$values"}, options=TableIndexOptions()
        )

    @pytest.mark.describe("test of parsing of vector index definitions")
    def test_vectorindexdefinition_parsing(self) -> None:
        tvi_full = TableVectorIndexDefinition.coerce(
            VECTOR_INDEX_DEFINITION_DICT_FULL,
        )
        assert VECTOR_INDEX_DEFINITION_DICT_FULL == tvi_full.as_dict()
        assert tvi_full == VECTOR_INDEX_DEFINITION_FULL

        tvi_partial = TableVectorIndexDefinition.coerce(
            VECTOR_INDEX_DEFINITION_DICT_PARTIAL,
        )
        assert VECTOR_INDEX_DEFINITION_DICT_PARTIAL == tvi_partial.as_dict()
        tvi_minimal = TableVectorIndexDefinition.coerce(
            VECTOR_INDEX_DEFINITION_DICT_MINIMAL,
        )
        assert VECTOR_INDEX_DEFINITION_DICT_MINIMAL == tvi_minimal.as_dict()

        tvi_coerceable_minimal = TableVectorIndexDefinition.coerce(
            VECTOR_INDEX_DEFINITION_DICT_COERCEABLE_MINIMAL,
        )
        assert VECTOR_INDEX_DEFINITION_DICT_MINIMAL == tvi_coerceable_minimal.as_dict()

    @pytest.mark.describe("test of parsing of text index definitions")
    def test_textindexdefinition_parsing(self) -> None:
        tti_short = TableTextIndexDefinition.coerce(
            TEXT_INDEX_DEFINITION_SHORT_DICT,
        )
        assert TEXT_INDEX_DEFINITION_SHORT_DICT == tti_short.as_dict()
        assert tti_short == TEXT_INDEX_DEFINITION_SHORT

        tti_long = TableTextIndexDefinition.coerce(
            TEXT_INDEX_DEFINITION_LONG_DICT,
        )
        assert TEXT_INDEX_DEFINITION_LONG_DICT == tti_long.as_dict()
        assert tti_long == TEXT_INDEX_DEFINITION_LONG

    @pytest.mark.describe("test of parsing of unsupported index definitions")
    def test_unsupportedindexdefinition_parsing(self) -> None:
        tui_full = TableUnsupportedIndexDefinition.coerce(
            UNSUPPORTED_INDEX_DEFINITION_DICT,
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

    @pytest.mark.describe(
        "test of coerce normalizing column types for AlterTableAddColumns"
    )
    def test_altertableaddcolumns_normalizetypes(self) -> None:
        short_form_dict = {
            "columns": {
                "col1": "int",
                "col2": "text",
            }
        }
        normalized_dict = {
            "columns": {
                "col1": {"type": "int"},
                "col2": {"type": "text"},
            }
        }
        alter_table_add_cols = AlterTableAddColumns._from_dict(short_form_dict)
        assert alter_table_add_cols.as_dict() == normalized_dict

    @pytest.mark.parametrize(
        ("operation_class", "test_dict"),
        [
            (
                AlterTableAddColumns,
                {
                    "columns": {
                        "col1": {"type": "int"},
                        "col2": {"type": "text"},
                    }
                },
            ),
            (
                AlterTableAddVectorize,
                {
                    "columns": {
                        "vec_col": {
                            "provider": "openai",
                            "modelName": "text-embedding-3-small",
                        }
                    }
                },
            ),
            (
                AlterTableDropColumns,
                {
                    "columns": ["col1", "col2"],
                },
            ),
            (
                AlterTableDropVectorize,
                {
                    "columns": ["vec_col1", "vec_col2"],
                },
            ),
        ],
        ids=["addcolumns", "addvectorize", "dropcolumns", "dropvectorize"],
    )
    @pytest.mark.describe(
        "test of coerce and as_dict for alter table operation classes"
    )
    def test_altertableoperation_coerce_asdict(
        self,
        operation_class: type[AlterTableOperation],
        test_dict: dict[str, Any],
    ) -> None:
        # Test coerce with dict input
        operation_from_dict = operation_class.coerce(test_dict)

        # Test coerce with object input (should return same object)
        operation_from_obj = operation_class.coerce(operation_from_dict)
        assert operation_from_obj == operation_from_dict

        # Test as_dict returns the original dict
        assert operation_from_dict.as_dict() == test_dict

    @pytest.mark.describe("test of AlterTypeOperation classes")
    def test_altertypeoperation_parsing_coerce_asdict(self) -> None:
        addf = AlterTypeAddFields(
            fields={
                "p_text": TableScalarColumnTypeDescriptor(column_type=ColumnType.TEXT),
                "p_int": TableScalarColumnTypeDescriptor(column_type=ColumnType.INT),
            }
        )

        # constructor signatures
        addf_constr_simple = AlterTypeAddFields(
            fields={
                "p_text": "text",
                "p_int": "int",
            }
        )
        addf_constr_dicts = AlterTypeAddFields(
            fields={
                "p_text": {"type": "text"},
                "p_int": {"type": "int"},
            }
        )

        assert addf == addf_constr_simple
        assert addf == addf_constr_dicts

        # other ways to construct
        addf_o = AlterTypeOperation.from_full_dict(
            {"add": {"fields": {"p_text": "text", "p_int": {"type": "int"}}}},
        )
        assert addf_o == addf

        assert AlterTypeAddFields.coerce(addf.as_dict()) == addf

        renf_o = AlterTypeOperation.from_full_dict(
            {"rename": {"fields": {"p_text": "new_p_text", "p_int": "new_p_int"}}},
        )
        renf = AlterTypeRenameFields(
            fields={
                "p_text": "new_p_text",
                "p_int": "new_p_int",
            }
        )
        assert renf_o == renf

        assert AlterTypeRenameFields.coerce(renf.as_dict()) == renf

    @pytest.mark.parametrize(
        ["ops_to_stack", "expected_op", "op_class"],
        [
            (
                [
                    AlterTypeAddFields(fields={"f_text": "blob", "f_int": "int"}),
                    AlterTypeAddFields(fields={"f_float": "blob"}),
                    AlterTypeAddFields(fields={"f_text": "text", "f_float": "float"}),
                ],
                AlterTypeAddFields(
                    fields={
                        "f_text": "text",
                        "f_int": "int",
                        "f_float": "float",
                    }
                ),
                AlterTypeAddFields,
            ),
            (
                [
                    AlterTypeRenameFields(fields={"f_text": "x", "f_int": "F_INT"}),
                    AlterTypeRenameFields(fields={"f_float": "x"}),
                    AlterTypeRenameFields(
                        fields={"f_text": "F_TEXT", "f_float": "F_FLOAT"}
                    ),
                ],
                AlterTypeRenameFields(
                    fields={
                        "f_text": "F_TEXT",
                        "f_int": "F_INT",
                        "f_float": "F_FLOAT",
                    }
                ),
                AlterTypeRenameFields,
            ),
        ],
        ids=["add", "rename"],
    )
    @pytest.mark.describe("test of AlterTypeOperation class stacking")
    def test_altertypeoperation_stacking(
        self,
        ops_to_stack: list[AlterTypeOperation],
        expected_op: AlterTypeOperation,
        op_class: type[AlterTypeOperation],
    ) -> None:
        assert op_class.stack(ops_to_stack) == expected_op

    @pytest.mark.describe("test of CreateTypeDefinition")
    def test_createtypedefinition_parsing(self) -> None:
        type_definition_objects = CreateTypeDefinition(
            fields={
                "tagline": TableScalarColumnTypeDescriptor(ColumnType.TEXT),
                "score": TableScalarColumnTypeDescriptor(ColumnType.INT),
                "weight": TableScalarColumnTypeDescriptor(ColumnType.FLOAT),
            },
        )

        type_definition_fluent = (
            CreateTypeDefinition.builder()
            .add_field("tagline", ColumnType.TEXT)
            .add_field("score", "int")
            .add_field("weight", ColumnType.FLOAT)
            .build()
        )

        fields_dict_simple = {
            "fields": {
                "tagline": "text",
                "score": "int",
                "weight": "float",
            },
        }
        type_definition_simple = CreateTypeDefinition.coerce(fields_dict_simple)

        fields_dict_dicts = {
            "fields": {
                "tagline": {"type": "text"},
                "score": {"type": "int"},
                "weight": {"type": "float"},
            },
        }
        type_definition_dicts = CreateTypeDefinition.coerce(fields_dict_dicts)

        fields_dict_mixed = {
            "fields": {
                "tagline": TableScalarColumnTypeDescriptor(ColumnType.TEXT),
                "score": "int",
                "weight": {"type": "float"},
            },
        }
        type_definition_mixed = CreateTypeDefinition.coerce(fields_dict_mixed)

        assert type_definition_objects == type_definition_fluent
        assert type_definition_objects == type_definition_simple
        assert type_definition_objects == type_definition_dicts
        assert type_definition_objects == type_definition_mixed

        assert type_definition_objects.as_dict() == fields_dict_dicts

        assert (
            CreateTypeDefinition.coerce(type_definition_objects.as_dict())
            == type_definition_objects
        )
        assert (
            CreateTypeDefinition.coerce(type_definition_objects)
            == type_definition_objects
        )

    @pytest.mark.describe("test of AlterTypeOperation stack_by_name")
    def test_altertypeoperation_stackbybname(self) -> None:
        ops = [
            AlterTypeAddFields(fields={"a0": "text", "a1": {"type": "int"}}),
            AlterTypeRenameFields(fields={"b0": "rb0", "b1": "rb1"}),
            AlterTypeAddFields(fields={"a1": "int", "a2": "float", "a3": "timestamp"}),
            AlterTypeRenameFields(fields={"b1": "rz1", "b2": "rz2"}),
            AlterTypeAddFields(fields={"a2": "ascii"}),
        ]

        expected_op_map = {
            "add": AlterTypeAddFields(
                fields={
                    "a0": "text",
                    "a1": "int",
                    "a2": "ascii",
                    "a3": "timestamp",
                }
            ),
            "rename": AlterTypeRenameFields(
                fields={
                    "b0": "rb0",
                    "b1": "rz1",
                    "b2": "rz2",
                }
            ),
        }

        stacked_op_map = AlterTypeOperation.stack_by_name(ops)

        assert stacked_op_map == expected_op_map
