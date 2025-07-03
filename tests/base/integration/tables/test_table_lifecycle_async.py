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

import os
from typing import TYPE_CHECKING, Any

import pytest

from astrapy.exceptions import DataAPIResponseException
from astrapy.info import (
    AlterTableAddColumns,
    AlterTableAddVectorize,
    AlterTableDropColumns,
    AlterTableDropVectorize,
    CreateTableDefinition,
    TableIndexDefinition,
    TableIndexDescriptor,
    TableIndexOptions,
    TableIndexType,
    TableKeyValuedColumnTypeDescriptor,
    TablePrimaryKeyDescriptor,
    TableScalarColumnTypeDescriptor,
    TableTextIndexDefinition,
    TableTextIndexOptions,
    TableUserDefinedColumnTypeDescriptor,
    TableValuedColumnTypeDescriptor,
    TableVectorColumnTypeDescriptor,
    TableVectorIndexDefinition,
    TableVectorIndexOptions,
    VectorServiceOptions,
)

from ..conftest import IS_ASTRA_DB

if TYPE_CHECKING:
    from astrapy import AsyncDatabase


def _remove_apisupport(def_dict: dict[str, Any]) -> dict[str, Any]:
    """
    Strip apiSupport keys from columns since its presence
    is != between sending and receiving.
    """

    def _clean_col(col_dict: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in col_dict.items() if k != "apiSupport"}

    return {
        k: v if k != "columns" else {colk: _clean_col(colv) for colk, colv in v.items()}
        for k, v in def_dict.items()
    }


def _remove_definition(def_dict: dict[str, Any]) -> dict[str, Any]:
    """
    Strip definition keys from columns since its presence
    is != between sending and receiving. Useful for UDT columns
    """

    def _clean_col(col_dict: dict[str, Any]) -> dict[str, Any]:
        # 'definition' may appear nested, e.g. for a map with UDT values
        return {
            k: _clean_col(v) if isinstance(v, dict) else v
            for k, v in col_dict.items()
            if k != "definition"
        }

    return {
        k: v if k != "columns" else {colk: _clean_col(colv) for colk, colv in v.items()}
        for k, v in def_dict.items()
    }


class TestTableLifecycle:
    @pytest.mark.describe("test of create/verify/delete tables, async")
    async def test_table_basic_crd_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        pre_tables = await async_database.list_table_names()
        created_table_names = {
            "table_whole_obj",
            "table_whole_dict",
            "table_hybrid",
            "table_fluent",
        }
        assert set(pre_tables) & created_table_names == set()

        table_whole_obj_definition = CreateTableDefinition(
            columns={
                "p_text": TableScalarColumnTypeDescriptor(column_type="text"),
                "p_int": TableScalarColumnTypeDescriptor(column_type="int"),
                "p_boolean": TableScalarColumnTypeDescriptor(column_type="boolean"),
                "p_float": TableScalarColumnTypeDescriptor(column_type="float"),
                "p_set": TableValuedColumnTypeDescriptor(
                    column_type="set", value_type="int"
                ),
                "p_map": TableKeyValuedColumnTypeDescriptor(
                    column_type="map", key_type="text", value_type="int"
                ),
                "p_vector": TableVectorColumnTypeDescriptor(
                    column_type="vector", dimension=191, service=None
                ),
                "p_vectorize": TableVectorColumnTypeDescriptor(
                    column_type="vector",
                    dimension=1024,
                    service=VectorServiceOptions(
                        provider="mistral",
                        model_name="mistral-embed",
                    ),
                ),
            },
            primary_key=TablePrimaryKeyDescriptor(
                partition_by=[
                    "p_text",
                    "p_int",
                ],
                partition_sort={"p_boolean": -1, "p_float": 1},
            ),
        )
        await async_database.create_table(
            "table_whole_obj",
            definition=table_whole_obj_definition,
        )
        await async_database.create_table(
            "table_whole_dict",
            definition={
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
            },
        )
        await async_database.create_table(
            "table_hybrid",
            definition={
                "columns": {
                    "p_text": TableScalarColumnTypeDescriptor(column_type="text"),
                    "p_int": {"type": "int"},
                    "p_boolean": TableScalarColumnTypeDescriptor(column_type="boolean"),
                    "p_float": {"type": "float"},
                    "p_set": TableValuedColumnTypeDescriptor(
                        column_type="set", value_type="int"
                    ),
                    "p_map": {"type": "map", "keyType": "text", "valueType": "int"},
                    "p_vector": TableVectorColumnTypeDescriptor(
                        column_type="vector", dimension=191, service=None
                    ),
                    "p_vectorize": {
                        "type": "vector",
                        "dimension": 1024,
                        "service": {
                            "provider": "mistral",
                            "modelName": "mistral-embed",
                        },
                    },
                },
                "primaryKey": TablePrimaryKeyDescriptor(
                    partition_by=[
                        "p_text",
                        "p_int",
                    ],
                    partition_sort={"p_boolean": -1, "p_float": 1},
                ),
            },
        )

        ct_fluent_definition = (
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
        atable_fluent = await async_database.create_table(
            "table_fluent",
            definition=ct_fluent_definition,
        )

        # definition and info
        atf_def = await atable_fluent.definition()
        assert (
            _remove_apisupport(atf_def.as_dict())
            == table_whole_obj_definition.as_dict()
        )
        if IS_ASTRA_DB:
            fl_info = await atable_fluent.info()
            assert fl_info.name == "table_fluent"

        with pytest.raises(DataAPIResponseException):
            await async_database.create_table(
                "table_fluent",
                definition=ct_fluent_definition,
            )
        await async_database.create_table(
            "table_fluent",
            definition=ct_fluent_definition,
            if_not_exists=True,
        )

        assert (
            set(await async_database.list_table_names()) - set(pre_tables)
            == created_table_names
        )

        created_table_descs = [
            table_desc
            for table_desc in await async_database.list_tables()
            if table_desc.name in created_table_names
        ]

        for table_desc in created_table_descs[1:]:
            assert table_desc.definition == created_table_descs[0].definition

        for created_table_name in created_table_names:
            await async_database.drop_table(created_table_name)

        assert await async_database.list_table_names() == pre_tables

    @pytest.mark.describe("test of table create/delete index, async")
    async def test_tableindex_basic_crd_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        table = await async_database.create_table(
            "table_for_indexes",
            definition=CreateTableDefinition(
                columns={
                    "p_key": TableScalarColumnTypeDescriptor(column_type="text"),
                    "p_text": TableScalarColumnTypeDescriptor(column_type="text"),
                    "p_int": TableScalarColumnTypeDescriptor(column_type="int"),
                    "p_vector": TableVectorColumnTypeDescriptor(
                        column_type="vector", dimension=191, service=None
                    ),
                    "p_vector_sm": TableVectorColumnTypeDescriptor(
                        column_type="vector", dimension=37, service=None
                    ),
                },
                primary_key=TablePrimaryKeyDescriptor(
                    partition_by=["p_key"],
                    partition_sort={},
                ),
            ),
        )

        await table.create_index(
            "tfi_idx_p_text",
            "p_text",
            options=TableIndexOptions(
                ascii=False,
                normalize=True,
                case_sensitive=False,
            ),
        )
        await table.create_index(
            "tfi_idx_p_int",
            "p_int",
        )
        await table.create_vector_index(
            "tfi_idx_p_vector_sm",
            "p_vector_sm",
            options=TableVectorIndexOptions(
                metric="cosine",
            ),
        )
        await table.create_vector_index(
            "tfi_idx_p_vector",
            "p_vector",
            options=TableVectorIndexOptions(
                source_model="openai-v3-large",
            ),
        )

        expected_indexes = {
            "tfi_idx_p_text",
            "tfi_idx_p_int",
            "tfi_idx_p_vector",
            "tfi_idx_p_vector_sm",
        }
        assert set(await table.list_index_names()) == expected_indexes
        # this data needs metric and source_model always specified
        # to match how the API behaves. Also metric is auto-selected based
        # on sourceModel for example.
        expected_index_list = [
            TableIndexDescriptor(
                name="tfi_idx_p_text",
                definition=TableIndexDefinition(
                    column="p_text",
                    options=TableIndexOptions(
                        ascii=False,
                        normalize=True,
                        case_sensitive=False,
                    ),
                ),
            ),
            TableIndexDescriptor(
                name="tfi_idx_p_int",
                definition=TableIndexDefinition(
                    column="p_int",
                    options=TableIndexOptions(),
                ),
            ),
            TableIndexDescriptor(
                name="tfi_idx_p_vector_sm",
                definition=TableVectorIndexDefinition(
                    column="p_vector_sm",
                    options=TableVectorIndexOptions(
                        metric="cosine",
                        source_model="other",
                    ),
                ),
            ),
            TableIndexDescriptor(
                name="tfi_idx_p_vector",
                definition=TableVectorIndexDefinition(
                    column="p_vector",
                    options=TableVectorIndexOptions(
                        metric="dot_product",
                        source_model="openai-v3-large",
                    ),
                ),
            ),
        ]
        assert sorted(await table.list_indexes(), key=lambda id: id.name) == sorted(
            expected_index_list, key=lambda id: id.name
        )

        await async_database.drop_table_index("tfi_idx_p_text")
        await async_database.drop_table_index("tfi_idx_p_int")
        await async_database.drop_table_index("tfi_idx_p_vector")
        await async_database.drop_table_index("tfi_idx_p_vector_sm")
        await async_database.drop_table(table.name)

    @pytest.mark.describe("test of alter table, async")
    async def test_alter_table_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        orig_table_def = CreateTableDefinition(
            columns={
                "p_text": TableScalarColumnTypeDescriptor(column_type="text"),
                "p_int": TableScalarColumnTypeDescriptor(column_type="int"),
                "p_vector": TableVectorColumnTypeDescriptor(
                    column_type="vector", dimension=1024, service=None
                ),
            },
            primary_key=TablePrimaryKeyDescriptor(
                partition_by=["p_text"],
                partition_sort={},
            ),
        )
        atable = await async_database.create_table(
            "table_to_alter",
            definition=orig_table_def,
        )

        try:
            await atable.alter(
                operation=AlterTableAddColumns.coerce({"columns": {"added_1": "float"}})
            )
            await atable.alter(
                # one with a dictionary to parse
                operation={"drop": {"columns": ["added_1"]}}
            )
            await atable.alter(
                operation=AlterTableAddVectorize.coerce(
                    {
                        "columns": {
                            "p_vector": {
                                "provider": "mistral",
                                "modelName": "mistral-embed",
                            }
                        }
                    }
                )
            )
            await atable.alter(
                operation=AlterTableDropVectorize.coerce({"columns": ["p_vector"]})
            )
            # back to the original table:
            adef = await atable.definition()
            assert _remove_apisupport(adef.as_dict()) == orig_table_def.as_dict()
        finally:
            await atable.drop()

    @pytest.mark.describe("test of collection indexes, async")
    async def test_table_collectionindexes_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        table_colidx_def = CreateTableDefinition(
            columns={
                "id": TableScalarColumnTypeDescriptor(column_type="text"),
                "t": TableScalarColumnTypeDescriptor(column_type="text"),
                "set_int": TableValuedColumnTypeDescriptor(
                    column_type="set",
                    value_type="int",
                ),
                "set_int2": TableValuedColumnTypeDescriptor(
                    column_type="set",
                    value_type="int",
                ),
                "list_int": TableValuedColumnTypeDescriptor(
                    column_type="list",
                    value_type="int",
                ),
                "list_int2": TableValuedColumnTypeDescriptor(
                    column_type="list",
                    value_type="int",
                ),
                "map_text_int_e": TableKeyValuedColumnTypeDescriptor(
                    key_type="text",
                    value_type="int",
                ),
                "map_text_int_k": TableKeyValuedColumnTypeDescriptor(
                    key_type="text",
                    value_type="int",
                ),
                "map_text_int_v": TableKeyValuedColumnTypeDescriptor(
                    key_type="text",
                    value_type="int",
                ),
            },
            primary_key=TablePrimaryKeyDescriptor(
                partition_by=["id"],
                partition_sort={},
            ),
        )
        atable = await async_database.create_table(
            "table_collectionindexes",
            definition=table_colidx_def,
        )

        try:
            # create, list and drop baseline + various collection-column indexes
            idx_t_t_column = "t"
            idx_t_t_options = {
                "caseSensitive": False,
                "normalize": False,
                "ascii": True,
            }
            idx_t_set_int_column = "set_int"
            idx_t_set_int_column2 = {"set_int2": "$values"}
            idx_t_list_int_column = "list_int"
            idx_t_list_int_column2 = {"list_int2": "$values"}
            idx_t_map_text_int_e_column = "map_text_int_e"
            idx_t_map_text_int_k_column = {"map_text_int_k": "$keys"}
            idx_t_map_text_int_v_column = {"map_text_int_v": "$values"}

            await atable.create_index(
                "idx_t_t", idx_t_t_column, options=idx_t_t_options
            )
            await atable.create_index("idx_t_set_int", idx_t_set_int_column)
            await atable.create_index("idx_t_set_int2", idx_t_set_int_column2)
            await atable.create_index("idx_t_list_int", idx_t_list_int_column)
            await atable.create_index("idx_t_list_int2", idx_t_list_int_column2)
            await atable.create_index(
                "idx_t_map_text_int_e", idx_t_map_text_int_e_column
            )
            await atable.create_index(
                "idx_t_map_text_int_k", idx_t_map_text_int_k_column
            )
            await atable.create_index(
                "idx_t_map_text_int_v", idx_t_map_text_int_v_column
            )

            listed_indexes = sorted(
                await atable.list_indexes(),
                key=lambda idx_desc: idx_desc.name,
            )
            expected_indexes = sorted(
                [
                    TableIndexDescriptor(
                        name="idx_t_t",
                        definition=TableIndexDefinition(
                            column=idx_t_t_column,
                            options=TableIndexOptions.coerce(idx_t_t_options),
                        ),
                        index_type=TableIndexType.REGULAR,
                    ),
                    TableIndexDescriptor(
                        name="idx_t_set_int",
                        definition=TableIndexDefinition(
                            column={idx_t_set_int_column: "$values"},
                            options=TableIndexOptions(),
                        ),
                        index_type=TableIndexType.REGULAR,
                    ),
                    TableIndexDescriptor(
                        name="idx_t_set_int2",
                        definition=TableIndexDefinition(
                            column=idx_t_set_int_column2,
                            options=TableIndexOptions(),
                        ),
                        index_type=TableIndexType.REGULAR,
                    ),
                    TableIndexDescriptor(
                        name="idx_t_list_int",
                        definition=TableIndexDefinition(
                            column={idx_t_list_int_column: "$values"},
                            options=TableIndexOptions(),
                        ),
                        index_type=TableIndexType.REGULAR,
                    ),
                    TableIndexDescriptor(
                        name="idx_t_list_int2",
                        definition=TableIndexDefinition(
                            column=idx_t_list_int_column2,
                            options=TableIndexOptions(),
                        ),
                        index_type=TableIndexType.REGULAR,
                    ),
                    TableIndexDescriptor(
                        name="idx_t_map_text_int_e",
                        definition=TableIndexDefinition(
                            column=idx_t_map_text_int_e_column,
                            options=TableIndexOptions(),
                        ),
                        index_type=TableIndexType.REGULAR,
                    ),
                    TableIndexDescriptor(
                        name="idx_t_map_text_int_k",
                        definition=TableIndexDefinition(
                            column=idx_t_map_text_int_k_column,
                            options=TableIndexOptions(),
                        ),
                        index_type=TableIndexType.REGULAR,
                    ),
                    TableIndexDescriptor(
                        name="idx_t_map_text_int_v",
                        definition=TableIndexDefinition(
                            column=idx_t_map_text_int_v_column,
                            options=TableIndexOptions(),
                        ),
                        index_type=TableIndexType.REGULAR,
                    ),
                ],
                key=lambda idx_desc: idx_desc.name,
            )
            assert listed_indexes == expected_indexes

            await atable.database.drop_table_index("idx_t_t")
            await atable.database.drop_table_index("idx_t_set_int")
            await atable.database.drop_table_index("idx_t_set_int2")
            await atable.database.drop_table_index("idx_t_list_int")
            await atable.database.drop_table_index("idx_t_list_int2")
            await atable.database.drop_table_index("idx_t_map_text_int_e")
            await atable.database.drop_table_index("idx_t_map_text_int_k")
            await atable.database.drop_table_index("idx_t_map_text_int_v")
        finally:
            await atable.drop()

    @pytest.mark.skipif(
        "ASTRAPY_TEST_LATEST_MAIN" not in os.environ,
        reason="Text indexes testable only on latest main for now",
    )
    @pytest.mark.describe("test of text indexes, async")
    async def test_table_textindexes_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        table_textidx_def = CreateTableDefinition(
            columns={
                "id": TableScalarColumnTypeDescriptor(column_type="text"),
                "txt_d": TableScalarColumnTypeDescriptor(column_type="text"),
                "txt_s": TableScalarColumnTypeDescriptor(column_type="text"),
                "txt_l": TableScalarColumnTypeDescriptor(column_type="text"),
            },
            primary_key=TablePrimaryKeyDescriptor(
                partition_by=["id"],
                partition_sort={},
            ),
        )
        atable = await async_database.create_table(
            "table_textindexes",
            definition=table_textidx_def,
        )

        try:
            # create, list and drop various analyzer text-column indexes
            tx_id_opts_s = TableTextIndexOptions(analyzer="whitespace")
            tx_id_opts_l = TableTextIndexOptions(
                analyzer={
                    "tokenizer": {"name": "standard", "args": {}},
                    "filters": [
                        {"name": "lowercase"},
                        {"name": "stop"},
                        {"name": "porterstem"},
                        {"name": "asciifolding"},
                    ],
                    "charFilters": [],
                },
            )
            await atable.create_text_index("idx_txt_d", "txt_d")
            await atable.create_text_index("idx_txt_s", "txt_s", options=tx_id_opts_s)
            await atable.create_text_index("idx_txt_l", "txt_l", options=tx_id_opts_l)

            listed_indexes = sorted(
                await atable.list_indexes(),
                key=lambda idx_desc: idx_desc.name,
            )
            expected_indexes = sorted(
                [
                    TableIndexDescriptor(
                        name="idx_txt_d",
                        definition=TableTextIndexDefinition(
                            column="txt_d",
                            options=TableTextIndexOptions(analyzer="standard"),
                        ),
                        index_type=TableIndexType.TEXT,
                    ),
                    TableIndexDescriptor(
                        name="idx_txt_s",
                        definition=TableTextIndexDefinition(
                            column="txt_s",
                            options=tx_id_opts_s,
                        ),
                        index_type=TableIndexType.TEXT,
                    ),
                    TableIndexDescriptor(
                        name="idx_txt_l",
                        definition=TableTextIndexDefinition(
                            column="txt_l",
                            options=tx_id_opts_l,
                        ),
                        index_type=TableIndexType.TEXT,
                    ),
                ],
                key=lambda idx_desc: idx_desc.name,
            )
            assert listed_indexes == expected_indexes

            await atable.database.drop_table_index("idx_txt_d")
            await atable.database.drop_table_index("idx_txt_s")
            await atable.database.drop_table_index("idx_txt_l")
        finally:
            await atable.drop()

    @pytest.mark.skipif(
        "ASTRAPY_TEST_UDT" not in os.environ,
        reason="UDT testing not enabled",
    )
    @pytest.mark.skip(
        reason="Suspended until udt-cache issues fixed - TODO RESTRICT_UDT_TEST"
    )
    @pytest.mark.describe("test of create/verify/delete table with a simple UDT, async")
    async def test_table_simpleudt_crd_async(
        self,
        async_database: AsyncDatabase,
        simple_udt: str,
    ) -> None:
        udt_col_desc = TableUserDefinedColumnTypeDescriptor(udt_name=simple_udt)
        table_simple_udt_def = CreateTableDefinition(
            columns={
                "id": TableScalarColumnTypeDescriptor("text"),
                "udt_map": TableKeyValuedColumnTypeDescriptor(
                    key_type=TableScalarColumnTypeDescriptor("ascii"),
                    value_type=udt_col_desc,
                ),
                "udt_set": TableValuedColumnTypeDescriptor(
                    column_type="set",
                    value_type=udt_col_desc,
                ),
                "udt_list": TableValuedColumnTypeDescriptor(
                    column_type="list",
                    value_type=udt_col_desc,
                ),
                "udt_scalar": udt_col_desc,
            },
            primary_key=TablePrimaryKeyDescriptor(
                partition_by=["id"],
                partition_sort={},
            ),
        )
        try:
            atable = await async_database.create_table(
                "table_simple_udt",
                definition=table_simple_udt_def,
            )
            assert (
                _remove_definition(
                    _remove_apisupport((await atable.definition()).as_dict())
                )
                == table_simple_udt_def.as_dict()
            )

            add_udt_columns = AlterTableAddColumns(
                columns={
                    "altered_udt_map": TableKeyValuedColumnTypeDescriptor(
                        key_type=TableScalarColumnTypeDescriptor("ascii"),
                        value_type=udt_col_desc,
                    ),
                    "altered_udt_set": TableValuedColumnTypeDescriptor(
                        column_type="set",
                        value_type=udt_col_desc,
                    ),
                    "altered_udt_list": TableValuedColumnTypeDescriptor(
                        column_type="list",
                        value_type=udt_col_desc,
                    ),
                    "altered_udt_scalar": udt_col_desc,
                },
            )
            await atable.alter(add_udt_columns)
            drop_udt_columns = AlterTableDropColumns(
                columns=["udt_map", "udt_set", "udt_list", "udt_scalar"]
            )
            await atable.alter(drop_udt_columns)
            altered_table_simple_udt_def = CreateTableDefinition(
                columns={
                    "id": TableScalarColumnTypeDescriptor("text"),
                    "altered_udt_map": TableKeyValuedColumnTypeDescriptor(
                        key_type=TableScalarColumnTypeDescriptor("ascii"),
                        value_type=udt_col_desc,
                    ),
                    "altered_udt_set": TableValuedColumnTypeDescriptor(
                        column_type="set",
                        value_type=udt_col_desc,
                    ),
                    "altered_udt_list": TableValuedColumnTypeDescriptor(
                        column_type="list",
                        value_type=udt_col_desc,
                    ),
                    "altered_udt_scalar": udt_col_desc,
                },
                primary_key=TablePrimaryKeyDescriptor(
                    partition_by=["id"],
                    partition_sort={},
                ),
            )
            assert (
                _remove_definition(
                    _remove_apisupport((await atable.definition()).as_dict())
                )
                == altered_table_simple_udt_def.as_dict()
            )
        finally:
            await atable.drop()
