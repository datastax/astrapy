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

from typing import TYPE_CHECKING

import pytest

from astrapy.exceptions import DataAPIException
from astrapy.info import (
    AlterTableAddColumns,
    AlterTableAddVectorize,
    AlterTableDropColumns,
    AlterTableDropVectorize,
    TableDefinition,
    TableIndexDefinition,
    TableIndexOptions,
    TableKeyValuedColumnTypeDescriptor,
    TablePrimaryKeyDescriptor,
    TableScalarColumnTypeDescriptor,
    TableValuedColumnTypeDescriptor,
    TableVectorColumnTypeDescriptor,
    TableVectorIndexDefinition,
    TableVectorIndexOptions,
    VectorServiceOptions,
)

if TYPE_CHECKING:
    from astrapy import Database


class TestTableLifecycle:
    @pytest.mark.describe("test of create/verify/delete tables, sync")
    def test_table_basic_crd_sync(
        self,
        sync_database: Database,
    ) -> None:
        pre_tables = sync_database.list_table_names()
        created_table_names = {
            "table_whole_obj",
            "table_whole_dict",
            "table_hybrid",
            "table_fluent",
        }
        assert set(pre_tables) & created_table_names == set()

        sync_database.create_table(
            "table_whole_obj",
            definition=TableDefinition(
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
            ),
        )
        sync_database.create_table(
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
        sync_database.create_table(
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
            TableDefinition.zero()
            .add_column("p_text", "text")
            .add_column("p_int", "int")
            .add_column("p_boolean", "boolean")
            .add_primitive_column("p_float", "float")
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
        )
        sync_database.create_table(
            "table_fluent",
            definition=ct_fluent_definition,
        )
        # TODO replace with DataAPIResponseException here
        with pytest.raises(DataAPIException):
            sync_database.create_table(
                "table_fluent",
                definition=ct_fluent_definition,
            )
        sync_database.create_table(
            "table_fluent",
            definition=ct_fluent_definition,
            if_not_exists=True,
        )

        assert (
            set(sync_database.list_table_names()) - set(pre_tables)
            == created_table_names
        )

        created_table_descs = [
            table_desc
            for table_desc in sync_database.list_tables()
            if table_desc.name in created_table_names
        ]

        for table_desc in created_table_descs[1:]:
            assert table_desc.definition == created_table_descs[0].definition

        for created_table_name in created_table_names:
            sync_database.drop_table(created_table_name)

        assert sync_database.list_table_names() == pre_tables

    @pytest.mark.describe("test of table create/delete index, sync")
    def test_tableindex_basic_crd_sync(
        self,
        sync_database: Database,
    ) -> None:
        # TODO add test of list indexes once endpoint available
        table = sync_database.create_table(
            "table_for_indexes",
            definition=TableDefinition(
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

        table.create_index(
            "tfi_idx_p_text",
            definition=TableIndexDefinition(
                column="p_text",
                options=TableIndexOptions(
                    ascii=False,
                    normalize=True,
                    case_sensitive=False,
                ),
            ),
        )
        table.create_index(
            "tfi_idx_p_int",
            definition=TableIndexDefinition(
                column="p_int",
                options=TableIndexOptions(),
            ),
        )
        table.create_vector_index(
            "tfi_idx_p_vector",
            definition=TableVectorIndexDefinition(
                column="p_vector_sm",
                options=TableVectorIndexOptions(
                    metric="cosine",
                ),
            ),
        )
        table.create_vector_index(
            "tfi_idx_p_vector_sm",
            definition=TableVectorIndexDefinition(
                column="p_vector",
                options=TableVectorIndexOptions(
                    source_model="openai-v3-large",
                ),
            ),
        )

        sync_database.drop_table_index("tfi_idx_p_text")
        sync_database.drop_table_index("tfi_idx_p_int")
        sync_database.drop_table_index("tfi_idx_p_vector")
        sync_database.drop_table_index("tfi_idx_p_vector_sm")
        sync_database.drop_table(table)

    @pytest.mark.describe("test of alter table, sync")
    def test_alter_tableindex_sync(
        self,
        sync_database: Database,
    ) -> None:
        orig_table_def = TableDefinition(
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
        table = sync_database.create_table(
            "table_to_alter",
            definition=orig_table_def,
        )

        try:
            table.alter(
                operation=AlterTableAddColumns.coerce({"columns": {"added_1": "float"}})
            )
            table.alter(
                operation=AlterTableDropColumns.coerce({"columns": ["added_1"]})
            )
            table.alter(
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
            # add column
            table.alter(
                operation=AlterTableDropVectorize.coerce({"columns": ["p_vector"]})
            )
            # back to the original table:
            assert table.definition() == orig_table_def
        finally:
            table.drop()
