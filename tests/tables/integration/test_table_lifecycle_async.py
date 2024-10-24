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
    TableColumnTypeDescriptor,
    TableDefinition,
    TableIndexDefinition,
    TableIndexOptions,
    TableKeyValuedColumnTypeDescriptor,
    TablePrimaryKeyDescriptor,
    TableValuedColumnTypeDescriptor,
    TableVectorColumnTypeDescriptor,
    TableVectorIndexDefinition,
    TableVectorIndexOptions,
    VectorServiceOptions,
)

if TYPE_CHECKING:
    from astrapy import AsyncDatabase


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

        await async_database.create_table(
            "table_whole_obj",
            definition=TableDefinition(
                columns={
                    "p_text": TableColumnTypeDescriptor(column_type="text"),
                    "p_int": TableColumnTypeDescriptor(column_type="int"),
                    "p_boolean": TableColumnTypeDescriptor(column_type="boolean"),
                    "p_float": TableColumnTypeDescriptor(column_type="float"),
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
                    "p_text": TableColumnTypeDescriptor(column_type="text"),
                    "p_int": {"type": "int"},
                    "p_boolean": TableColumnTypeDescriptor(column_type="boolean"),
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
        await async_database.create_table(
            "table_fluent",
            definition=ct_fluent_definition,
        )
        with pytest.raises(DataAPIException):
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
        # TODO add test of list indexes once endpoint available
        table = await async_database.create_table(
            "table_for_indexes",
            definition=TableDefinition(
                columns={
                    "p_key": TableColumnTypeDescriptor(column_type="text"),
                    "p_text": TableColumnTypeDescriptor(column_type="text"),
                    "p_int": TableColumnTypeDescriptor(column_type="int"),
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
            definition=TableIndexDefinition(
                column="p_text",
                options=TableIndexOptions(
                    ascii=False,
                    normalize=True,
                    case_sensitive=False,
                ),
            ),
        )
        await table.create_index(
            "tfi_idx_p_int",
            definition=TableIndexDefinition(
                column="p_int",
                options=TableIndexOptions(),
            ),
        )
        await table.create_vector_index(
            "tfi_idx_p_vector",
            definition=TableVectorIndexDefinition(
                column="p_vector_sm",
                options=TableVectorIndexOptions(
                    metric="cosine",
                ),
            ),
        )
        await table.create_vector_index(
            "tfi_idx_p_vector_sm",
            definition=TableVectorIndexDefinition(
                column="p_vector",
                options=TableVectorIndexOptions(
                    source_model="openai_v3_large",
                ),
            ),
        )

        await table.drop_index("tfi_idx_p_text")
        await table.drop_index("tfi_idx_p_int")
        await table.drop_index("tfi_idx_p_vector")
        await table.drop_index("tfi_idx_p_vector_sm")
        await async_database.drop_table(table)