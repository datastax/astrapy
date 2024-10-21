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

from astrapy import DataAPIClient
from astrapy.api_options import APIOptions
from astrapy.authentication import UsernamePasswordTokenProvider
from astrapy.constants import Environment
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

# TODO: adapt this whole test module: to fixtures, conftest to target various DBs etc


class TestTableLifecycle:
    @pytest.mark.describe("test of create/verify/delete tables")
    def test_table_basic_crd(self) -> None:
        client = DataAPIClient(
            environment=Environment.HCD,
            api_options=APIOptions(
                database_additional_headers={"Feature-Flag-tables": "true"}
            ),
        )
        database = client.get_database(
            "http://localhost:8181",
            token=UsernamePasswordTokenProvider("cassandra", "cassandra"),
        )
        database.get_database_admin().create_keyspace(
            "default_keyspace", update_db_keyspace=True
        )

        pre_tables = database.list_table_names()
        created_table_names = {
            "table_whole_obj",
            "table_whole_dict",
            "table_hybrid",
            "table_fluent",
        }
        assert set(pre_tables) & created_table_names == set()

        database.create_table(
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
        database.create_table(
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
        database.create_table(
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
        database.create_table(
            "table_fluent",
            definition=ct_fluent_definition,
        )
        with pytest.raises(DataAPIException):
            database.create_table(
                "table_fluent",
                definition=ct_fluent_definition,
            )
        database.create_table(
            "table_fluent",
            definition=ct_fluent_definition,
            if_not_exists=True,
        )

        assert set(database.list_table_names()) - set(pre_tables) == created_table_names

        created_table_descs = [
            table_desc
            for table_desc in database.list_tables()
            if table_desc.name in created_table_names
        ]

        for table_desc in created_table_descs[1:]:
            assert table_desc.definition == created_table_descs[0].definition

        for created_table_name in created_table_names:
            database.drop_table(created_table_name)

        assert database.list_table_names() == pre_tables

    @pytest.mark.describe("test of table create/delete index")
    def test_tableindex_basic_crd(self) -> None:
        # TODO add test of list indexes once endpoint available
        client = DataAPIClient(
            environment=Environment.HCD,
            api_options=APIOptions(
                database_additional_headers={"Feature-Flag-tables": "true"}
            ),
        )
        database = client.get_database(
            "http://localhost:8181",
            token=UsernamePasswordTokenProvider("cassandra", "cassandra"),
        )
        database.get_database_admin().create_keyspace(
            "default_keyspace", update_db_keyspace=True
        )
        table = database.create_table(
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
                    source_model="openai_v3_large",
                ),
            ),
        )

        table.drop_index("tfi_idx_p_text")
        table.drop_index("tfi_idx_p_int")
        table.drop_index("tfi_idx_p_vector")
        table.drop_index("tfi_idx_p_vector_sm")
        database.drop_table(table)
