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

from astrapy.constants import SortMode
from astrapy.info import (
    ColumnType,
    CreateTableDefinition,
    TableKeyValuedColumnTypeDescriptor,
    TablePrimaryKeyDescriptor,
    TableScalarColumnTypeDescriptor,
    TableValuedColumnTypeDescriptor,
    TableVectorColumnTypeDescriptor,
    TableVectorIndexOptions,
    VectorServiceOptions,
)

TEST_ALL_RETURNS_TABLE_NAME = "test_table_all_returns"
TEST_ALL_RETURNS_TABLE_DEFINITION = CreateTableDefinition(
    columns={
        "p_ascii": TableScalarColumnTypeDescriptor(column_type="ascii"),
        "p_bigint": TableScalarColumnTypeDescriptor(column_type="bigint"),
        "p_blob": TableScalarColumnTypeDescriptor(column_type="blob"),
        "p_boolean": TableScalarColumnTypeDescriptor(column_type="boolean"),
        "p_date": TableScalarColumnTypeDescriptor(column_type="date"),
        "p_decimal": TableScalarColumnTypeDescriptor(column_type="decimal"),
        "p_double": TableScalarColumnTypeDescriptor(column_type="double"),
        "p_duration": TableScalarColumnTypeDescriptor(column_type="duration"),
        "p_float": TableScalarColumnTypeDescriptor(column_type="float"),
        "p_inet": TableScalarColumnTypeDescriptor(column_type="inet"),
        "p_int": TableScalarColumnTypeDescriptor(column_type="int"),
        "p_smallint": TableScalarColumnTypeDescriptor(column_type="smallint"),
        "p_text": TableScalarColumnTypeDescriptor(column_type="text"),
        "p_text_nulled": TableScalarColumnTypeDescriptor(column_type="text"),
        "p_text_omitted": TableScalarColumnTypeDescriptor(column_type="text"),
        "p_time": TableScalarColumnTypeDescriptor(column_type="time"),
        "p_timestamp": TableScalarColumnTypeDescriptor(column_type="timestamp"),
        "p_tinyint": TableScalarColumnTypeDescriptor(column_type="tinyint"),
        "p_varint": TableScalarColumnTypeDescriptor(column_type="varint"),
        "p_uuid": TableScalarColumnTypeDescriptor(column_type="uuid"),
        "p_vector": TableVectorColumnTypeDescriptor(
            column_type="vector",
            dimension=3,
            service=None,
        ),
        "p_list_int": TableValuedColumnTypeDescriptor(
            column_type="list",
            value_type="int",
        ),
        "p_map_text_text": TableKeyValuedColumnTypeDescriptor(
            column_type="map", key_type="text", value_type="text"
        ),
        "p_set_int": TableValuedColumnTypeDescriptor(
            column_type="set",
            value_type="int",
        ),
        "p_double_minf": TableScalarColumnTypeDescriptor(column_type="double"),
        "p_double_pinf": TableScalarColumnTypeDescriptor(column_type="double"),
        "p_float_nan": TableScalarColumnTypeDescriptor(column_type="float"),
    },
    primary_key=TablePrimaryKeyDescriptor(
        partition_by=["p_ascii", "p_bigint"],
        partition_sort={
            "p_int": SortMode.ASCENDING,
            "p_boolean": SortMode.DESCENDING,
        },
    ),
)

TEST_SIMPLE_TABLE_NAME = "test_table_simple"
TEST_SIMPLE_TABLE_DEFINITION = CreateTableDefinition(
    columns={
        "p_text": TableScalarColumnTypeDescriptor(column_type="text"),
        "p_int": TableScalarColumnTypeDescriptor(column_type="int"),
        "p_vector": TableVectorColumnTypeDescriptor(
            column_type="vector",
            dimension=3,
            service=None,
        ),
    },
    primary_key=TablePrimaryKeyDescriptor(
        partition_by=["p_text"],
        partition_sort={},
    ),
)
TEST_SIMPLE_TABLE_VECTOR_INDEX_NAME = "test_table_simple_p_vector_idx"
TEST_SIMPLE_TABLE_VECTOR_INDEX_COLUMN = "p_vector"
TEST_SIMPLE_TABLE_VECTOR_INDEX_OPTIONS = TableVectorIndexOptions(
    metric="cosine",
)

TEST_COMPOSITE_TABLE_NAME = "test_table_composite"
TEST_COMPOSITE_TABLE_DEFINITION = CreateTableDefinition(
    columns={
        "p_text": TableScalarColumnTypeDescriptor(column_type="text"),
        "p_int": TableScalarColumnTypeDescriptor(column_type="int"),
        "p_boolean": TableScalarColumnTypeDescriptor(column_type="boolean"),
        "p_vector": TableVectorColumnTypeDescriptor(
            column_type="vector",
            dimension=3,
            service=None,
        ),
    },
    primary_key=TablePrimaryKeyDescriptor(
        partition_by=["p_text"],
        partition_sort={"p_int": SortMode.ASCENDING},
    ),
)
TEST_COMPOSITE_TABLE_VECTOR_INDEX_NAME = "test_table_composite_p_vector_idx"
TEST_COMPOSITE_TABLE_VECTOR_INDEX_COLUMN = TEST_SIMPLE_TABLE_VECTOR_INDEX_COLUMN
TEST_COMPOSITE_TABLE_VECTOR_INDEX_OPTIONS = TEST_SIMPLE_TABLE_VECTOR_INDEX_OPTIONS
TEST_COMPOSITE_TABLE_BOOLEAN_INDEX_NAME = "test_table_composite_p_boolean_idx"
TEST_COMPOSITE_TABLE_BOOLEAN_INDEX_COLUMN = "p_boolean"
TEST_COMPOSITE_TABLE_BOOLEAN_INDEX_OPTIONS = None

TEST_ALLMAPS_TABLE_NAME = "test_table_allmaps"
TEST_ALLMAPS_TABLE_DEFINITION = (
    CreateTableDefinition.builder()
    .add_column("id", ColumnType.TEXT)
    .add_map_column("ascii_map", ColumnType.ASCII, ColumnType.ASCII)
    .add_map_column("bigint_map", ColumnType.BIGINT, ColumnType.BIGINT)
    .add_map_column("blob_map", ColumnType.BLOB, ColumnType.BLOB)
    .add_map_column("boolean_map", ColumnType.BOOLEAN, ColumnType.BOOLEAN)
    .add_map_column("date_map", ColumnType.DATE, ColumnType.DATE)
    .add_map_column("decimal_map", ColumnType.DECIMAL, ColumnType.DECIMAL)
    .add_map_column("double_map", ColumnType.DOUBLE, ColumnType.DOUBLE)
    .add_map_column("duration_map", ColumnType.TEXT, ColumnType.DURATION)
    .add_map_column("float_map", ColumnType.FLOAT, ColumnType.FLOAT)
    .add_map_column("inet_map", ColumnType.INET, ColumnType.INET)
    .add_map_column("int_map", ColumnType.INT, ColumnType.INT)
    .add_map_column("smallint_map", ColumnType.SMALLINT, ColumnType.SMALLINT)
    .add_map_column("text_map", ColumnType.TEXT, ColumnType.TEXT)
    .add_map_column("time_map", ColumnType.TIME, ColumnType.TIME)
    .add_map_column("timestamp_map", ColumnType.TIMESTAMP, ColumnType.TIMESTAMP)
    .add_map_column("tinyint_map", ColumnType.TINYINT, ColumnType.TINYINT)
    .add_map_column("uuid_map", ColumnType.UUID, ColumnType.UUID)
    .add_map_column("varint_map", ColumnType.VARINT, ColumnType.VARINT)
    .add_partition_by("id")
    .build()
)

TEST_VECTORIZE_TABLE_NAME = "test_table_vectorize"
TEST_VECTORIZE_TABLE_DEFINITION = CreateTableDefinition(
    columns={
        "p_text": TableScalarColumnTypeDescriptor(column_type="text"),
        "p_vector": TableVectorColumnTypeDescriptor(
            column_type="vector",
            dimension=64,
            service=VectorServiceOptions(
                provider="openai",
                model_name="text-embedding-3-small",
            ),
        ),
    },
    primary_key=TablePrimaryKeyDescriptor(
        partition_by=["p_text"],
        partition_sort={},
    ),
)
TEST_VECTORIZE_TABLE_VECTOR_INDEX_NAME = "test_table_vectorize_p_vector_idx"
TEST_VECTORIZE_TABLE_VECTOR_INDEX_COLUMN = TEST_SIMPLE_TABLE_VECTOR_INDEX_COLUMN
TEST_VECTORIZE_TABLE_VECTOR_INDEX_OPTIONS = TEST_SIMPLE_TABLE_VECTOR_INDEX_OPTIONS

TEST_KMS_VECTORIZE_TABLE_NAME = "test_table_kms_vectorize"
TEST_KMS_VECTORIZE_TABLE_DEFINITION = CreateTableDefinition(
    columns={
        "p_text": TableScalarColumnTypeDescriptor(column_type="text"),
        "p_vector": TableVectorColumnTypeDescriptor(
            column_type="vector",
            dimension=64,
            service=VectorServiceOptions(
                provider="openai",
                model_name="text-embedding-3-small",
                authentication={
                    "providerKey": "SHARED_SECRET_EMBEDDING_API_KEY_OPENAI"
                },
            ),
        ),
    },
    primary_key=TablePrimaryKeyDescriptor(
        partition_by=["p_text"],
        partition_sort={},
    ),
)
TEST_KMS_VECTORIZE_TABLE_VECTOR_INDEX_NAME = "test_table_kms_vectorize_p_vector_idx"
TEST_KMS_VECTORIZE_TABLE_VECTOR_INDEX_COLUMN = TEST_SIMPLE_TABLE_VECTOR_INDEX_COLUMN
TEST_KMS_VECTORIZE_TABLE_VECTOR_INDEX_DEFINITION = (
    TEST_SIMPLE_TABLE_VECTOR_INDEX_OPTIONS
)

VECTORIZE_TEXTS = [
    "The world is the totality of facts, not of things.",
    "The world is determined by the facts, and by these being all the facts.",
    "For the totality of facts determines both what is the case, and also all that is not the case.",
    "The facts in logical space are the world.",
    "The world divides into facts.",
    "Any one can either be the case or not be the case, and everything else remain the same.",
    "What is the case, the fact, is the existence of atomic facts.",
    "An atomic fact is a combination of objects (entities, things).",
    "It is essential to a thing that it can be a constituent part of an atomic fact.",
    "In logic nothing is accidental: if a thing can occur in an atomic fact the possibility of that atomic fact must already be prejudged in the thing.",
    "It would, so to speak, appear as an accident, when to a thing that could exist alone on its own account, subsequently a state of affairs could be made to fit.",
    "If things can occur in atomic facts, this possibility must already lie in them.",
    "(A logical entity cannot be merely possible. Logic treats of every possibility, and all possibilities are its facts.)",
    "Just as we cannot think of spatial objects at all apart from space, or temporal objects apart from time, so we cannot think of any object apart from the possibility of its connection with other things.",
    "If I can think of an object in the context of an atomic fact, I cannot think of it apart from the possibility of this context.",
    "The thing is independent, in so far as it can occur in all possible circumstances, but this form of independence is a form of connection with the atomic fact, a form of dependence. (It is impossible for words to occur in two different ways, alone and in the proposition.)",
    "If I know an object, then I also know all the possibilities of its occurrence in atomic facts.",
    "(Every such possibility must lie in the nature of the object.)",
    "A new possibility cannot subsequently be found.",
    "In order to know an object, I must know not its external but all its internal qualities.",
    "If all objects are given, then thereby are all possible atomic facts also given.",
    "Every thing is, as it were, in a space of possible atomic facts. I can think of this space as empty, but not of the thing without the space.",
    "A spatial object must lie in infinite space. (A point in space is an argument place.)",
    "A speck in a visual field need not be red, but it must have a colour; it has, so to speak, a colour space round it. A tone must have a pitch, the object of the sense of touch a hardness, etc.",
    "Objects contain the possibility of all states of affairs.",
    "The possibility of its occurrence in atomic facts is the form of the object.",
    "The object is simple.",
]
