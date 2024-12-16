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

"""Fixtures specific to the idiomatic testing."""

from __future__ import annotations

import math
from decimal import Decimal
from typing import Any, Dict, Iterable

import pytest

from astrapy import AsyncDatabase, AsyncTable, DataAPIClient, Database, Table
from astrapy.api_options import APIOptions
from astrapy.constants import SortMode
from astrapy.data_types import DataAPIMap, DataAPISet
from astrapy.info import (
    CreateTableDefinition,
    TableKeyValuedColumnTypeDescriptor,
    TablePrimaryKeyDescriptor,
    TableScalarColumnTypeDescriptor,
    TableValuedColumnTypeDescriptor,
    TableVectorColumnTypeDescriptor,
    TableVectorIndexOptions,
    VectorServiceOptions,
)

from ..conftest import (
    IS_ASTRA_DB,
    SECONDARY_KEYSPACE,
    DataAPICredentials,
    DataAPICredentialsInfo,
)

DefaultTable = Table[Dict[str, Any]]
DefaultAsyncTable = AsyncTable[Dict[str, Any]]

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

_NaN = object()
_DNaN = object()


def _repaint_NaNs(val: Any) -> Any:
    if isinstance(val, float) and math.isnan(val):
        return _NaN
    if isinstance(val, Decimal) and math.isnan(val):
        return _DNaN
    elif isinstance(val, dict):
        return {_repaint_NaNs(k): _repaint_NaNs(v) for k, v in val.items()}
    elif isinstance(val, list):
        return [_repaint_NaNs(x) for x in val]
    elif isinstance(val, DataAPISet):
        return DataAPISet(_repaint_NaNs(v) for v in val)
    elif isinstance(val, DataAPIMap):
        return DataAPIMap((_repaint_NaNs(k), _repaint_NaNs(v)) for k, v in val.items())
    elif isinstance(val, set):
        return {_repaint_NaNs(v) for v in val}
    elif isinstance(val, dict):
        return {_repaint_NaNs(k): _repaint_NaNs(v) for k, v in val.items()}
    else:
        return val


def _typify_tuple(tpl: tuple[Any, ...]) -> tuple[Any, ...]:
    return tuple([(v, type(v)) for v in tpl])


@pytest.fixture(scope="session")
def client(
    data_api_credentials_info: DataAPICredentialsInfo,
) -> Iterable[DataAPIClient]:
    env = data_api_credentials_info["environment"]
    client = DataAPIClient(
        environment=env,
        api_options=APIOptions(
            database_additional_headers={"Feature-Flag-tables": "true"}
        ),
    )
    yield client


@pytest.fixture(scope="session")
def sync_database(
    data_api_credentials_kwargs: DataAPICredentials,
    data_api_credentials_info: DataAPICredentialsInfo,
    client: DataAPIClient,
) -> Iterable[Database]:
    database = client.get_database(
        data_api_credentials_kwargs["api_endpoint"],
        token=data_api_credentials_kwargs["token"],
        keyspace=data_api_credentials_kwargs["keyspace"],
    )

    yield database


@pytest.fixture(scope="function")
def async_database(
    sync_database: Database,
) -> Iterable[AsyncDatabase]:
    yield sync_database.to_async()


@pytest.fixture(scope="session")
def sync_table_all_returns(
    data_api_credentials_kwargs: DataAPICredentials,
    sync_database: Database,
) -> Iterable[DefaultTable]:
    """An actual table on DB, in the main keyspace"""
    table = sync_database.create_table(
        TEST_ALL_RETURNS_TABLE_NAME,
        definition=TEST_ALL_RETURNS_TABLE_DEFINITION,
    )
    yield table

    sync_database.drop_table(TEST_ALL_RETURNS_TABLE_NAME)


@pytest.fixture(scope="function")
def sync_empty_table_all_returns(
    sync_table_all_returns: DefaultTable,
) -> Iterable[DefaultTable]:
    """Emptied for each test function"""
    sync_table_all_returns.delete_many({})
    yield sync_table_all_returns


@pytest.fixture(scope="function")
def async_table_all_returns(
    sync_table_all_returns: DefaultTable,
) -> Iterable[DefaultAsyncTable]:
    """An actual table on DB, the same as the sync counterpart"""
    yield sync_table_all_returns.to_async()


@pytest.fixture(scope="function")
def async_empty_table_all_returns(
    sync_empty_table_all_returns: DefaultTable,
) -> Iterable[DefaultAsyncTable]:
    """Emptied for each test function"""
    yield sync_empty_table_all_returns.to_async()


@pytest.fixture(scope="session")
def sync_table_simple(
    data_api_credentials_kwargs: DataAPICredentials,
    sync_database: Database,
) -> Iterable[DefaultTable]:
    """An actual table on DB, in the main keyspace"""
    table = sync_database.create_table(
        TEST_SIMPLE_TABLE_NAME,
        definition=TEST_SIMPLE_TABLE_DEFINITION,
    )
    table.create_vector_index(
        TEST_SIMPLE_TABLE_VECTOR_INDEX_NAME,
        column=TEST_SIMPLE_TABLE_VECTOR_INDEX_COLUMN,
        options=TEST_SIMPLE_TABLE_VECTOR_INDEX_OPTIONS,
    )
    yield table

    sync_database.drop_table(TEST_SIMPLE_TABLE_NAME)


@pytest.fixture(scope="function")
def sync_empty_table_simple(
    sync_table_simple: DefaultTable,
) -> Iterable[DefaultTable]:
    """Emptied for each test function"""
    sync_table_simple.delete_many({})
    yield sync_table_simple


@pytest.fixture(scope="function")
def async_table_simple(
    sync_table_simple: DefaultTable,
) -> Iterable[DefaultAsyncTable]:
    """An actual table on DB, the same as the sync counterpart"""
    yield sync_table_simple.to_async()


@pytest.fixture(scope="function")
def async_empty_table_simple(
    sync_empty_table_simple: DefaultTable,
) -> Iterable[DefaultAsyncTable]:
    """Emptied for each test function"""
    yield sync_empty_table_simple.to_async()


@pytest.fixture(scope="session")
def sync_table_composite(
    data_api_credentials_kwargs: DataAPICredentials,
    sync_database: Database,
) -> Iterable[DefaultTable]:
    """An actual table on DB, in the main keyspace"""
    table = sync_database.create_table(
        TEST_COMPOSITE_TABLE_NAME,
        definition=TEST_COMPOSITE_TABLE_DEFINITION,
    )
    table.create_vector_index(
        TEST_COMPOSITE_TABLE_VECTOR_INDEX_NAME,
        column=TEST_COMPOSITE_TABLE_VECTOR_INDEX_COLUMN,
        options=TEST_COMPOSITE_TABLE_VECTOR_INDEX_OPTIONS,
    )
    table.create_index(
        TEST_COMPOSITE_TABLE_BOOLEAN_INDEX_NAME,
        column=TEST_COMPOSITE_TABLE_BOOLEAN_INDEX_COLUMN,
        options=TEST_COMPOSITE_TABLE_BOOLEAN_INDEX_OPTIONS,
    )
    yield table

    sync_database.drop_table(TEST_COMPOSITE_TABLE_NAME)


@pytest.fixture(scope="function")
def sync_empty_table_composite(
    sync_table_composite: DefaultTable,
) -> Iterable[DefaultTable]:
    """Emptied for each test function"""
    sync_table_composite.delete_many({})
    yield sync_table_composite


@pytest.fixture(scope="function")
def async_table_composite(
    sync_table_composite: DefaultTable,
) -> Iterable[DefaultAsyncTable]:
    """An actual table on DB, the same as the sync counterpart"""
    yield sync_table_composite.to_async()


@pytest.fixture(scope="function")
def async_empty_table_composite(
    sync_empty_table_composite: DefaultTable,
) -> Iterable[DefaultAsyncTable]:
    """Emptied for each test function"""
    yield sync_empty_table_composite.to_async()


@pytest.fixture(scope="session")
def sync_table_vectorize(
    data_api_credentials_kwargs: DataAPICredentials,
    sync_database: Database,
) -> Iterable[DefaultTable]:
    """An actual table on DB, in the main keyspace"""
    table = sync_database.create_table(
        TEST_VECTORIZE_TABLE_NAME,
        definition=TEST_VECTORIZE_TABLE_DEFINITION,
    )
    table.create_vector_index(
        TEST_VECTORIZE_TABLE_VECTOR_INDEX_NAME,
        column=TEST_SIMPLE_TABLE_VECTOR_INDEX_COLUMN,
        options=TEST_SIMPLE_TABLE_VECTOR_INDEX_OPTIONS,
    )
    yield table

    sync_database.drop_table(TEST_VECTORIZE_TABLE_NAME)


@pytest.fixture(scope="function")
def sync_empty_table_vectorize(
    sync_table_vectorize: DefaultTable,
) -> Iterable[DefaultTable]:
    """Emptied for each test function"""
    sync_table_vectorize.delete_many({})
    yield sync_table_vectorize


@pytest.fixture(scope="function")
def async_table_vectorize(
    sync_table_vectorize: DefaultTable,
) -> Iterable[DefaultAsyncTable]:
    """An actual table on DB, the same as the sync counterpart"""
    yield sync_table_vectorize.to_async()


@pytest.fixture(scope="function")
def async_empty_table_vectorize(
    sync_empty_table_vectorize: DefaultTable,
) -> Iterable[DefaultAsyncTable]:
    """Emptied for each test function"""
    yield sync_empty_table_vectorize.to_async()


@pytest.fixture(scope="session")
def sync_table_kms_vectorize(
    data_api_credentials_kwargs: DataAPICredentials,
    sync_database: Database,
) -> Iterable[DefaultTable]:
    """An actual table on DB, in the main keyspace"""
    table = sync_database.create_table(
        TEST_KMS_VECTORIZE_TABLE_NAME,
        definition=TEST_KMS_VECTORIZE_TABLE_DEFINITION,
    )
    table.create_vector_index(
        TEST_KMS_VECTORIZE_TABLE_VECTOR_INDEX_NAME,
        column=TEST_SIMPLE_TABLE_VECTOR_INDEX_COLUMN,
        options=TEST_SIMPLE_TABLE_VECTOR_INDEX_OPTIONS,
    )
    yield table

    sync_database.drop_table(TEST_KMS_VECTORIZE_TABLE_NAME)


@pytest.fixture(scope="function")
def sync_empty_table_kms_vectorize(
    sync_table_kms_vectorize: DefaultTable,
) -> Iterable[DefaultTable]:
    """Emptied for each test function"""
    sync_table_kms_vectorize.delete_many({})
    yield sync_table_kms_vectorize


@pytest.fixture(scope="function")
def async_table_kms_vectorize(
    sync_table_kms_vectorize: DefaultTable,
) -> Iterable[DefaultAsyncTable]:
    """An actual table on DB, the same as the sync counterpart"""
    yield sync_table_kms_vectorize.to_async()


@pytest.fixture(scope="function")
def async_empty_table_kms_vectorize(
    sync_empty_table_kms_vectorize: DefaultTable,
) -> Iterable[DefaultAsyncTable]:
    """Emptied for each test function"""
    yield sync_empty_table_kms_vectorize.to_async()


__all__ = [
    "DataAPICredentials",
    "DataAPICredentialsInfo",
    "sync_database",
    "async_database",
    "IS_ASTRA_DB",
    "SECONDARY_KEYSPACE",
    "_repaint_NaNs",
    "_typify_tuple",
]
