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

import math
import os
from decimal import Decimal
from typing import Any, Dict, Iterable

import pytest

from astrapy import (
    AsyncCollection,
    AsyncTable,
    Collection,
    Database,
    Table,
)
from astrapy.api_options import APIOptions, SerdesOptions
from astrapy.constants import VectorMetric
from astrapy.data_types import DataAPIMap, DataAPISet
from astrapy.info import (
    CollectionDefinition,
    CollectionLexicalOptions,
    CollectionRerankOptions,
    CollectionVectorOptions,
    RerankServiceOptions,
)
from astrapy.utils.unset import _UNSET, UnsetType

from ..conftest import (
    ADMIN_ENV_LIST,
    ADMIN_ENV_VARIABLE_MAP,
    CQL_AVAILABLE,
    HEADER_EMBEDDING_API_KEY_OPENAI,
    HEADER_RERANKING_API_KEY_NVIDIA,
    IS_ASTRA_DB,
    SECONDARY_KEYSPACE,
    DataAPICredentials,
    DataAPICredentialsInfo,
    async_fail_if_not_removed,
    clean_nulls_from_dict,
    is_future_version,
    sync_fail_if_not_removed,
)
from .table_structure_assets import (
    TEST_ALL_RETURNS_TABLE_DEFINITION,
    TEST_ALL_RETURNS_TABLE_NAME,
    TEST_ALLMAPS_TABLE_DEFINITION,
    TEST_ALLMAPS_TABLE_NAME,
    TEST_COLLINDEXED_TABLE_DEFINITION,
    TEST_COLLINDEXED_TABLE_INDEXES,
    TEST_COLLINDEXED_TABLE_NAME,
    TEST_COMPOSITE_TABLE_BOOLEAN_INDEX_COLUMN,
    TEST_COMPOSITE_TABLE_BOOLEAN_INDEX_NAME,
    TEST_COMPOSITE_TABLE_BOOLEAN_INDEX_OPTIONS,
    TEST_COMPOSITE_TABLE_DEFINITION,
    TEST_COMPOSITE_TABLE_NAME,
    TEST_COMPOSITE_TABLE_VECTOR_INDEX_COLUMN,
    TEST_COMPOSITE_TABLE_VECTOR_INDEX_NAME,
    TEST_COMPOSITE_TABLE_VECTOR_INDEX_OPTIONS,
    TEST_KMS_VECTORIZE_TABLE_DEFINITION,
    TEST_KMS_VECTORIZE_TABLE_NAME,
    TEST_KMS_VECTORIZE_TABLE_VECTOR_INDEX_NAME,
    TEST_SIMPLE_TABLE_DEFINITION,
    TEST_SIMPLE_TABLE_NAME,
    TEST_SIMPLE_TABLE_VECTOR_INDEX_COLUMN,
    TEST_SIMPLE_TABLE_VECTOR_INDEX_NAME,
    TEST_SIMPLE_TABLE_VECTOR_INDEX_OPTIONS,
    TEST_SIMPLE_UDT_DEFINITION,
    TEST_SIMPLE_UDT_NAME,
    TEST_VECTORIZE_TABLE_DEFINITION,
    TEST_VECTORIZE_TABLE_NAME,
    TEST_VECTORIZE_TABLE_VECTOR_INDEX_NAME,
    VECTORIZE_TEXTS,
)
from .table_udt_assets import (
    EXTENDED_PLAYER_TABLE_DEFINITION,
    EXTENDED_PLAYER_TABLE_NAME,
    EXTENDED_PLAYER_TYPE_DEFINITION,
    EXTENDED_PLAYER_TYPE_NAME,
    PLAYER_TABLE_DEFINITION,
    PLAYER_TABLE_NAME,
    PLAYER_TYPE_DEFINITION,
    PLAYER_TYPE_NAME,
)

DefaultCollection = Collection[Dict[str, Any]]
DefaultAsyncCollection = AsyncCollection[Dict[str, Any]]
DefaultTable = Table[Dict[str, Any]]
DefaultAsyncTable = AsyncTable[Dict[str, Any]]

TEST_COLLECTION_INSTANCE_NAME = "test_coll_instance"
TEST_COLLECTION_NAME = "id_test_collection"
TEST_SERVICE_COLLECTION_NAME = "test_indepth_vectorize_collection"
TEST_TABLE_INSTANCE_NAME = "test_tbl_instance"
TEST_FARR_VECTORIZE_COLLECTION_NAME = "test_farr_collection_vectorize"
TEST_FARR_VECTOR_COLLECTION_NAME = "test_farr_collection_vector"

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
def sync_collection_instance(
    data_api_credentials_kwargs: DataAPICredentials,
    sync_database: Database,
) -> Iterable[DefaultCollection]:
    """Just an instance of the class, no DB-level stuff."""
    yield sync_database.get_collection(TEST_COLLECTION_INSTANCE_NAME)


@pytest.fixture(scope="function")
def async_collection_instance(
    sync_collection_instance: DefaultCollection,
) -> Iterable[DefaultAsyncCollection]:
    """Just an instance of the class, no DB-level stuff."""
    yield sync_collection_instance.to_async()


@pytest.fixture(scope="session")
def sync_collection(
    data_api_credentials_kwargs: DataAPICredentials,
    sync_database: Database,
) -> Iterable[DefaultCollection]:
    """An actual collection on DB, in the main keyspace"""
    collection = sync_database.create_collection(
        TEST_COLLECTION_NAME,
        definition=CollectionDefinition(
            indexing={"deny": ["not_indexed"]},
            vector=CollectionVectorOptions(
                dimension=2,
                metric=VectorMetric.COSINE,
            ),
        ),
        spawn_api_options=APIOptions(
            serdes_options=SerdesOptions(
                binary_encode_vectors=True,
                custom_datatypes_in_reading=True,
                unroll_iterables_to_lists=True,
            ),
        ),
    )
    yield collection

    sync_database.drop_collection(TEST_COLLECTION_NAME)


@pytest.fixture(scope="function")
def sync_empty_collection(
    sync_collection: DefaultCollection,
) -> Iterable[DefaultCollection]:
    """Emptied for each test function"""
    sync_collection.delete_many({})
    yield sync_collection


@pytest.fixture(scope="function")
def async_collection(
    sync_collection: DefaultCollection,
) -> Iterable[DefaultAsyncCollection]:
    """An actual collection on DB, the same as the sync counterpart"""
    yield sync_collection.to_async()


@pytest.fixture(scope="function")
def async_empty_collection(
    sync_empty_collection: DefaultCollection,
) -> Iterable[DefaultAsyncCollection]:
    """Emptied for each test function"""
    yield sync_empty_collection.to_async()


@pytest.fixture(scope="session")
def service_collection_parameters() -> Iterable[dict[str, Any]]:
    yield {
        "dimension": 1536,
        "provider": "openai",
        "modelName": "text-embedding-ada-002",
        "api_key": HEADER_EMBEDDING_API_KEY_OPENAI,
        "reranking_api_key": HEADER_RERANKING_API_KEY_NVIDIA,
    }


@pytest.fixture(scope="session")
def sync_service_collection(
    data_api_credentials_kwargs: DataAPICredentials,
    sync_database: Database,
    service_collection_parameters: dict[str, Any],
) -> Iterable[DefaultCollection]:
    """
    An actual collection on DB, in the main keyspace.
    """
    params = service_collection_parameters
    collection = sync_database.create_collection(
        TEST_SERVICE_COLLECTION_NAME,
        definition=(
            CollectionDefinition.builder()
            .set_vector_metric(VectorMetric.DOT_PRODUCT)
            .set_vector_service(
                provider=params["provider"],
                model_name=params["modelName"],
            )
            .build()
        ),
        embedding_api_key=params["api_key"],
    )
    yield collection

    sync_database.drop_collection(TEST_SERVICE_COLLECTION_NAME)


@pytest.fixture(scope="function")
def sync_empty_service_collection(
    sync_service_collection: DefaultCollection,
) -> Iterable[DefaultCollection]:
    """Emptied for each test function"""
    sync_service_collection.delete_many({})
    yield sync_service_collection


@pytest.fixture(scope="function")
def async_empty_service_collection(
    sync_empty_service_collection: DefaultCollection,
) -> Iterable[DefaultAsyncCollection]:
    """Emptied for each test function"""
    yield sync_empty_service_collection.to_async()


@pytest.fixture(scope="session")
def rerankservice_collection_parameters() -> Iterable[CollectionRerankOptions]:
    yield CollectionRerankOptions(
        service=RerankServiceOptions(
            provider="nvidia",
            model_name="nvidia/llama-3.2-nv-rerankqa-1b-v2",
        ),
    )


@pytest.fixture(scope="session")
def lexical_collection_parameters() -> Iterable[CollectionLexicalOptions]:
    yield CollectionLexicalOptions(
        analyzer="STANDARD",
    )


@pytest.fixture(scope="module")
def sync_farr_vectorize_collection(
    data_api_credentials_kwargs: DataAPICredentials,
    sync_database: Database,
    service_collection_parameters: dict[str, Any],
    rerankservice_collection_parameters: CollectionRerankOptions,
    lexical_collection_parameters: CollectionLexicalOptions,
) -> Iterable[DefaultCollection]:
    """
    An actual collection on DB, in the main keyspace.
    """
    params = service_collection_parameters

    # reranker credentials through header is needed for non-Astra (HCD) runs
    reranking_api_key: str | UnsetType
    if "ASTRAPY_FINDANDRERANK_USE_RERANKER_HEADER" in os.environ:
        assert params["reranking_api_key"] is not None
        reranking_api_key = params["reranking_api_key"]
    else:
        reranking_api_key = _UNSET

    collection = sync_database.create_collection(
        TEST_FARR_VECTORIZE_COLLECTION_NAME,
        definition=(
            CollectionDefinition.builder()
            .set_vector_metric(VectorMetric.DOT_PRODUCT)
            .set_vector_service(
                provider=params["provider"],
                model_name=params["modelName"],
            )
            .set_rerank(rerankservice_collection_parameters)
            .set_lexical(lexical_collection_parameters)
            .build()
        ),
        embedding_api_key=params["api_key"],
        reranking_api_key=reranking_api_key,
    )
    yield collection

    sync_database.drop_collection(TEST_FARR_VECTORIZE_COLLECTION_NAME)


@pytest.fixture(scope="function")
def sync_empty_farr_vectorize_collection(
    sync_farr_vectorize_collection: DefaultCollection,
) -> Iterable[DefaultCollection]:
    """Emptied for each test function"""
    sync_farr_vectorize_collection.delete_many({})
    yield sync_farr_vectorize_collection


@pytest.fixture(scope="function")
def async_empty_farr_vectorize_collection(
    sync_empty_farr_vectorize_collection: DefaultCollection,
) -> Iterable[DefaultAsyncCollection]:
    """Emptied for each test function"""
    yield sync_empty_farr_vectorize_collection.to_async()


@pytest.fixture(scope="module")
def sync_farr_vector_collection(
    data_api_credentials_kwargs: DataAPICredentials,
    sync_database: Database,
    service_collection_parameters: dict[str, Any],
    rerankservice_collection_parameters: CollectionRerankOptions,
    lexical_collection_parameters: CollectionLexicalOptions,
) -> Iterable[DefaultCollection]:
    """
    An actual collection on DB, in the main keyspace.
    """
    params = service_collection_parameters

    # reranker credentials through header is needed for non-Astra (HCD) runs
    reranking_api_key: str | UnsetType
    if "ASTRAPY_FINDANDRERANK_USE_RERANKER_HEADER" in os.environ:
        assert params["reranking_api_key"] is not None
        reranking_api_key = params["reranking_api_key"]
    else:
        reranking_api_key = _UNSET

    collection = sync_database.create_collection(
        TEST_FARR_VECTOR_COLLECTION_NAME,
        definition=(
            CollectionDefinition.builder()
            .set_vector_metric(VectorMetric.DOT_PRODUCT)
            .set_vector_dimension(2)
            .set_rerank(rerankservice_collection_parameters)
            .set_lexical(lexical_collection_parameters)
            .build()
        ),
        reranking_api_key=reranking_api_key,
    )
    yield collection

    sync_database.drop_collection(TEST_FARR_VECTOR_COLLECTION_NAME)


@pytest.fixture(scope="function")
def sync_empty_farr_vector_collection(
    sync_farr_vector_collection: DefaultCollection,
) -> Iterable[DefaultCollection]:
    """Emptied for each test function"""
    sync_farr_vector_collection.delete_many({})
    yield sync_farr_vector_collection


@pytest.fixture(scope="function")
def async_empty_farr_vector_collection(
    sync_empty_farr_vector_collection: DefaultCollection,
) -> Iterable[DefaultAsyncCollection]:
    """Emptied for each test function"""
    yield sync_empty_farr_vector_collection.to_async()


@pytest.fixture(scope="session")
def simple_udt(
    sync_database: Database,
) -> Iterable[str]:
    """Creation (and then cleanup) of a UDT. Returns the type name."""
    sync_database.create_type(
        TEST_SIMPLE_UDT_NAME,
        definition=TEST_SIMPLE_UDT_DEFINITION,
    )
    yield TEST_SIMPLE_UDT_NAME

    sync_database.drop_type(TEST_SIMPLE_UDT_NAME)


@pytest.fixture(scope="session")
def player_udt(
    sync_database: Database,
) -> Iterable[str]:
    """Creation (and then cleanup) of a UDT. Returns the type name."""
    sync_database.create_type(
        PLAYER_TYPE_NAME,
        definition=PLAYER_TYPE_DEFINITION,
    )
    yield PLAYER_TYPE_NAME

    sync_database.drop_type(PLAYER_TYPE_NAME)


@pytest.fixture(scope="session")
def extended_player_udt(
    sync_database: Database,
) -> Iterable[str]:
    """Creation (and then cleanup) of a UDT. Returns the type name."""
    sync_database.create_type(
        EXTENDED_PLAYER_TYPE_NAME,
        definition=EXTENDED_PLAYER_TYPE_DEFINITION,
    )
    yield EXTENDED_PLAYER_TYPE_NAME

    sync_database.drop_type(EXTENDED_PLAYER_TYPE_NAME)


@pytest.fixture(scope="session")
def sync_table_instance(
    data_api_credentials_kwargs: DataAPICredentials,
    sync_database: Database,
) -> Iterable[DefaultTable]:
    """Just an instance of the class, no DB-level stuff."""
    yield sync_database.get_table(TEST_TABLE_INSTANCE_NAME)


@pytest.fixture(scope="function")
def async_table_instance(
    sync_table_instance: DefaultTable,
) -> Iterable[DefaultAsyncTable]:
    """Just an instance of the class, no DB-level stuff."""
    yield sync_table_instance.to_async()


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
        TEST_SIMPLE_TABLE_VECTOR_INDEX_COLUMN,
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
        TEST_COMPOSITE_TABLE_VECTOR_INDEX_COLUMN,
        options=TEST_COMPOSITE_TABLE_VECTOR_INDEX_OPTIONS,
    )
    table.create_index(
        TEST_COMPOSITE_TABLE_BOOLEAN_INDEX_NAME,
        TEST_COMPOSITE_TABLE_BOOLEAN_INDEX_COLUMN,
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
        TEST_SIMPLE_TABLE_VECTOR_INDEX_COLUMN,
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
        TEST_SIMPLE_TABLE_VECTOR_INDEX_COLUMN,
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


@pytest.fixture(scope="session")
def sync_table_allmaps(
    data_api_credentials_kwargs: DataAPICredentials,
    sync_database: Database,
) -> Iterable[DefaultTable]:
    """An actual table on DB, in the main keyspace"""
    table = sync_database.create_table(
        TEST_ALLMAPS_TABLE_NAME,
        definition=TEST_ALLMAPS_TABLE_DEFINITION,
    )
    yield table

    sync_database.drop_table(TEST_ALLMAPS_TABLE_NAME)


@pytest.fixture(scope="function")
def sync_empty_table_allmaps(
    sync_table_allmaps: DefaultTable,
) -> Iterable[DefaultTable]:
    """Emptied for each test function"""
    sync_table_allmaps.delete_many({})
    yield sync_table_allmaps


@pytest.fixture(scope="function")
def async_table_allmaps(
    sync_table_allmaps: DefaultTable,
) -> Iterable[DefaultAsyncTable]:
    """An actual table on DB, the same as the sync counterpart"""
    yield sync_table_allmaps.to_async()


@pytest.fixture(scope="function")
def async_empty_table_allmaps(
    sync_empty_table_allmaps: DefaultTable,
) -> Iterable[DefaultAsyncTable]:
    """Emptied for each test function"""
    yield sync_empty_table_allmaps.to_async()


@pytest.fixture(scope="session")
def sync_table_udt_player(
    sync_database: Database,
    player_udt: str,
) -> Iterable[DefaultTable]:
    """An actual table on DB, in the main keyspace"""
    table = sync_database.create_table(
        PLAYER_TABLE_NAME,
        definition=PLAYER_TABLE_DEFINITION,
    )
    yield table

    sync_database.drop_table(PLAYER_TABLE_NAME)


@pytest.fixture(scope="session")
def sync_table_udt_extended_player(
    sync_database: Database,
    extended_player_udt: str,
) -> Iterable[DefaultTable]:
    """An actual table on DB, in the main keyspace"""
    table = sync_database.create_table(
        EXTENDED_PLAYER_TABLE_NAME,
        definition=EXTENDED_PLAYER_TABLE_DEFINITION,
    )
    yield table


@pytest.fixture(scope="session")
def sync_table_collindexed(
    sync_database: Database,
) -> Iterable[DefaultTable]:
    """An actual table on DB, in the main keyspace"""
    table = sync_database.create_table(
        TEST_COLLINDEXED_TABLE_NAME,
        definition=TEST_COLLINDEXED_TABLE_DEFINITION,
    )
    for index_name, index_column in TEST_COLLINDEXED_TABLE_INDEXES:
        table.create_index(index_name, index_column)

    yield table

    sync_database.drop_table(TEST_COLLINDEXED_TABLE_NAME)


@pytest.fixture(scope="function")
def sync_empty_table_udt_player(
    sync_table_udt_player: DefaultTable,
) -> Iterable[DefaultTable]:
    """Emptied for each test function"""
    sync_table_udt_player.delete_many({})
    yield sync_table_udt_player


@pytest.fixture(scope="function")
def sync_empty_table_udt_extended_player(
    sync_table_udt_extended_player: DefaultTable,
) -> Iterable[DefaultTable]:
    """Emptied for each test function"""
    sync_table_udt_extended_player.delete_many({})
    yield sync_table_udt_extended_player


@pytest.fixture(scope="function")
def sync_empty_table_collindexed(
    sync_table_collindexed: DefaultTable,
) -> Iterable[DefaultTable]:
    """Emptied for each test function"""
    sync_table_collindexed.delete_many({})
    yield sync_table_collindexed


@pytest.fixture(scope="function")
def async_empty_table_udt_player(
    sync_empty_table_udt_player: DefaultTable,
) -> Iterable[DefaultAsyncTable]:
    """Emptied for each test function"""
    yield sync_empty_table_udt_player.to_async()


@pytest.fixture(scope="function")
def async_empty_table_udt_extended_player(
    sync_empty_table_udt_extended_player: DefaultTable,
) -> Iterable[DefaultAsyncTable]:
    """Emptied for each test function"""
    yield sync_empty_table_udt_extended_player.to_async()


@pytest.fixture(scope="function")
def async_empty_table_collindexed(
    sync_empty_table_collindexed: DefaultTable,
) -> Iterable[DefaultAsyncTable]:
    """Emptied for each test function"""
    yield sync_empty_table_collindexed.to_async()


__all__ = [
    "DataAPICredentials",
    "DataAPICredentialsInfo",
    "async_fail_if_not_removed",
    "clean_nulls_from_dict",
    "is_future_version",
    "sync_fail_if_not_removed",
    "HEADER_EMBEDDING_API_KEY_OPENAI",
    "IS_ASTRA_DB",
    "ADMIN_ENV_LIST",
    "ADMIN_ENV_VARIABLE_MAP",
    "CQL_AVAILABLE",
    "SECONDARY_KEYSPACE",
    "VECTORIZE_TEXTS",
    "_repaint_NaNs",
    "_typify_tuple",
]
