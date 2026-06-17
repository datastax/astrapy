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

import pytest

from astrapy import Database

from ..conftest import (
    ADMIN_ENV_LIST,
    ADMIN_ENV_VARIABLE_MAP,
    CQL_AVAILABLE,
    EMBEDDING_PROVIDER_API_KEY,
    EMBEDDING_PROVIDER_DIMENSION,
    EMBEDDING_PROVIDER_SHARED_SECRET_KEY_NAME,
    IS_ASTRA_DB,
    RUN_SHARED_SECRET_VECTORIZE_TESTS,
    SECONDARY_KEYSPACE,
    TEST_COLLECTION_NAME,
    USE_RERANKER_API_KEY_HEADER,
    VECTORIZE_TEXTS,
    DataAPICredentials,
    DataAPICredentialsInfo,
    DefaultAsyncCollection,
    DefaultAsyncTable,
    DefaultCollection,
    DefaultTable,
    _repaint_NaNs,
    _typify_tuple,
    async_fail_if_not_removed,
    clean_nulls_from_dict,
    sync_fail_if_not_removed,
)
from ..table_structure_assets import dict_equal_same_class
from ..table_udt_assets import (
    EXTENDED_PLAYER_TYPE_NAME,
    PLAYER_TYPE_NAME,
    THE_BYTES,
    THE_DATETIME,
    THE_TIMESTAMP,
    THE_TIMEZONE,
    ExtendedPlayer,
    NullablePlayer,
    Player,
    _extended_player_from_dict,
    _extended_player_serializer,
    _nullable_player_from_dict,
    _nullable_player_serializer,
    _player_from_dict,
    _player_serializer,
)


@pytest.fixture(scope="session", autouse=True)
def require_empty_target_database(sync_database: Database) -> None:
    """
    Refuse to run the integration suite against a populated database.

    The base integration tests freely create and drop collections, tables and
    keyspaces on the target database. Running them against a database that is
    already in use risks clobbering its data, so the working keyspace must be
    empty (no collections and no tables).

    When the keyspace is not empty the behaviour depends on the environment:

    - in CI the run is failed hard (``pytest.exit`` with a non-zero code). A
      skipped suite would exit 0 and report a green check, which could let an
      untested PR be merged; failing instead keeps the merge gate honest (and
      flags that the CI test database needs cleaning).
    - locally the suite is simply skipped, to protect the developer's data.

    Being an autouse, session-scoped fixture, this check runs once and before
    any test collection/table is provisioned (autouse session fixtures are set
    up ahead of the fixtures that create the test data), so a populated database
    is never mutated either way.
    """
    collection_names = sorted(sync_database.list_collection_names())
    table_names = sorted(sync_database.list_table_names())
    if not (collection_names or table_names):
        return

    message = (
        f"Target database keyspace '{sync_database.keyspace}' is not empty "
        f"(collections={collection_names}, tables={table_names}). The "
        "integration tests require an empty database (they create and drop "
        "collections and tables); clean the target keyspace or point the tests "
        "at a dedicated, empty one."
    )
    # GitHub Actions (and most CI systems) set CI=true.
    in_ci = os.environ.get("CI", "").lower() in {"true", "1", "yes"}
    if in_ci:
        pytest.exit(message, returncode=1)
    pytest.skip(message)


__all__ = [
    "DataAPICredentials",
    "DataAPICredentialsInfo",
    "async_fail_if_not_removed",
    "clean_nulls_from_dict",
    "sync_fail_if_not_removed",
    "EMBEDDING_PROVIDER_API_KEY",
    "EMBEDDING_PROVIDER_DIMENSION",
    "EMBEDDING_PROVIDER_SHARED_SECRET_KEY_NAME",
    "IS_ASTRA_DB",
    "ADMIN_ENV_LIST",
    "ADMIN_ENV_VARIABLE_MAP",
    "CQL_AVAILABLE",
    "EXTENDED_PLAYER_TYPE_NAME",
    "PLAYER_TYPE_NAME",
    "RUN_SHARED_SECRET_VECTORIZE_TESTS",
    "SECONDARY_KEYSPACE",
    "TEST_COLLECTION_NAME",
    "THE_BYTES",
    "THE_DATETIME",
    "THE_TIMEZONE",
    "THE_TIMESTAMP",
    "USE_RERANKER_API_KEY_HEADER",
    "VECTORIZE_TEXTS",
    "dict_equal_same_class",
    "_extended_player_from_dict",
    "_extended_player_serializer",
    "_nullable_player_from_dict",
    "_nullable_player_serializer",
    "_player_from_dict",
    "_player_serializer",
    "_repaint_NaNs",
    "_typify_tuple",
    "DefaultCollection",
    "DefaultAsyncCollection",
    "DefaultAsyncTable",
    "DefaultTable",
    "ExtendedPlayer",
    "NullablePlayer",
    "Player",
]
