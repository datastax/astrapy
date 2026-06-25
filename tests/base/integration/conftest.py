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

TOLERATE_POPULATED_DATABASE_ENV = "TOLERATE_POPULATED_DATABASE"

ASTRA_DB_SYSTEM_KEYSPACES = frozenset(
    {
        "data_endpoint_auth",
        "datastax_sla",
        "system",
        "system_auth",
        "system_schema",
        "system_traces",
        "system_views",
        "system_virtual_schema",
    }
)
HCD_SYSTEM_KEYSPACES = frozenset(
    {
        "system",
        "system_auth",
        "system_distributed",
        "system_schema",
        "system_traces",
        "system_views",
        "system_virtual_schema",
    }
)


def _is_populated_database_tolerated() -> bool:
    return os.environ.get(TOLERATE_POPULATED_DATABASE_ENV, "").strip().lower() in {
        "1",
        "true",
        "yes",
    }


def _user_keyspace_names(database: Database) -> list[str]:
    system_keyspaces = (
        ASTRA_DB_SYSTEM_KEYSPACES if IS_ASTRA_DB else HCD_SYSTEM_KEYSPACES
    )
    return sorted(
        keyspace
        for keyspace in database.get_database_admin().list_keyspaces()
        if keyspace not in system_keyspaces
    )


def _collect_existing_database_items(database: Database) -> dict[str, list[str]]:
    collected_items: dict[str, list[str]] = {
        "collections": [],
        "tables": [],
        "types": [],
    }

    for keyspace in _user_keyspace_names(database):
        keyspace_database = database.with_options(keyspace=keyspace)
        collected_items["collections"].extend(
            f"{keyspace}.{name}" for name in keyspace_database.list_collection_names()
        )
        collected_items["tables"].extend(
            f"{keyspace}.{name}" for name in keyspace_database.list_table_names()
        )
        collected_items["types"].extend(
            f"{keyspace}.{name}" for name in keyspace_database.list_type_names()
        )

    return {
        item_kind: sorted(item_names)
        for item_kind, item_names in collected_items.items()
    }


def _existing_items_summary(collected_items: dict[str, list[str]]) -> str:
    return "; ".join(
        f"{item_kind}: {', '.join(item_names)}"
        for item_kind, item_names in collected_items.items()
        if item_names
    )


@pytest.fixture(scope="session", autouse=True)
def require_empty_target_database(sync_database: Database) -> None:
    """
    Refuse to run the integration suite against a populated database.

    The base integration tests freely create and drop collections, tables and
    keyspaces on the target database. Starting from a non-empty database can
    make the suite fail much later with avoidable object-limit or conflict
    errors, so all non-system keyspaces must be free of collections, tables and
    user-defined types.
    """
    if _is_populated_database_tolerated():
        return

    collected_items = _collect_existing_database_items(sync_database)
    if not any(collected_items.values()):
        return

    items_summary = _existing_items_summary(collected_items)
    pytest.exit(
        "Non-empty target database detected. "
        "The base integration tests require no collections, tables or UDTs in "
        f"any non-system keyspace before they start. Items found: {items_summary}. "
        "Clean the target database, point the tests at a dedicated empty one, "
        f"or set {TOLERATE_POPULATED_DATABASE_ENV}=yes for an intentional "
        "narrow test run.",
        returncode=1,
    )


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
