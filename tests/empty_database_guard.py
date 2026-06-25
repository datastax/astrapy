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


def _user_keyspace_names(database: Database, *, is_astra_db: bool) -> list[str]:
    system_keyspaces = (
        ASTRA_DB_SYSTEM_KEYSPACES if is_astra_db else HCD_SYSTEM_KEYSPACES
    )
    return sorted(
        keyspace
        for keyspace in database.get_database_admin().list_keyspaces()
        if keyspace not in system_keyspaces
    )


def _collect_existing_database_items(
    database: Database,
    *,
    is_astra_db: bool,
) -> dict[str, list[str]]:
    collected_items: dict[str, list[str]] = {
        "collections": [],
        "tables": [],
        "types": [],
    }

    for keyspace in _user_keyspace_names(database, is_astra_db=is_astra_db):
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


def ensure_empty_target_database(
    database: Database,
    *,
    is_astra_db: bool,
    test_suite_name: str,
) -> None:
    if _is_populated_database_tolerated():
        return

    collected_items = _collect_existing_database_items(
        database,
        is_astra_db=is_astra_db,
    )
    if not any(collected_items.values()):
        return

    items_summary = _existing_items_summary(collected_items)
    pytest.exit(
        "Non-empty target database detected. "
        f"The {test_suite_name} require no collections, tables or UDTs in "
        f"any non-system keyspace before they start. Items found: {items_summary}. "
        "Clean the target database, point the tests at a dedicated empty one, "
        f"or set {TOLERATE_POPULATED_DATABASE_ENV}=yes for an intentional "
        "narrow test run.",
        returncode=1,
    )
