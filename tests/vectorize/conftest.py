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

from astrapy import Database

from ..conftest import (
    IS_ASTRA_DB,
)
from ..empty_database_guard import ensure_empty_target_database


@pytest.fixture(scope="session", autouse=True)
def require_empty_target_database(sync_database: Database) -> None:
    """
    Refuse to run the vectorize integration suite against a populated database.

    Starting from a non-empty database can make the suite fail much later with
    avoidable object-limit or conflict errors, so all non-system keyspaces must
    be free of collections, tables and user-defined types.
    """
    ensure_empty_target_database(
        sync_database,
        is_astra_db=IS_ASTRA_DB,
        test_suite_name="vectorize integration tests",
    )


__all__ = [
    "IS_ASTRA_DB",
]
