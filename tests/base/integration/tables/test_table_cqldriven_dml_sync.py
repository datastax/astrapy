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

import time
from typing import TYPE_CHECKING

import pytest

from astrapy import Database
from astrapy.exceptions import DataAPIResponseException

from ..conftest import CQL_AVAILABLE
from .table_cql_assets import (
    CREATE_TABLE_COUNTER,
    CREATE_TABLE_LOWSUPPORT,
    CREATE_TYPE_LOWSUPPORT,
    DROP_TABLE_COUNTER,
    DROP_TABLE_LOWSUPPORT,
    DROP_TYPE_LOWSUPPORT,
    EXPECTED_ROW_COUNTER,
    EXPECTED_ROW_LOWSUPPORT,
    FILTER_COUNTER,
    FILTER_LOWSUPPORT,
    ILLEGAL_PROJECTIONS_LOWSUPPORT,
    INSERTS_TABLE_COUNTER,
    INSERTS_TABLE_LOWSUPPORT,
    PROJECTION_LOWSUPPORT,
    TABLE_NAME_COUNTER,
    TABLE_NAME_LOWSUPPORT,
)

if TYPE_CHECKING:
    from cassandra.cluster import Session

try:
    from cassandra.cluster import Session
except ImportError:
    pass


@pytest.mark.skipif(not CQL_AVAILABLE, reason="Not CQL session available")
class TestTableCQLDrivenDMLSync:
    @pytest.mark.describe(
        "test of reading from a CQL-driven table with a Counter, sync"
    )
    def test_table_cqldriven_counter_sync(
        self,
        cql_session: Session,
        sync_database: Database,
    ) -> None:
        try:
            cql_session.execute(CREATE_TABLE_COUNTER)
            for insert_statement in INSERTS_TABLE_COUNTER:
                cql_session.execute(insert_statement)
            time.sleep(1.5)  # delay for schema propagation

            table = sync_database.get_table(TABLE_NAME_COUNTER)
            table.definition()
            row = table.find_one(filter=FILTER_COUNTER)
            assert row == EXPECTED_ROW_COUNTER
            table.delete_one(filter=FILTER_COUNTER)
            row = table.find_one(filter=FILTER_COUNTER)
            assert row is None
        finally:
            cql_session.execute(DROP_TABLE_COUNTER)

    @pytest.mark.describe(
        "test of reading from a CQL-driven table with limited-support columns, sync"
    )
    def test_table_cqldriven_lowsupport_sync(
        self,
        cql_session: Session,
        sync_database: Database,
    ) -> None:
        try:
            cql_session.execute(CREATE_TYPE_LOWSUPPORT)
            cql_session.execute(CREATE_TABLE_LOWSUPPORT)
            for insert_statement in INSERTS_TABLE_LOWSUPPORT:
                cql_session.execute(insert_statement)
            time.sleep(1.5)  # delay for schema propagation

            table = sync_database.get_table(TABLE_NAME_LOWSUPPORT)
            table.definition()
            for ill_proj in ILLEGAL_PROJECTIONS_LOWSUPPORT:
                with pytest.raises(DataAPIResponseException):
                    table.find_one(filter=FILTER_LOWSUPPORT)
            row = table.find_one(
                filter=FILTER_LOWSUPPORT, projection=PROJECTION_LOWSUPPORT
            )
            assert row == EXPECTED_ROW_LOWSUPPORT
            table.delete_one(filter=FILTER_LOWSUPPORT)
            row = table.find_one(
                filter=FILTER_LOWSUPPORT, projection=PROJECTION_LOWSUPPORT
            )
            assert row is None
        finally:
            cql_session.execute(DROP_TABLE_LOWSUPPORT)
            cql_session.execute(DROP_TYPE_LOWSUPPORT)
