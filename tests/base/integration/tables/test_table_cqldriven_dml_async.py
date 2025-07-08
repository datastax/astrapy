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

from astrapy import AsyncDatabase
from astrapy.exceptions import DataAPIResponseException
from astrapy.ids import UUID

from ..conftest import CQL_AVAILABLE
from .table_cql_assets import (
    CREATE_TABLE_COUNTER,
    CREATE_TABLE_LOWSUPPORT,
    DROP_TABLE_COUNTER,
    DROP_TABLE_LOWSUPPORT,
    EXPECTED_ROW_COUNTER,
    EXPECTED_ROW_LOWSUPPORT,
    FILTER_COUNTER,
    FILTER_LOWSUPPORT,
    ILLEGAL_PROJECTIONS_LOWSUPPORT,
    INSERTS_TABLE_COUNTER,
    INSERTS_TABLE_LOWSUPPORT,
    LOWSUPPORT_TIMEUUID_DOC0,
    LOWSUPPORT_TIMEUUID_DOC1,
    LOWSUPPORT_TIMEUUID_PK0,
    LOWSUPPORT_TIMEUUID_PK1,
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


@pytest.mark.skipif(not CQL_AVAILABLE, reason="No CQL session available")
class TestTableCQLDrivenDMLAsync:
    @pytest.mark.describe(
        "test of reading from a CQL-driven table with a Counter, async"
    )
    async def test_table_cqldriven_counter_async(
        self,
        cql_session: Session,
        async_database: AsyncDatabase,
    ) -> None:
        try:
            cql_session.execute(CREATE_TABLE_COUNTER)
            for insert_statement in INSERTS_TABLE_COUNTER:
                cql_session.execute(insert_statement)
            time.sleep(1.5)  # delay for schema propagation

            atable = async_database.get_table(TABLE_NAME_COUNTER)
            await atable.definition()
            row = await atable.find_one(filter=FILTER_COUNTER)
            assert row == EXPECTED_ROW_COUNTER
            await atable.delete_one(filter=FILTER_COUNTER)
            row = await atable.find_one(filter=FILTER_COUNTER)
            assert row is None
        finally:
            cql_session.execute(DROP_TABLE_COUNTER)

    @pytest.mark.describe(
        "test of reading from a CQL-driven table with limited-support columns, async"
    )
    async def test_table_cqldriven_lowsupport_async(
        self,
        cql_session: Session,
        async_database: AsyncDatabase,
    ) -> None:
        try:
            cql_session.execute(CREATE_TABLE_LOWSUPPORT)
            for insert_statement in INSERTS_TABLE_LOWSUPPORT:
                cql_session.execute(insert_statement)
            time.sleep(1.5)  # delay for schema propagation

            atable = async_database.get_table(TABLE_NAME_LOWSUPPORT)
            await atable.definition()
            for ill_proj in ILLEGAL_PROJECTIONS_LOWSUPPORT:
                with pytest.raises(DataAPIResponseException):
                    await atable.find_one(filter=FILTER_LOWSUPPORT)
            row = await atable.find_one(
                filter=FILTER_LOWSUPPORT, projection=PROJECTION_LOWSUPPORT
            )
            assert row == EXPECTED_ROW_LOWSUPPORT
            await atable.delete_one(filter=FILTER_LOWSUPPORT)
            row = await atable.find_one(
                filter=FILTER_LOWSUPPORT, projection=PROJECTION_LOWSUPPORT
            )
            assert row is None

            # writing timeuuid, passing strings and an UUID object to insert_one
            await atable.insert_one(LOWSUPPORT_TIMEUUID_DOC0)
            r_timeuuid_doc0 = await atable.find_one(
                filter=LOWSUPPORT_TIMEUUID_PK0,
                projection={col: True for col in LOWSUPPORT_TIMEUUID_DOC0},
            )
            assert r_timeuuid_doc0 == LOWSUPPORT_TIMEUUID_DOC0
            await atable.insert_one(LOWSUPPORT_TIMEUUID_DOC1)
            r_timeuuid_doc1 = await atable.find_one(
                filter=LOWSUPPORT_TIMEUUID_PK1,
                projection={col: True for col in LOWSUPPORT_TIMEUUID_DOC1},
            )
            # the UUID form is expected when reading:
            assert r_timeuuid_doc1 is not None
            assert r_timeuuid_doc1["col_timeuuid"] == UUID(
                LOWSUPPORT_TIMEUUID_DOC1["col_timeuuid"]
            )
        finally:
            cql_session.execute(DROP_TABLE_LOWSUPPORT)
