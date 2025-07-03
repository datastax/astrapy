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
from typing import TYPE_CHECKING

import pytest

from astrapy import AsyncDatabase
from astrapy.exceptions import DataAPIResponseException

from ..conftest import CQL_AVAILABLE, DataAPICredentials
from .table_cql_assets import _extract_udt_definition
from .table_row_assets import (
    UDT_ALTER_OP_1,
    UDT_ALTER_OP_2,
    UDT_DEF0,
    UDT_DEF1,
    UDT_NAME,
    WEIRD_BASE_DOCUMENT,
    WEIRD_BASE_DOCUMENT_PK,
    WEIRD_NESTED_EXPECTED_DOCUMENT,
    WEIRD_UDT_BASE_CLOSE_STATEMENTS,
    WEIRD_UDT_BASE_INITIALIZE_STATEMENTS,
    WEIRD_UDT_BASE_TABLE_NAME,
    WEIRD_UDT_NESTED_CLOSE_STATEMENTS,
    WEIRD_UDT_NESTED_DOCUMENT_PK,
    WEIRD_UDT_NESTED_INITIALIZE_STATEMENTS,
    WEIRD_UDT_NESTED_TABLE_NAME,
)

if TYPE_CHECKING:
    from cassandra.cluster import Session


@pytest.mark.skipif(
    "ASTRAPY_TEST_UDT" not in os.environ,
    reason="UDT testing not enabled",
)
@pytest.mark.skipif(not CQL_AVAILABLE, reason="No CQL session available")
class TestTableUserDefinedTypes:
    @pytest.mark.describe("Test of UDT lifecycle, async")
    async def test_table_udt_lifecycle_async(
        self,
        data_api_credentials_kwargs: DataAPICredentials,
        cql_session: Session,
        async_database: AsyncDatabase,
    ) -> None:
        try:
            await async_database.create_type(UDT_NAME, definition=UDT_DEF0)
            with pytest.raises(DataAPIResponseException):
                await async_database.create_type(UDT_NAME, definition=UDT_DEF0)
            await async_database.create_type(
                UDT_NAME,
                definition=UDT_DEF0,
                if_not_exists=True,
            )

            assert UDT_DEF0 == _extract_udt_definition(
                cql_session,
                data_api_credentials_kwargs["keyspace"],
                UDT_NAME,
            )

            await async_database.alter_type(UDT_NAME, operation=UDT_ALTER_OP_1)
            await async_database.alter_type(UDT_NAME, operation=UDT_ALTER_OP_2)
            with pytest.raises(DataAPIResponseException):
                await async_database.alter_type(UDT_NAME, operation=UDT_ALTER_OP_1)

            assert UDT_DEF1 == _extract_udt_definition(
                cql_session,
                data_api_credentials_kwargs["keyspace"],
                UDT_NAME,
            )

            await async_database.drop_type(UDT_NAME)
            assert (
                _extract_udt_definition(
                    cql_session,
                    data_api_credentials_kwargs["keyspace"],
                    UDT_NAME,
                )
                is None
            )
            with pytest.raises(DataAPIResponseException):
                await async_database.drop_type(UDT_NAME)
            await async_database.drop_type(UDT_NAME, if_exists=True)

            await async_database.create_type(UDT_NAME, definition=UDT_DEF0)
            await async_database.drop_type(UDT_NAME)
        finally:
            await async_database.drop_type(UDT_NAME, if_exists=True)

    @pytest.mark.skipif(not CQL_AVAILABLE, reason="No CQL session available")
    @pytest.mark.describe("Test of weird UDT columns, async")
    async def test_table_udt_weirdcolumns_async(
        self,
        cql_session: Session,
        async_database: AsyncDatabase,
    ) -> None:
        try:
            for cql_statement in WEIRD_UDT_BASE_INITIALIZE_STATEMENTS:
                cql_session.execute(cql_statement)

            # test a read and a write for 'base weird'
            atable_weird_base = async_database.get_table(WEIRD_UDT_BASE_TABLE_NAME)
            ins_result = await atable_weird_base.insert_one(WEIRD_BASE_DOCUMENT)
            assert ins_result.inserted_id == WEIRD_BASE_DOCUMENT_PK
            doc_weird_base = await atable_weird_base.find_one(WEIRD_BASE_DOCUMENT_PK)
            assert doc_weird_base == WEIRD_BASE_DOCUMENT
        finally:
            for cql_statement in WEIRD_UDT_BASE_CLOSE_STATEMENTS:
                cql_session.execute(cql_statement)

    @pytest.mark.skipif(not CQL_AVAILABLE, reason="No CQL session available")
    @pytest.mark.describe("Test of weird UDT columns, async")
    async def test_table_udt_weirdnested_async(
        self,
        cql_session: Session,
        async_database: AsyncDatabase,
    ) -> None:
        try:
            for cql_statement in WEIRD_UDT_NESTED_INITIALIZE_STATEMENTS:
                cql_session.execute(cql_statement)

            # test a read for 'nested weird'
            atable_weird_nst = async_database.get_table(WEIRD_UDT_NESTED_TABLE_NAME)
            doc_weird_nst = await atable_weird_nst.find_one(
                WEIRD_UDT_NESTED_DOCUMENT_PK
            )
            assert doc_weird_nst == WEIRD_NESTED_EXPECTED_DOCUMENT
        finally:
            for cql_statement in WEIRD_UDT_NESTED_CLOSE_STATEMENTS:
                cql_session.execute(cql_statement)
