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

from typing import Iterable, TypedDict

import pytest

from astrapy import AsyncDatabase, AsyncTable, Database, Table
from astrapy.info import AlterTableAddColumns, AlterTableDropColumns

from ..conftest import DefaultAsyncTable, DefaultTable


class MyTestDoc(TypedDict):
    p_bigint: int
    p_ascii: str


class AlteredMyTestDoc(TypedDict):
    p_bigint: int
    p_ascii: str
    added: int


class MyTestMiniDoc(TypedDict):
    p_bigint: int


FIND_PROJECTION = {"p_bigint": True}
TYPING_TABLE_NAME = "test_typing_table"
TYPING_TABLE_DEFINITION = {
    "columns": {
        "p_ascii": "text",
        "p_bigint": "bigint",
        "p_float": "float",
    },
    "primaryKey": {
        "partitionBy": ["p_ascii", "p_bigint"],
        "partitionSort": {},
    },
}
ROW = {"p_ascii": "abc", "p_bigint": 10000, "p_float": 0.123}
TYPED_ROW: MyTestDoc = {"p_ascii": "abc", "p_bigint": 10000}
FIND_FILTER = {"p_ascii": "abc", "p_bigint": 10000}

ALTER_ADD_COLUMN = AlterTableAddColumns.coerce({"columns": {"added": "int"}})
ALTER_DROP_COLUMN = AlterTableDropColumns.coerce({"columns": ["added"]})


@pytest.fixture(scope="module")
def sync_typing_test_table(sync_database: Database) -> Iterable[DefaultTable]:
    table = sync_database.create_table(
        TYPING_TABLE_NAME,
        definition=TYPING_TABLE_DEFINITION,
        if_not_exists=True,
    )
    yield table

    table.drop()


@pytest.fixture
def async_typing_test_table(
    sync_typing_test_table: DefaultTable,
) -> DefaultAsyncTable:
    return sync_typing_test_table.to_async()


class TestTableTyping:
    @pytest.mark.describe("test of typing create_table and alter_table, sync")
    def test_create_alter_table_typing_sync(
        self,
        sync_database: Database,
        sync_typing_test_table: DefaultTable,
    ) -> None:
        """Test of creating typed tables with generics (and not), sync."""

        # Untyped baseline
        c_tb_untyped = sync_database.create_table(
            TYPING_TABLE_NAME,
            definition=TYPING_TABLE_DEFINITION,
            if_not_exists=True,
        )
        c_tb_untyped.insert_one(ROW)
        cu_doc = c_tb_untyped.find_one(FIND_FILTER)
        assert cu_doc is not None
        cu_a: str
        cu_b: int
        cu_a = cu_doc["p_ascii"]  # noqa: F841
        cu_b = cu_doc["p_bigint"]  # noqa: F841
        assert set(cu_doc.keys()) == {"p_ascii", "p_bigint", "p_float"}
        # untyped, these are all ok:
        cu_x: int
        cu_y: float
        cu_x = cu_doc["p_ascii"]  # noqa: F841
        with pytest.raises(KeyError):
            cu_y = cu_doc["c"]  # noqa: F841

        # untyped, cursors type inference on find
        c_tb_untyped_cursor = c_tb_untyped.find({}, projection=FIND_PROJECTION)
        cucur_doc = c_tb_untyped_cursor.__next__()
        assert cucur_doc is not None
        cucur_b: int
        cucur_b = cucur_doc["p_bigint"]  # noqa: F841
        assert set(cucur_doc.keys()) == {"p_bigint"}
        # untyped, these are all ok:
        cucur_x: str
        cucur_y: float
        cucur_x = cucur_doc["p_bigint"]  # noqa: F841
        with pytest.raises(KeyError):
            cucur_y = cucur_doc["c"]  # noqa: F841

        # untyped, check using alter table's return value
        altered_c_tb_untyped = c_tb_untyped.alter(ALTER_ADD_COLUMN)
        altered_cu_doc = altered_c_tb_untyped.find_one(FIND_FILTER)
        assert altered_cu_doc is not None
        alt_cu_a: str
        alt_cu_b: int
        alt_cu_a = altered_cu_doc["added"]  # noqa: F841
        alt_cu_b = altered_cu_doc["added"]  # noqa: F841
        altered_c_tb_untyped.alter(ALTER_DROP_COLUMN)

        # Typed
        c_tb_typed: Table[MyTestDoc] = sync_database.create_table(
            TYPING_TABLE_NAME,
            definition=TYPING_TABLE_DEFINITION,
            if_not_exists=True,
            row_type=MyTestDoc,
        )
        c_tb_typed.insert_one(TYPED_ROW)
        ct_doc = c_tb_typed.find_one(FIND_FILTER)
        assert ct_doc is not None
        ct_a: str
        ct_b: int
        ct_a = ct_doc["p_ascii"]  # noqa: F841
        ct_b = ct_doc["p_bigint"]  # noqa: F841
        assert set(ct_doc.keys()) == {"p_ascii", "p_bigint", "p_float"}
        # these two SHOULD NOT typecheck (i.e. require the ignore directive)
        ct_x: int
        ct_y: float
        ct_x = ct_doc["p_ascii"]  # type: ignore[assignment]  # noqa: F841
        with pytest.raises(KeyError):
            ct_y = ct_doc["c"]  # type: ignore[typeddict-item]  # noqa: F841

        # typed, cursors type inference on find
        c_tb_typed_cursor = c_tb_typed.find(
            {}, projection=FIND_PROJECTION, row_type=MyTestMiniDoc
        )
        ctcur_doc = c_tb_typed_cursor.__next__()
        assert ctcur_doc is not None
        ctcur_b: int
        ctcur_b = ctcur_doc["p_bigint"]  # noqa: F841
        assert set(ctcur_doc.keys()) == {"p_bigint"}
        # these two SHOULD NOT typecheck (i.e. require the ignore directive)
        ctcur_x: str
        ctcur_y: float
        ctcur_x = ctcur_doc["p_bigint"]  # type: ignore[assignment]  # noqa: F841
        with pytest.raises(KeyError):
            ctcur_y = ctcur_doc["c"]  # type: ignore[typeddict-item]  # noqa: F841

        # typed, check using alter table's return value
        altered_c_tb_typed = c_tb_typed.alter(
            ALTER_ADD_COLUMN, row_type=AlteredMyTestDoc
        )
        altered_ct_doc = altered_c_tb_typed.find_one(FIND_FILTER)
        assert altered_ct_doc is not None
        alt_ct_a: str
        alt_ct_b: int
        alt_ct_a = altered_ct_doc["added"]  # type: ignore[assignment] # noqa: F841
        alt_ct_b = altered_ct_doc["added"]  # noqa: F841
        altered_c_tb_typed.alter(ALTER_DROP_COLUMN)

    @pytest.mark.describe("test of typing get_table, sync")
    def test_get_table_typing_sync(
        self,
        sync_database: Database,
        sync_typing_test_table: DefaultTable,
    ) -> None:
        """Test of getting typed tables with generics (and not), sync."""

        # Untyped baseline
        g_tb_untyped = sync_database.get_table(TYPING_TABLE_NAME)
        g_tb_untyped.insert_one(ROW)
        gu_doc = g_tb_untyped.find_one(FIND_FILTER)
        assert gu_doc is not None
        gu_a: str
        gu_b: int
        gu_a = gu_doc["p_ascii"]  # noqa: F841
        gu_b = gu_doc["p_bigint"]  # noqa: F841
        assert set(gu_doc.keys()) == {"p_ascii", "p_bigint", "p_float"}
        # untyped, these are all ok:
        gu_x: int
        gu_y: float
        gu_x = gu_doc["p_ascii"]  # noqa: F841
        with pytest.raises(KeyError):
            gu_y = gu_doc["c"]  # noqa: F841

        # Typed
        g_tb_typed: Table[MyTestDoc] = sync_database.get_table(
            TYPING_TABLE_NAME, row_type=MyTestDoc
        )
        g_tb_typed.insert_one(TYPED_ROW)
        gt_doc = g_tb_typed.find_one(FIND_FILTER)
        assert gt_doc is not None
        gt_a: str
        gt_b: int
        gt_a = gt_doc["p_ascii"]  # noqa: F841
        gt_b = gt_doc["p_bigint"]  # noqa: F841
        assert set(gt_doc.keys()) == {"p_ascii", "p_bigint", "p_float"}
        # these two SHOULD NOT typecheck (i.e. require the ignore directive)
        gt_x: int
        gt_y: float
        gt_x = gt_doc["p_ascii"]  # type: ignore[assignment]  # noqa: F841
        with pytest.raises(KeyError):
            gt_y = gt_doc["c"]  # type: ignore[typeddict-item]  # noqa: F841

    @pytest.mark.describe("test of typing create_table and alter_table, async")
    async def test_create_alter_table_typing_async(
        self,
        async_database: AsyncDatabase,
        async_typing_test_table: DefaultAsyncTable,
    ) -> None:
        """Test of creating typed tables with generics (and not), async."""

        # Untyped baseline
        ac_tb_untyped = await async_database.create_table(
            TYPING_TABLE_NAME,
            definition=TYPING_TABLE_DEFINITION,
            if_not_exists=True,
        )
        await ac_tb_untyped.insert_one(ROW)
        cu_doc = await ac_tb_untyped.find_one(FIND_FILTER)
        assert cu_doc is not None
        cu_a: str
        cu_b: int
        cu_a = cu_doc["p_ascii"]  # noqa: F841
        cu_b = cu_doc["p_bigint"]  # noqa: F841
        assert set(cu_doc.keys()) == {"p_ascii", "p_bigint", "p_float"}
        # untyped, these are all ok:
        cu_x: int
        cu_y: float
        cu_x = cu_doc["p_ascii"]  # noqa: F841
        with pytest.raises(KeyError):
            cu_y = cu_doc["c"]  # noqa: F841

        # untyped, cursors type inference on find
        c_tb_untyped_acursor = ac_tb_untyped.find({}, projection=FIND_PROJECTION)
        cucur_doc = await c_tb_untyped_acursor.__anext__()
        assert cucur_doc is not None
        cucur_b: int
        cucur_b = cucur_doc["p_bigint"]  # noqa: F841
        assert set(cucur_doc.keys()) == {"p_bigint"}
        # untyped, these are all ok:
        cucur_x: str
        cucur_y: float
        cucur_x = cucur_doc["p_bigint"]  # noqa: F841
        with pytest.raises(KeyError):
            cucur_y = cucur_doc["c"]  # noqa: F841

        # untyped, check using alter table's return value
        altered_ac_tb_untyped = await ac_tb_untyped.alter(ALTER_ADD_COLUMN)
        altered_cu_doc = await altered_ac_tb_untyped.find_one(FIND_FILTER)
        assert altered_cu_doc is not None
        alt_cu_a: str
        alt_cu_b: int
        alt_cu_a = altered_cu_doc["added"]  # noqa: F841
        alt_cu_b = altered_cu_doc["added"]  # noqa: F841
        await altered_ac_tb_untyped.alter(ALTER_DROP_COLUMN)

        # Typed
        ac_tb_typed: AsyncTable[MyTestDoc] = await async_database.create_table(
            TYPING_TABLE_NAME,
            definition=TYPING_TABLE_DEFINITION,
            if_not_exists=True,
            row_type=MyTestDoc,
        )
        await ac_tb_typed.insert_one(TYPED_ROW)
        ct_doc = await ac_tb_typed.find_one(FIND_FILTER)
        assert ct_doc is not None
        ct_a: str
        ct_b: int
        ct_a = ct_doc["p_ascii"]  # noqa: F841
        ct_b = ct_doc["p_bigint"]  # noqa: F841
        assert set(ct_doc.keys()) == {"p_ascii", "p_bigint", "p_float"}
        # these two SHOULD NOT typecheck (i.e. require the ignore directive)
        ct_x: int
        ct_y: float
        ct_x = ct_doc["p_ascii"]  # type: ignore[assignment]  # noqa: F841
        with pytest.raises(KeyError):
            ct_y = ct_doc["c"]  # type: ignore[typeddict-item]  # noqa: F841

        # typed, cursors type inference on find
        c_tb_typed_acursor = ac_tb_typed.find(
            {}, projection=FIND_PROJECTION, row_type=MyTestMiniDoc
        )
        ctcur_doc = await c_tb_typed_acursor.__anext__()
        assert ctcur_doc is not None
        ctcur_b: int
        ctcur_b = ctcur_doc["p_bigint"]  # noqa: F841
        assert set(ctcur_doc.keys()) == {"p_bigint"}
        # these two SHOULD NOT typecheck (i.e. require the ignore directive)
        ctcur_x: str
        ctcur_y: float
        ctcur_x = ctcur_doc["p_bigint"]  # type: ignore[assignment]  # noqa: F841
        with pytest.raises(KeyError):
            ctcur_y = ctcur_doc["c"]  # type: ignore[typeddict-item]  # noqa: F841

        # typed, check using alter table's return value
        altered_ac_tb_typed = await ac_tb_typed.alter(
            ALTER_ADD_COLUMN, row_type=AlteredMyTestDoc
        )
        altered_ct_doc = await altered_ac_tb_typed.find_one(FIND_FILTER)
        assert altered_ct_doc is not None
        alt_ct_a: str
        alt_ct_b: int
        alt_ct_a = altered_ct_doc["added"]  # type: ignore[assignment] # noqa: F841
        alt_ct_b = altered_ct_doc["added"]  # noqa: F841
        await altered_ac_tb_typed.alter(ALTER_DROP_COLUMN)

    @pytest.mark.describe("test of typing get_table, async")
    async def test_get_table_typing_async(
        self,
        async_database: AsyncDatabase,
        async_typing_test_table: DefaultAsyncTable,
    ) -> None:
        """Test of getting typed tables with generics (and not), async."""

        # Untyped baseline
        ag_tb_untyped = async_database.get_table(TYPING_TABLE_NAME)
        await ag_tb_untyped.insert_one(ROW)
        gu_doc = await ag_tb_untyped.find_one(FIND_FILTER)
        assert gu_doc is not None
        gu_a: str
        gu_b: int
        gu_a = gu_doc["p_ascii"]  # noqa: F841
        gu_b = gu_doc["p_bigint"]  # noqa: F841
        assert set(gu_doc.keys()) == {"p_ascii", "p_bigint", "p_float"}
        # untyped, these are all ok:
        gu_x: int
        gu_y: float
        gu_x = gu_doc["p_ascii"]  # noqa: F841
        with pytest.raises(KeyError):
            gu_y = gu_doc["c"]  # noqa: F841

        # Typed
        ag_tb_typed: AsyncTable[MyTestDoc] = async_database.get_table(
            TYPING_TABLE_NAME, row_type=MyTestDoc
        )
        await ag_tb_typed.insert_one(TYPED_ROW)
        gt_doc = await ag_tb_typed.find_one(FIND_FILTER)
        assert gt_doc is not None
        gt_a: str
        gt_b: int
        gt_a = gt_doc["p_ascii"]  # noqa: F841
        gt_b = gt_doc["p_bigint"]  # noqa: F841
        assert set(gt_doc.keys()) == {"p_ascii", "p_bigint", "p_float"}
        # these two SHOULD NOT typecheck (i.e. require the ignore directive)
        gt_x: int
        gt_y: float
        gt_x = gt_doc["p_ascii"]  # type: ignore[assignment]  # noqa: F841
        with pytest.raises(KeyError):
            gt_y = gt_doc["c"]  # type: ignore[typeddict-item]  # noqa: F841
