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

from typing import Any, Iterable, TypedDict

import pytest

from astrapy import Table, Database

from ..conftest import DefaultTable

class TestDoc(TypedDict):
  p_bigint: int
  p_ascii: str

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
TYPED_ROW: TestDoc = {"p_ascii": "abc", "p_bigint": 10000}
FIND_FILTER = {"p_ascii": "abc", "p_bigint": 10000}


@pytest.fixture(scope="module")
def typing_test_table(sync_database: Database) -> Iterable[DefaultTable]:
    table = sync_database.create_table(
        TYPING_TABLE_NAME,
        definition=TYPING_TABLE_DEFINITION,
        if_not_exists=True,
    )
    yield table

    table.drop()

class TestTableDMLSync:
    @pytest.mark.describe("test of typing create_table, sync")
    def test_create_table_typing_sync(
        self,
        sync_database: Database,
        typing_test_table: DefaultTable,
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
        cu_a = cu_doc["p_ascii"]
        cu_b = cu_doc["p_bigint"]
        assert set(cu_doc.keys()) == {"p_ascii", "p_bigint", "p_float"}
        # untyped, these are all ok:
        cu_x: int
        cu_y: float
        cu_x = cu_doc["p_ascii"]
        with pytest.raises(KeyError):
            cu_y = cu_doc["c"]

        # Typed
        c_tb_typed: Table[TestDoc] = sync_database.create_table(
            TYPING_TABLE_NAME,
            definition=TYPING_TABLE_DEFINITION,
            if_not_exists=True,
            row_type=TestDoc,
        )
        c_tb_typed.insert_one(TYPED_ROW)
        ct_doc = c_tb_typed.find_one(FIND_FILTER)
        assert ct_doc is not None
        ct_a: str
        ct_b: int
        ct_a = ct_doc["p_ascii"]
        ct_b = ct_doc["p_bigint"]
        assert set(ct_doc.keys()) == {"p_ascii", "p_bigint", "p_float"}
        # these two SHOULD NOT typecheck (i.e. require the ignore directive)
        ct_x: int
        ct_y: float
        ct_x = ct_doc["p_ascii"]  # type: ignore[assignment]
        with pytest.raises(KeyError):
            ct_y = ct_doc["c"]  # type: ignore[typeddict-item]

    @pytest.mark.describe("test of typing get_table, sync")
    def test_get_table_typing_sync(
        self,
        sync_database: Database,
        typing_test_table: DefaultTable,
    ) -> None:
        """Test of getting typed tables with generics (and not), sync."""

        # Untyped baseline
        g_tb_untyped = sync_database.get_table(TYPING_TABLE_NAME)
        g_tb_untyped.insert_one(ROW)
        gu_doc = g_tb_untyped.find_one(FIND_FILTER)
        assert gu_doc is not None
        gu_a: str
        gu_b: int
        gu_a = gu_doc["p_ascii"]
        gu_b = gu_doc["p_bigint"]
        assert set(gu_doc.keys()) == {"p_ascii", "p_bigint", "p_float"}
        # untyped, these are all ok:
        gu_x: int
        gu_y: float
        gu_x = gu_doc["p_ascii"]
        with pytest.raises(KeyError):
            gu_y = gu_doc["c"]

        # Typed
        g_tb_typed: Table[TestDoc] = sync_database.get_table(TYPING_TABLE_NAME, row_type=TestDoc)
        g_tb_typed.insert_one(TYPED_ROW)
        gt_doc = g_tb_typed.find_one(FIND_FILTER)
        assert gt_doc is not None
        gt_a: str
        gt_b: int
        gt_a = gt_doc["p_ascii"]
        gt_b = gt_doc["p_bigint"]
        assert set(gt_doc.keys()) == {"p_ascii", "p_bigint", "p_float"}
        # these two SHOULD NOT typecheck (i.e. require the ignore directive)
        gt_x: int
        gt_y: float
        gt_x = gt_doc["p_ascii"]  # type: ignore[assignment]
        with pytest.raises(KeyError):
            gt_y = gt_doc["c"]  # type: ignore[typeddict-item]
