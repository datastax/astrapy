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

from typing import Any

import pytest

from astrapy.api_options import APIOptions, SerdesOptions
from astrapy.constants import SortMode
from astrapy.cursors import CursorState
from astrapy.data_types import DataAPIVector
from astrapy.exceptions import CursorException

from ..conftest import DefaultAsyncTable

NUM_ROWS = 25  # keep this between 20 and 39


@pytest.fixture
async def filled_composite_atable(
    async_empty_table_composite: DefaultAsyncTable,
) -> DefaultAsyncTable:
    await async_empty_table_composite.insert_many(
        [
            {
                "p_text": "pA",
                "p_int": i,
                "p_vector": DataAPIVector([i, 1, 0]),
            }
            for i in range(NUM_ROWS)
        ]
    )
    return async_empty_table_composite


class TestTableCursorSync:
    @pytest.mark.describe("test of an IDLE table cursors properties, async")
    async def test_table_cursors_idle_properties_async(
        self,
        filled_composite_atable: DefaultAsyncTable,
    ) -> None:
        cur = filled_composite_atable.find()
        assert cur.state == CursorState.IDLE

        assert cur.consumed == 0
        assert cur.consume_buffer(3) == []
        assert cur.buffered_count == 0
        assert cur.consumed == 0

        toclose = cur.clone()
        toclose.close()
        toclose.close()
        assert toclose.state == CursorState.CLOSED
        with pytest.raises(CursorException):
            async for row in toclose:
                pass
        with pytest.raises(StopAsyncIteration):
            await toclose.__anext__()
        with pytest.raises(CursorException):
            await toclose.for_each(lambda row: None)
        with pytest.raises(CursorException):
            await toclose.to_list()

        cur.rewind()
        assert cur.state == CursorState.IDLE
        assert cur.consumed == 0
        assert cur.buffered_count == 0

        cur.filter({"c": True})
        cur.project({"c": True})
        cur.sort({"c": SortMode.ASCENDING})
        cur.limit(1)
        cur.include_similarity(False)
        cur.include_sort_vector(False)
        cur.skip(1)
        cur.map(lambda rw: None)

        cur.project({}).map(lambda rw: None)
        with pytest.raises(CursorException):
            cur.map(lambda rw: None).project({})

    @pytest.mark.describe("test of a CLOSED table cursors properties, async")
    async def test_table_cursors_closed_properties_async(
        self,
        filled_composite_atable: DefaultAsyncTable,
    ) -> None:
        cur0 = filled_composite_atable.find()
        cur0.close()
        cur0.rewind()
        assert cur0.state == CursorState.IDLE

        cur1 = filled_composite_atable.find()
        assert cur1.consumed == 0
        [None async for _ in cur1]
        assert cur1.state == CursorState.CLOSED
        assert cur1.consume_buffer(2) == []
        assert cur1.consumed == NUM_ROWS
        assert cur1.buffered_count == 0
        cloned = cur1.clone()
        assert cloned.consumed == 0
        assert cloned.buffered_count == 0
        assert cloned.state == CursorState.IDLE

        with pytest.raises(CursorException):
            cur1.filter({"c": True})
        with pytest.raises(CursorException):
            cur1.project({"c": True})
        with pytest.raises(CursorException):
            cur1.sort({"c": SortMode.ASCENDING})
        with pytest.raises(CursorException):
            cur1.limit(1)
        with pytest.raises(CursorException):
            cur1.include_similarity(False)
        with pytest.raises(CursorException):
            cur1.include_sort_vector(False)
        with pytest.raises(CursorException):
            cur1.skip(1)
        with pytest.raises(CursorException):
            cur1.map(lambda rw: None)

    @pytest.mark.describe("test of a STARTED table cursors properties, async")
    async def test_table_cursors_started_properties_async(
        self,
        filled_composite_atable: DefaultAsyncTable,
    ) -> None:
        cur = filled_composite_atable.find()
        await cur.__anext__()
        # now this has 19 items in buffer, one is consumed
        assert cur.consumed == 1
        assert cur.buffered_count == 19
        assert len(cur.consume_buffer(3)) == 3
        assert cur.consumed == 4
        assert cur.buffered_count == 16
        # from time to time the buffer is empty:
        for _ in range(16):
            await cur.__anext__()
        assert cur.buffered_count == 0
        assert cur.consume_buffer(3) == []
        assert cur.consumed == 20
        assert cur.buffered_count == 0

        with pytest.raises(CursorException):
            cur.filter({"c": True})
        with pytest.raises(CursorException):
            cur.project({"c": True})
        with pytest.raises(CursorException):
            cur.sort({"c": SortMode.ASCENDING})
        with pytest.raises(CursorException):
            cur.limit(1)
        with pytest.raises(CursorException):
            cur.include_similarity(False)
        with pytest.raises(CursorException):
            cur.include_sort_vector(False)
        with pytest.raises(CursorException):
            cur.skip(1)
        with pytest.raises(CursorException):
            cur.map(lambda rw: None)

    @pytest.mark.describe("test of table cursors has_next, async")
    async def test_table_cursors_has_next_async(
        self,
        filled_composite_atable: DefaultAsyncTable,
    ) -> None:
        cur = filled_composite_atable.find()
        assert cur.state == CursorState.IDLE
        assert cur.consumed == 0
        assert cur.has_next()
        assert cur.state == CursorState.IDLE
        assert cur.consumed == 0
        [None async for _ in cur]
        assert cur.consumed == NUM_ROWS
        assert cur.state == CursorState.CLOSED  # type: ignore[comparison-overlap]

        curmf = filled_composite_atable.find()
        await curmf.__anext__()
        await curmf.__anext__()
        assert curmf.consumed == 2
        assert curmf.state == CursorState.STARTED
        assert await curmf.has_next()
        assert curmf.consumed == 2
        assert curmf.state == CursorState.STARTED
        for _ in range(18):
            await curmf.__anext__()
        assert await curmf.has_next()
        assert curmf.consumed == 20
        assert curmf.state == CursorState.STARTED
        assert curmf.buffered_count == NUM_ROWS - 20

        cur0 = filled_composite_atable.find()
        cur0.close()
        assert not await cur0.has_next()

    @pytest.mark.describe("test of table cursors zero matches, async")
    async def test_table_cursors_zeromatches_async(
        self,
        filled_composite_atable: DefaultAsyncTable,
    ) -> None:
        cur = filled_composite_atable.find({"p_text": "ZZ"})
        assert not await cur.has_next()
        assert [row async for row in cur] == []

    @pytest.mark.describe("test of prematurely closing table cursors, async")
    async def test_table_cursors_early_closing_async(
        self,
        filled_composite_atable: DefaultAsyncTable,
    ) -> None:
        cur = filled_composite_atable.find()
        for _ in range(12):
            await cur.__anext__()
        cur.close()
        assert cur.state == CursorState.CLOSED
        assert cur.buffered_count == 0
        assert cur.consumed == 12
        # rewind test
        cur.rewind()
        assert len([row async for row in cur]) == NUM_ROWS

    @pytest.mark.describe("test of mappings with table cursors, async")
    async def test_table_cursors_mapping_async(
        self,
        filled_composite_atable: DefaultAsyncTable,
    ) -> None:
        base_rows = [row async for row in filled_composite_atable.find()]
        assert len(base_rows) == NUM_ROWS

        def mint(row: dict[str, Any]) -> int:
            return row["p_int"]  # type: ignore[no-any-return]

        def mmult(val: int) -> int:
            return 1000 * val

        # map, base
        mcur = filled_composite_atable.find().map(mint)
        mints = [val async for val in mcur]
        assert mints == [mint(row) for row in base_rows]

        # map composition
        mmcur = filled_composite_atable.find().map(mint).map(mmult)
        mmints = [val async for val in mmcur]
        assert mmints == [mmult(mint(row)) for row in base_rows]

        # consume_buffer skips the map
        hmcur = filled_composite_atable.find().map(mint)
        for _ in range(10):
            await hmcur.__anext__()
        conbuf = hmcur.consume_buffer(2)
        assert len(conbuf) == 2
        assert all(isinstance(itm, dict) for itm in conbuf)

        # rewind preserves the mapping
        rwcur = filled_composite_atable.find().map(mint)
        for _ in range(10):
            await rwcur.__anext__()
        rwcur.rewind()
        assert await rwcur.__anext__() == mints[0]

        # clone strips the mapping
        cl_unmapped = rwcur.clone()
        assert await cl_unmapped.__anext__() == base_rows[0]

    @pytest.mark.describe("test of table cursors, for_each and to_list, async")
    async def test_table_cursors_collective_methods_async(
        self,
        filled_composite_atable: DefaultAsyncTable,
    ) -> None:
        base_rows = [row async for row in filled_composite_atable.find()]

        # full to_list
        tl_cur = filled_composite_atable.find()
        assert await tl_cur.to_list() == base_rows

        # partially-consumed to_list
        ptl_cur = filled_composite_atable.find()
        for _ in range(15):
            await ptl_cur.__anext__()
        assert await ptl_cur.to_list() == base_rows  # TODO: reinstate? [15:]

        # mapped to_list

        def mint(row: dict[str, Any]) -> int:
            return row["p_int"]  # type: ignore[no-any-return]

        mtl_cur = filled_composite_atable.find().map(mint)
        for _ in range(13):
            await mtl_cur.__anext__()
        assert await mtl_cur.to_list() == [
            mint(row) for row in base_rows
        ]  # TODO: reinstate? [13:]]

        # full for_each
        accum0: list[dict[str, Any]] = []

        def marker0(row: dict[str, Any], acc: list[dict[str, Any]] = accum0) -> None:
            acc += [row]

        fe_cur = filled_composite_atable.find()
        await fe_cur.for_each(marker0)
        assert accum0 == base_rows

        # partially-consumed for_each
        accum1: list[dict[str, Any]] = []

        def marker1(row: dict[str, Any], acc: list[dict[str, Any]] = accum1) -> None:
            acc += [row]

        pfe_cur = filled_composite_atable.find()
        for _ in range(11):
            await pfe_cur.__anext__()
        await pfe_cur.for_each(marker1)
        assert accum1 == base_rows  # TODO: reinstate? [11:]

        # mapped for_each
        accum2: list[int] = []

        def marker2(val: int, acc: list[int] = accum2) -> None:
            acc += [val]

        mfe_cur = filled_composite_atable.find().map(mint)
        for _ in range(17):
            await mfe_cur.__anext__()
        await mfe_cur.for_each(marker2)
        assert accum2 == [mint(row) for row in base_rows]  # TODO: reinstate? [17:]]

    @pytest.mark.describe("test of table cursors, serdes options obeyance, async")
    async def test_table_cursors_serdes_options_async(
        self,
        filled_composite_atable: DefaultAsyncTable,
    ) -> None:
        # serdes options obeyance check
        noncustom_compo_table = filled_composite_atable.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(custom_datatypes_in_reading=False),
            )
        )
        custom_compo_table = filled_composite_atable.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(custom_datatypes_in_reading=True),
            )
        )
        noncustom_rows = await noncustom_compo_table.find({}).to_list()
        assert len(noncustom_rows) == NUM_ROWS
        assert all(isinstance(ncrow["p_vector"], list) for ncrow in noncustom_rows)
        custom_rows = await custom_compo_table.find({}).to_list()
        assert len(custom_rows) == NUM_ROWS
        assert all(isinstance(crow["p_vector"], DataAPIVector) for crow in custom_rows)
