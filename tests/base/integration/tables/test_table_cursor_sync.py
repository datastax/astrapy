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

from ..conftest import DefaultTable

NUM_ROWS = 25  # keep this between 20 and 39
NUM_DOCS_PAGINATION = 90  # keep this above 2 * (2 * 20) and below 2 * (3 * 20)


@pytest.fixture
def filled_composite_table(sync_empty_table_composite: DefaultTable) -> DefaultTable:
    sync_empty_table_composite.insert_many(
        [
            {
                "p_text": "pA",
                "p_int": i,
                "p_vector": DataAPIVector([i, 1, 0]),
            }
            for i in range(NUM_ROWS)
        ]
    )
    return sync_empty_table_composite


@pytest.fixture
def filled_pagination_composite_table(
    sync_empty_table_composite: DefaultTable,
) -> DefaultTable:
    sync_empty_table_composite.insert_many(
        [
            {
                "p_text": "pA",
                "p_int": i,
                "p_boolean": i % 2 == 0,
                "p_vector": DataAPIVector([i, 1, 0]),
            }
            for i in range(NUM_DOCS_PAGINATION)
        ]
    )
    return sync_empty_table_composite


class TestTableCursorSync:
    @pytest.mark.describe("test of an IDLE table cursors properties, sync")
    def test_table_cursors_idle_properties_sync(
        self,
        filled_composite_table: DefaultTable,
    ) -> None:
        cur = filled_composite_table.find()
        assert cur.state == CursorState.IDLE

        assert cur.data_source == filled_composite_table

        assert cur.consumed == 0
        assert cur.consume_buffer(3) == []
        assert cur.buffered_count == 0
        assert cur.consumed == 0

        toclose = cur.clone()
        toclose.close()
        toclose.close()
        assert toclose.state == CursorState.CLOSED
        with pytest.raises(CursorException):
            for row in toclose:
                pass
        with pytest.raises(StopIteration):
            next(toclose)
        with pytest.raises(CursorException):
            toclose.for_each(lambda row: None)
        with pytest.raises(CursorException):
            toclose.to_list()

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

    @pytest.mark.describe("test of a CLOSED table cursors properties, sync")
    def test_table_cursors_closed_properties_sync(
        self,
        filled_composite_table: DefaultTable,
    ) -> None:
        cur0 = filled_composite_table.find()
        cur0.close()
        cur0.rewind()
        assert cur0.state == CursorState.IDLE

        cur1 = filled_composite_table.find()
        assert cur1.consumed == 0
        list(cur1)
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

    @pytest.mark.describe("test of a STARTED table cursors properties, sync")
    def test_table_cursors_started_properties_sync(
        self,
        filled_composite_table: DefaultTable,
    ) -> None:
        cur = filled_composite_table.find()
        next(cur)
        # now this has 19 items in buffer, one is consumed
        assert cur.consumed == 1
        assert cur.buffered_count == 19
        assert len(cur.consume_buffer(3)) == 3
        assert cur.consumed == 4
        assert cur.buffered_count == 16
        # from time to time the buffer is empty:
        for _ in range(16):
            next(cur)
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

    @pytest.mark.describe("test of table cursors has_next, sync")
    def test_table_cursors_has_next_sync(
        self,
        filled_composite_table: DefaultTable,
    ) -> None:
        cur = filled_composite_table.find()
        assert cur.state == CursorState.IDLE
        assert cur.consumed == 0
        assert cur.has_next()
        assert cur.state == CursorState.IDLE
        assert cur.consumed == 0
        list(cur)
        assert cur.consumed == NUM_ROWS
        assert cur.state == CursorState.CLOSED  # type: ignore[comparison-overlap]

        curmf = filled_composite_table.find()
        next(curmf)
        next(curmf)
        assert curmf.consumed == 2
        assert curmf.state == CursorState.STARTED
        assert curmf.has_next()
        assert curmf.consumed == 2
        assert curmf.state == CursorState.STARTED
        for _ in range(18):
            next(curmf)
        assert curmf.has_next()
        assert curmf.consumed == 20
        assert curmf.state == CursorState.STARTED
        assert curmf.buffered_count == NUM_ROWS - 20

        cur0 = filled_composite_table.find()
        cur0.close()
        assert not cur0.has_next()

    @pytest.mark.describe("test of table cursors zero matches, sync")
    def test_table_cursors_zeromatches_sync(
        self,
        filled_composite_table: DefaultTable,
    ) -> None:
        cur = filled_composite_table.find({"p_text": "ZZ"})
        assert not cur.has_next()
        assert list(cur) == []

    @pytest.mark.describe("test of prematurely closing table cursors, sync")
    def test_table_cursors_early_closing_sync(
        self,
        filled_composite_table: DefaultTable,
    ) -> None:
        cur = filled_composite_table.find()
        for _ in range(12):
            next(cur)
        cur.close()
        assert cur.state == CursorState.CLOSED
        assert cur.buffered_count == 0
        assert cur.consumed == 12
        # rewind test
        cur.rewind()
        assert len(list(cur)) == NUM_ROWS

    @pytest.mark.describe("test of mappings with table cursors, sync")
    def test_table_cursors_mapping_sync(
        self,
        filled_composite_table: DefaultTable,
    ) -> None:
        base_rows = list(filled_composite_table.find())
        assert len(base_rows) == NUM_ROWS

        def mint(row: dict[str, Any]) -> int:
            return row["p_int"]  # type: ignore[no-any-return]

        def mmult(val: int) -> int:
            return 1000 * val

        # map, base
        mcur = filled_composite_table.find().map(mint)
        mints = [val for val in mcur]
        assert mints == [mint(row) for row in base_rows]

        # map composition
        mmcur = filled_composite_table.find().map(mint).map(mmult)
        mmints = [val for val in mmcur]
        assert mmints == [mmult(mint(row)) for row in base_rows]

        # consume_buffer skips the map
        hmcur = filled_composite_table.find().map(mint)
        for _ in range(10):
            next(hmcur)
        conbuf = hmcur.consume_buffer(2)
        assert len(conbuf) == 2
        assert all(isinstance(itm, dict) for itm in conbuf)

        # rewind preserves the mapping
        rwcur = filled_composite_table.find().map(mint)
        for _ in range(10):
            next(rwcur)
        rwcur.rewind()
        assert next(rwcur) == mints[0]

        # clone rewinds
        cl_unmapped = rwcur.clone()
        assert next(cl_unmapped) == mint(base_rows[0])

    @pytest.mark.describe("test of table cursors, for_each and to_list, sync")
    def test_table_cursors_collective_methods_sync(
        self,
        filled_composite_table: DefaultTable,
    ) -> None:
        base_rows = list(filled_composite_table.find())

        # full to_list
        tl_cur = filled_composite_table.find()
        assert tl_cur.to_list() == base_rows
        assert tl_cur.state == CursorState.CLOSED

        # partially-consumed to_list
        ptl_cur = filled_composite_table.find()
        for _ in range(15):
            next(ptl_cur)
        assert ptl_cur.to_list() == base_rows[15:]
        assert ptl_cur.state == CursorState.CLOSED

        # mapped to_list

        def mint(row: dict[str, Any]) -> int:
            return row["p_int"]  # type: ignore[no-any-return]

        mtl_cur = filled_composite_table.find().map(mint)
        for _ in range(13):
            next(mtl_cur)
        assert mtl_cur.to_list() == [mint(row) for row in base_rows[13:]]
        assert mtl_cur.state == CursorState.CLOSED

        # full for_each
        accum0: list[dict[str, Any]] = []

        def marker0(row: dict[str, Any], acc: list[dict[str, Any]] = accum0) -> None:
            acc += [row]

        fe_cur = filled_composite_table.find()
        fe_cur.for_each(marker0)
        assert accum0 == base_rows
        assert fe_cur.state == CursorState.CLOSED

        # partially-consumed for_each
        accum1: list[dict[str, Any]] = []

        def marker1(row: dict[str, Any], acc: list[dict[str, Any]] = accum1) -> None:
            acc += [row]

        pfe_cur = filled_composite_table.find()
        for _ in range(11):
            next(pfe_cur)
        pfe_cur.for_each(marker1)
        assert accum1 == base_rows[11:]
        assert pfe_cur.state == CursorState.CLOSED

        # mapped for_each
        accum2: list[int] = []

        def marker2(val: int, acc: list[int] = accum2) -> None:
            acc += [val]

        mfe_cur = filled_composite_table.find().map(mint)
        for _ in range(17):
            next(mfe_cur)
        mfe_cur.for_each(marker2)
        assert accum2 == [mint(row) for row in base_rows[17:]]
        assert mfe_cur.state == CursorState.CLOSED

        # breaking (early) for_each
        accum3: list[dict[str, Any]] = []

        def marker3(row: dict[str, Any], acc: list[dict[str, Any]] = accum3) -> bool:
            acc += [row]
            return len(acc) < 5

        bfe_cur = filled_composite_table.find()
        bfe_cur.for_each(marker3)
        assert accum3 == base_rows[:5]
        assert bfe_cur.state == CursorState.STARTED
        bfe_another = next(bfe_cur)
        assert bfe_another == base_rows[5]

        # nonbool-nonbreaking for_each
        accum4: list[dict[str, Any]] = []

        def marker4(row: dict[str, Any], acc: list[dict[str, Any]] = accum4) -> int:
            acc += [row]
            return 8 if len(acc) < 5 else 0

        nbfe_cur = filled_composite_table.find()
        nbfe_cur.for_each(marker4)  # type: ignore[arg-type]
        assert accum4 == base_rows
        assert nbfe_cur.state == CursorState.CLOSED

    @pytest.mark.describe("test of table cursors, serdes options obeyance, sync")
    def test_table_cursors_serdes_options_sync(
        self,
        filled_composite_table: DefaultTable,
    ) -> None:
        # serdes options obeyance check
        noncustom_compo_table = filled_composite_table.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(custom_datatypes_in_reading=False),
            )
        )
        custom_compo_table = filled_composite_table.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(custom_datatypes_in_reading=True),
            )
        )
        noncustom_rows = noncustom_compo_table.find({}).to_list()
        assert len(noncustom_rows) == NUM_ROWS
        assert all(isinstance(ncrow["p_vector"], list) for ncrow in noncustom_rows)
        custom_rows = custom_compo_table.find({}).to_list()
        assert len(custom_rows) == NUM_ROWS
        assert all(isinstance(crow["p_vector"], DataAPIVector) for crow in custom_rows)

    @pytest.mark.describe("test of table cursors, initial_page_state, sync")
    def test_table_cursors_initialpagestate_sync(
        self,
        filled_pagination_composite_table: DefaultTable,
    ) -> None:
        page_size = 20

        cur0 = filled_pagination_composite_table.find(filter={"p_boolean": True})
        ids0 = [doc["p_int"] for _, doc in zip(range(page_size), cur0)]
        nps0 = cur0._next_page_state
        assert isinstance(nps0, str)

        cur1 = filled_pagination_composite_table.find(
            filter={"p_boolean": True},
            initial_page_state=nps0,
        )
        ids1 = [doc["p_int"] for _, doc in zip(range(page_size), cur1)]
        nps1 = cur1._next_page_state
        assert isinstance(nps1, str)

        cur2 = filled_pagination_composite_table.find(
            filter={"p_boolean": True},
            initial_page_state=nps1,
        )
        ids2 = [doc["p_int"] for _, doc in zip(range(page_size), cur2)]
        assert cur2._next_page_state is None

        expected_ids = [i for i in range(NUM_DOCS_PAGINATION) if i % 2 == 0]
        retrieved_ids = ids0 + ids1 + ids2
        assert len(retrieved_ids) == len(set(retrieved_ids))
        assert sorted(retrieved_ids) == expected_ids

        # rewind behaviour
        cur2.rewind()
        cur2.rewind(initial_page_state="some string")
        with pytest.raises(ValueError, match="null"):
            cur2.rewind(initial_page_state=None)  # type: ignore[arg-type]

    @pytest.mark.describe("test of table cursors, fetch_next_page, sync")
    def test_table_cursors_fetchnextpage_sync(
        self,
        filled_pagination_composite_table: DefaultTable,
    ) -> None:
        cur0 = filled_pagination_composite_table.find(filter={"p_boolean": True})
        page0 = cur0.fetch_next_page()
        ids0 = [doc["p_int"] for doc in page0.results]
        nps0 = page0.next_page_state
        assert isinstance(nps0, str)

        cur1 = filled_pagination_composite_table.find(
            filter={"p_boolean": True},
            initial_page_state=nps0,
        )
        page1 = cur1.fetch_next_page()
        ids1 = [doc["p_int"] for doc in page1.results]
        nps1 = page1.next_page_state
        assert isinstance(nps1, str)

        cur2 = filled_pagination_composite_table.find(
            filter={"p_boolean": True},
            initial_page_state=nps1,
        )
        page2 = cur2.fetch_next_page()
        ids2 = [doc["p_int"] for doc in page2.results]
        assert page2.next_page_state is None

        expected_ids = [i for i in range(NUM_DOCS_PAGINATION) if i % 2 == 0]
        retrieved_ids = ids0 + ids1 + ids2
        assert len(retrieved_ids) == len(set(retrieved_ids))
        assert sorted(retrieved_ids) == expected_ids

        # fetching consecutive pages on a given cursor
        cur0x = filled_pagination_composite_table.find(filter={"p_boolean": True})
        cur0x.fetch_next_page()
        page1x = cur0x.fetch_next_page()
        assert page1x == page1

        # forbidden: mixing pagination and ordinary usage
        cur0x.__next__()
        with pytest.raises(CursorException):
            cur0x.fetch_next_page()
        cur0x.to_list()
        with pytest.raises(CursorException):
            cur0x.fetch_next_page()

        # mapping
        cur0y = filled_pagination_composite_table.find(filter={"p_boolean": True}).map(
            lambda doc: doc["p_int"]
        )
        page0y = cur0y.fetch_next_page()
        assert page0y.results == ids0

        # vector ANN one-page behaviour re: include_sort_vector and its format
        table_noncustom = filled_pagination_composite_table.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(custom_datatypes_in_reading=False),
            )
        )
        table_custom = filled_pagination_composite_table.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(custom_datatypes_in_reading=True),
            )
        )

        vcur0_nc = table_noncustom.find(
            sort={"p_vector": [1, 1, 1]},
            include_sort_vector=True,
            limit=15,
        )
        vpage0_nc = vcur0_nc.fetch_next_page()
        assert vpage0_nc.next_page_state is None
        assert len(vpage0_nc.results) == 15
        assert isinstance(vpage0_nc.sort_vector, list)
        assert not isinstance(vpage0_nc.sort_vector, DataAPIVector)

        vcur0_c = table_custom.find(
            sort={"p_vector": [1, 1, 1]},
            include_sort_vector=True,
            limit=15,
        )
        vpage0_c = vcur0_c.fetch_next_page()
        assert vpage0_c.next_page_state is None
        assert len(vpage0_c.results) == 15
        assert isinstance(vpage0_c.sort_vector, DataAPIVector)
