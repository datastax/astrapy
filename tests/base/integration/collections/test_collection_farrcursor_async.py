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

from astrapy.api_options import APIOptions, SerdesOptions
from astrapy.constants import DefaultDocumentType
from astrapy.cursors import CursorState, RerankedResult
from astrapy.data_types import DataAPIVector
from astrapy.exceptions import CursorException

from ..conftest import DefaultAsyncCollection

NUM_DOCS = 25  # keep this between 20 and 39


@pytest.fixture
def afilled_vectorize_collection(
    async_empty_farr_vectorize_collection: DefaultAsyncCollection,
) -> DefaultAsyncCollection:
    async_empty_farr_vectorize_collection.to_sync().insert_many(
        [
            {
                "_id": f"doc_{i:02}",
                "$vectorize": "this is sentence #{i} (vecze)",
                "$lexical": "this is sentence #{i} (lexi)",
                "parity": ["even", "odd"][i % 2],
            }
            for i in range(NUM_DOCS)
        ]
    )
    return async_empty_farr_vectorize_collection


class TestCollectionCursorSync:
    @pytest.mark.describe("test of an IDLE collection farr-cursors properties, async")
    async def test_collection_farrcursors_idle_properties_async(
        self,
        afilled_vectorize_collection: DefaultAsyncCollection,
    ) -> None:
        cur = afilled_vectorize_collection.find_and_rerank(
            sort={"$hybrid": "a sentence."},
            limit=NUM_DOCS,
        )
        assert cur.state == CursorState.IDLE

        assert cur.data_source == afilled_vectorize_collection

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
        cur.sort({"$hybrid": "query"})
        cur.limit(1)
        cur.hybrid_limits(2)
        cur.include_sort_vector(False)
        cur.include_scores(True)
        cur.rerank_on("field")
        cur.map(lambda r_res: None)

        cur.project({}).map(lambda r_res: None)
        with pytest.raises(CursorException):
            cur.map(lambda r_res: None).project({})

    @pytest.mark.describe("test of a CLOSED collection farr-cursors properties, async")
    async def test_collection_farrcursors_closed_properties_async(
        self,
        afilled_vectorize_collection: DefaultAsyncCollection,
    ) -> None:
        cur0 = afilled_vectorize_collection.find_and_rerank(
            sort={"$hybrid": "a sentence."},
            limit=NUM_DOCS,
        )
        cur0.close()
        cur0.rewind()
        assert cur0.state == CursorState.IDLE

        cur1 = afilled_vectorize_collection.find_and_rerank(
            sort={"$hybrid": "a sentence."},
            limit=NUM_DOCS,
        )
        assert cur1.consumed == 0
        [r_res async for r_res in cur1]
        assert cur1.state == CursorState.CLOSED
        assert cur1.consume_buffer(2) == []
        assert cur1.consumed == NUM_DOCS
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
            cur1.sort({"$hybrid": "query"})
        with pytest.raises(CursorException):
            cur1.limit(1)
        with pytest.raises(CursorException):
            cur1.hybrid_limits(2)
        with pytest.raises(CursorException):
            cur1.include_scores(False)
        with pytest.raises(CursorException):
            cur1.include_sort_vector(False)
        with pytest.raises(CursorException):
            cur1.rerank_on("field")
        with pytest.raises(CursorException):
            cur1.map(lambda rw: None)

    @pytest.mark.describe("test of a STARTED collection farr-cursors properties, async")
    async def test_collection_farrcursors_started_properties_async(
        self,
        afilled_vectorize_collection: DefaultAsyncCollection,
    ) -> None:
        LIMIT = NUM_DOCS - 1
        cur = afilled_vectorize_collection.find_and_rerank(
            sort={"$hybrid": "a sentence."},
            limit=LIMIT,
        )
        await cur.__anext__()
        # now this is a one-page cursor and has LIMIT-1 items in buffer (one gone)
        assert cur.consumed == 1
        assert cur.buffered_count == LIMIT - 1
        assert len(cur.consume_buffer(3)) == 3
        assert cur.consumed == 4
        assert cur.buffered_count == LIMIT - 4
        # from time to time the buffer is empty:
        for _ in range(LIMIT - 4):
            await cur.__anext__()
        assert cur.buffered_count == 0
        assert cur.consume_buffer(3) == []
        assert cur.consumed == LIMIT
        assert cur.buffered_count == 0

        with pytest.raises(CursorException):
            cur.filter({"c": True})
        with pytest.raises(CursorException):
            cur.project({"c": True})
        with pytest.raises(CursorException):
            cur.sort({"$hybrid": "query"})
        with pytest.raises(CursorException):
            cur.limit(1)
        with pytest.raises(CursorException):
            cur.hybrid_limits(2)
        with pytest.raises(CursorException):
            cur.include_scores(False)
        with pytest.raises(CursorException):
            cur.include_sort_vector(False)
        with pytest.raises(CursorException):
            cur.rerank_on("field")
        with pytest.raises(CursorException):
            cur.map(lambda rw: None)

    @pytest.mark.describe("test of collection farr-cursors has_next, async")
    async def test_collection_farrcursors_has_next_async(
        self,
        afilled_vectorize_collection: DefaultAsyncCollection,
    ) -> None:
        cur = afilled_vectorize_collection.find_and_rerank(
            sort={"$hybrid": "a sentence."},
            limit=NUM_DOCS,
        )
        assert cur.state == CursorState.IDLE
        assert cur.consumed == 0
        assert await cur.has_next()
        assert cur.state == CursorState.IDLE
        assert cur.consumed == 0
        [r_res async for r_res in cur]
        assert cur.consumed == NUM_DOCS
        assert cur.state == CursorState.CLOSED  # type: ignore[comparison-overlap]

        curmf = afilled_vectorize_collection.find_and_rerank(
            sort={"$hybrid": "a sentence."},
            limit=NUM_DOCS,
        )
        await curmf.__anext__()
        await curmf.__anext__()
        assert curmf.consumed == 2
        assert curmf.state == CursorState.STARTED
        assert await curmf.has_next()
        assert curmf.consumed == 2
        assert curmf.state == CursorState.STARTED
        for _ in range(18):
            await curmf.__anext__()
        # not very relevant as there's no actual pages here
        assert await curmf.has_next()
        assert curmf.consumed == 20
        assert curmf.state == CursorState.STARTED
        assert curmf.buffered_count == NUM_DOCS - 20

        cur0 = afilled_vectorize_collection.find_and_rerank(
            sort={"$hybrid": "a sentence."},
            limit=NUM_DOCS,
        )
        cur0.close()
        assert not (await cur0.has_next())

    @pytest.mark.describe("test of collection farr-cursors zero matches, async")
    async def test_collection_farrcursors_zeromatches_async(
        self,
        afilled_vectorize_collection: DefaultAsyncCollection,
    ) -> None:
        cur = afilled_vectorize_collection.find_and_rerank(
            {"parity": -1},
            sort={"$hybrid": "a sentence."},
            limit=NUM_DOCS,
        )
        assert not await cur.has_next()
        assert [r_res async for r_res in cur] == []

        cur_no_sv = afilled_vectorize_collection.find_and_rerank(
            {"parity": -1},
            sort={"$hybrid": "a sentence."},
            limit=NUM_DOCS,
            include_sort_vector=False,
        )
        assert await cur_no_sv.get_sort_vector() is None
        assert await cur_no_sv.to_list() == []
        assert await cur_no_sv.get_sort_vector() is None

        cur_with_sv = afilled_vectorize_collection.find_and_rerank(
            {"parity": -1},
            sort={"$hybrid": "a sentence."},
            limit=NUM_DOCS,
            include_sort_vector=True,
        )
        sort_vector = await cur_with_sv.get_sort_vector()
        assert sort_vector is not None
        assert isinstance(sort_vector, DataAPIVector)

    @pytest.mark.describe("test of prematurely farr-closing collection cursors, async")
    async def test_collection_farrcursors_early_closing_async(
        self,
        afilled_vectorize_collection: DefaultAsyncCollection,
    ) -> None:
        cur = afilled_vectorize_collection.find_and_rerank(
            sort={"$hybrid": "a sentence."},
            limit=NUM_DOCS,
        )
        for _ in range(12):
            await cur.__anext__()
        cur.close()
        assert cur.state == CursorState.CLOSED
        assert cur.buffered_count == 0
        assert cur.consumed == 12
        # rewind test
        cur.rewind()
        assert len([r_res async for r_res in cur]) == NUM_DOCS

    @pytest.mark.describe("test of mappings with collection farr-cursors, async")
    async def test_collection_farrcursors_mapping_async(
        self,
        afilled_vectorize_collection: DefaultAsyncCollection,
    ) -> None:
        base_rows = [
            r_res
            async for r_res in afilled_vectorize_collection.find_and_rerank(
                sort={"$hybrid": "a sentence."},
                limit=NUM_DOCS,
            )
        ]
        assert len(base_rows) == NUM_DOCS
        base_rows_mu = [
            r_res
            async for r_res in afilled_vectorize_collection.find_and_rerank(
                sort={"$hybrid": "a sentence."},
                limit=NUM_DOCS - 3,
            )
        ]
        assert len(base_rows_mu) == NUM_DOCS - 3

        parity_backmap = {"odd": 1, "even": 0}

        def mint(r_result: RerankedResult[DefaultDocumentType]) -> int:
            return 1 + parity_backmap[r_result.document["parity"]]

        def mmult(val: int) -> int:
            return 1000 * val

        # map, base
        mcur = afilled_vectorize_collection.find_and_rerank(
            sort={"$hybrid": "a sentence."},
            limit=NUM_DOCS,
        ).map(mint)
        mints = [val async for val in mcur]
        assert mints == [mint(row) for row in base_rows]

        # map composition
        mmcur = (
            afilled_vectorize_collection.find_and_rerank(
                sort={"$hybrid": "a sentence."},
                limit=NUM_DOCS,
            )
            .map(mint)
            .map(mmult)
        )
        mmints = [val async for val in mmcur]
        assert mmints == [mmult(mint(row)) for row in base_rows]

        # consume_buffer skips the map
        hmcur = afilled_vectorize_collection.find_and_rerank(
            sort={"$hybrid": "a sentence."},
            limit=NUM_DOCS,
        ).map(mint)
        for _ in range(10):
            await hmcur.__anext__()
        conbuf = hmcur.consume_buffer(2)
        assert len(conbuf) == 2
        assert all(isinstance(itm, RerankedResult) for itm in conbuf)

        # rewind preserves the mapping
        rwcur = afilled_vectorize_collection.find_and_rerank(
            sort={"$hybrid": "a sentence."},
            limit=NUM_DOCS,
        ).map(mint)
        for _ in range(10):
            await rwcur.__anext__()
        rwcur.rewind()
        assert await rwcur.__anext__() == mints[0]

        # clone rewinds
        cl_unmapped = rwcur.clone()
        assert await cl_unmapped.__anext__() == mint(base_rows[0])

    @pytest.mark.describe(
        "test of collection farr-cursors, for_each and to_list, async"
    )
    async def test_collection_farrcursors_collective_methods_async(
        self,
        afilled_vectorize_collection: DefaultAsyncCollection,
    ) -> None:
        base_rows = [
            r_res
            async for r_res in afilled_vectorize_collection.find_and_rerank(
                sort={"$hybrid": "a sentence."},
                limit=NUM_DOCS,
            )
        ]

        # full to_list
        tl_cur = afilled_vectorize_collection.find_and_rerank(
            sort={"$hybrid": "a sentence."},
            limit=NUM_DOCS,
        )
        assert await tl_cur.to_list() == base_rows
        assert tl_cur.state == CursorState.CLOSED

        # partially-consumed to_list
        ptl_cur = afilled_vectorize_collection.find_and_rerank(
            sort={"$hybrid": "a sentence."},
            limit=NUM_DOCS,
        )
        for _ in range(15):
            await ptl_cur.__anext__()
        assert await ptl_cur.to_list() == base_rows[15:]
        assert ptl_cur.state == CursorState.CLOSED

        # mapped to_list

        parity_backmap = {"odd": 1, "even": 0}

        def mint(r_result: RerankedResult[DefaultDocumentType]) -> int:
            return parity_backmap[r_result.document["parity"]]

        mtl_cur = afilled_vectorize_collection.find_and_rerank(
            sort={"$hybrid": "a sentence."},
            limit=NUM_DOCS,
        ).map(mint)
        for _ in range(13):
            await mtl_cur.__anext__()
        assert await mtl_cur.to_list() == [mint(row) for row in base_rows[13:]]
        assert mtl_cur.state == CursorState.CLOSED

        # full for_each
        accum0: list[RerankedResult[DefaultDocumentType]] = []

        def marker0(
            r_result: RerankedResult[DefaultDocumentType],
            acc: list[RerankedResult[DefaultDocumentType]] = accum0,
        ) -> None:
            acc += [r_result]

        fe_cur = afilled_vectorize_collection.find_and_rerank(
            sort={"$hybrid": "a sentence."},
            limit=NUM_DOCS,
        )
        await fe_cur.for_each(marker0)
        assert accum0 == base_rows
        assert fe_cur.state == CursorState.CLOSED

        # partially-consumed for_each
        accum1: list[RerankedResult[DefaultDocumentType]] = []

        def marker1(
            r_result: RerankedResult[DefaultDocumentType],
            acc: list[RerankedResult[DefaultDocumentType]] = accum1,
        ) -> None:
            acc += [r_result]

        pfe_cur = afilled_vectorize_collection.find_and_rerank(
            sort={"$hybrid": "a sentence."},
            limit=NUM_DOCS,
        )
        for _ in range(11):
            await pfe_cur.__anext__()
        await pfe_cur.for_each(marker1)
        assert accum1 == base_rows[11:]
        assert pfe_cur.state == CursorState.CLOSED

        # mapped for_each
        accum2: list[int] = []

        def marker2(val: int, acc: list[int] = accum2) -> None:
            acc += [val]

        mfe_cur = afilled_vectorize_collection.find_and_rerank(
            sort={"$hybrid": "a sentence."},
            limit=NUM_DOCS,
        ).map(mint)
        for _ in range(17):
            await mfe_cur.__anext__()
        await mfe_cur.for_each(marker2)
        assert accum2 == [mint(row) for row in base_rows[17:]]
        assert mfe_cur.state == CursorState.CLOSED

        # breaking (early) for_each
        accum3: list[RerankedResult[DefaultDocumentType]] = []

        def marker3(
            r_result: RerankedResult[DefaultDocumentType],
            acc: list[RerankedResult[DefaultDocumentType]] = accum3,
        ) -> bool:
            acc += [r_result]
            return len(acc) < 5

        bfe_cur = afilled_vectorize_collection.find_and_rerank(
            sort={"$hybrid": "a sentence."},
            limit=NUM_DOCS,
        )
        await bfe_cur.for_each(marker3)
        assert accum3 == base_rows[:5]
        assert bfe_cur.state == CursorState.STARTED
        bfe_another = await bfe_cur.__anext__()
        assert bfe_another == base_rows[5]

        # nonbool-nonbreaking for_each
        accum4: list[RerankedResult[DefaultDocumentType]] = []

        def marker4(
            r_result: RerankedResult[DefaultDocumentType],
            acc: list[RerankedResult[DefaultDocumentType]] = accum4,
        ) -> int:
            acc += [r_result]
            return 8 if len(acc) < 5 else 0

        nbfe_cur = afilled_vectorize_collection.find_and_rerank(
            sort={"$hybrid": "a sentence."},
            limit=NUM_DOCS,
        )
        await nbfe_cur.for_each(marker4)  # type: ignore[arg-type]
        assert accum4 == base_rows
        assert nbfe_cur.state == CursorState.CLOSED

    @pytest.mark.describe(
        "test of collection farr-cursors, serdes options obeyance, async"
    )
    async def test_collection_farrcursors_serdes_options_async(
        self,
        afilled_vectorize_collection: DefaultAsyncCollection,
    ) -> None:
        # serdes options obeyance check
        noncustom_compo_collection = afilled_vectorize_collection.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(custom_datatypes_in_reading=False),
            )
        )
        custom_compo_collection = afilled_vectorize_collection.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(custom_datatypes_in_reading=True),
            )
        )
        noncustom_rows = await noncustom_compo_collection.find_and_rerank(
            sort={"$hybrid": "a sentence."},
            projection={"$vector": True},
            limit=NUM_DOCS,
        ).to_list()
        assert len(noncustom_rows) == NUM_DOCS
        assert all(
            isinstance(nc_r_res.document["$vector"], list)
            for nc_r_res in noncustom_rows
        )
        custom_rows = await custom_compo_collection.find_and_rerank(
            sort={"$hybrid": "a sentence."},
            projection={"$vector": True},
            limit=NUM_DOCS,
        ).to_list()
        assert len(custom_rows) == NUM_DOCS
        assert all(
            isinstance(c_r_res.document["$vector"], DataAPIVector)
            for c_r_res in custom_rows
        )

    @pytest.mark.describe(
        "test of collection farr-cursors, include_sort_vector and serdes, async"
    )
    async def test_collection_farrcursors_include_sort_vector_serdes_async(
        self,
        afilled_vectorize_collection: DefaultAsyncCollection,
    ) -> None:
        col_v0_d0 = afilled_vectorize_collection.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    custom_datatypes_in_reading=False,
                    use_decimals_in_collections=False,
                ),
            ),
        )
        col_v0_d1 = afilled_vectorize_collection.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    custom_datatypes_in_reading=False,
                    use_decimals_in_collections=True,
                ),
            ),
        )
        col_v1_d0 = afilled_vectorize_collection.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    custom_datatypes_in_reading=True,
                    use_decimals_in_collections=False,
                ),
            ),
        )
        col_v1_d1 = afilled_vectorize_collection.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    custom_datatypes_in_reading=True,
                    use_decimals_in_collections=True,
                ),
            ),
        )

        cur0_v0_d0 = col_v0_d0.find_and_rerank(sort={"$hybrid": "query"})
        cur0_v0_d1 = col_v0_d1.find_and_rerank(sort={"$hybrid": "query"})
        cur0_v1_d0 = col_v1_d0.find_and_rerank(sort={"$hybrid": "query"})
        cur0_v1_d1 = col_v1_d1.find_and_rerank(sort={"$hybrid": "query"})
        assert await cur0_v0_d0.get_sort_vector() is None
        assert await cur0_v0_d1.get_sort_vector() is None
        assert await cur0_v1_d0.get_sort_vector() is None
        assert await cur0_v1_d1.get_sort_vector() is None

        cur1_v0_d0 = col_v0_d0.find_and_rerank(
            sort={"$hybrid": "query"},
            include_sort_vector=True,
        )
        cur1_v0_d1 = col_v0_d1.find_and_rerank(
            sort={"$hybrid": "query"},
            include_sort_vector=True,
        )
        cur1_v1_d0 = col_v1_d0.find_and_rerank(
            sort={"$hybrid": "query"},
            include_sort_vector=True,
        )
        cur1_v1_d1 = col_v1_d1.find_and_rerank(
            sort={"$hybrid": "query"},
            include_sort_vector=True,
        )
        sv_v0_d0 = await cur1_v0_d0.get_sort_vector()
        sv_v0_d1 = await cur1_v0_d1.get_sort_vector()
        sv_v1_d0 = await cur1_v1_d0.get_sort_vector()
        sv_v1_d1 = await cur1_v1_d1.get_sort_vector()

        assert isinstance(sv_v0_d0, list)
        assert isinstance(sv_v0_d1, list)
        assert isinstance(sv_v0_d0[0], float)
        assert isinstance(sv_v0_d1[0], float)
        assert isinstance(sv_v1_d0, DataAPIVector)
        assert isinstance(sv_v1_d1, DataAPIVector)
