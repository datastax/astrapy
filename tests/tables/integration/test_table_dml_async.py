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
from astrapy.data_types import DataAPITimestamp, DataAPIVector
from astrapy.exceptions import DataAPIException, TableInsertManyException
from astrapy.results import TableInsertManyResult

from ..conftest import (
    AR_DOC_0,
    AR_DOC_PK_0,
    DISTINCT_AR_DOCS,
    S_DOCS,
    DefaultAsyncTable,
    _repaint_NaNs,
)
from .table_row_assets import (
    SIMPLE_SEVEN_ROWS_F2,
    SIMPLE_SEVEN_ROWS_F4,
    SIMPLE_SEVEN_ROWS_OK,
)


class TestTableDMLSync:
    @pytest.mark.describe("test of table insert_one and find_one, async")
    async def test_table_insert_one_find_one_async(
        self,
        async_empty_table_all_returns: DefaultAsyncTable,
    ) -> None:
        # TODO enlarge the test with all values + a partial row
        # TODO + the different custom/nonocustom types and serdes options interplay
        # TODO cross check with CQL direct (!), astra only
        no_doc_0a = await async_empty_table_all_returns.find_one(filter=AR_DOC_PK_0)
        assert no_doc_0a is None
        await async_empty_table_all_returns.insert_one(row=AR_DOC_0)
        doc_0 = await async_empty_table_all_returns.find_one(filter=AR_DOC_PK_0)
        assert doc_0 is not None
        assert {doc_0[k] == v for k, v in AR_DOC_0.items()}
        # projection:
        projected_fields = {"p_bigint", "p_boolean"}
        doc_0 = await async_empty_table_all_returns.find_one(
            filter=AR_DOC_PK_0,
            projection={pf: True for pf in projected_fields},
        )
        assert doc_0 is not None
        assert {doc_0[k] == AR_DOC_0[k] for k in projected_fields}
        assert doc_0.keys() == projected_fields
        # delete and retry
        await async_empty_table_all_returns.delete_one(filter=AR_DOC_PK_0)
        no_doc_0b = await async_empty_table_all_returns.find_one(filter=AR_DOC_PK_0)
        assert no_doc_0b is None

    @pytest.mark.describe("test of table delete_one, async")
    async def test_table_delete_one_async(
        self,
        async_empty_table_simple: DefaultAsyncTable,
    ) -> None:
        # TODO cross check with CQL direct (!), astra only
        # TODO replace below with insert_many once landed
        for s_doc in S_DOCS:
            await async_empty_table_simple.insert_one(s_doc)
        assert len(await async_empty_table_simple.find({}).to_list()) == 3

        await async_empty_table_simple.delete_one({"p_text": "Z"})
        assert len(await async_empty_table_simple.find({}).to_list()) == 3

        await async_empty_table_simple.delete_one({"p_text": "A1"})
        assert len(await async_empty_table_simple.find({}).to_list()) == 2

    @pytest.mark.describe("test of table delete_many, async")
    async def test_table_delete_many_async(
        self,
        async_empty_table_simple: DefaultAsyncTable,
    ) -> None:
        # TODO cross check with CQL direct (!), astra only
        # TODO replace below with insert_many once landed
        # TODO enrich with an index-based clause not using $in
        for s_doc in S_DOCS:
            await async_empty_table_simple.insert_one(s_doc)
        assert len(await async_empty_table_simple.find({}).to_list()) == 3

        await async_empty_table_simple.delete_many({"p_text": {"$in": ["Z", "Y"]}})
        assert len(await async_empty_table_simple.find({}).to_list()) == 3

        await async_empty_table_simple.delete_many({"p_text": {"$in": ["A1", "A2"]}})
        assert len(await async_empty_table_simple.find({}).to_list()) == 1

    @pytest.mark.describe("test of table distinct, async")
    async def test_table_distinct_async(
        self,
        async_empty_table_all_returns: DefaultAsyncTable,
    ) -> None:
        # TODO replace with insert many:
        for s_doc in DISTINCT_AR_DOCS:
            await async_empty_table_all_returns.insert_one(s_doc)

        d_float = await async_empty_table_all_returns.distinct("p_float")
        exp_d_float = {0.1, 0.2, float("NaN")}
        assert set(_repaint_NaNs(d_float)) == _repaint_NaNs(exp_d_float)

        d_text = await async_empty_table_all_returns.distinct("p_text")
        exp_d_text = {"a", "b", None}
        assert set(d_text) == set(exp_d_text)

        d_timestamp = await async_empty_table_all_returns.distinct("p_timestamp")
        exp_d_timestamp = {
            DataAPITimestamp.from_string("1111-01-01T01:01:01Z"),
            DataAPITimestamp.from_string("1221-01-01T01:01:01Z"),
            None,
        }
        assert set(d_timestamp) == set(exp_d_timestamp)

        d_list_int = await async_empty_table_all_returns.distinct("p_list_int")
        exp_d_list_int = {1, 2, 3}
        assert set(d_list_int) == set(exp_d_list_int)

        d_p_map_text_text = await async_empty_table_all_returns.distinct(
            "p_map_text_text.a"
        )
        exp_d_p_map_text_text = {"va", "VA"}
        assert set(d_p_map_text_text) == set(exp_d_p_map_text_text)

        d_set_int = await async_empty_table_all_returns.distinct("p_set_int")
        exp_d_set_int = {100, 200, 300}
        assert set(d_set_int) == set(exp_d_set_int)

    @pytest.mark.describe("test of table insert_many, async")
    async def test_table_insert_many_async(
        self,
        async_table_simple: DefaultAsyncTable,
    ) -> None:
        """ordered/unordered, concurrent/nonconcurrent; with good/failing rows"""

        async def _pkeys() -> set[str]:
            return {
                row["p_text"]
                async for row in async_table_simple.find(
                    {}, projection={"p_text": True}
                )
            }

        async def _assert_consistency(
            p_text_values: list[str], ins_result: TableInsertManyResult
        ) -> None:
            pkeys = await _pkeys()
            assert pkeys == set(p_text_values)
            assert ins_result.inserted_id_tuples == [(pk,) for pk in p_text_values]
            assert ins_result.inserted_ids == [{"p_text": pk} for pk in p_text_values]

        # ordered, good rows
        await async_table_simple.delete_many({})
        i_result = await async_table_simple.insert_many(
            SIMPLE_SEVEN_ROWS_OK, ordered=True, chunk_size=2
        )
        await _assert_consistency(["p1", "p2", "p3", "p4", "p5", "p6", "p7"], i_result)

        # ordered, failing first batch
        await async_table_simple.delete_many({})
        with pytest.raises(TableInsertManyException) as exc:
            await async_table_simple.insert_many(
                SIMPLE_SEVEN_ROWS_F2, ordered=True, chunk_size=2
            )
        await _assert_consistency([], exc.value.partial_result)

        # ordered, failing later batch
        await async_table_simple.delete_many({})
        with pytest.raises(TableInsertManyException) as exc:
            await async_table_simple.insert_many(
                SIMPLE_SEVEN_ROWS_F4, ordered=True, chunk_size=2
            )
        await _assert_consistency(["p1", "p2"], exc.value.partial_result)

        # unordered/concurrency=1, good rows
        await async_table_simple.delete_many({})
        await async_table_simple.insert_many(
            SIMPLE_SEVEN_ROWS_OK, ordered=False, chunk_size=2, concurrency=1
        )
        await _assert_consistency(["p1", "p2", "p3", "p4", "p5", "p6", "p7"], i_result)

        # unordered/concurrency=1, failing first batch
        await async_table_simple.delete_many({})
        with pytest.raises(TableInsertManyException) as exc:
            await async_table_simple.insert_many(
                SIMPLE_SEVEN_ROWS_F2, ordered=False, chunk_size=2, concurrency=1
            )
        await _assert_consistency(
            ["p1", "p3", "p4", "p5", "p6", "p7"], exc.value.partial_result
        )

        # unordered/concurrency=1, failing later batch
        await async_table_simple.delete_many({})
        with pytest.raises(TableInsertManyException) as exc:
            await async_table_simple.insert_many(
                SIMPLE_SEVEN_ROWS_F4, ordered=False, chunk_size=2, concurrency=1
            )
        await _assert_consistency(
            ["p1", "p2", "p3", "p5", "p6", "p7"], exc.value.partial_result
        )

        # unordered/concurrency=2, good rows
        await async_table_simple.delete_many({})
        i_result = await async_table_simple.insert_many(
            SIMPLE_SEVEN_ROWS_OK, ordered=False, chunk_size=2, concurrency=2
        )
        await _assert_consistency(["p1", "p2", "p3", "p4", "p5", "p6", "p7"], i_result)

        # unordered/concurrency=2, failing first batch
        await async_table_simple.delete_many({})
        with pytest.raises(TableInsertManyException) as exc:
            await async_table_simple.insert_many(
                SIMPLE_SEVEN_ROWS_F2, ordered=False, chunk_size=2, concurrency=2
            )
        await _assert_consistency(
            ["p1", "p3", "p4", "p5", "p6", "p7"], exc.value.partial_result
        )

        # unordered/concurrency=2, failing later batch
        await async_table_simple.delete_many({})
        with pytest.raises(TableInsertManyException) as exc:
            await async_table_simple.insert_many(
                SIMPLE_SEVEN_ROWS_F4, ordered=False, chunk_size=2, concurrency=2
            )
        await _assert_consistency(
            ["p1", "p2", "p3", "p5", "p6", "p7"], exc.value.partial_result
        )

    @pytest.mark.describe("test of table update_one, async")
    async def test_table_update_one_async(
        self,
        async_table_simple: DefaultAsyncTable,
    ) -> None:
        # erroring updates
        with pytest.raises(DataAPIException):
            await async_table_simple.update_one(
                {"p_text": {"$gt": "x"}},
                update={"$set": {"p_int": -111}},
            )
        with pytest.raises(DataAPIException):
            await async_table_simple.update_one(
                {"p_text": "x"},
                update={"$set": {"p_int": -111, "p_text": "x"}},
            )
        with pytest.raises(DataAPIException):
            await async_table_simple.update_one(
                {"p_text": "x"},
                update={"$unset": {"p_text": ""}},
            )
        with pytest.raises(DataAPIException):
            await async_table_simple.update_one(
                {"p_text": "x"},
                update={"$set": {"p_fake": "x"}},
            )
        with pytest.raises(DataAPIException):
            await async_table_simple.update_one(
                {"p_int": 147},
                update={"$set": {"p_int": -111}},
            )

        # $set
        await async_table_simple.delete_many({})
        assert await async_table_simple.find_one({"p_text": "A"}) is None

        # $set, new row
        await async_table_simple.update_one(
            {"p_text": "A"},
            update={"$set": {"p_int": 10, "p_vector": DataAPIVector([1, 2, 3])}},
        )
        assert await async_table_simple.find_one({"p_text": "A"}) == {
            "p_text": "A",
            "p_int": 10,
            "p_vector": DataAPIVector([1, 2, 3]),
        }

        # $set, partial write
        await async_table_simple.update_one(
            {"p_text": "A"},
            update={"$set": {"p_int": 11}},
        )
        assert await async_table_simple.find_one({"p_text": "A"}) == {
            "p_text": "A",
            "p_int": 11,
            "p_vector": DataAPIVector([1, 2, 3]),
        }

        # $set, overwriting row
        await async_table_simple.update_one(
            {"p_text": "A"},
            update={"$set": {"p_int": 10, "p_vector": DataAPIVector([1, 2, 3])}},
        )
        assert await async_table_simple.find_one({"p_text": "A"}) == {
            "p_text": "A",
            "p_int": 10,
            "p_vector": DataAPIVector([1, 2, 3]),
        }

        # $set, partial set-to-null
        await async_table_simple.update_one(
            {"p_text": "A"},
            update={"$set": {"p_int": None}},
        )
        assert await async_table_simple.find_one({"p_text": "A"}) == {
            "p_text": "A",
            "p_int": None,
            "p_vector": DataAPIVector([1, 2, 3]),
        }

        # $set, full set-to-null (which deletes the row)
        await async_table_simple.update_one(
            {"p_text": "A"},
            update={"$set": {"p_int": None, "p_vector": None}},
        )
        assert await async_table_simple.find_one({"p_text": "A"}) is None

        # $unset
        await async_table_simple.delete_many({})
        await async_table_simple.insert_one(
            {"p_text": "B", "p_int": 123, "p_vector": DataAPIVector([9, 8, 7])}
        )
        assert await async_table_simple.find_one({"p_text": "B"}) == {
            "p_text": "B",
            "p_int": 123,
            "p_vector": DataAPIVector([9, 8, 7]),
        }

        # $unset, on existing(partial)
        await async_table_simple.update_one(
            {"p_text": "B"},
            update={"$unset": {"p_int": ""}},
        )
        assert await async_table_simple.find_one({"p_text": "B"}) == {
            "p_text": "B",
            "p_int": None,
            "p_vector": DataAPIVector([9, 8, 7]),
        }

        # $unset, on existing(partial) 2: the other column
        await async_table_simple.update_one(
            {"p_text": "B"},
            update={"$unset": {"p_vector": ""}},
        )
        assert await async_table_simple.find_one({"p_text": "B"}) == {
            "p_text": "B",
            "p_int": None,
            "p_vector": None,
        }

        # $unset, on existing(full) (which DOES NOT DELETE the row)
        await async_table_simple.update_one(
            {"p_text": "B"},
            update={"$unset": {"p_int": "", "p_vector": ""}},
        )
        assert await async_table_simple.find_one({"p_text": "B"}) == {
            "p_text": "B",
            "p_int": None,
            "p_vector": None,
        }

        # $unset, on nonexisting
        await async_table_simple.update_one(
            {"p_text": "Z"},
            update={"$unset": {"p_int": ""}},
        )
        assert await async_table_simple.find_one({"p_text": "Z"}) is None

    @pytest.mark.describe("test of include_sort_vector with serdes options, async")
    async def test_collection_include_sort_vector_serdes_options_async(
        self,
        async_table_simple: DefaultAsyncTable,
    ) -> None:
        acol_v0 = async_table_simple.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    custom_datatypes_in_reading=False,
                ),
            ),
        )
        acol_v1 = async_table_simple.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    custom_datatypes_in_reading=True,
                ),
            ),
        )
        cur0_v0_inpf = acol_v0.find(sort={"p_vector": [1, 2, 3]})
        cur0_v1_inpf = acol_v1.find(sort={"p_vector": [1, 2, 3]})

        assert await cur0_v0_inpf.get_sort_vector() is None
        assert await cur0_v1_inpf.get_sort_vector() is None

        cur1_v0_inpf = acol_v0.find(
            sort={"p_vector": [1, 2, 3]}, include_sort_vector=True
        )
        cur1_v1_inpf = acol_v1.find(
            sort={"p_vector": [1, 2, 3]}, include_sort_vector=True
        )

        assert await cur1_v0_inpf.get_sort_vector() == [1, 2, 3]
        assert await cur1_v1_inpf.get_sort_vector() == DataAPIVector([1, 2, 3])

        cur0_v0_inpv = acol_v0.find(sort={"p_vector": DataAPIVector([1, 2, 3])})
        cur0_v1_inpv = acol_v1.find(sort={"p_vector": DataAPIVector([1, 2, 3])})

        assert await cur0_v0_inpv.get_sort_vector() is None
        assert await cur0_v1_inpv.get_sort_vector() is None

        cur1_v0_inpv = acol_v0.find(
            sort={"p_vector": DataAPIVector([1, 2, 3])}, include_sort_vector=True
        )
        cur1_v1_inpv = acol_v1.find(
            sort={"p_vector": DataAPIVector([1, 2, 3])}, include_sort_vector=True
        )

        assert await cur1_v0_inpv.get_sort_vector() == [1, 2, 3]
        assert await cur1_v1_inpv.get_sort_vector() == DataAPIVector([1, 2, 3])
