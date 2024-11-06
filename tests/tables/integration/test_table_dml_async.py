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

from astrapy.data_types import DataAPITimestamp

from ..conftest import (
    AR_DOC_0,
    AR_DOC_PK_0,
    DISTINCT_AR_DOCS,
    S_DOCS,
    DefaultAsyncTable,
    _repaint_NaNs,
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
