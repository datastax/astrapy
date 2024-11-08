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
from astrapy.exceptions import TableInsertManyException
from astrapy.results import TableInsertManyResult

from ..conftest import (
    AR_DOC_0,
    AR_DOC_PK_0,
    DISTINCT_AR_DOCS,
    S_DOCS,
    DefaultTable,
    _repaint_NaNs,
)
from .table_row_assets import (
    SIMPLE_SEVEN_ROWS_F2,
    SIMPLE_SEVEN_ROWS_F4,
    SIMPLE_SEVEN_ROWS_OK,
)


class TestTableDMLSync:
    @pytest.mark.describe("test of table insert_one and find_one, sync")
    def test_table_insert_one_find_one_sync(
        self,
        sync_empty_table_all_returns: DefaultTable,
    ) -> None:
        # TODO enlarge the test with all values + a partial row,
        # TODO + the different custom/nonocustom types and serdes options interplay
        # TODO cross check with CQL direct (!), astra only
        no_doc_0a = sync_empty_table_all_returns.find_one(filter=AR_DOC_PK_0)
        assert no_doc_0a is None
        sync_empty_table_all_returns.insert_one(row=AR_DOC_0)
        doc_0 = sync_empty_table_all_returns.find_one(filter=AR_DOC_PK_0)
        assert doc_0 is not None
        assert {doc_0[k] == v for k, v in AR_DOC_0.items()}
        # projection:
        projected_fields = {"p_bigint", "p_boolean"}
        doc_0 = sync_empty_table_all_returns.find_one(
            filter=AR_DOC_PK_0,
            projection={pf: True for pf in projected_fields},
        )
        assert doc_0 is not None
        assert {doc_0[k] == AR_DOC_0[k] for k in projected_fields}
        assert doc_0.keys() == projected_fields
        # delete and retry
        sync_empty_table_all_returns.delete_one(filter=AR_DOC_PK_0)
        no_doc_0b = sync_empty_table_all_returns.find_one(filter=AR_DOC_PK_0)
        assert no_doc_0b is None

    @pytest.mark.describe("test of table delete_one, sync")
    def test_table_delete_one_sync(
        self,
        sync_empty_table_simple: DefaultTable,
    ) -> None:
        # TODO cross check with CQL direct (!), astra only
        # TODO replace below with insert_many once landed
        for s_doc in S_DOCS:
            sync_empty_table_simple.insert_one(s_doc)
        assert len(sync_empty_table_simple.find({}).to_list()) == 3

        sync_empty_table_simple.delete_one({"p_text": "Z"})
        assert len(sync_empty_table_simple.find({}).to_list()) == 3

        sync_empty_table_simple.delete_one({"p_text": "A1"})
        assert len(sync_empty_table_simple.find({}).to_list()) == 2

    @pytest.mark.describe("test of table delete_many, sync")
    def test_table_delete_many_sync(
        self,
        sync_empty_table_simple: DefaultTable,
    ) -> None:
        # TODO cross check with CQL direct (!), astra only
        # TODO replace below with insert_many once landed
        # TODO enrich with an index-based clause not using $in
        for s_doc in S_DOCS:
            sync_empty_table_simple.insert_one(s_doc)
        assert len(sync_empty_table_simple.find({}).to_list()) == 3

        sync_empty_table_simple.delete_many({"p_text": {"$in": ["Z", "Y"]}})
        assert len(sync_empty_table_simple.find({}).to_list()) == 3

        sync_empty_table_simple.delete_many({"p_text": {"$in": ["A1", "A2"]}})
        assert len(sync_empty_table_simple.find({}).to_list()) == 1

    @pytest.mark.describe("test of table distinct, sync")
    def test_table_distinct_sync(
        self,
        sync_empty_table_all_returns: DefaultTable,
    ) -> None:
        # TODO replace with insert many:
        for s_doc in DISTINCT_AR_DOCS:
            sync_empty_table_all_returns.insert_one(s_doc)

        d_float = sync_empty_table_all_returns.distinct("p_float")
        exp_d_float = {0.1, 0.2, float("NaN")}
        assert set(_repaint_NaNs(d_float)) == _repaint_NaNs(exp_d_float)

        d_text = sync_empty_table_all_returns.distinct("p_text")
        exp_d_text = {"a", "b", None}
        assert set(d_text) == set(exp_d_text)

        d_timestamp = sync_empty_table_all_returns.distinct("p_timestamp")
        exp_d_timestamp = {
            DataAPITimestamp.from_string("1111-01-01T01:01:01Z"),
            DataAPITimestamp.from_string("1221-01-01T01:01:01Z"),
            None,
        }
        assert set(d_timestamp) == set(exp_d_timestamp)

        d_list_int = sync_empty_table_all_returns.distinct("p_list_int")
        exp_d_list_int = {1, 2, 3}
        assert set(d_list_int) == set(exp_d_list_int)

        d_p_map_text_text = sync_empty_table_all_returns.distinct("p_map_text_text.a")
        exp_d_p_map_text_text = {"va", "VA"}
        assert set(d_p_map_text_text) == set(exp_d_p_map_text_text)

        d_set_int = sync_empty_table_all_returns.distinct("p_set_int")
        exp_d_set_int = {100, 200, 300}
        assert set(d_set_int) == set(exp_d_set_int)

    @pytest.mark.describe("test of table insert_many, sync")
    def test_table_insert_many_sync(
        self,
        sync_table_simple: DefaultTable,
    ) -> None:
        """ordered/unordered, concurrent/nonconcurrent; with good/failing rows"""

        def _pkeys() -> set[str]:
            return {
                row["p_text"]
                for row in sync_table_simple.find({}, projection={"p_text": True})
            }

        def _assert_consistency(
            p_text_values: list[str], ins_result: TableInsertManyResult
        ) -> None:
            assert _pkeys() == set(p_text_values)
            assert ins_result.inserted_id_tuples == [(pk,) for pk in p_text_values]
            assert ins_result.inserted_ids == [{"p_text": pk} for pk in p_text_values]

        # ordered, good rows
        sync_table_simple.delete_many({})
        i_result = sync_table_simple.insert_many(
            SIMPLE_SEVEN_ROWS_OK, ordered=True, chunk_size=2
        )
        _assert_consistency(["p1", "p2", "p3", "p4", "p5", "p6", "p7"], i_result)

        # ordered, failing first batch
        sync_table_simple.delete_many({})
        with pytest.raises(TableInsertManyException) as exc:
            sync_table_simple.insert_many(
                SIMPLE_SEVEN_ROWS_F2, ordered=True, chunk_size=2
            )
        _assert_consistency([], exc.value.partial_result)

        # ordered, failing later batch
        sync_table_simple.delete_many({})
        with pytest.raises(TableInsertManyException) as exc:
            sync_table_simple.insert_many(
                SIMPLE_SEVEN_ROWS_F4, ordered=True, chunk_size=2
            )
        _assert_consistency(["p1", "p2"], exc.value.partial_result)

        # unordered/concurrency=1, good rows
        sync_table_simple.delete_many({})
        sync_table_simple.insert_many(
            SIMPLE_SEVEN_ROWS_OK, ordered=False, chunk_size=2, concurrency=1
        )
        _assert_consistency(["p1", "p2", "p3", "p4", "p5", "p6", "p7"], i_result)

        # unordered/concurrency=1, failing first batch
        sync_table_simple.delete_many({})
        with pytest.raises(TableInsertManyException) as exc:
            sync_table_simple.insert_many(
                SIMPLE_SEVEN_ROWS_F2, ordered=False, chunk_size=2, concurrency=1
            )
        _assert_consistency(
            ["p1", "p3", "p4", "p5", "p6", "p7"], exc.value.partial_result
        )

        # unordered/concurrency=1, failing later batch
        sync_table_simple.delete_many({})
        with pytest.raises(TableInsertManyException) as exc:
            sync_table_simple.insert_many(
                SIMPLE_SEVEN_ROWS_F4, ordered=False, chunk_size=2, concurrency=1
            )
        _assert_consistency(
            ["p1", "p2", "p3", "p5", "p6", "p7"], exc.value.partial_result
        )

        # unordered/concurrency=2, good rows
        sync_table_simple.delete_many({})
        i_result = sync_table_simple.insert_many(
            SIMPLE_SEVEN_ROWS_OK, ordered=False, chunk_size=2, concurrency=2
        )
        _assert_consistency(["p1", "p2", "p3", "p4", "p5", "p6", "p7"], i_result)

        # unordered/concurrency=2, failing first batch
        sync_table_simple.delete_many({})
        with pytest.raises(TableInsertManyException) as exc:
            sync_table_simple.insert_many(
                SIMPLE_SEVEN_ROWS_F2, ordered=False, chunk_size=2, concurrency=2
            )
        _assert_consistency(
            ["p1", "p3", "p4", "p5", "p6", "p7"], exc.value.partial_result
        )

        # unordered/concurrency=2, failing later batch
        sync_table_simple.delete_many({})
        with pytest.raises(TableInsertManyException) as exc:
            sync_table_simple.insert_many(
                SIMPLE_SEVEN_ROWS_F4, ordered=False, chunk_size=2, concurrency=2
            )
        _assert_consistency(
            ["p1", "p2", "p3", "p5", "p6", "p7"], exc.value.partial_result
        )
