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
    DefaultTable,
    _repaint_NaNs,
    _typify_tuple,
)
from .table_row_assets import (
    AR_DOC_0,
    AR_DOC_0_B,
    AR_DOC_PK_0,
    AR_DOC_PK_0_TUPLE,
    DISTINCT_AR_DOCS,
    DISTINCT_AR_DOCS_PK_TUPLES,
    DISTINCT_AR_DOCS_PKS,
    SIMPLE_FULL_DOCS,
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
        ins1_res_0 = sync_empty_table_all_returns.insert_one(row=AR_DOC_0)
        doc_0 = sync_empty_table_all_returns.find_one(filter=AR_DOC_PK_0)
        doc_0_nofilter = sync_empty_table_all_returns.find_one(filter={})
        assert doc_0 is not None
        assert doc_0 == doc_0_nofilter
        assert {doc_0[k] == v for k, v in AR_DOC_0.items()}
        assert ins1_res_0.inserted_id == AR_DOC_PK_0
        assert _typify_tuple(ins1_res_0.inserted_id_tuple) == _typify_tuple(
            AR_DOC_PK_0_TUPLE
        )
        # overwrite:
        ins1_res_0_b = sync_empty_table_all_returns.insert_one(row=AR_DOC_0_B)
        doc_0_b = sync_empty_table_all_returns.find_one(filter=AR_DOC_PK_0)
        assert doc_0_b is not None
        assert {doc_0_b[k] == v for k, v in AR_DOC_0_B.items()}
        assert ins1_res_0_b.inserted_id == AR_DOC_PK_0
        assert _typify_tuple(ins1_res_0_b.inserted_id_tuple) == _typify_tuple(
            AR_DOC_PK_0_TUPLE
        )
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
        sync_empty_table_all_returns.delete_one(filter=AR_DOC_PK_0)
        no_doc_0b = sync_empty_table_all_returns.find_one(filter=AR_DOC_PK_0)
        assert no_doc_0b is None

    @pytest.mark.describe("test of table delete_one, sync")
    def test_table_delete_one_sync(
        self,
        sync_empty_table_simple: DefaultTable,
    ) -> None:
        # TODO cross check with CQL direct (!), astra only
        im_result = sync_empty_table_simple.insert_many(SIMPLE_FULL_DOCS)
        assert len(im_result.inserted_ids) == len(SIMPLE_FULL_DOCS)
        assert len(im_result.inserted_id_tuples) == len(SIMPLE_FULL_DOCS)
        assert len(sync_empty_table_simple.find({}).to_list()) == len(SIMPLE_FULL_DOCS)

        sync_empty_table_simple.delete_one({"p_text": "Z"})
        assert len(sync_empty_table_simple.find({}).to_list()) == len(SIMPLE_FULL_DOCS)

        sync_empty_table_simple.delete_one({"p_text": "A1"})
        assert (
            len(sync_empty_table_simple.find({}).to_list()) == len(SIMPLE_FULL_DOCS) - 1
        )

    @pytest.mark.describe("test of table delete_many, sync")
    def test_table_delete_many_sync(
        self,
        sync_empty_table_simple: DefaultTable,
    ) -> None:
        # TODO cross check with CQL direct (!), astra only
        im_result = sync_empty_table_simple.insert_many(SIMPLE_FULL_DOCS)
        assert len(im_result.inserted_ids) == len(SIMPLE_FULL_DOCS)
        assert len(im_result.inserted_id_tuples) == len(SIMPLE_FULL_DOCS)
        assert len(sync_empty_table_simple.find({}).to_list()) == len(SIMPLE_FULL_DOCS)

        sync_empty_table_simple.delete_many({"p_text": {"$in": ["Z", "Y"]}})
        assert len(sync_empty_table_simple.find({}).to_list()) == 3

        sync_empty_table_simple.delete_many({"p_text": {"$in": ["A1", "A2"]}})
        assert len(sync_empty_table_simple.find({}).to_list()) == 1

    @pytest.mark.describe("test of table insert_many returned ids, sync")
    def test_table_insert_many_returned_ids_sync(
        self,
        sync_empty_table_all_returns: DefaultTable,
    ) -> None:
        im_result = sync_empty_table_all_returns.insert_many(DISTINCT_AR_DOCS)
        assert im_result.inserted_ids == DISTINCT_AR_DOCS_PKS
        assert [_typify_tuple(tpl) for tpl in im_result.inserted_id_tuples] == [
            _typify_tuple(tpl) for tpl in DISTINCT_AR_DOCS_PK_TUPLES
        ]

    @pytest.mark.describe("test of table distinct, sync")
    def test_table_distinct_sync(
        self,
        sync_empty_table_all_returns: DefaultTable,
    ) -> None:
        sync_empty_table_all_returns.insert_many(DISTINCT_AR_DOCS)

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

        # with massive amount of rows:
        many_rows = [{"p_text": f"r_{i}", "p_int": i} for i in range(200)]
        exp_tuple_set = {(row["p_text"],) for row in many_rows}
        # ordered
        sync_table_simple.delete_many({})
        ins_res_o = sync_table_simple.insert_many(many_rows, ordered=True)
        assert set(ins_res_o.inserted_id_tuples) == exp_tuple_set
        assert len(sync_table_simple.find().to_list()) == len(many_rows)
        # unordered, concurrency=1
        sync_table_simple.delete_many({})
        ins_res_u_c1 = sync_table_simple.insert_many(
            many_rows, ordered=False, concurrency=1
        )
        assert set(ins_res_u_c1.inserted_id_tuples) == exp_tuple_set
        assert len(sync_table_simple.find().to_list()) == len(many_rows)
        # unordered, concurrency>1
        sync_table_simple.delete_many({})
        ins_res_u_cn = sync_table_simple.insert_many(
            many_rows, ordered=False, concurrency=10
        )
        assert set(ins_res_u_cn.inserted_id_tuples) == exp_tuple_set
        assert len(sync_table_simple.find().to_list()) == len(many_rows)

    @pytest.mark.describe("test of table update_one, sync")
    def test_table_update_one_sync(
        self,
        sync_table_simple: DefaultTable,
    ) -> None:
        # erroring updates
        with pytest.raises(DataAPIException):
            sync_table_simple.update_one(
                {"p_text": {"$gt": "x"}},
                update={"$set": {"p_int": -111}},
            )
        with pytest.raises(DataAPIException):
            sync_table_simple.update_one(
                {"p_text": "x"},
                update={"$set": {"p_int": -111, "p_text": "x"}},
            )
        with pytest.raises(DataAPIException):
            sync_table_simple.update_one(
                {"p_text": "x"},
                update={"$unset": {"p_text": ""}},
            )
        with pytest.raises(DataAPIException):
            sync_table_simple.update_one(
                {"p_text": "x"},
                update={"$set": {"p_fake": "x"}},
            )
        with pytest.raises(DataAPIException):
            sync_table_simple.update_one(
                {"p_int": 147},
                update={"$set": {"p_int": -111}},
            )

        # $set
        sync_table_simple.delete_many({})
        assert sync_table_simple.find_one({"p_text": "A"}) is None

        # $set, new row
        sync_table_simple.update_one(
            {"p_text": "A"},
            update={"$set": {"p_int": 10, "p_vector": DataAPIVector([1, 2, 3])}},
        )
        assert sync_table_simple.find_one({"p_text": "A"}) == {
            "p_text": "A",
            "p_int": 10,
            "p_vector": DataAPIVector([1, 2, 3]),
        }

        # $set, partial write
        sync_table_simple.update_one(
            {"p_text": "A"},
            update={"$set": {"p_int": 11}},
        )
        assert sync_table_simple.find_one({"p_text": "A"}) == {
            "p_text": "A",
            "p_int": 11,
            "p_vector": DataAPIVector([1, 2, 3]),
        }

        # $set, overwriting row
        sync_table_simple.update_one(
            {"p_text": "A"},
            update={"$set": {"p_int": 10, "p_vector": DataAPIVector([1, 2, 3])}},
        )
        assert sync_table_simple.find_one({"p_text": "A"}) == {
            "p_text": "A",
            "p_int": 10,
            "p_vector": DataAPIVector([1, 2, 3]),
        }

        # $set, partial set-to-null
        sync_table_simple.update_one(
            {"p_text": "A"},
            update={"$set": {"p_int": None}},
        )
        assert sync_table_simple.find_one({"p_text": "A"}) == {
            "p_text": "A",
            "p_int": None,
            "p_vector": DataAPIVector([1, 2, 3]),
        }

        # $set, full set-to-null (which deletes the row as it was created by update)
        sync_table_simple.update_one(
            {"p_text": "A"},
            update={"$set": {"p_int": None, "p_vector": None}},
        )
        assert sync_table_simple.find_one({"p_text": "A"}) is None

        # $unset
        sync_table_simple.delete_many({})
        sync_table_simple.insert_one(
            {"p_text": "B", "p_int": 123, "p_vector": DataAPIVector([9, 8, 7])}
        )
        assert sync_table_simple.find_one({"p_text": "B"}) == {
            "p_text": "B",
            "p_int": 123,
            "p_vector": DataAPIVector([9, 8, 7]),
        }

        # $unset, on existing(partial)
        sync_table_simple.update_one(
            {"p_text": "B"},
            update={"$unset": {"p_int": ""}},
        )
        assert sync_table_simple.find_one({"p_text": "B"}) == {
            "p_text": "B",
            "p_int": None,
            "p_vector": DataAPIVector([9, 8, 7]),
        }

        # $unset, on existing(partial) 2: the other column
        sync_table_simple.update_one(
            {"p_text": "B"},
            update={"$unset": {"p_vector": ""}},
        )
        assert sync_table_simple.find_one({"p_text": "B"}) == {
            "p_text": "B",
            "p_int": None,
            "p_vector": None,
        }

        # $unset, on existing(full) (which DOES NOT DELETE the row)
        sync_table_simple.update_one(
            {"p_text": "B"},
            update={"$unset": {"p_int": "", "p_vector": ""}},
        )
        assert sync_table_simple.find_one({"p_text": "B"}) == {
            "p_text": "B",
            "p_int": None,
            "p_vector": None,
        }

        # $unset, on nonexisting
        sync_table_simple.update_one(
            {"p_text": "Z"},
            update={"$unset": {"p_int": ""}},
        )
        assert sync_table_simple.find_one({"p_text": "Z"}) is None

    @pytest.mark.describe("test of include_sort_vector with serdes options, sync")
    def test_table_include_sort_vector_serdes_options_sync(
        self,
        sync_table_simple: DefaultTable,
    ) -> None:
        col_v0 = sync_table_simple.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    custom_datatypes_in_reading=False,
                ),
            ),
        )
        col_v1 = sync_table_simple.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    custom_datatypes_in_reading=True,
                ),
            ),
        )
        cur0_v0_inpf = col_v0.find(sort={"p_vector": [1, 2, 3]})
        cur0_v1_inpf = col_v1.find(sort={"p_vector": [1, 2, 3]})

        assert cur0_v0_inpf.get_sort_vector() is None
        assert cur0_v1_inpf.get_sort_vector() is None

        cur1_v0_inpf = col_v0.find(
            sort={"p_vector": [1, 2, 3]}, include_sort_vector=True
        )
        cur1_v1_inpf = col_v1.find(
            sort={"p_vector": [1, 2, 3]}, include_sort_vector=True
        )

        assert cur1_v0_inpf.get_sort_vector() == [1, 2, 3]
        assert cur1_v1_inpf.get_sort_vector() == DataAPIVector([1, 2, 3])

        cur0_v0_inpv = col_v0.find(sort={"p_vector": DataAPIVector([1, 2, 3])})
        cur0_v1_inpv = col_v1.find(sort={"p_vector": DataAPIVector([1, 2, 3])})

        assert cur0_v0_inpv.get_sort_vector() is None
        assert cur0_v1_inpv.get_sort_vector() is None

        cur1_v0_inpv = col_v0.find(
            sort={"p_vector": DataAPIVector([1, 2, 3])}, include_sort_vector=True
        )
        cur1_v1_inpv = col_v1.find(
            sort={"p_vector": DataAPIVector([1, 2, 3])}, include_sort_vector=True
        )

        assert cur1_v0_inpv.get_sort_vector() == [1, 2, 3]
        assert cur1_v1_inpv.get_sort_vector() == DataAPIVector([1, 2, 3])

    @pytest.mark.describe("test of table find, sync")
    def test_table_find_sync(
        self,
        sync_empty_table_composite: DefaultTable,
    ) -> None:
        # TODO do more than just pagination (distinct, maps etc)
        sync_empty_table_composite.insert_many(
            [
                {"p_text": "pA", "p_int": i, "p_vector": DataAPIVector([i, 5, 6])}
                for i in range(120)
            ]
        )
        sync_empty_table_composite.insert_many(
            [
                {"p_text": "pB", "p_int": i, "p_vector": DataAPIVector([i, 6, 5])}
                for i in range(120)
            ]
        )

        rows_a = sync_empty_table_composite.find({"p_text": "pA"}).to_list()
        assert len(rows_a) == 120
        assert all(row["p_text"] == "pA" for row in rows_a)

        rows_all = sync_empty_table_composite.find({}).to_list()
        assert len(rows_all) == 240

        rows_all_2 = sync_empty_table_composite.find(
            {"$or": [{"p_text": "pA"}, {"p_text": "pB"}]}
        ).to_list()
        assert len(rows_all_2) == 240

        # projection
        projected_fields = {"p_int", "p_vector"}
        rows_proj_a = sync_empty_table_composite.find(
            {"p_text": "pA"},
            projection={f: True for f in projected_fields},
        ).to_list()
        assert all(row.keys() == projected_fields for row in rows_proj_a)
