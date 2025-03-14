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

from typing import Any, Iterable, cast

import pytest

from astrapy.api_options import APIOptions, SerdesOptions
from astrapy.constants import SortMode
from astrapy.data_types import DataAPITimestamp, DataAPIVector
from astrapy.exceptions import (
    DataAPIException,
    DataAPIResponseException,
    TableInsertManyException,
)
from astrapy.results import TableInsertManyResult

from ..conftest import (
    DefaultTable,
    _repaint_NaNs,
    _typify_tuple,
)
from .table_row_assets import (
    AR_ROW_0,
    AR_ROW_0_B,
    AR_ROW_PK_0,
    AR_ROW_PK_0_TUPLE,
    COMPOSITE_VECTOR_ROWS,
    COMPOSITE_VECTOR_ROWS_N,
    DISTINCT_AR_ROWS,
    INSMANY_AR_ROW_HALFN,
    INSMANY_AR_ROWS,
    INSMANY_AR_ROWS_PK_TUPLES,
    INSMANY_AR_ROWS_PKS,
    SIMPLE_FULL_ROWS,
    SIMPLE_SEVEN_ROWS_F2,
    SIMPLE_SEVEN_ROWS_F4,
    SIMPLE_SEVEN_ROWS_OK,
)


class TestTableDMLSync:
    @pytest.mark.describe("test of table insert_one and find_one, sync")
    def test_table_insert_one_find_one_sync(
        self,
        sync_empty_table_all_returns: DefaultTable,
        sync_empty_table_composite: DefaultTable,
    ) -> None:
        no_row_0a = sync_empty_table_all_returns.find_one(filter=AR_ROW_PK_0)
        assert no_row_0a is None
        ins1_res_0 = sync_empty_table_all_returns.insert_one(row=AR_ROW_0)
        row_0 = sync_empty_table_all_returns.find_one(filter=AR_ROW_PK_0)
        row_0_nofilter = sync_empty_table_all_returns.find_one(filter={})
        assert row_0 is not None
        assert row_0 == row_0_nofilter
        assert {row_0[k] == v for k, v in AR_ROW_0.items()}
        assert ins1_res_0.inserted_id == AR_ROW_PK_0
        assert _typify_tuple(ins1_res_0.inserted_id_tuple) == _typify_tuple(
            AR_ROW_PK_0_TUPLE
        )
        # overwrite:
        ins1_res_0_b = sync_empty_table_all_returns.insert_one(row=AR_ROW_0_B)
        row_0_b = sync_empty_table_all_returns.find_one(filter=AR_ROW_PK_0)
        assert row_0_b is not None
        assert {row_0_b[k] == v for k, v in AR_ROW_0_B.items()}
        assert ins1_res_0_b.inserted_id == AR_ROW_PK_0
        assert _typify_tuple(ins1_res_0_b.inserted_id_tuple) == _typify_tuple(
            AR_ROW_PK_0_TUPLE
        )
        # projection:
        projected_fields = {"p_bigint", "p_boolean"}
        row_0 = sync_empty_table_all_returns.find_one(
            filter=AR_ROW_PK_0,
            projection={pf: True for pf in projected_fields},
        )
        assert row_0 is not None
        assert {row_0[k] == AR_ROW_0[k] for k in projected_fields}
        assert row_0.keys() == projected_fields
        # delete and retry
        sync_empty_table_all_returns.delete_one(filter=AR_ROW_PK_0)
        sync_empty_table_all_returns.delete_one(filter=AR_ROW_PK_0)
        no_row_0b = sync_empty_table_all_returns.find_one(filter=AR_ROW_PK_0)
        assert no_row_0b is None

        # ANN and non-ANN sorting in find_one
        sync_empty_table_composite.insert_many(
            [
                {
                    "p_text": "pA",
                    "p_int": i,
                    "p_vector": DataAPIVector([i, 1, 0]),
                }
                for i in range(3)
            ]
        )
        sync_empty_table_composite.insert_many(
            [
                {
                    "p_text": "pB",
                    "p_int": 100 - i,
                    "p_vector": DataAPIVector([i + 0.1, 1, 0]),
                }
                for i in range(3)
            ]
        )
        # sort by just ANN
        fo_unf_ann_row = sync_empty_table_composite.find_one(
            sort={"p_vector": DataAPIVector([-0.1, 1, 0])}
        )
        assert fo_unf_ann_row is not None
        assert (fo_unf_ann_row["p_text"], fo_unf_ann_row["p_int"]) == ("pA", 0)
        # sort by ANN, filtered
        fo_fil_ann_row = sync_empty_table_composite.find_one(
            filter={"p_text": "pB"}, sort={"p_vector": DataAPIVector([-0.1, 1, 0])}
        )
        assert fo_fil_ann_row is not None
        assert (fo_fil_ann_row["p_text"], fo_fil_ann_row["p_int"]) == ("pB", 100)
        # just regular sort
        if False:
            # TODO: reinstate this part on Astra/nonAstra once patch in docker image and Astra prod
            fo_unf_srt_row = sync_empty_table_composite.find_one(
                sort={"p_int": SortMode.DESCENDING}
            )
            assert fo_unf_srt_row is not None
            assert (fo_unf_srt_row["p_text"], fo_unf_srt_row["p_int"]) == ("pB", 100)
        # regular sort, filtered
        fo_fil_srt_row = sync_empty_table_composite.find_one(
            filter={"p_text": "pA"},
            sort={"p_int": SortMode.DESCENDING},
        )
        assert fo_fil_srt_row is not None
        assert (fo_fil_srt_row["p_text"], fo_fil_srt_row["p_int"]) == ("pA", 2)

        # serdes options obeyance check
        noncustom_compo_table = sync_empty_table_composite.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(custom_datatypes_in_reading=False),
            )
        )
        custom_compo_table = sync_empty_table_composite.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(custom_datatypes_in_reading=True),
            )
        )
        noncustom_row = noncustom_compo_table.find_one(
            sort={"p_vector": [-0.1, 1, 0]}  # both would actually work (as arguments)
        )
        custom_row = custom_compo_table.find_one(
            sort={"p_vector": [-0.1, 1, 0]}  # both would actually work (as arguments)
        )
        assert noncustom_row is not None
        assert isinstance(noncustom_row["p_vector"], list)
        assert custom_row is not None
        assert isinstance(custom_row["p_vector"], DataAPIVector)

    @pytest.mark.describe("test of table delete_one, sync")
    def test_table_delete_one_sync(
        self,
        sync_empty_table_simple: DefaultTable,
    ) -> None:
        # TODO cross check with CQL direct (!)
        im_result = sync_empty_table_simple.insert_many(SIMPLE_FULL_ROWS)
        assert len(im_result.inserted_ids) == len(SIMPLE_FULL_ROWS)
        assert len(im_result.inserted_id_tuples) == len(SIMPLE_FULL_ROWS)
        assert len(sync_empty_table_simple.find({}).to_list()) == len(SIMPLE_FULL_ROWS)

        sync_empty_table_simple.delete_one({"p_text": "Z"})
        assert len(sync_empty_table_simple.find({}).to_list()) == len(SIMPLE_FULL_ROWS)

        sync_empty_table_simple.delete_one({"p_text": "A1"})
        assert (
            len(sync_empty_table_simple.find({}).to_list()) == len(SIMPLE_FULL_ROWS) - 1
        )

    @pytest.mark.describe("test of table delete_many, sync")
    def test_table_delete_many_sync(
        self,
        sync_empty_table_simple: DefaultTable,
    ) -> None:
        # TODO cross check with CQL direct (!)
        im_result = sync_empty_table_simple.insert_many(SIMPLE_FULL_ROWS)
        assert len(im_result.inserted_ids) == len(SIMPLE_FULL_ROWS)
        assert len(im_result.inserted_id_tuples) == len(SIMPLE_FULL_ROWS)
        assert len(sync_empty_table_simple.find({}).to_list()) == len(SIMPLE_FULL_ROWS)

        sync_empty_table_simple.delete_many({"p_text": {"$in": ["Z", "Y"]}})
        assert len(sync_empty_table_simple.find({}).to_list()) == 3

        sync_empty_table_simple.delete_many({"p_text": {"$in": ["A1", "A2"]}})
        assert len(sync_empty_table_simple.find({}).to_list()) == 1

    @pytest.mark.describe("test of table insert_many returned ids, sync")
    def test_table_insert_many_returned_ids_sync(
        self,
        sync_empty_table_all_returns: DefaultTable,
    ) -> None:
        im_result = sync_empty_table_all_returns.insert_many(INSMANY_AR_ROWS)
        assert im_result.inserted_ids == INSMANY_AR_ROWS_PKS
        assert [_typify_tuple(tpl) for tpl in im_result.inserted_id_tuples] == [
            _typify_tuple(tpl) for tpl in INSMANY_AR_ROWS_PK_TUPLES
        ]

    @pytest.mark.describe("test of table distinct, sync")
    def test_table_distinct_sync(
        self,
        sync_empty_table_all_returns: DefaultTable,
    ) -> None:
        sync_empty_table_all_returns.insert_many(DISTINCT_AR_ROWS)

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
        exp_d_list_int = {
            itm
            for doc in DISTINCT_AR_ROWS
            for itm in cast(Iterable[int], doc.get("p_list_int", []))
        }
        assert set(d_list_int) == set(exp_d_list_int)

        d_list_int_ind = sync_empty_table_all_returns.distinct("p_list_int.1")
        exp_d_list_int_ind = {1}
        assert set(d_list_int_ind) == set(exp_d_list_int_ind)

        d_p_map_text_text = sync_empty_table_all_returns.distinct("p_map_text_text")
        exp_d_p_map_text_text = [
            {"a": "va", "b": "vb"},
            {"b": "VB"},
            {"a": "VA", "b": "VB"},
            {},
        ]
        assert len(d_p_map_text_text) == len(exp_d_p_map_text_text)
        for _exp in exp_d_p_map_text_text:
            assert any(_d == _exp for _d in d_p_map_text_text)

        d_p_map_text_text_a = sync_empty_table_all_returns.distinct("p_map_text_text.a")
        exp_d_p_map_text_text_a = {"va", "VA"}
        assert set(d_p_map_text_text_a) == set(exp_d_p_map_text_text_a)

        d_set_int = sync_empty_table_all_returns.distinct("p_set_int")
        exp_d_set_int = {
            itm
            for doc in DISTINCT_AR_ROWS
            for itm in cast(Iterable[int], doc.get("p_set_int", []))
        }
        assert set(d_set_int) == set(exp_d_set_int)

    @pytest.mark.describe("test of table distinct key-as-list, sync")
    def test_table_distinct_key_as_list_sync(
        self,
        sync_empty_table_all_returns: DefaultTable,
    ) -> None:
        sync_empty_table_all_returns.insert_many(DISTINCT_AR_ROWS)

        d_float = sync_empty_table_all_returns.distinct(["p_float"])
        exp_d_float = {0.1, 0.2, float("NaN")}
        assert set(_repaint_NaNs(d_float)) == _repaint_NaNs(exp_d_float)

        d_text = sync_empty_table_all_returns.distinct(["p_text"])
        exp_d_text = {"a", "b", None}
        assert set(d_text) == set(exp_d_text)

        d_timestamp = sync_empty_table_all_returns.distinct(["p_timestamp"])
        exp_d_timestamp = {
            DataAPITimestamp.from_string("1111-01-01T01:01:01Z"),
            DataAPITimestamp.from_string("1221-01-01T01:01:01Z"),
            None,
        }
        assert set(d_timestamp) == set(exp_d_timestamp)

        d_list_int = sync_empty_table_all_returns.distinct(["p_list_int"])
        exp_d_list_int = {
            itm
            for doc in DISTINCT_AR_ROWS
            for itm in cast(Iterable[int], doc.get("p_list_int", []))
        }
        assert set(d_list_int) == set(exp_d_list_int)

        d_list_int_ind = sync_empty_table_all_returns.distinct(["p_list_int", 1])
        exp_d_list_int_ind = {1}
        assert set(d_list_int_ind) == set(exp_d_list_int_ind)

        d_list_int_sind = sync_empty_table_all_returns.distinct(["p_list_int", "1"])
        exp_d_list_int_sind: set[int] = set()
        assert set(d_list_int_sind) == set(exp_d_list_int_sind)

        d_p_map_text_text = sync_empty_table_all_returns.distinct(["p_map_text_text"])
        exp_d_p_map_text_text = [
            {"a": "va", "b": "vb"},
            {"b": "VB"},
            {"a": "VA", "b": "VB"},
            {},
        ]
        assert len(d_p_map_text_text) == len(exp_d_p_map_text_text)
        for _exp in exp_d_p_map_text_text:
            assert any(_d == _exp for _d in d_p_map_text_text)

        d_p_map_text_text_a = sync_empty_table_all_returns.distinct(
            ["p_map_text_text", "a"]
        )
        exp_d_p_map_text_text_a = {"va", "VA"}
        assert set(d_p_map_text_text_a) == set(exp_d_p_map_text_text_a)

        d_set_int = sync_empty_table_all_returns.distinct(["p_set_int"])
        exp_d_set_int = {
            itm
            for doc in DISTINCT_AR_ROWS
            for itm in cast(Iterable[int], doc.get("p_set_int", []))
        }
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
            p_text_values: list[str],
            ins_result: TableInsertManyResult | TableInsertManyException,
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
        _assert_consistency([], exc.value)

        # ordered, failing later batch
        sync_table_simple.delete_many({})
        with pytest.raises(TableInsertManyException) as exc:
            sync_table_simple.insert_many(
                SIMPLE_SEVEN_ROWS_F4, ordered=True, chunk_size=2
            )
        _assert_consistency(["p1", "p2"], exc.value)

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
        _assert_consistency(["p1", "p3", "p4", "p5", "p6", "p7"], exc.value)

        # unordered/concurrency=1, failing later batch
        sync_table_simple.delete_many({})
        with pytest.raises(TableInsertManyException) as exc:
            sync_table_simple.insert_many(
                SIMPLE_SEVEN_ROWS_F4, ordered=False, chunk_size=2, concurrency=1
            )
        _assert_consistency(["p1", "p2", "p3", "p5", "p6", "p7"], exc.value)

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
        _assert_consistency(["p1", "p3", "p4", "p5", "p6", "p7"], exc.value)

        # unordered/concurrency=2, failing later batch
        sync_table_simple.delete_many({})
        with pytest.raises(TableInsertManyException) as exc:
            sync_table_simple.insert_many(
                SIMPLE_SEVEN_ROWS_F4, ordered=False, chunk_size=2, concurrency=2
            )
        _assert_consistency(["p1", "p2", "p3", "p5", "p6", "p7"], exc.value)

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
    def test_table_insert_many_failures_sync(
        self,
        sync_table_simple: DefaultTable,
    ) -> None:
        # The main goal here is to keep the switch to returnDocumentResponses in check.
        N = 110
        ins_res0 = sync_table_simple.insert_many(
            [{"p_text": f"{i}"} for i in range(N)],
            concurrency=1,
        )
        assert set(ins_res0.inserted_id_tuples) == {(f"{i}",) for i in range(N)}

        ins_res1 = sync_table_simple.insert_many(
            [{"p_text": f"{N + i}"} for i in range(N)],
            concurrency=20,
        )
        assert set(ins_res1.inserted_id_tuples) == {(f"{N + i}",) for i in range(N)}

        # unordered insertion [good, bad]
        err2: TableInsertManyException | None = None
        try:
            sync_table_simple.insert_many([{"p_text": f"{2 * N}"}, {"p_text": -1}])
        except TableInsertManyException as e:
            err2 = e
        assert err2 is not None
        assert len(err2.exceptions) == 1
        assert isinstance(err2.exceptions[0], DataAPIResponseException)
        assert len(err2.exceptions[0].error_descriptors) == 1
        assert err2.inserted_id_tuples == [(f"{2 * N}",)]

        # unordered insertion [bad, bad]
        err3: TableInsertManyException | None = None
        try:
            sync_table_simple.insert_many([{"p_text": -2}, {"p_text": -3}])
        except TableInsertManyException as e:
            err3 = e
        assert err3 is not None
        assert len(err3.exceptions) == 1
        assert isinstance(err3.exceptions[0], DataAPIResponseException)
        assert len(err3.exceptions[0].error_descriptors) == 2
        assert err3.inserted_id_tuples == []

        # ordered insertion [good, bad, good_skipped]
        # Tables do not insert anything in this case! (as opposed to Collections)
        err4: TableInsertManyException | None = None
        try:
            sync_table_simple.insert_many(
                [{"p_text": "n0"}, {"p_text": -4}, {"p_text": "n1"}],
                ordered=True,
            )
        except TableInsertManyException as e:
            err4 = e
        assert err4 is not None
        assert len(err4.exceptions) == 1
        assert isinstance(err4.exceptions[0], DataAPIResponseException)
        assert len(err4.exceptions[0].error_descriptors) == 1
        assert err4.inserted_id_tuples == []

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
        sync_empty_table_all_returns: DefaultTable,
    ) -> None:
        sync_empty_table_composite.insert_many(
            [
                {
                    "p_text": "pA",
                    "p_int": i,
                    "p_boolean": i % 2 == 0,
                    "p_vector": DataAPIVector([i, 5, 6]),
                }
                for i in range(120)
            ]
        )
        sync_empty_table_composite.insert_many(
            [
                {
                    "p_text": "pB",
                    "p_int": i,
                    "p_boolean": i % 2 == 0,
                    "p_vector": DataAPIVector([i, 6, 5]),
                }
                for i in range(120)
            ]
        )

        # partition filter
        rows_a = sync_empty_table_composite.find({"p_text": "pA"}).to_list()
        assert len(rows_a) == 120
        assert all(row["p_text"] == "pA" for row in rows_a)
        # no filters
        rows_all = sync_empty_table_composite.find({}).to_list()
        assert len(rows_all) == 240
        # sophisticated (but partition) filter
        rows_all_2 = sync_empty_table_composite.find(
            {"$or": [{"p_text": "pA"}, {"p_text": "pB"}]}
        ).to_list()
        assert len(rows_all_2) == 240
        # non-pk-column filter, alone
        rows_even_allps = sync_empty_table_composite.find({"p_boolean": True}).to_list()
        assert len(rows_even_allps) == 2 * sum(1 - i % 2 for i in range(120))
        assert all(row["p_boolean"] for row in rows_even_allps)
        # non-pk-column + partition key filter
        rows_even_a = sync_empty_table_composite.find(
            {"p_text": "pA", "p_boolean": True}
        ).to_list()
        assert len(rows_even_a) == sum(1 - i % 2 for i in range(120))
        assert all(row["p_text"] == "pA" for row in rows_even_a)
        assert all(row["p_boolean"] for row in rows_even_a)

        # projection
        projected_fields = {"p_int", "p_vector"}
        rows_proj_a = sync_empty_table_composite.find(
            {"p_text": "pA"},
            projection={f: True for f in projected_fields},
        ).to_list()
        assert all(row.keys() == projected_fields for row in rows_proj_a)

        # (nonvector) sorting
        sync_empty_table_all_returns.insert_many(INSMANY_AR_ROWS)
        # sorting as per clustering column
        if False:
            # TODO: reinstate this part on Astra/nonAstra once patch in docker image and Astra prod
            srows_in_part = sync_empty_table_all_returns.find(
                filter={"p_ascii": "A", "p_bigint": 100},
                sort={"p_int": SortMode.DESCENDING},
                limit=INSMANY_AR_ROW_HALFN + 1,
            ).to_list()
            assert len(srows_in_part) == INSMANY_AR_ROW_HALFN
            srows_in_part_pints = [row["p_int"] for row in srows_in_part]
            assert sorted(srows_in_part_pints) == srows_in_part_pints[::-1]
        # sorting by any regular column
        srows_anycol = sync_empty_table_all_returns.find(
            filter={"p_ascii": "A", "p_bigint": 100},
            sort={"p_float": SortMode.DESCENDING},
            limit=INSMANY_AR_ROW_HALFN + 1,
        ).to_list()
        # sorted finds in this case return at most one page and that's it:
        assert len(srows_anycol) == 20
        srows_anycol_pints = [row["p_int"] for row in srows_anycol]
        assert sorted(srows_anycol_pints) == srows_anycol_pints[::-1]

        # use of limit+skip: two ways of getting the first 30 items
        ls_rows_A0 = sync_empty_table_all_returns.find(
            filter={"p_ascii": "A", "p_bigint": 200},
            sort={"p_int": SortMode.DESCENDING},
            skip=0,
            limit=20,
        ).to_list()
        ls_rows_A1 = sync_empty_table_all_returns.find(
            filter={"p_ascii": "A", "p_bigint": 200},
            sort={"p_int": SortMode.DESCENDING},
            skip=20,
            limit=10,
        ).to_list()
        ls_rows_B0 = sync_empty_table_all_returns.find(
            filter={"p_ascii": "A", "p_bigint": 200},
            sort={"p_int": SortMode.DESCENDING},
            skip=0,
            limit=10,
        ).to_list()
        ls_rows_B1 = sync_empty_table_all_returns.find(
            filter={"p_ascii": "A", "p_bigint": 200},
            sort={"p_int": SortMode.DESCENDING},
            skip=10,
            limit=20,
        ).to_list()
        ls_rows_A = ls_rows_A0 + ls_rows_A1
        ls_rows_B = ls_rows_B0 + ls_rows_B1
        assert len(ls_rows_A) == 30
        assert ls_rows_A == ls_rows_B

        # find with ANN
        sync_empty_table_composite.delete_many({})
        sync_empty_table_composite.insert_many(COMPOSITE_VECTOR_ROWS)
        # in a partition
        vrows_in_part = sync_empty_table_composite.find(
            filter={"p_text": "A"},
            sort={"p_vector": DataAPIVector([COMPOSITE_VECTOR_ROWS_N, 0, 0])},
            limit=COMPOSITE_VECTOR_ROWS_N + 2,
        ).to_list()
        assert len(vrows_in_part) == COMPOSITE_VECTOR_ROWS_N
        ints = [row["p_int"] for row in vrows_in_part]
        assert sorted(ints, reverse=True) == ints
        # across all partitions
        vrows_in_part = sync_empty_table_composite.find(
            sort={"p_vector": DataAPIVector([COMPOSITE_VECTOR_ROWS_N, 0, 0])},
            limit=2 * COMPOSITE_VECTOR_ROWS_N + 2,
        ).to_list()
        assert len(vrows_in_part) == 2 * COMPOSITE_VECTOR_ROWS_N
        ints = [row["p_int"] for row in vrows_in_part]
        assert sorted(ints, reverse=True) == ints
        # filtering on a non-pk column
        vrows_in_part = sync_empty_table_composite.find(
            filter={"p_boolean": True},
            sort={"p_vector": DataAPIVector([COMPOSITE_VECTOR_ROWS_N, 0, 0])},
            limit=2 * COMPOSITE_VECTOR_ROWS_N + 2,
        ).to_list()
        assert len(vrows_in_part) == 2 * sum(
            1 - i % 2 for i in range(COMPOSITE_VECTOR_ROWS_N)
        )
        ints = [row["p_int"] for row in vrows_in_part]
        assert all(i % 2 == 0 for i in ints)
        assert sorted(ints, reverse=True) == ints

        # mapper and to_list at once
        def _mapping(dct: dict[str, Any]) -> dict[str, str]:
            return {k: str(v) for k, v in dct.items()}

        mapped_cursor = sync_empty_table_composite.find(
            sort={"p_vector": DataAPIVector([1, 0, 0])},
            limit=COMPOSITE_VECTOR_ROWS_N,
        ).map(_mapping)
        unmapped_list = sync_empty_table_composite.find(
            sort={"p_vector": DataAPIVector([1, 0, 0])},
            limit=COMPOSITE_VECTOR_ROWS_N,
        ).to_list()
        postmapped_list = [_mapping(row) for row in unmapped_list]
        premapped_list = [mrow for mrow in mapped_cursor]
        assert postmapped_list == premapped_list

    @pytest.mark.describe("test of table command, sync")
    def test_table_command_sync(
        self,
        sync_empty_table_simple: DefaultTable,
    ) -> None:
        ins_response = sync_empty_table_simple.command(
            body={
                "insertOne": {
                    "document": {
                        "p_text": "t_command",
                        "p_int": 101,
                        "p_vector": [0.1, -0.2, 0.3],
                    }
                }
            }
        )
        assert ins_response == {
            "status": {
                "primaryKeySchema": {"p_text": {"type": "text"}},
                "insertedIds": [["t_command"]],
            }
        }
