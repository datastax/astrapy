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

# TODO from astrapy.api_options import APIOptions, SerdesOptions
from astrapy.data_types import DataAPIMap, DataAPISet

from ..conftest import DefaultTable

# TODO from .table_row_assets import (
#     ALLMAPS_CUSTOMTYPES_ROW,
#     ALLMAPS_STDLIB_ROW,
#     DISTINCT_AR_ROWS,
# )


class TestTableCollectionColumnsSync:
    @pytest.mark.describe("test of table list and set columns, filtering, sync")
    def test_table_listset_columns_filtering_sync(
        self,
        sync_empty_table_collindexed: DefaultTable,
    ) -> None:
        table = sync_empty_table_collindexed
        table.insert_one(
            {
                "id": "bls_low",
                "set_int": DataAPISet([10, 11, 12]),
                "list_int": [20, 21, 22],
            }
        )
        table.insert_many(
            [
                {
                    "id": "bls_high",
                    "set_int": [110, 111, 112],
                    "list_int": [120, 121, 122],
                }
            ]
        )

        res_s0 = table.find_one({"set_int": {"$in": [11]}})
        assert res_s0 is not None
        assert res_s0["id"] == "bls_low"
        assert table.find({"set_int": {"$in": [11]}}, limit=1).to_list()[0] == res_s0

        res_s1 = table.find_one({"set_int": {"$in": [11, 99]}})
        assert res_s1 is not None
        assert res_s1["id"] == "bls_low"
        assert (
            table.find({"set_int": {"$in": [11, 99]}}, limit=1).to_list()[0] == res_s1
        )

        res_s2 = table.find_one({"set_int": {"$all": [11, 12]}})
        assert res_s2 is not None
        assert res_s2["id"] == "bls_low"
        assert (
            table.find({"set_int": {"$all": [11, 12]}}, limit=1).to_list()[0] == res_s2
        )

        res_s3 = table.find_one({"set_int": {"$all": [11, 99]}})
        assert res_s3 is None
        assert table.find({"set_int": {"$all": [11, 99]}}, limit=1).to_list() == []

        res_l0 = table.find_one({"list_int": {"$in": [21]}})
        assert res_l0 is not None
        assert res_l0["id"] == "bls_low"
        assert table.find({"list_int": {"$in": [21]}}, limit=1).to_list()[0] == res_l0

        res_l1 = table.find_one({"list_int": {"$in": [21, 99]}})
        assert res_l1 is not None
        assert res_l1["id"] == "bls_low"
        assert (
            table.find({"list_int": {"$in": [21, 99]}}, limit=1).to_list()[0] == res_l1
        )

        res_l2 = table.find_one({"list_int": {"$all": [21, 22]}})
        assert res_l2 is not None
        assert res_l2["id"] == "bls_low"
        assert (
            table.find({"list_int": {"$all": [21, 22]}}, limit=1).to_list()[0] == res_l2
        )

        res_l3 = table.find_one({"list_int": {"$all": [21, 99]}})
        assert res_l3 is None
        assert table.find({"list_int": {"$all": [21, 99]}}, limit=1).to_list() == []

    @pytest.mark.describe("test of table map columns, filtering, sync")
    def test_table_map_columns_filtering_sync(
        self,
        sync_empty_table_collindexed: DefaultTable,
    ) -> None:
        table = sync_empty_table_collindexed
        table.insert_one(
            {
                "id": "bm_low",
                "map_text_int_e": DataAPIMap({"a": 1, "b": 2, "c": 3}),
                "map_text_int_k": DataAPIMap({"A": 1, "B": 2, "C": 3}),
                "map_text_int_v": DataAPIMap({"x": 11, "y": 12, "z": 13}),
            }
        )
        table.insert_many(
            [
                {
                    "id": "bm_high",
                    "map_text_int_e": DataAPIMap({"a": 101, "b": 102, "c": 103}),
                    "map_text_int_k": DataAPIMap({"A": 101, "B": 102, "C": 103}),
                    "map_text_int_v": DataAPIMap({"x": 101, "y": 102, "z": 103}),
                }
            ]
        )

        res_me0 = table.find_one({"map_text_int_e": {"$in": [["a", 1]]}})
        assert res_me0 is not None
        assert res_me0["id"] == "bm_low"
        assert (
            table.find({"map_text_int_e": {"$in": [["a", 1]]}}, limit=1).to_list()[0]
            == res_me0
        )

        res_me1 = table.find_one({"map_text_int_e": {"$in": [["a", 1], ["q", 9]]}})
        assert res_me1 is not None
        assert res_me1["id"] == "bm_low"
        assert (
            table.find(
                {"map_text_int_e": {"$in": [["a", 1], ["q", 9]]}}, limit=1
            ).to_list()[0]
            == res_me1
        )

        res_me2 = table.find_one({"map_text_int_e": {"$all": [["a", 1], ["b", 2]]}})
        assert res_me2 is not None
        assert res_me2["id"] == "bm_low"
        assert (
            table.find(
                {"map_text_int_e": {"$all": [["a", 1], ["b", 2]]}}, limit=1
            ).to_list()[0]
            == res_me2
        )

        res_me3 = table.find_one({"map_text_int_e": {"$all": [["a", 1], ["q", 9]]}})
        assert res_me3 is None
        assert (
            table.find(
                {"map_text_int_e": {"$all": [["a", 1], ["q", 9]]}}, limit=1
            ).to_list()
            == []
        )

        res_mk0 = table.find_one({"map_text_int_k": {"$keys": {"$in": ["A"]}}})
        assert res_mk0 is not None
        assert res_mk0["id"] == "bm_low"
        assert (
            table.find(
                {"map_text_int_k": {"$keys": {"$in": ["A"]}}}, limit=1
            ).to_list()[0]
            == res_mk0
        )

        res_mk1 = table.find_one({"map_text_int_k": {"$keys": {"$in": ["A", "Q"]}}})
        assert res_mk1 is not None
        assert res_mk1["id"] == "bm_low"
        assert (
            table.find(
                {"map_text_int_k": {"$keys": {"$in": ["A", "Q"]}}}, limit=1
            ).to_list()[0]
            == res_mk1
        )

        res_mk2 = table.find_one({"map_text_int_k": {"$keys": {"$all": ["A", "B"]}}})
        assert res_mk2 is not None
        assert res_mk2["id"] == "bm_low"
        assert (
            table.find(
                {"map_text_int_k": {"$keys": {"$all": ["A", "B"]}}}, limit=1
            ).to_list()[0]
            == res_mk2
        )

        res_mk3 = table.find_one({"map_text_int_k": {"$keys": {"$all": ["A", "Q"]}}})
        assert res_mk3 is None
        assert (
            table.find(
                {"map_text_int_k": {"$keys": {"$all": ["A", "Q"]}}}, limit=1
            ).to_list()
            == []
        )

        res_mv0 = table.find_one({"map_text_int_v": {"$values": {"$in": [11]}}})
        assert res_mv0 is not None
        assert res_mv0["id"] == "bm_low"
        assert (
            table.find(
                {"map_text_int_v": {"$values": {"$in": [11]}}}, limit=1
            ).to_list()[0]
            == res_mv0
        )

        res_mv1 = table.find_one({"map_text_int_v": {"$values": {"$in": [11, 99]}}})
        assert res_mv1 is not None
        assert res_mv1["id"] == "bm_low"
        assert (
            table.find(
                {"map_text_int_v": {"$values": {"$in": [11, 99]}}}, limit=1
            ).to_list()[0]
            == res_mv1
        )

        res_mv2 = table.find_one({"map_text_int_v": {"$values": {"$all": [11, 12]}}})
        assert res_mv2 is not None
        assert res_mv2["id"] == "bm_low"
        assert (
            table.find(
                {"map_text_int_v": {"$values": {"$all": [11, 12]}}}, limit=1
            ).to_list()[0]
            == res_mv2
        )

        res_mv3 = table.find_one({"map_text_int_v": {"$values": {"$all": [11, 99]}}})
        assert res_mv3 is None
        assert (
            table.find(
                {"map_text_int_v": {"$values": {"$all": [11, 99]}}}, limit=1
            ).to_list()
            == []
        )

        # TODO investigate
        qres_mv0 = table.find_one({"map_text_int_k": {"$values": {"$in": [11]}}})
        assert qres_mv0 is not None
