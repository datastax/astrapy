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

from astrapy.data_types import DataAPIMap, DataAPISet

from ..conftest import DefaultAsyncTable


class TestTableCollectionColumnsSync:
    @pytest.mark.describe("test of table list and set columns, filtering, async")
    async def test_table_listset_columns_filtering_async(
        self,
        async_empty_table_collindexed: DefaultAsyncTable,
    ) -> None:
        atable = async_empty_table_collindexed
        await atable.insert_one(
            {
                "id": "bls_low",
                "set_int": DataAPISet([10, 11, 12]),
                "list_int": [20, 21, 22],
            }
        )
        await atable.insert_many(
            [
                {
                    "id": "bls_high",
                    "set_int": [110, 111, 112],
                    "list_int": [120, 121, 122],
                }
            ]
        )

        res_s0 = await atable.find_one({"set_int": {"$in": [11]}})
        assert res_s0 is not None
        assert res_s0["id"] == "bls_low"
        assert (await atable.find({"set_int": {"$in": [11]}}, limit=1).to_list())[
            0
        ] == res_s0

        res_s1 = await atable.find_one({"set_int": {"$in": [11, 99]}})
        assert res_s1 is not None
        assert res_s1["id"] == "bls_low"
        assert (await atable.find({"set_int": {"$in": [11, 99]}}, limit=1).to_list())[
            0
        ] == res_s1

        res_s0n = await atable.find_one({"set_int": {"$nin": [111]}})
        assert res_s0n is not None
        assert res_s0n["id"] == "bls_low"
        assert (await atable.find({"set_int": {"$nin": [111]}}, limit=1).to_list())[
            0
        ] == res_s0n

        res_s1n = await atable.find_one({"set_int": {"$nin": [10, 110]}})
        assert res_s1n is None
        assert (
            await atable.find({"set_int": {"$nin": [10, 110]}}, limit=1).to_list()
        ) == []

        res_s2 = await atable.find_one({"set_int": {"$all": [11, 12]}})
        assert res_s2 is not None
        assert res_s2["id"] == "bls_low"
        assert (await atable.find({"set_int": {"$all": [11, 12]}}, limit=1).to_list())[
            0
        ] == res_s2

        res_s3 = await atable.find_one({"set_int": {"$all": [11, 99]}})
        assert res_s3 is None
        assert (
            await atable.find({"set_int": {"$all": [11, 99]}}, limit=1).to_list()
        ) == []

        res_l0 = await atable.find_one({"list_int": {"$in": [21]}})
        assert res_l0 is not None
        assert res_l0["id"] == "bls_low"
        assert (await atable.find({"list_int": {"$in": [21]}}, limit=1).to_list())[
            0
        ] == res_l0

        res_l1 = await atable.find_one({"list_int": {"$in": [21, 99]}})
        assert res_l1 is not None
        assert res_l1["id"] == "bls_low"
        assert (await atable.find({"list_int": {"$in": [21, 99]}}, limit=1).to_list())[
            0
        ] == res_l1

        res_l0n = await atable.find_one({"list_int": {"$nin": [120]}})
        assert res_l0n is not None
        assert res_l0n["id"] == "bls_low"
        assert (await atable.find({"list_int": {"$nin": [120]}}, limit=1).to_list())[
            0
        ] == res_l0n

        res_l1n = await atable.find_one({"list_int": {"$nin": [20, 120]}})
        assert res_l1n is None
        assert (
            await atable.find({"list_int": {"$nin": [20, 120]}}, limit=1).to_list()
            == []
        )

        res_l2 = await atable.find_one({"list_int": {"$all": [21, 22]}})
        assert res_l2 is not None
        assert res_l2["id"] == "bls_low"
        assert (await atable.find({"list_int": {"$all": [21, 22]}}, limit=1).to_list())[
            0
        ] == res_l2

        res_l3 = await atable.find_one({"list_int": {"$all": [21, 99]}})
        assert res_l3 is None
        assert (
            await atable.find({"list_int": {"$all": [21, 99]}}, limit=1).to_list() == []
        )

    @pytest.mark.describe("test of table map columns, filtering, async")
    async def test_table_map_columns_filtering_async(
        self,
        async_empty_table_collindexed: DefaultAsyncTable,
    ) -> None:
        atable = async_empty_table_collindexed
        await atable.insert_one(
            {
                "id": "bm_low",
                "map_text_int_e": DataAPIMap({"a": 1, "b": 2, "c": 3}),
                "map_text_int_k": DataAPIMap({"A": 1, "B": 2, "C": 3}),
                "map_text_int_v": DataAPIMap({"x": 11, "y": 12, "z": 13}),
            }
        )
        await atable.insert_many(
            [
                {
                    "id": "bm_high",
                    "map_text_int_e": DataAPIMap({"a": 101, "b": 102, "c": 103}),
                    "map_text_int_k": DataAPIMap({"A": 101, "B": 102, "C2": 103}),
                    "map_text_int_v": DataAPIMap({"x": 101, "y": 102, "z": 103}),
                }
            ]
        )

        res_me0 = await atable.find_one({"map_text_int_e": {"$in": [["a", 1]]}})
        assert res_me0 is not None
        assert res_me0["id"] == "bm_low"
        assert (
            await atable.find(
                {"map_text_int_e": {"$in": [["a", 1]]}}, limit=1
            ).to_list()
        )[0] == res_me0

        res_me1 = await atable.find_one(
            {"map_text_int_e": {"$in": [["a", 1], ["q", 9]]}}
        )
        assert res_me1 is not None
        assert res_me1["id"] == "bm_low"
        assert (
            await atable.find(
                {"map_text_int_e": {"$in": [["a", 1], ["q", 9]]}}, limit=1
            ).to_list()
        )[0] == res_me1

        res_me0n = await atable.find_one({"map_text_int_e": {"$nin": [["a", 101]]}})
        assert res_me0n is not None
        assert res_me0n["id"] == "bm_low"
        assert (
            await atable.find(
                {"map_text_int_e": {"$nin": [["a", 101]]}}, limit=1
            ).to_list()
        )[0] == res_me0n

        res_me1n = await atable.find_one(
            {"map_text_int_e": {"$nin": [["a", 101], ["a", 1]]}}
        )
        assert res_me1n is None
        assert (
            await atable.find(
                {"map_text_int_e": {"$nin": [["a", 101], ["a", 1]]}}, limit=1
            ).to_list()
            == []
        )

        res_me2 = await atable.find_one(
            {"map_text_int_e": {"$all": [["a", 1], ["b", 2]]}}
        )
        assert res_me2 is not None
        assert res_me2["id"] == "bm_low"
        assert (
            await atable.find(
                {"map_text_int_e": {"$all": [["a", 1], ["b", 2]]}}, limit=1
            ).to_list()
        )[0] == res_me2

        res_me3 = await atable.find_one(
            {"map_text_int_e": {"$all": [["a", 1], ["q", 9]]}}
        )
        assert res_me3 is None
        assert (
            await atable.find(
                {"map_text_int_e": {"$all": [["a", 1], ["q", 9]]}}, limit=1
            ).to_list()
            == []
        )

        res_mk0 = await atable.find_one({"map_text_int_k": {"$keys": {"$in": ["A"]}}})
        assert res_mk0 is not None
        assert res_mk0["id"] == "bm_low"
        assert (
            await atable.find(
                {"map_text_int_k": {"$keys": {"$in": ["A"]}}}, limit=1
            ).to_list()
        )[0] == res_mk0

        res_mk1 = await atable.find_one(
            {"map_text_int_k": {"$keys": {"$in": ["A", "Q"]}}}
        )
        assert res_mk1 is not None
        assert res_mk1["id"] == "bm_low"
        assert (
            await atable.find(
                {"map_text_int_k": {"$keys": {"$in": ["A", "Q"]}}}, limit=1
            ).to_list()
        )[0] == res_mk1

        res_mk0n = await atable.find_one(
            {"map_text_int_k": {"$keys": {"$nin": ["C2"]}}}
        )
        assert res_mk0n is not None
        assert res_mk0n["id"] == "bm_low"
        assert (
            await atable.find(
                {"map_text_int_k": {"$keys": {"$nin": ["C2"]}}}, limit=1
            ).to_list()
        )[0] == res_mk0n

        res_mk1n = await atable.find_one(
            {"map_text_int_k": {"$keys": {"$nin": ["C", "C2"]}}}
        )
        assert res_mk1n is None
        assert (
            await atable.find(
                {"map_text_int_k": {"$keys": {"$nin": ["A", "Q"]}}}, limit=1
            ).to_list()
            == []
        )

        res_mk2 = await atable.find_one(
            {"map_text_int_k": {"$keys": {"$all": ["A", "B"]}}}
        )
        assert res_mk2 is not None
        assert res_mk2["id"] == "bm_low"
        assert (
            await atable.find(
                {"map_text_int_k": {"$keys": {"$all": ["A", "B"]}}}, limit=1
            ).to_list()
        )[0] == res_mk2

        res_mk3 = await atable.find_one(
            {"map_text_int_k": {"$keys": {"$all": ["A", "Q"]}}}
        )
        assert res_mk3 is None
        assert (
            await atable.find(
                {"map_text_int_k": {"$keys": {"$all": ["A", "Q"]}}}, limit=1
            ).to_list()
            == []
        )

        res_mv0 = await atable.find_one({"map_text_int_v": {"$values": {"$in": [11]}}})
        assert res_mv0 is not None
        assert res_mv0["id"] == "bm_low"
        assert (
            await atable.find(
                {"map_text_int_v": {"$values": {"$in": [11]}}}, limit=1
            ).to_list()
        )[0] == res_mv0

        res_mv1 = await atable.find_one(
            {"map_text_int_v": {"$values": {"$in": [11, 99]}}}
        )
        assert res_mv1 is not None
        assert res_mv1["id"] == "bm_low"
        assert (
            await atable.find(
                {"map_text_int_v": {"$values": {"$in": [11, 99]}}}, limit=1
            ).to_list()
        )[0] == res_mv1

        res_mv0n = await atable.find_one(
            {"map_text_int_v": {"$values": {"$nin": [101]}}}
        )
        assert res_mv0n is not None
        assert res_mv0n["id"] == "bm_low"
        assert (
            await atable.find(
                {"map_text_int_v": {"$values": {"$nin": [101]}}}, limit=1
            ).to_list()
        )[0] == res_mv0n

        res_mv1n = await atable.find_one(
            {"map_text_int_v": {"$values": {"$nin": [101, 11]}}}
        )
        assert res_mv1n is None
        assert (
            await atable.find(
                {"map_text_int_v": {"$values": {"$nin": [101, 11]}}}, limit=1
            ).to_list()
            == []
        )

        res_mv2 = await atable.find_one(
            {"map_text_int_v": {"$values": {"$all": [11, 12]}}}
        )
        assert res_mv2 is not None
        assert res_mv2["id"] == "bm_low"
        assert (
            await atable.find(
                {"map_text_int_v": {"$values": {"$all": [11, 12]}}}, limit=1
            ).to_list()
        )[0] == res_mv2

        res_mv3 = await atable.find_one(
            {"map_text_int_v": {"$values": {"$all": [11, 99]}}}
        )
        assert res_mv3 is None
        assert (
            await atable.find(
                {"map_text_int_v": {"$values": {"$all": [11, 99]}}}, limit=1
            ).to_list()
            == []
        )

    @pytest.mark.describe("test of table list and set columns, dollar-writes, async")
    async def test_table_listset_columns_dollarwriting_async(
        self,
        async_empty_table_collindexed: DefaultAsyncTable,
    ) -> None:
        atable = async_empty_table_collindexed
        await atable.insert_one(
            {
                "id": "dwr_bls",
                "set_int": DataAPISet([30, 31, 32]),
                "list_int": [40, 41, 42],
            }
        )

        await atable.update_one(
            {"id": "dwr_bls"},
            {"$pullAll": {"set_int": [32, 30]}},
        )
        post_pa_s = await atable.find_one({"id": "dwr_bls"})
        assert post_pa_s is not None
        assert post_pa_s["set_int"] == DataAPISet([31])

        await atable.update_one(
            {"id": "dwr_bls"},
            {"$pullAll": {"list_int": [42, 40]}},
        )
        post_pa_l = await atable.find_one({"id": "dwr_bls"})
        assert post_pa_l is not None
        assert post_pa_l["list_int"] == [41]

        await atable.update_one(
            {"id": "dwr_bls"},
            {
                "$pullAll": {
                    "set_int": [31],
                    "list_int": [41],
                },
            },
        )
        post_pa_ls = await atable.find_one({"id": "dwr_bls"})
        assert post_pa_ls is not None
        assert post_pa_ls["list_int"] == []
        assert post_pa_ls["set_int"] == DataAPISet()

        await atable.update_one(
            {"id": "dwr_bls"},
            {
                "$push": {
                    "set_int": 930,
                }
            },
        )
        post_pu_s = await atable.find_one({"id": "dwr_bls"})
        assert post_pu_s is not None
        assert post_pu_s["list_int"] == []
        assert post_pu_s["set_int"] == DataAPISet([930])

        await atable.update_one(
            {"id": "dwr_bls"},
            {
                "$push": {
                    "list_int": 940,
                }
            },
        )
        post_pu_l = await atable.find_one({"id": "dwr_bls"})
        assert post_pu_l is not None
        assert post_pu_l["list_int"] == [940]
        assert post_pu_l["set_int"] == DataAPISet([930])

        await atable.update_one(
            {"id": "dwr_bls"},
            {
                "$push": {
                    "set_int": {
                        "$each": [931, 932],
                    },
                    "list_int": {"$each": [941, 942]},
                }
            },
        )
        post_pu_ls = await atable.find_one({"id": "dwr_bls"})
        assert post_pu_ls is not None
        assert post_pu_ls["list_int"] == [940, 941, 942]
        assert post_pu_ls["set_int"] == DataAPISet([930, 931, 932])

    @pytest.mark.describe("test of table map columns, dollar-writes, async")
    async def test_table_map_columns_dollarwriting_async(
        self,
        async_empty_table_collindexed: DefaultAsyncTable,
    ) -> None:
        atable = async_empty_table_collindexed
        await atable.insert_one(
            {
                "id": "dwr_m",
                "map_text_int_e": DataAPIMap({"a": 1, "b": 2, "c": 3}),
            }
        )

        await atable.update_one(
            {"id": "dwr_m"},
            {
                "$pullAll": {
                    "map_text_int_e": ["b", "c"],
                },
            },
        )
        post_pa_m = await atable.find_one({"id": "dwr_m"})
        assert post_pa_m is not None
        assert post_pa_m["map_text_int_e"] == DataAPIMap({"a": 1})

        await atable.update_one(
            {"id": "dwr_m"},
            {
                "$pullAll": {
                    "map_text_int_e": ["a", "b"],
                },
            },
        )
        post_pa_m2 = await atable.find_one({"id": "dwr_m"})
        assert post_pa_m2 is not None
        assert post_pa_m2["map_text_int_e"] == DataAPIMap({})

        await atable.update_one(
            {"id": "dwr_m"},
            {
                "$push": {
                    "map_text_int_e": ["z", 60],
                },
            },
        )
        post_pu_m = await atable.find_one({"id": "dwr_m"})
        assert post_pu_m is not None
        assert post_pu_m["map_text_int_e"] == DataAPIMap({"z": 60})

        await atable.update_one(
            {"id": "dwr_m"},
            {
                "$push": {
                    "map_text_int_e": {"y": 61},
                },
            },
        )
        post_pu_m2 = await atable.find_one({"id": "dwr_m"})
        assert post_pu_m2 is not None
        assert post_pu_m2["map_text_int_e"] == DataAPIMap({"z": 60, "y": 61})

        await atable.update_one(
            {"id": "dwr_m"},
            {
                "$push": {
                    "map_text_int_e": {
                        "$each": [
                            {"x": 62},
                            {"w": 63},
                        ]
                    },
                },
            },
        )
        post_pu_m3 = await atable.find_one({"id": "dwr_m"})
        assert post_pu_m3 is not None
        assert post_pu_m3["map_text_int_e"] == DataAPIMap(
            {"z": 60, "y": 61, "x": 62, "w": 63},
        )

        await atable.update_one(
            {"id": "dwr_m"},
            {
                "$push": {
                    "map_text_int_e": {
                        "$each": [
                            ["v", 64],
                            ["u", 65],
                        ]
                    },
                },
            },
        )
        post_pu_m4 = await atable.find_one({"id": "dwr_m"})
        assert post_pu_m4 is not None
        assert post_pu_m4["map_text_int_e"] == DataAPIMap(
            {"z": 60, "y": 61, "x": 62, "w": 63, "v": 64, "u": 65},
        )

        await atable.update_one(
            {"id": "dwr_m"},
            {
                "$set": {
                    "map_text_int_k": {
                        "r": 70,
                        "s": 71,
                    },
                },
            },
        )
        post_se_m = await atable.find_one({"id": "dwr_m"})
        assert post_se_m is not None
        assert post_se_m["map_text_int_k"] == DataAPIMap({"r": 70, "s": 71})

        await atable.update_one(
            {"id": "dwr_m"},
            {
                "$set": {
                    "map_int_text": [
                        [72, "r2"],
                        [73, "s2"],
                    ],
                },
            },
        )
        post_se_m2 = await atable.find_one({"id": "dwr_m"})
        assert post_se_m2 is not None
        assert post_se_m2["map_int_text"] == DataAPIMap({72: "r2", 73: "s2"})

        await atable.update_one(
            {"id": "dwr_m"},
            {
                "$set": {
                    "map_int_text": DataAPIMap(
                        [
                            (74, "r3"),
                            (75, "s3"),
                        ]
                    ),
                },
            },
        )
        post_se_m3 = await atable.find_one({"id": "dwr_m"})
        assert post_se_m3 is not None
        assert post_se_m3["map_int_text"] == DataAPIMap({74: "r3", 75: "s3"})
