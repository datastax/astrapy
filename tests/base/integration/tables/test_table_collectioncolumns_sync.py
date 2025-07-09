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
from astrapy.data_types import DataAPISet

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
                "id": "b_low",
                "set_int": DataAPISet([10, 11, 12]),
                "list_int": [20, 21, 22],
            }
        )
        table.insert_many([
            {
                "id": "b_high",
                "set_int": [110, 111, 112],
                "list_int": [120, 121, 122],
            }
        ])

        res_s0 = table.find_one({"set_int": {"$in": [11]}})
        assert res_s0 is not None
        assert res_s0["id"] == "b_low"

        res_s1 = table.find_one({"set_int": {"$in": [11, 99]}})
        assert res_s1 is not None
        assert res_s1["id"] == "b_low"

        res_s2 = table.find_one({"set_int": {"$all": [11, 12]}})
        assert res_s2 is not None
        assert res_s2["id"] == "b_low"

        res_s3 = table.find_one({"set_int": {"$all": [11, 99]}})
        assert res_s3 is None

        res_l0 = table.find_one({"list_int": {"$in": [21]}})
        assert res_l0 is not None
        assert res_l0["id"] == "b_low"

        res_l1 = table.find_one({"list_int": {"$in": [21, 99]}})
        assert res_l1 is not None
        assert res_l1["id"] == "b_low"

        res_l2 = table.find_one({"list_int": {"$all": [21, 22]}})
        assert res_l2 is not None
        assert res_l2["id"] == "b_low"

        res_l3 = table.find_one({"list_int": {"$all": [21, 99]}})
        assert res_l3 is None
