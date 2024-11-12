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

from astrapy.data_types import DataAPIDuration

from ..conftest import DefaultTable


class TestTableColumnTypesSync:
    @pytest.mark.describe("test of type DataAPIDuration on tables, sync")
    def test_table_insert_one_find_one_sync(
        self,
        sync_empty_table_all_returns: DefaultTable,
    ) -> None:
        # store some "round-trip" representative values, read them back, compare
        rt_values = [
            DataAPIDuration.from_string("P12Y44DT1H1M1S"),
            DataAPIDuration.from_string("PT1.19S"),
        ]
        rt_rows = [
            {
                "p_ascii": "x",
                "p_bigint": 111,
                "p_int": i,
                "p_boolean": True,
                "p_duration": rtv,
            }
            for i, rtv in enumerate(rt_values)
        ]
        assert len(
            sync_empty_table_all_returns.insert_many(rt_rows).inserted_ids
        ) == len(rt_values)

        for i, rtv in enumerate(rt_values):
            retr_row = sync_empty_table_all_returns.find_one(
                filter={
                    "p_ascii": "x",
                    "p_bigint": 111,
                    "p_int": i,
                    "p_boolean": True,
                },
                projection={"p_duration": True},
            )
            assert retr_row is not None
            assert retr_row["p_duration"] == rtv
