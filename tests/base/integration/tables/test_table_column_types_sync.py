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

from ..conftest import DefaultTable, _repaint_NaNs
from .table_row_assets import (
    FULL_AR_ROW_CUSTOMTYPED,
    FULL_AR_ROW_NONCUSTOMTYPED,
)


class TestTableColumnTypesSync:
    @pytest.mark.describe("test of table write-and-read, with custom types, sync")
    def test_table_write_then_read_customtypes_sync(
        self,
        sync_empty_table_all_returns: DefaultTable,
    ) -> None:
        cdtypes_table = sync_empty_table_all_returns.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    custom_datatypes_in_reading=True,
                ),
            ),
        )
        cdtypes_table.insert_one(FULL_AR_ROW_CUSTOMTYPED)
        retrieved = cdtypes_table.find_one({})
        assert _repaint_NaNs(retrieved) == _repaint_NaNs(FULL_AR_ROW_CUSTOMTYPED)

    @pytest.mark.describe("test of table write-and-read, with noncustom types, sync")
    def test_table_write_then_read_noncustomtypes_sync(
        self,
        sync_empty_table_all_returns: DefaultTable,
    ) -> None:
        rdtypes_table = sync_empty_table_all_returns.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    custom_datatypes_in_reading=False,
                ),
            ),
        )
        rdtypes_table.insert_one(FULL_AR_ROW_NONCUSTOMTYPED)
        retrieved = rdtypes_table.find_one({})
        assert _repaint_NaNs(retrieved) == _repaint_NaNs(FULL_AR_ROW_NONCUSTOMTYPED)
