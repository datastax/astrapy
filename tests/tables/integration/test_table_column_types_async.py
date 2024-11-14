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

from ..conftest import DefaultAsyncTable, _repaint_NaNs
from .table_row_assets import (
    FULL_AR_DOC_CUSTOMTYPED,
    FULL_AR_DOC_NONCUSTOMTYPED,
)


class TestTableColumnTypesSync:
    @pytest.mark.describe("test of table write-and-read, with custom types, async")
    async def test_table_write_then_read_customtypes_async(
        self,
        async_empty_table_all_returns: DefaultAsyncTable,
    ) -> None:
        cdtypes_atable = async_empty_table_all_returns.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    custom_datatypes_in_reading=True,
                ),
            ),
        )
        await cdtypes_atable.insert_one(FULL_AR_DOC_CUSTOMTYPED)
        retrieved = await cdtypes_atable.find_one({})
        assert _repaint_NaNs(retrieved) == _repaint_NaNs(FULL_AR_DOC_CUSTOMTYPED)

    @pytest.mark.describe("test of table write-and-read, with noncustom types, async")
    async def test_table_write_then_read_noncustomtypes_async(
        self,
        async_empty_table_all_returns: DefaultAsyncTable,
    ) -> None:
        rdtypes_atable = async_empty_table_all_returns.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    custom_datatypes_in_reading=False,
                ),
            ),
        )
        await rdtypes_atable.insert_one(FULL_AR_DOC_NONCUSTOMTYPED)
        retrieved = await rdtypes_atable.find_one({})
        assert _repaint_NaNs(retrieved) == _repaint_NaNs(FULL_AR_DOC_NONCUSTOMTYPED)
