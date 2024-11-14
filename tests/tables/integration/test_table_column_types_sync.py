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

from ..conftest import DefaultTable, _repaint_NaNs
from .table_row_assets import (
    FULL_AR_DOC_CUSTOMTYPED,
)


class TestTableColumnTypesSync:
    @pytest.mark.describe("test of table write-and-read, with custom types")
    def test_table_write_then_read_customtypes(
        self,
        sync_empty_table_all_returns: DefaultTable,
    ) -> None:
        sync_empty_table_all_returns.insert_one(FULL_AR_DOC_CUSTOMTYPED)
        retrieved = sync_empty_table_all_returns.find_one({})
        assert _repaint_NaNs(retrieved) == _repaint_NaNs(FULL_AR_DOC_CUSTOMTYPED)
