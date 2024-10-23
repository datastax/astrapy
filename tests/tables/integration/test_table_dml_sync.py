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

from ..conftest import DefaultTable

DOC_PK_0 = {
    "p_ascii": "abc",
    "p_bigint": 10000,
    "p_int": 987,
    "p_boolean": False,
}
DOC_0 = {
    "p_text": "Ålesund",
    **DOC_PK_0,
}


class TestTableDMLSync:
    @pytest.mark.describe("test of table insert_one, sync")
    def test_table_insert_one_sync(
        self,
        sync_table_all_returns: DefaultTable,
    ) -> None:
        # TODO enlarge the test with all values + a partial row
        # TODO check returned is sparse
        # TODO rearrange docs as fixtures/constants together with table def
        # TODO cross check with CQL direct (!), astra only
        no_doc_0a = sync_table_all_returns.find_one(filter=DOC_PK_0)
        assert no_doc_0a is None
        sync_table_all_returns.insert_one(row=DOC_0)
        doc_0 = sync_table_all_returns.find_one(filter=DOC_PK_0)
        assert doc_0 is not None
        # TODO restore following match once sparse. For now, next line:
        # assert doc_0 == DOC_0
        assert {doc_0[k] == v for k, v in DOC_0.items()}
        sync_table_all_returns.delete_one(filter=DOC_PK_0)
        no_doc_0b = sync_table_all_returns.find_one(filter=DOC_PK_0)
        assert no_doc_0b is None
