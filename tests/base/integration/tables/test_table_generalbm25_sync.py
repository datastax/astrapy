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


@pytest.mark.skip("Feature not merged to main yet")
class TestTableGeneralBM25Sync:
    @pytest.mark.describe("test of table bm25 DML, sync")
    def test_table_bm25_dml_sync(
        self,
        sync_empty_table_textindex: DefaultTable,
    ) -> None:
        table = sync_empty_table_textindex
        table.insert_many(
            [
                {
                    "id": "tree_0",
                    "body": "A tree in the woods.",
                },
                {
                    "id": "tree_1",
                    "body": "The lone tree in the plain.",
                },
                {
                    "id": "fish",
                    "body": "A fish in the ocean.",
                },
            ]
        )

        # filtering:
        frows_f = table.find({"body": {"$match": "fish"}}).to_list()
        assert frows_f != []
        assert frows_f[0]["id"] == "fish"

        frow_f = table.find_one({"body": {"$match": "fish"}})
        assert frow_f is not None
        assert frow_f["id"] == "fish"

        frows_t = table.find({"body": {"$match": "tree"}}).to_list()
        assert {frow["id"] for frow in frows_t} == {"tree_0", "tree_1"}

        # no-match filtering:
        frow_n = table.find_one({"body": {"$match": "toadstool"}})
        assert frow_n is None

        frows_n = table.find({"body": {"$match": "toadstool"}}).to_list()
        assert frows_n == []

        # sorting:
        srows_f = table.find(sort={"body": "fish"}).to_list()
        assert srows_f != []
        assert srows_f[0]["id"] == "fish"

        srows_t = table.find(sort={"body": "tree"}).to_list()
        assert {srow["id"] for srow in srows_t[:2]} == {"tree_0", "tree_1"}

        srow_f = table.find_one(sort={"body": "fish"})
        assert srow_f is not None
        assert srow_f["id"] == "fish"

        # no-match sorting
        srows_n = table.find(sort={"body": "toadstool"}).to_list()
        assert srows_n == []

        srow_n = table.find_one(sort={"body": "toadstool"})
        assert srow_n is None
