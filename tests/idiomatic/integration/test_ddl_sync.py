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

import pytest

from astrapy import Database


class TestDDLSync:
    @pytest.mark.describe("test of collection creation, get, and then drop, sync")
    def test_collection_lifecycle_sync(
        self,
        sync_database: Database,
    ) -> None:
        TEST_COLLECTION_NAME = "test_coll"
        col1 = sync_database.create_collection(
            TEST_COLLECTION_NAME,
            dimension=123,
            metric="euclidean",
            indexing={"deny": ["a", "b", "c"]},
        )
        col2 = sync_database.get_collection(TEST_COLLECTION_NAME)
        assert col1 == col2
        sync_database.drop_collection(TEST_COLLECTION_NAME)