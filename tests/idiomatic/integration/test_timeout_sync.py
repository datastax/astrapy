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

from astrapy import Collection, Database

from astrapy.exceptions import DataAPITimeoutException
from astrapy.info import get_database_info


class TestTimeoutSync:
    @pytest.mark.describe("test of collection count_documents timeout, sync")
    def test_collection_count_documents_timeout_sync(
        self,
        sync_empty_collection: Collection,
    ) -> None:
        sync_empty_collection.insert_many([{"a": 1}] * 100)
        assert sync_empty_collection.count_documents({}, upper_bound=150) == 100

        with pytest.raises(DataAPITimeoutException) as exc:
            sync_empty_collection.count_documents({}, upper_bound=150, max_time_ms=1)
        assert exc.value.timeout_type == "read"
        assert exc.value.endpoint is not None
        assert exc.value.raw_payload is not None

    @pytest.mark.describe("test of database info timeout, sync")
    def test_database_info_timeout_sync(
        self,
        sync_database: Database,
    ) -> None:
        get_database_info(
            sync_database._astra_db.api_endpoint,
            token=sync_database._astra_db.token,
            namespace=sync_database.namespace,
        )

        with pytest.raises(DataAPITimeoutException) as exc:
            get_database_info(
                sync_database._astra_db.api_endpoint,
                token=sync_database._astra_db.token,
                namespace=sync_database.namespace,
                max_time_ms=1,
            )
        assert exc.value.timeout_type == "read"
        assert exc.value.endpoint is not None
        assert exc.value.raw_payload is not None

    @pytest.mark.describe("test of cursor-based timeouts, sync")
    def test_cursor_timeouts_sync(
        self,
        sync_empty_collection: Collection,
    ) -> None:
        sync_empty_collection.insert_one({"a": 1})

        cur0 = sync_empty_collection.find({})
        cur1 = sync_empty_collection.find({}, max_time_ms=1)
        cur0.__next__()
        with pytest.raises(DataAPITimeoutException):
            cur1.__next__()

        sync_empty_collection.find_one({})
        with pytest.raises(DataAPITimeoutException):
            sync_empty_collection.find_one({}, max_time_ms=1)
