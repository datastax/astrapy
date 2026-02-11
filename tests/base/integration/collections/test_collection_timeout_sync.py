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

import time

import pytest

from astrapy import Database
from astrapy.admin.admin import fetch_database_info
from astrapy.exceptions import DataAPITimeoutException, DevOpsAPITimeoutException

from ..conftest import IS_ASTRA_DB, DefaultCollection


class TestCollectionTimeoutSync:
    @pytest.mark.describe("test of collection count_documents timeout, sync")
    def test_collection_count_documents_timeout_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        sync_empty_collection.insert_many([{"a": 1}] * 500)
        time.sleep(2)

        with pytest.raises(DataAPITimeoutException) as exc:
            sync_empty_collection.count_documents(
                {}, upper_bound=800, general_method_timeout_ms=1
            )
        assert sync_empty_collection.count_documents({}, upper_bound=800) >= 500
        assert exc.value.timeout_type in {"connect", "read"}
        assert exc.value.endpoint is not None
        assert exc.value.raw_payload is not None

    @pytest.mark.skipif(not IS_ASTRA_DB, reason="Not supported outside of Astra DB")
    @pytest.mark.describe("test of database info timeout, sync")
    def test_database_info_timeout_sync(
        self,
        sync_database: Database,
    ) -> None:
        info = fetch_database_info(
            sync_database.api_endpoint,
            token=sync_database.api_options.token,
        )
        assert info is not None

        with pytest.raises(DevOpsAPITimeoutException) as exc:
            info = fetch_database_info(
                sync_database.api_endpoint,
                token=sync_database.api_options.token,
                request_timeout_ms=50,
            )
            assert info is not None
        assert exc.value.timeout_type in {"connect", "read"}
        assert exc.value.endpoint is not None
        assert exc.value.raw_payload is not None

    @pytest.mark.describe("test of cursor-based overall timeouts, sync")
    def test_cursor_overalltimeout_exceptions_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        col = sync_empty_collection
        col.insert_many([{"a": 1}] * 1000)

        col.distinct("a", general_method_timeout_ms=40000)
        with pytest.raises(DataAPITimeoutException):
            col.distinct("a", timeout_ms=50)

        col.distinct("a", general_method_timeout_ms=40000)
        with pytest.raises(DataAPITimeoutException):
            col.distinct("a", general_method_timeout_ms=50)

    @pytest.mark.describe("test of insert_many timeouts, sync")
    def test_insert_many_timeout_exceptions_sync(
        self,
        sync_collection: DefaultCollection,
    ) -> None:
        fifty_docs = [{"seq": i} for i in range(50)]
        sync_collection.insert_many(
            fifty_docs, ordered=True, general_method_timeout_ms=20000
        )
        sync_collection.insert_many(
            fifty_docs, ordered=False, concurrency=1, general_method_timeout_ms=20000
        )
        sync_collection.insert_many(
            fifty_docs, ordered=False, concurrency=2, general_method_timeout_ms=20000
        )

        with pytest.raises(DataAPITimeoutException):
            sync_collection.insert_many(
                fifty_docs, ordered=True, general_method_timeout_ms=2
            )
        with pytest.raises(DataAPITimeoutException):
            sync_collection.insert_many(
                fifty_docs, ordered=False, concurrency=1, general_method_timeout_ms=2
            )
        with pytest.raises(DataAPITimeoutException):
            sync_collection.insert_many(
                fifty_docs, ordered=False, concurrency=2, general_method_timeout_ms=2
            )

    @pytest.mark.describe("test of update_many timeouts, sync")
    def test_update_many_timeout_exceptions_sync(
        self,
        sync_collection: DefaultCollection,
    ) -> None:
        fifty_docs = [{"seq": i, "f": "update_many"} for i in range(50)]
        sync_collection.insert_many(fifty_docs, ordered=False, concurrency=3)

        sync_collection.update_many({"f": "update_many"}, {"$inc": {"seq": 100}})
        sync_collection.update_many(
            {"f": "update_many"},
            {"$inc": {"seq": 100}},
            general_method_timeout_ms=20000,
        )

        with pytest.raises(DataAPITimeoutException):
            sync_collection.update_many(
                {"f": "update_many"},
                {"$inc": {"seq": 100}},
                general_method_timeout_ms=2,
            )

    @pytest.mark.describe("test of delete_many timeouts, sync")
    def test_delete_many_timeout_exceptions_sync(
        self,
        sync_collection: DefaultCollection,
    ) -> None:
        fifty_docs1 = [{"seq": i, "f": "delete_many1"} for i in range(50)]
        fifty_docs2 = [{"seq": i, "f": "delete_many2"} for i in range(50)]
        fifty_docs3 = [{"seq": i, "f": "delete_many3"} for i in range(50)]
        sync_collection.insert_many(
            fifty_docs1 + fifty_docs2 + fifty_docs3,
            ordered=False,
            concurrency=5,
        )

        sync_collection.delete_many({"f": "delete_many1"})
        sync_collection.delete_many(
            {"f": "delete_many2"}, general_method_timeout_ms=20000
        )
        with pytest.raises(DataAPITimeoutException):
            sync_collection.delete_many(
                {"f": "delete_many3"}, general_method_timeout_ms=2
            )

    @pytest.mark.describe("test of collection find-with-collective timeout, sync")
    def test_collection_find_with_collective_timeout_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        sync_empty_collection.insert_many([{"a": 1}] * 55)
        time.sleep(1)

        with pytest.raises(DataAPITimeoutException):
            sync_empty_collection.distinct("a", general_method_timeout_ms=1)

        cur_tl = sync_empty_collection.find()
        with pytest.raises(DataAPITimeoutException):
            cur_tl.to_list(general_method_timeout_ms=1)

        cur_fe = sync_empty_collection.find()
        with pytest.raises(DataAPITimeoutException):
            cur_fe.for_each(lambda doc: None, general_method_timeout_ms=1)
