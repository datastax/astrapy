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

import time
import pytest

from astrapy import Collection, Database

from astrapy.exceptions import DataAPITimeoutException
from astrapy.operations import DeleteMany, InsertMany
from astrapy.admin import fetch_database_info


class TestTimeoutSync:
    @pytest.mark.describe("test of collection count_documents timeout, sync")
    def test_collection_count_documents_timeout_sync(
        self,
        sync_empty_collection: Collection,
    ) -> None:
        sync_empty_collection.insert_many([{"a": 1}] * 100)
        time.sleep(10)
        assert sync_empty_collection.count_documents({}, upper_bound=150) >= 100

        with pytest.raises(DataAPITimeoutException) as exc:
            sync_empty_collection.count_documents({}, upper_bound=150, max_time_ms=1)
        assert exc.value.timeout_type in {"connect", "read"}
        assert exc.value.endpoint is not None
        assert exc.value.raw_payload is not None

    @pytest.mark.describe("test of database info timeout, sync")
    def test_database_info_timeout_sync(
        self,
        sync_database: Database,
    ) -> None:
        fetch_database_info(
            sync_database._astra_db.api_endpoint,
            token=sync_database._astra_db.token,
            namespace=sync_database.namespace,
        )

        with pytest.raises(DataAPITimeoutException) as exc:
            fetch_database_info(
                sync_database._astra_db.api_endpoint,
                token=sync_database._astra_db.token,
                namespace=sync_database.namespace,
                max_time_ms=1,
            )
        assert exc.value.timeout_type in {"connect", "read"}
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

    @pytest.mark.describe("test of cursor-based overall timeouts, sync")
    def test_cursor_overalltimeout_exceptions_sync(
        self,
        sync_empty_collection: Collection,
    ) -> None:
        col = sync_empty_collection
        col.insert_many([{"a": 1}] * 1000)

        col.distinct("a", max_time_ms=20000)
        with pytest.raises(DataAPITimeoutException):
            col.distinct("a", max_time_ms=1)

        cur1 = col.find({})
        cur2 = col.find({})
        cur1.distinct("a", max_time_ms=20000)
        with pytest.raises(DataAPITimeoutException):
            cur2.distinct("a", max_time_ms=1)

    @pytest.mark.describe("test of insert_many timeouts, sync")
    def test_insert_many_timeout_exceptions_sync(
        self,
        sync_collection: Collection,
    ) -> None:
        fifty_docs = [{"seq": i} for i in range(50)]
        sync_collection.insert_many(fifty_docs, ordered=True, max_time_ms=20000)
        sync_collection.insert_many(
            fifty_docs, ordered=False, concurrency=1, max_time_ms=20000
        )
        sync_collection.insert_many(
            fifty_docs, ordered=False, concurrency=2, max_time_ms=20000
        )

        with pytest.raises(DataAPITimeoutException):
            sync_collection.insert_many(fifty_docs, ordered=True, max_time_ms=2)
        with pytest.raises(DataAPITimeoutException):
            sync_collection.insert_many(
                fifty_docs, ordered=False, concurrency=1, max_time_ms=2
            )
        with pytest.raises(DataAPITimeoutException):
            sync_collection.insert_many(
                fifty_docs, ordered=False, concurrency=2, max_time_ms=2
            )

    @pytest.mark.describe("test of update_many timeouts, sync")
    def test_update_many_timeout_exceptions_sync(
        self,
        sync_collection: Collection,
    ) -> None:
        fifty_docs = [{"seq": i, "f": "update_many"} for i in range(50)]
        sync_collection.insert_many(fifty_docs, ordered=False, concurrency=3)

        sync_collection.update_many({"f": "update_many"}, {"$inc": {"seq": 100}})
        sync_collection.update_many(
            {"f": "update_many"}, {"$inc": {"seq": 100}}, max_time_ms=20000
        )

        with pytest.raises(DataAPITimeoutException):
            sync_collection.update_many(
                {"f": "update_many"}, {"$inc": {"seq": 100}}, max_time_ms=2
            )

    @pytest.mark.describe("test of delete_many timeouts, sync")
    def test_delete_many_timeout_exceptions_sync(
        self,
        sync_collection: Collection,
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
        sync_collection.delete_many({"f": "delete_many2"}, max_time_ms=20000)
        with pytest.raises(DataAPITimeoutException):
            sync_collection.delete_many({"f": "delete_many3"}, max_time_ms=2)

    @pytest.mark.describe("test of bulk_write timeouts, sync")
    def test_bulk_write_ordered_timeout_exceptions_sync(
        self,
        sync_empty_collection: Collection,
    ) -> None:
        im_a = InsertMany([{"seq": i, "group": "A"} for i in range(100)])
        im_b = InsertMany([{"seq": i, "group": "B"} for i in range(100)])
        dm = DeleteMany(filter={"group": "A"})

        sync_empty_collection.bulk_write([im_a, im_b, dm], ordered=True)
        sync_empty_collection.bulk_write(
            [im_a, im_b, dm], ordered=True, max_time_ms=50000
        )
        with pytest.raises(DataAPITimeoutException):
            sync_empty_collection.bulk_write(
                [im_a, im_b, dm], ordered=True, max_time_ms=500
            )

    @pytest.mark.describe("test of bulk_write timeouts, sync")
    def test_bulk_write_unordered_timeout_exceptions_sync(
        self,
        sync_empty_collection: Collection,
    ) -> None:
        im_a = InsertMany([{"seq": i, "group": "A"} for i in range(100)])
        im_b = InsertMany([{"seq": i, "group": "B"} for i in range(100)])
        dm = DeleteMany(filter={"group": "A"})

        sync_empty_collection.bulk_write([im_a, im_b, dm], ordered=False)
        sync_empty_collection.bulk_write(
            [im_a, im_b, dm], ordered=False, max_time_ms=50000
        )
        with pytest.raises(DataAPITimeoutException):
            sync_empty_collection.bulk_write(
                [im_a, im_b, dm], ordered=False, max_time_ms=5
            )
