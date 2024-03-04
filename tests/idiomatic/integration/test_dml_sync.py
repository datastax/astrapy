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

from astrapy import Collection
from astrapy.results import DeleteResult, InsertOneResult


class TestDMLSync:
    @pytest.mark.describe("test of collection count_documents, sync")
    def test_collection_count_documents_sync(
        self,
        sync_empty_collection: Collection,
    ) -> None:
        assert sync_empty_collection.count_documents(filter={}) == 0
        sync_empty_collection.insert_one({"doc": 1, "group": "A"})
        sync_empty_collection.insert_one({"doc": 2, "group": "B"})
        sync_empty_collection.insert_one({"doc": 3, "group": "A"})
        assert sync_empty_collection.count_documents(filter={}) == 3
        assert sync_empty_collection.count_documents(filter={"group": "A"}) == 2

    @pytest.mark.describe("test of collection insert_one, sync")
    def test_collection_insert_one_sync(
        self,
        sync_empty_collection: Collection,
    ) -> None:
        io_result1 = sync_empty_collection.insert_one({"doc": 1, "group": "A"})
        assert isinstance(io_result1, InsertOneResult)
        assert io_result1.acknowledged is True
        io_result2 = sync_empty_collection.insert_one(
            {"_id": "xxx", "doc": 2, "group": "B"}
        )
        assert io_result2.inserted_id == "xxx"
        assert sync_empty_collection.count_documents(filter={"group": "A"}) == 1

    @pytest.mark.describe("test of collection delete_one, sync")
    def test_collection_delete_one_sync(
        self,
        sync_empty_collection: Collection,
    ) -> None:
        sync_empty_collection.insert_one({"doc": 1, "group": "A"})
        sync_empty_collection.insert_one({"doc": 2, "group": "B"})
        sync_empty_collection.insert_one({"doc": 3, "group": "A"})
        assert sync_empty_collection.count_documents(filter={}) == 3
        do_result1 = sync_empty_collection.delete_one({"group": "A"})
        assert isinstance(do_result1, DeleteResult)
        assert do_result1.acknowledged is True
        assert do_result1.deleted_count == 1
        assert sync_empty_collection.count_documents(filter={}) == 2

    @pytest.mark.describe("test of collection delete_many, sync")
    def test_collection_delete_many_sync(
        self,
        sync_empty_collection: Collection,
    ) -> None:
        sync_empty_collection.insert_one({"doc": 1, "group": "A"})
        sync_empty_collection.insert_one({"doc": 2, "group": "B"})
        sync_empty_collection.insert_one({"doc": 3, "group": "A"})
        assert sync_empty_collection.count_documents(filter={}) == 3
        do_result1 = sync_empty_collection.delete_many({"group": "A"})
        assert isinstance(do_result1, DeleteResult)
        assert do_result1.acknowledged is True
        assert do_result1.deleted_count == 2
        assert sync_empty_collection.count_documents(filter={}) == 1

    @pytest.mark.describe("test of collection find, sync")
    def test_collection_find_sync(
        self,
        sync_empty_collection: Collection,
    ) -> None:
        sync_empty_collection.insert_many([{"seq": i} for i in range(30)])
        Nski = 1
        Nlim = 28
        Nsor = {"seq": -1}
        Nfil = {"seq": {"$exists": True}}

        # case 0000 of find-pattern matrix
        assert (
            len(
                list(
                    sync_empty_collection.find(
                        skip=None, limit=None, sort=None, filter=None
                    )
                )
            )
            == 30
        )

        # case 0001
        assert (
            len(
                list(
                    sync_empty_collection.find(
                        skip=None, limit=None, sort=None, filter=Nfil
                    )
                )
            )
            == 30
        )

        # case 0010
        assert (
            len(
                list(
                    sync_empty_collection.find(
                        skip=None, limit=None, sort=Nsor, filter=None
                    )
                )
            )
            == 20
        )  # NONPAGINATED

        # case 0011
        assert (
            len(
                list(
                    sync_empty_collection.find(
                        skip=None, limit=None, sort=Nsor, filter=Nfil
                    )
                )
            )
            == 20
        )  # NONPAGINATED

        # case 0100
        assert (
            len(
                list(
                    sync_empty_collection.find(
                        skip=None, limit=Nlim, sort=None, filter=None
                    )
                )
            )
            == 28
        )

        # case 0101
        assert (
            len(
                list(
                    sync_empty_collection.find(
                        skip=None, limit=Nlim, sort=None, filter=Nfil
                    )
                )
            )
            == 28
        )

        # case 0110
        assert (
            len(
                list(
                    sync_empty_collection.find(
                        skip=None, limit=Nlim, sort=Nsor, filter=None
                    )
                )
            )
            == 20
        )  # NONPAGINATED

        # case 0111
        assert (
            len(
                list(
                    sync_empty_collection.find(
                        skip=None, limit=Nlim, sort=Nsor, filter=Nfil
                    )
                )
            )
            == 20
        )  # NONPAGINATED

        # case 1000
        # len(list(sync_empty_collection.find(skip=Nski, limit=None, sort=None, filter=None)))

        # case 1001
        # len(list(sync_empty_collection.find(skip=Nski, limit=None, sort=None, filter=Nfil)))

        # case 1010
        assert (
            len(
                list(
                    sync_empty_collection.find(
                        skip=Nski, limit=None, sort=Nsor, filter=None
                    )
                )
            )
            == 20
        )  # NONPAGINATED

        # case 1011
        assert (
            len(
                list(
                    sync_empty_collection.find(
                        skip=Nski, limit=None, sort=Nsor, filter=Nfil
                    )
                )
            )
            == 20
        )  # NONPAGINATED

        # case 1100
        # len(list(sync_empty_collection.find(skip=Nski, limit=Nlim, sort=None, filter=None)))

        # case 1101
        # len(list(sync_empty_collection.find(skip=Nski, limit=Nlim, sort=None, filter=Nfil)))

        # case 1110
        assert (
            len(
                list(
                    sync_empty_collection.find(
                        skip=Nski, limit=Nlim, sort=Nsor, filter=None
                    )
                )
            )
            == 20
        )  # NONPAGINATED

        # case 1111
        assert (
            len(
                list(
                    sync_empty_collection.find(
                        skip=Nski, limit=Nlim, sort=Nsor, filter=Nfil
                    )
                )
            )
            == 20
        )  # NONPAGINATED

        # rewinding and slicing
        cursor1 = sync_empty_collection.find(sort={"seq": 1})
        cursor1.__next__()
        cursor1.__next__()
        items1 = list(cursor1)[:2]
        assert list(cursor1.rewind()) == list(
            sync_empty_collection.find(sort={"seq": 1})
        )
        cursor1.rewind()
        assert items1 == list(cursor1[2:4])
