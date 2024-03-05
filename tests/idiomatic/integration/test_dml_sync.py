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
from astrapy.api import APIRequestError


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

    @pytest.mark.describe("test of collection truncating delete_many, sync")
    def test_collection_truncating_delete_many_sync(
        self,
        sync_empty_collection: Collection,
    ) -> None:
        sync_empty_collection.insert_one({"doc": 1, "group": "A"})
        sync_empty_collection.insert_one({"doc": 2, "group": "B"})
        sync_empty_collection.insert_one({"doc": 3, "group": "A"})
        assert sync_empty_collection.count_documents(filter={}) == 3
        do_result1 = sync_empty_collection.delete_many({})
        assert isinstance(do_result1, DeleteResult)
        assert do_result1.acknowledged is True
        assert do_result1.deleted_count is None
        assert sync_empty_collection.count_documents(filter={}) == 0

    @pytest.mark.describe("test of collection chunk-requiring delete_many, sync")
    def test_collection_chunked_delete_many_sync(
        self,
        sync_empty_collection: Collection,
    ) -> None:
        sync_empty_collection.insert_many([{"doc": i, "group": "A"} for i in range(50)])
        sync_empty_collection.insert_many([{"doc": i, "group": "B"} for i in range(10)])
        assert sync_empty_collection.count_documents(filter={}) == 60
        do_result1 = sync_empty_collection.delete_many({"group": "A"})
        assert isinstance(do_result1, DeleteResult)
        assert do_result1.acknowledged is True
        assert do_result1.deleted_count == 50
        assert sync_empty_collection.count_documents(filter={}) == 10

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

    @pytest.mark.describe("test of cursors from collection.find, sync")
    def test_collection_cursors_sync(
        self,
        sync_empty_collection: Collection,
    ) -> None:
        """
        Functionalities of cursors from find, other than the various
        combinations of skip/limit/sort/filter specified above.
        """
        sync_empty_collection.insert_many(
            [{"seq": i, "ternary": (i % 3)} for i in range(10)]
        )

        # projection
        cursor0 = sync_empty_collection.find(projection={"ternary": False})
        document0 = cursor0.__next__()
        assert "ternary" not in document0
        cursor0b = sync_empty_collection.find(projection={"ternary": True})
        document0b = cursor0b.__next__()
        assert "ternary" in document0b

        # rewinding, slicing and retrieved
        cursor1 = sync_empty_collection.find(sort={"seq": 1})
        cursor1.__next__()
        cursor1.__next__()
        items1 = list(cursor1)[:2]
        assert list(cursor1.rewind()) == list(
            sync_empty_collection.find(sort={"seq": 1})
        )
        cursor1.rewind()
        assert items1 == list(cursor1[2:4])
        assert cursor1.retrieved == 2

        # address, cursor_id, collection
        assert cursor1.address == sync_empty_collection._astra_db_collection.base_path
        assert isinstance(cursor1.cursor_id, int)
        assert cursor1.collection == sync_empty_collection

        # clone, alive
        cursor2 = sync_empty_collection.find()
        assert cursor2.alive is True
        for _ in range(8):
            cursor2.__next__()
        assert cursor2.alive is True
        cursor3 = cursor2.clone()
        assert len(list(cursor2)) == 2
        assert len(list(cursor3)) == 10
        assert cursor2.alive is False

        # close
        cursor4 = sync_empty_collection.find()
        for _ in range(8):
            cursor4.__next__()
        cursor4.close()
        assert cursor4.alive is False
        with pytest.raises(StopIteration):
            cursor4.__next__()

        # distinct
        cursor5 = sync_empty_collection.find()
        dist5 = cursor5.distinct("ternary")
        assert (len(list(cursor5))) == 10
        assert set(dist5) == {0, 1, 2}
        cursor6 = sync_empty_collection.find()
        for _ in range(9):
            cursor6.__next__()
        dist6 = cursor6.distinct("ternary")
        assert (len(list(cursor6))) == 1
        assert set(dist6) == {0, 1, 2}

        # distinct from collections
        assert set(sync_empty_collection.distinct("ternary")) == {0, 1, 2}
        assert set(sync_empty_collection.distinct("nonfield")) == set()

        # indexing by integer
        cursor7 = sync_empty_collection.find(sort={"seq": 1})
        assert cursor7[5]["seq"] == 5

        # indexing by wrong type
        with pytest.raises(TypeError):
            cursor7.rewind()
            cursor7["wrong"]

    @pytest.mark.describe("test of collection insert_many, sync")
    def test_collection_insert_many_sync(
        self,
        sync_empty_collection: Collection,
    ) -> None:
        col = sync_empty_collection

        ins_result1 = col.insert_many([{"_id": "a"}, {"_id": "b"}])
        assert set(ins_result1.inserted_ids) == {"a", "b"}
        assert {doc["_id"] for doc in col.find()} == {"a", "b"}

        with pytest.raises(APIRequestError):
            col.insert_many([{"_id": "a"}, {"_id": "c"}])
        assert {doc["_id"] for doc in col.find()} == {"a", "b"}

        with pytest.raises(APIRequestError):
            col.insert_many([{"_id": "c"}, {"_id": "a"}, {"_id": "d"}])
        assert {doc["_id"] for doc in col.find()} == {"a", "b", "c"}

        with pytest.raises(ValueError):
            col.insert_many(
                [{"_id": "c"}, {"_id": "d"}, {"_id": "e"}],
                ordered=False,
            )
        assert {doc["_id"] for doc in col.find()} == {"a", "b", "c", "d", "e"}

    @pytest.mark.describe("test of collection find_one, sync")
    def test_collection_find_one_sync(
        self,
        sync_empty_collection: Collection,
    ) -> None:
        col = sync_empty_collection
        col.insert_many(
            [
                {"_id": "?", "seq": 0, "kind": "punctuation"},
                {"_id": "a", "seq": 1, "kind": "letter"},
                {"_id": "b", "seq": 2, "kind": "letter"},
            ]
        )

        fo1 = col.find_one({"kind": "frog"})
        assert fo1 is None

        Nski = 1
        Nlim = 10
        Nsor = {"seq": 1}
        Nfil = {"kind": "letter"}

        # case 0000 of find-pattern matrix
        doc0000 = col.find_one(skip=None, limit=None, sort=None, filter=None)
        assert doc0000 is not None
        assert doc0000["seq"] in {0, 1, 2}

        # case 0001
        doc0001 = col.find_one(skip=None, limit=None, sort=None, filter=Nfil)
        assert doc0001 is not None
        assert doc0001["seq"] in {1, 2}

        # case 0010
        doc0010 = col.find_one(skip=None, limit=None, sort=Nsor, filter=None)
        assert doc0010 is not None
        assert doc0010["seq"] == 0

        # case 0011
        doc0011 = col.find_one(skip=None, limit=None, sort=Nsor, filter=Nfil)
        assert doc0011 is not None
        assert doc0011["seq"] == 1

        # case 0100
        doc0100 = col.find_one(skip=None, limit=Nlim, sort=None, filter=None)
        assert doc0100 is not None
        assert doc0100["seq"] in {0, 1, 2}

        # case 0101
        doc0101 = col.find_one(skip=None, limit=Nlim, sort=None, filter=Nfil)
        assert doc0101 is not None
        assert doc0101["seq"] in {1, 2}

        # case 0110
        doc0110 = col.find_one(skip=None, limit=Nlim, sort=Nsor, filter=None)
        assert doc0110 is not None
        assert doc0110["seq"] == 0

        # case 0111
        doc0111 = col.find_one(skip=None, limit=Nlim, sort=Nsor, filter=Nfil)
        assert doc0111 is not None
        assert doc0111["seq"] == 1

        # case 1000
        # col.find_one(skip=Nski, limit=None, sort=None, filter=None) ...

        # case 1001
        # col.find_one(skip=Nski, limit=None, sort=None, filter=Nfil) ...

        # case 1010
        doc1010 = col.find_one(skip=Nski, limit=None, sort=Nsor, filter=None)
        assert doc1010 is not None
        assert doc1010["seq"] == 1

        # case 1011
        doc1011 = col.find_one(skip=Nski, limit=None, sort=Nsor, filter=Nfil)
        assert doc1011 is not None
        assert doc1011["seq"] == 2

        # case 1100
        # col.find_one(skip=Nski, limit=Nlim, sort=None, filter=None) ...

        # case 1101
        # col.find_one(skip=Nski, limit=Nlim, sort=None, filter=Nfil) ...

        # case 1110
        doc1110 = col.find_one(skip=Nski, limit=Nlim, sort=Nsor, filter=None)
        assert doc1110 is not None
        assert doc1110["seq"] == 1

        # case 1111
        doc1111 = col.find_one(skip=Nski, limit=Nlim, sort=Nsor, filter=Nfil)
        assert doc1111 is not None
        assert doc1111["seq"] == 2

        # projection
        doc_full = col.find_one(skip=Nski, limit=Nlim, sort=Nsor, filter=Nfil)
        doc_proj = col.find_one(
            skip=Nski, limit=Nlim, sort=Nsor, filter=Nfil, projection={"kind": True}
        )
        assert doc_proj == {"_id": "b", "kind": "letter"}
        assert doc_full == {"_id": "b", "seq": 2, "kind": "letter"}
