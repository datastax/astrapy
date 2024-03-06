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

from typing import List

import pytest

from astrapy import AsyncCollection
from astrapy.results import DeleteResult, InsertOneResult
from astrapy.api import APIRequestError
from astrapy.idiomatic.types import DocumentType
from astrapy.idiomatic.cursors import AsyncCursor
from astrapy.idiomatic.types import ReturnDocument
from astrapy.idiomatic.operations import (
    AsyncInsertOne,
    AsyncInsertMany,
    AsyncUpdateOne,
    AsyncUpdateMany,
    AsyncReplaceOne,
    AsyncDeleteOne,
    AsyncDeleteMany,
)


class TestDMLAsync:
    @pytest.mark.describe("test of collection count_documents, async")
    async def test_collection_count_documents_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        assert await async_empty_collection.count_documents(filter={}) == 0
        await async_empty_collection.insert_one({"doc": 1, "group": "A"})
        await async_empty_collection.insert_one({"doc": 2, "group": "B"})
        await async_empty_collection.insert_one({"doc": 3, "group": "A"})
        assert await async_empty_collection.count_documents(filter={}) == 3
        assert await async_empty_collection.count_documents(filter={"group": "A"}) == 2

    @pytest.mark.describe("test of collection insert_one, async")
    async def test_collection_insert_one_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        io_result1 = await async_empty_collection.insert_one({"doc": 1, "group": "A"})
        assert isinstance(io_result1, InsertOneResult)
        assert io_result1.acknowledged is True
        io_result2 = await async_empty_collection.insert_one(
            {"_id": "xxx", "doc": 2, "group": "B"}
        )
        assert io_result2.inserted_id == "xxx"
        assert await async_empty_collection.count_documents(filter={"group": "A"}) == 1

    @pytest.mark.describe("test of collection delete_one, async")
    async def test_collection_delete_one_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        await async_empty_collection.insert_one({"doc": 1, "group": "A"})
        await async_empty_collection.insert_one({"doc": 2, "group": "B"})
        await async_empty_collection.insert_one({"doc": 3, "group": "A"})
        assert await async_empty_collection.count_documents(filter={}) == 3
        do_result1 = await async_empty_collection.delete_one({"group": "A"})
        assert isinstance(do_result1, DeleteResult)
        assert do_result1.acknowledged is True
        assert do_result1.deleted_count == 1
        assert await async_empty_collection.count_documents(filter={}) == 2

    @pytest.mark.describe("test of collection delete_many, async")
    async def test_collection_delete_many_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        await async_empty_collection.insert_one({"doc": 1, "group": "A"})
        await async_empty_collection.insert_one({"doc": 2, "group": "B"})
        await async_empty_collection.insert_one({"doc": 3, "group": "A"})
        assert await async_empty_collection.count_documents(filter={}) == 3
        do_result1 = await async_empty_collection.delete_many({"group": "A"})
        assert isinstance(do_result1, DeleteResult)
        assert do_result1.acknowledged is True
        assert do_result1.deleted_count == 2
        assert await async_empty_collection.count_documents(filter={}) == 1

    @pytest.mark.describe("test of collection truncating delete_many, async")
    async def test_collection_truncating_delete_many_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        await async_empty_collection.insert_one({"doc": 1, "group": "A"})
        await async_empty_collection.insert_one({"doc": 2, "group": "B"})
        await async_empty_collection.insert_one({"doc": 3, "group": "A"})
        assert (await async_empty_collection.count_documents(filter={})) == 3
        do_result1 = await async_empty_collection.delete_many({})
        assert isinstance(do_result1, DeleteResult)
        assert do_result1.acknowledged is True
        assert do_result1.deleted_count is None
        assert (await async_empty_collection.count_documents(filter={})) == 0

    @pytest.mark.describe("test of collection chunk-requiring delete_many, async")
    async def test_collection_chunked_delete_many_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        await async_empty_collection.insert_many(
            [{"doc": i, "group": "A"} for i in range(50)]
        )
        await async_empty_collection.insert_many(
            [{"doc": i, "group": "B"} for i in range(10)]
        )
        assert (await async_empty_collection.count_documents(filter={})) == 60
        do_result1 = await async_empty_collection.delete_many({"group": "A"})
        assert isinstance(do_result1, DeleteResult)
        assert do_result1.acknowledged is True
        assert do_result1.deleted_count == 50
        assert (await async_empty_collection.count_documents(filter={})) == 10

    @pytest.mark.describe("test of collection find, async")
    async def test_collection_find_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        await async_empty_collection.insert_many([{"seq": i} for i in range(30)])
        Nski = 1
        Nlim = 28
        Nsor = {"seq": -1}
        Nfil = {"seq": {"$exists": True}}

        async def _alist(acursor: AsyncCursor) -> List[DocumentType]:
            return [doc async for doc in acursor]

        # case 0000 of find-pattern matrix
        assert (
            len(
                await _alist(
                    async_empty_collection.find(
                        skip=None, limit=None, sort=None, filter=None
                    )
                )
            )
            == 30
        )

        # case 0001
        assert (
            len(
                await _alist(
                    async_empty_collection.find(
                        skip=None, limit=None, sort=None, filter=Nfil
                    )
                )
            )
            == 30
        )

        # case 0010
        assert (
            len(
                await _alist(
                    async_empty_collection.find(
                        skip=None, limit=None, sort=Nsor, filter=None
                    )
                )
            )
            == 20
        )  # NONPAGINATED

        # case 0011
        assert (
            len(
                await _alist(
                    async_empty_collection.find(
                        skip=None, limit=None, sort=Nsor, filter=Nfil
                    )
                )
            )
            == 20
        )  # NONPAGINATED

        # case 0100
        assert (
            len(
                await _alist(
                    async_empty_collection.find(
                        skip=None, limit=Nlim, sort=None, filter=None
                    )
                )
            )
            == 28
        )

        # case 0101
        assert (
            len(
                await _alist(
                    async_empty_collection.find(
                        skip=None, limit=Nlim, sort=None, filter=Nfil
                    )
                )
            )
            == 28
        )

        # case 0110
        assert (
            len(
                await _alist(
                    async_empty_collection.find(
                        skip=None, limit=Nlim, sort=Nsor, filter=None
                    )
                )
            )
            == 20
        )  # NONPAGINATED

        # case 0111
        assert (
            len(
                await _alist(
                    async_empty_collection.find(
                        skip=None, limit=Nlim, sort=Nsor, filter=Nfil
                    )
                )
            )
            == 20
        )  # NONPAGINATED

        # case 1000
        # len(list(async_empty_collection.find(skip=Nski, limit=None, sort=None, filter=None)))

        # case 1001
        # len(list(async_empty_collection.find(skip=Nski, limit=None, sort=None, filter=Nfil)))

        # case 1010
        assert (
            len(
                await _alist(
                    async_empty_collection.find(
                        skip=Nski, limit=None, sort=Nsor, filter=None
                    )
                )
            )
            == 20
        )  # NONPAGINATED

        # case 1011
        assert (
            len(
                await _alist(
                    async_empty_collection.find(
                        skip=Nski, limit=None, sort=Nsor, filter=Nfil
                    )
                )
            )
            == 20
        )  # NONPAGINATED

        # case 1100
        # len(list(async_empty_collection.find(skip=Nski, limit=Nlim, sort=None, filter=None)))

        # case 1101
        # len(list(async_empty_collection.find(skip=Nski, limit=Nlim, sort=None, filter=Nfil)))

        # case 1110
        assert (
            len(
                await _alist(
                    async_empty_collection.find(
                        skip=Nski, limit=Nlim, sort=Nsor, filter=None
                    )
                )
            )
            == 20
        )  # NONPAGINATED

        # case 1111
        assert (
            len(
                await _alist(
                    async_empty_collection.find(
                        skip=Nski, limit=Nlim, sort=Nsor, filter=Nfil
                    )
                )
            )
            == 20
        )  # NONPAGINATED

    @pytest.mark.describe("test of cursors from collection.find, async")
    async def test_collection_cursors_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        """
        Functionalities of cursors from find, other than the various
        combinations of skip/limit/sort/filter specified above.
        """
        await async_empty_collection.insert_many(
            [{"seq": i, "ternary": (i % 3)} for i in range(10)]
        )

        # projection
        cursor0 = async_empty_collection.find(projection={"ternary": False})
        document0 = await cursor0.__anext__()
        assert "ternary" not in document0
        cursor0b = async_empty_collection.find(projection={"ternary": True})
        document0b = await cursor0b.__anext__()
        assert "ternary" in document0b

        async def _alist(acursor: AsyncCursor) -> List[DocumentType]:
            return [doc async for doc in acursor]

        # rewinding, slicing and retrieved
        cursor1 = async_empty_collection.find(sort={"seq": 1})
        await cursor1.__anext__()
        await cursor1.__anext__()
        items1 = (await _alist(cursor1))[:2]
        assert await _alist(cursor1.rewind()) == await _alist(
            async_empty_collection.find(sort={"seq": 1})
        )
        cursor1.rewind()
        assert items1 == await _alist(cursor1[2:4])  # type: ignore[arg-type]
        assert cursor1.retrieved == 2

        # address, cursor_id, collection
        assert cursor1.address == async_empty_collection._astra_db_collection.base_path
        assert isinstance(cursor1.cursor_id, int)
        assert cursor1.collection == async_empty_collection

        # clone, alive
        cursor2 = async_empty_collection.find()
        assert cursor2.alive is True
        for _ in range(8):
            await cursor2.__anext__()
        assert cursor2.alive is True
        cursor3 = cursor2.clone()
        assert len(await _alist(cursor2)) == 2
        assert len(await _alist(cursor3)) == 10
        assert cursor2.alive is False

        # close
        cursor4 = async_empty_collection.find()
        for _ in range(8):
            await cursor4.__anext__()
        cursor4.close()
        assert cursor4.alive is False
        with pytest.raises(StopAsyncIteration):
            await cursor4.__anext__()

        # distinct
        cursor5 = async_empty_collection.find()
        dist5 = await cursor5.distinct("ternary")
        assert (len(await _alist(cursor5))) == 10
        assert set(dist5) == {0, 1, 2}
        cursor6 = async_empty_collection.find()
        for _ in range(9):
            await cursor6.__anext__()
        dist6 = await cursor6.distinct("ternary")
        assert (len(await _alist(cursor6))) == 1
        assert set(dist6) == {0, 1, 2}

        # distinct from collections
        assert set(await async_empty_collection.distinct("ternary")) == {0, 1, 2}
        assert set(await async_empty_collection.distinct("nonfield")) == set()

        # indexing by integer
        cursor7 = async_empty_collection.find(sort={"seq": 1})
        assert cursor7[5]["seq"] == 5

        # indexing by wrong type
        with pytest.raises(TypeError):
            cursor7.rewind()
            cursor7["wrong"]

    @pytest.mark.describe("test of collection insert_many, async")
    async def test_collection_insert_many_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        acol = async_empty_collection

        ins_result1 = await acol.insert_many([{"_id": "a"}, {"_id": "b"}])
        assert set(ins_result1.inserted_ids) == {"a", "b"}
        assert {doc["_id"] async for doc in acol.find()} == {"a", "b"}

        with pytest.raises(APIRequestError):
            await acol.insert_many([{"_id": "a"}, {"_id": "c"}])
        assert {doc["_id"] async for doc in acol.find()} == {"a", "b"}

        with pytest.raises(APIRequestError):
            await acol.insert_many([{"_id": "c"}, {"_id": "a"}, {"_id": "d"}])
        assert {doc["_id"] async for doc in acol.find()} == {"a", "b", "c"}

        with pytest.raises(ValueError):
            await acol.insert_many(
                [{"_id": "c"}, {"_id": "d"}, {"_id": "e"}],
                ordered=False,
            )
        assert {doc["_id"] async for doc in acol.find()} == {"a", "b", "c", "d", "e"}

    @pytest.mark.describe("test of collection find_one, async")
    async def test_collection_find_one_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        col = async_empty_collection
        await col.insert_many(
            [
                {"_id": "?", "seq": 0, "kind": "punctuation"},
                {"_id": "a", "seq": 1, "kind": "letter"},
                {"_id": "b", "seq": 2, "kind": "letter"},
            ]
        )

        fo1 = await col.find_one({"kind": "frog"})
        assert fo1 is None

        Nski = 1
        Nlim = 10
        Nsor = {"seq": 1}
        Nfil = {"kind": "letter"}

        # case 0000 of find-pattern matrix
        doc0000 = await col.find_one(skip=None, limit=None, sort=None, filter=None)
        assert doc0000 is not None
        assert doc0000["seq"] in {0, 1, 2}

        # case 0001
        doc0001 = await col.find_one(skip=None, limit=None, sort=None, filter=Nfil)
        assert doc0001 is not None
        assert doc0001["seq"] in {1, 2}

        # case 0010
        doc0010 = await col.find_one(skip=None, limit=None, sort=Nsor, filter=None)
        assert doc0010 is not None
        assert doc0010["seq"] == 0

        # case 0011
        doc0011 = await col.find_one(skip=None, limit=None, sort=Nsor, filter=Nfil)
        assert doc0011 is not None
        assert doc0011["seq"] == 1

        # case 0100
        doc0100 = await col.find_one(skip=None, limit=Nlim, sort=None, filter=None)
        assert doc0100 is not None
        assert doc0100["seq"] in {0, 1, 2}

        # case 0101
        doc0101 = await col.find_one(skip=None, limit=Nlim, sort=None, filter=Nfil)
        assert doc0101 is not None
        assert doc0101["seq"] in {1, 2}

        # case 0110
        doc0110 = await col.find_one(skip=None, limit=Nlim, sort=Nsor, filter=None)
        assert doc0110 is not None
        assert doc0110["seq"] == 0

        # case 0111
        doc0111 = await col.find_one(skip=None, limit=Nlim, sort=Nsor, filter=Nfil)
        assert doc0111 is not None
        assert doc0111["seq"] == 1

        # case 1000
        # col.find_one(skip=Nski, limit=None, sort=None, filter=None) ...

        # case 1001
        # col.find_one(skip=Nski, limit=None, sort=None, filter=Nfil) ...

        # case 1010
        doc1010 = await col.find_one(skip=Nski, limit=None, sort=Nsor, filter=None)
        assert doc1010 is not None
        assert doc1010["seq"] == 1

        # case 1011
        doc1011 = await col.find_one(skip=Nski, limit=None, sort=Nsor, filter=Nfil)
        assert doc1011 is not None
        assert doc1011["seq"] == 2

        # case 1100
        # col.find_one(skip=Nski, limit=Nlim, sort=None, filter=None) ...

        # case 1101
        # col.find_one(skip=Nski, limit=Nlim, sort=None, filter=Nfil) ...

        # case 1110
        doc1110 = await col.find_one(skip=Nski, limit=Nlim, sort=Nsor, filter=None)
        assert doc1110 is not None
        assert doc1110["seq"] == 1

        # case 1111
        doc1111 = await col.find_one(skip=Nski, limit=Nlim, sort=Nsor, filter=Nfil)
        assert doc1111 is not None
        assert doc1111["seq"] == 2

        # projection
        doc_full = await col.find_one(skip=Nski, limit=Nlim, sort=Nsor, filter=Nfil)
        doc_proj = await col.find_one(
            skip=Nski, limit=Nlim, sort=Nsor, filter=Nfil, projection={"kind": True}
        )
        assert doc_proj == {"_id": "b", "kind": "letter"}
        assert doc_full == {"_id": "b", "seq": 2, "kind": "letter"}

    @pytest.mark.describe("test of find_one_and_replace, async")
    async def test_collection_find_one_and_replace_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        acol = async_empty_collection

        resp0000 = await acol.find_one_and_replace({"f": 0}, {"r": 1})
        assert resp0000 is None
        assert await acol.count_documents({}) == 0

        resp0001 = await acol.find_one_and_replace({"f": 0}, {"r": 1}, sort={"x": 1})
        assert resp0001 is None
        assert await acol.count_documents({}) == 0

        resp0010 = await acol.find_one_and_replace({"f": 0}, {"r": 1}, upsert=True)
        assert resp0010 is None
        assert await acol.count_documents({}) == 1
        await acol.delete_many({})

        resp0011 = await acol.find_one_and_replace(
            {"f": 0}, {"r": 1}, upsert=True, sort={"x": 1}
        )
        assert resp0011 is None
        assert await acol.count_documents({}) == 1
        await acol.delete_many({})

        await acol.insert_one({"f": 0})
        resp0100 = await acol.find_one_and_replace({"f": 0}, {"r": 1})
        assert resp0100 is not None
        assert resp0100["f"] == 0
        assert await acol.count_documents({}) == 1
        await acol.delete_many({})

        await acol.insert_one({"f": 0})
        resp0101 = await acol.find_one_and_replace({"f": 0}, {"r": 1}, sort={"x": 1})
        assert resp0101 is not None
        assert resp0101["f"] == 0
        assert await acol.count_documents({}) == 1
        await acol.delete_many({})

        await acol.insert_one({"f": 0})
        resp0110 = await acol.find_one_and_replace({"f": 0}, {"r": 1}, upsert=True)
        assert resp0110 is not None
        assert resp0110["f"] == 0
        assert await acol.count_documents({}) == 1
        await acol.delete_many({})

        await acol.insert_one({"f": 0})
        resp0111 = await acol.find_one_and_replace(
            {"f": 0}, {"r": 1}, upsert=True, sort={"x": 1}
        )
        assert resp0111 is not None
        assert resp0111["f"] == 0
        assert await acol.count_documents({}) == 1
        await acol.delete_many({})

        resp1000 = await acol.find_one_and_replace(
            {"f": 0}, {"r": 1}, return_document=ReturnDocument.AFTER
        )
        assert resp1000 is None
        assert await acol.count_documents({}) == 0

        resp1001 = await acol.find_one_and_replace(
            {"f": 0}, {"r": 1}, sort={"x": 1}, return_document=ReturnDocument.AFTER
        )
        assert resp1001 is None
        assert await acol.count_documents({}) == 0

        resp1010 = await acol.find_one_and_replace(
            {"f": 0}, {"r": 1}, upsert=True, return_document=ReturnDocument.AFTER
        )
        assert resp1010 is not None
        assert resp1010["r"] == 1
        assert await acol.count_documents({}) == 1
        await acol.delete_many({})

        resp1011 = await acol.find_one_and_replace(
            {"f": 0},
            {"r": 1},
            upsert=True,
            sort={"x": 1},
            return_document=ReturnDocument.AFTER,
        )
        assert resp1011 is not None
        assert resp1011["r"] == 1
        assert await acol.count_documents({}) == 1
        await acol.delete_many({})

        await acol.insert_one({"f": 0})
        resp1100 = await acol.find_one_and_replace(
            {"f": 0}, {"r": 1}, return_document=ReturnDocument.AFTER
        )
        assert resp1100 is not None
        assert resp1100["r"] == 1
        assert await acol.count_documents({}) == 1
        await acol.delete_many({})

        await acol.insert_one({"f": 0})
        resp1101 = await acol.find_one_and_replace(
            {"f": 0}, {"r": 1}, sort={"x": 1}, return_document=ReturnDocument.AFTER
        )
        assert resp1101 is not None
        assert resp1101["r"] == 1
        assert await acol.count_documents({}) == 1
        await acol.delete_many({})

        await acol.insert_one({"f": 0})
        resp1110 = await acol.find_one_and_replace(
            {"f": 0}, {"r": 1}, upsert=True, return_document=ReturnDocument.AFTER
        )
        assert resp1110 is not None
        assert resp1110["r"] == 1
        assert await acol.count_documents({}) == 1
        await acol.delete_many({})

        await acol.insert_one({"f": 0})
        resp1111 = await acol.find_one_and_replace(
            {"f": 0},
            {"r": 1},
            upsert=True,
            sort={"x": 1},
            return_document=ReturnDocument.AFTER,
        )
        assert resp1111 is not None
        assert resp1111["r"] == 1
        assert await acol.count_documents({}) == 1
        await acol.delete_many({})

        # projection
        await acol.insert_one({"f": 100, "name": "apple", "mode": "old"})
        resp_pr1 = await acol.find_one_and_replace(
            {"f": 100},
            {"f": 100, "name": "carrot", "mode": "replaced"},
            projection=["mode"],
            return_document=ReturnDocument.AFTER,
        )
        assert resp_pr1 is not None
        assert set(resp_pr1.keys()) == {"_id", "mode"}
        resp_pr2 = await acol.find_one_and_replace(
            {"f": 100},
            {"f": 100, "name": "turnip", "mode": "re-replaced"},
            projection={"name": False, "f": False, "_id": False},
            return_document=ReturnDocument.BEFORE,
        )
        assert resp_pr2 is not None
        assert set(resp_pr2.keys()) == {"mode"}
        await acol.delete_many({})

    @pytest.mark.describe("test of replace_one, async")
    async def test_collection_replace_one_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        acol = async_empty_collection

        result1 = await acol.replace_one(filter={"a": 1}, replacement={"b": 2})
        assert result1.update_info["n"] == 0
        assert result1.update_info["updatedExisting"] is False
        assert result1.update_info["nModified"] == 0
        assert "upserted" not in result1.update_info

        result2 = await acol.replace_one(
            filter={"a": 1}, replacement={"b": 2}, upsert=True
        )
        assert result2.update_info["n"] == 1
        assert result2.update_info["updatedExisting"] is False
        assert result2.update_info["nModified"] == 0
        assert "upserted" in result2.update_info

        result3 = await acol.replace_one(filter={"b": 2}, replacement={"c": 3})
        assert result3.update_info["n"] == 1
        assert result3.update_info["updatedExisting"] is True
        assert result3.update_info["nModified"] == 1
        assert "upserted" not in result3.update_info

        result4 = await acol.replace_one(
            filter={"c": 3}, replacement={"d": 4}, upsert=True
        )
        assert result4.update_info["n"] == 1
        assert result4.update_info["updatedExisting"] is True
        assert result4.update_info["nModified"] == 1
        assert "upserted" not in result4.update_info

    @pytest.mark.describe("test of update_one, async")
    async def test_collection_update_one_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        acol = async_empty_collection

        result1 = await acol.update_one(filter={"a": 1}, update={"$set": {"b": 2}})
        assert result1.update_info["n"] == 0
        assert result1.update_info["updatedExisting"] is False
        assert result1.update_info["nModified"] == 0
        assert "upserted" not in result1.update_info

        result2 = await acol.update_one(
            filter={"a": 1}, update={"$set": {"b": 2}}, upsert=True
        )
        assert result2.update_info["n"] == 1
        assert result2.update_info["updatedExisting"] is False
        assert result2.update_info["nModified"] == 0
        assert "upserted" in result2.update_info

        result3 = await acol.update_one(filter={"b": 2}, update={"$set": {"c": 3}})
        assert result3.update_info["n"] == 1
        assert result3.update_info["updatedExisting"] is True
        assert result3.update_info["nModified"] == 1
        assert "upserted" not in result3.update_info

        result4 = await acol.update_one(
            filter={"c": 3}, update={"$set": {"d": 4}}, upsert=True
        )
        assert result4.update_info["n"] == 1
        assert result4.update_info["updatedExisting"] is True
        assert result4.update_info["nModified"] == 1
        assert "upserted" not in result4.update_info

    @pytest.mark.describe("test of update_many, async")
    async def test_collection_update_many_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        acol = async_empty_collection
        await acol.insert_many([{"a": 1, "seq": i} for i in range(4)])
        await acol.insert_many([{"a": 2, "seq": i} for i in range(2)])

        resp1 = await acol.update_many({"a": 1}, {"$set": {"n": 1}})
        assert resp1.update_info["n"] == 4
        assert resp1.update_info["updatedExisting"] is True
        assert resp1.update_info["nModified"] == 4
        assert "upserted" not in resp1.update_info

        resp2 = await acol.update_many({"a": 1}, {"$set": {"n": 2}}, upsert=True)
        assert resp2.update_info["n"] == 4
        assert resp2.update_info["updatedExisting"] is True
        assert resp2.update_info["nModified"] == 4
        assert "upserted" not in resp2.update_info

        resp3 = await acol.update_many({"a": 3}, {"$set": {"n": 3}})
        assert resp3.update_info["n"] == 0
        assert resp3.update_info["updatedExisting"] is False
        assert resp3.update_info["nModified"] == 0
        assert "upserted" not in resp3.update_info

        resp4 = await acol.update_many({"a": 3}, {"$set": {"n": 4}}, upsert=True)
        assert resp4.update_info["n"] == 1
        assert resp4.update_info["updatedExisting"] is False
        assert resp4.update_info["nModified"] == 0
        assert "upserted" in resp4.update_info

    @pytest.mark.describe("test of collection find_one_and_delete, async")
    async def test_collection_find_one_and_delete_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        await async_empty_collection.insert_one({"doc": 1, "group": "A"})
        await async_empty_collection.insert_one({"doc": 2, "group": "B"})
        await async_empty_collection.insert_one({"doc": 3, "group": "A"})
        assert await async_empty_collection.count_documents(filter={}) == 3

        fo_result1 = await async_empty_collection.find_one_and_delete({"group": "A"})
        assert fo_result1 is not None
        assert set(fo_result1.keys()) == {"_id", "doc", "group"}
        assert await async_empty_collection.count_documents(filter={}) == 2

        fo_result2 = await async_empty_collection.find_one_and_delete(
            {"group": "B"}, projection=["doc"]
        )
        assert fo_result2 is not None
        assert set(fo_result2.keys()) == {"_id", "doc"}
        assert await async_empty_collection.count_documents(filter={}) == 1

        fo_result3 = await async_empty_collection.find_one_and_delete(
            {"group": "A"}, projection={"_id": False, "group": False}
        )
        assert fo_result3 is not None
        assert set(fo_result3.keys()) == {"_id", "doc"}
        assert await async_empty_collection.count_documents(filter={}) == 0

        fo_result4 = await async_empty_collection.find_one_and_delete({}, sort={"f": 1})
        assert fo_result4 is None

    @pytest.mark.describe("test of find_one_and_update, async")
    async def test_collection_find_one_and_update_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        acol = async_empty_collection

        resp0000 = await acol.find_one_and_update({"f": 0}, {"$set": {"n": 1}})
        assert resp0000 is None
        assert await acol.count_documents({}) == 0

        resp0001 = await acol.find_one_and_update(
            {"f": 0}, {"$set": {"n": 1}}, sort={"x": 1}
        )
        assert resp0001 is None
        assert await acol.count_documents({}) == 0

        resp0010 = await acol.find_one_and_update(
            {"f": 0}, {"$set": {"n": 1}}, upsert=True
        )
        assert resp0010 is None
        assert await acol.count_documents({}) == 1
        await acol.delete_many({})

        resp0011 = await acol.find_one_and_update(
            {"f": 0}, {"$set": {"n": 1}}, upsert=True, sort={"x": 1}
        )
        assert resp0011 is None
        assert await acol.count_documents({}) == 1
        await acol.delete_many({})

        await acol.insert_one({"f": 0})
        resp0100 = await acol.find_one_and_update({"f": 0}, {"$set": {"n": 1}})
        assert resp0100 is not None
        assert resp0100["f"] == 0
        assert "n" not in resp0100
        assert await acol.count_documents({}) == 1
        await acol.delete_many({})

        await acol.insert_one({"f": 0})
        resp0101 = await acol.find_one_and_update(
            {"f": 0}, {"$set": {"n": 1}}, sort={"x": 1}
        )
        assert resp0101 is not None
        assert resp0101["f"] == 0
        assert "n" not in resp0101
        assert await acol.count_documents({}) == 1
        await acol.delete_many({})

        await acol.insert_one({"f": 0})
        resp0110 = await acol.find_one_and_update(
            {"f": 0}, {"$set": {"n": 1}}, upsert=True
        )
        assert resp0110 is not None
        assert resp0110["f"] == 0
        assert "n" not in resp0110
        assert await acol.count_documents({}) == 1
        await acol.delete_many({})

        await acol.insert_one({"f": 0})
        resp0111 = await acol.find_one_and_update(
            {"f": 0}, {"$set": {"n": 1}}, upsert=True, sort={"x": 1}
        )
        assert resp0111 is not None
        assert resp0111["f"] == 0
        assert "n" not in resp0111
        assert await acol.count_documents({}) == 1
        await acol.delete_many({})

        resp1000 = await acol.find_one_and_update(
            {"f": 0}, {"$set": {"n": 1}}, return_document=ReturnDocument.AFTER
        )
        assert resp1000 is None
        assert await acol.count_documents({}) == 0

        resp1001 = await acol.find_one_and_update(
            {"f": 0},
            {"$set": {"n": 1}},
            sort={"x": 1},
            return_document=ReturnDocument.AFTER,
        )
        assert resp1001 is None
        assert await acol.count_documents({}) == 0

        resp1010 = await acol.find_one_and_update(
            {"f": 0},
            {"$set": {"n": 1}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        assert resp1010 is not None
        assert resp1010["n"] == 1
        assert await acol.count_documents({}) == 1
        await acol.delete_many({})

        resp1011 = await acol.find_one_and_update(
            {"f": 0},
            {"$set": {"n": 1}},
            upsert=True,
            sort={"x": 1},
            return_document=ReturnDocument.AFTER,
        )
        assert resp1011 is not None
        assert resp1011["n"] == 1
        assert await acol.count_documents({}) == 1
        await acol.delete_many({})

        await acol.insert_one({"f": 0})
        resp1100 = await acol.find_one_and_update(
            {"f": 0}, {"$set": {"n": 1}}, return_document=ReturnDocument.AFTER
        )
        assert resp1100 is not None
        assert resp1100["n"] == 1
        assert await acol.count_documents({}) == 1
        await acol.delete_many({})

        await acol.insert_one({"f": 0})
        resp1101 = await acol.find_one_and_update(
            {"f": 0},
            {"$set": {"n": 1}},
            sort={"x": 1},
            return_document=ReturnDocument.AFTER,
        )
        assert resp1101 is not None
        assert resp1101["n"] == 1
        assert await acol.count_documents({}) == 1
        await acol.delete_many({})

        await acol.insert_one({"f": 0})
        resp1110 = await acol.find_one_and_update(
            {"f": 0},
            {"$set": {"n": 1}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        assert resp1110 is not None
        assert resp1110["n"] == 1
        assert await acol.count_documents({}) == 1
        await acol.delete_many({})

        await acol.insert_one({"f": 0})
        resp1111 = await acol.find_one_and_update(
            {"f": 0},
            {"$set": {"n": 1}},
            upsert=True,
            sort={"x": 1},
            return_document=ReturnDocument.AFTER,
        )
        assert resp1111 is not None
        assert resp1111["n"] == 1
        assert await acol.count_documents({}) == 1
        await acol.delete_many({})

        # projection
        await acol.insert_one({"f": 100, "name": "apple", "mode": "old"})
        resp_pr1 = await acol.find_one_and_update(
            {"f": 100},
            {"$unset": {"mode": ""}},
            projection=["mode", "f"],
            return_document=ReturnDocument.AFTER,
        )
        assert resp_pr1 is not None
        assert set(resp_pr1.keys()) == {"_id", "f"}
        resp_pr2 = await acol.find_one_and_update(
            {"f": 100},
            {"$set": {"mode": "re-replaced"}},
            projection={"name": False, "_id": False},
            return_document=ReturnDocument.BEFORE,
        )
        assert resp_pr2 is not None
        assert set(resp_pr2.keys()) == {"f"}
        await acol.delete_many({})

    @pytest.mark.describe("test of ordered bulk_write, async")
    async def test_collection_ordered_bulk_write_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        acol = async_empty_collection

        bw_ops = [
            AsyncInsertOne({"seq": 0}),
            AsyncInsertMany([{"seq": 1}, {"seq": 2}, {"seq": 3}]),
            AsyncUpdateOne({"seq": 0}, {"$set": {"edited": 1}}),
            AsyncUpdateMany({"seq": {"$gt": 0}}, {"$set": {"positive": True}}),
            AsyncReplaceOne({"edited": 1}, {"seq": 0, "edited": 2}),
            AsyncDeleteOne({"seq": 1}),
            AsyncDeleteMany({"seq": {"$gt": 1}}),
            AsyncReplaceOne(
                {"no": "matches"}, {"_id": "seq4", "from_upsert": True}, upsert=True
            ),
        ]

        bw_result = await acol.bulk_write(bw_ops)

        assert bw_result.deleted_count == 3
        assert bw_result.inserted_count == 5
        assert bw_result.matched_count == 5
        assert bw_result.modified_count == 5
        assert bw_result.upserted_count == 1
        assert set(bw_result.upserted_ids.keys()) == {7}

        found_docs = sorted(
            [doc async for doc in acol.find({})],
            key=lambda doc: doc.get("seq", 10),
        )
        assert len(found_docs) == 2
        assert found_docs[0]["seq"] == 0
        assert found_docs[0]["edited"] == 2
        assert "_id" in found_docs[0]
        assert len(found_docs[0]) == 3
        assert found_docs[1] == {"_id": "seq4", "from_upsert": True}

    @pytest.mark.describe("test of unordered bulk_write, async")
    async def test_collection_unordered_bulk_write_async(
        self,
        async_empty_collection: AsyncCollection,
    ) -> None:
        acol = async_empty_collection

        bw_u_ops = [
            AsyncInsertOne({"a": 1}),
            AsyncUpdateOne({"b": 1}, {"$set": {"newfield": True}}, upsert=True),
            AsyncDeleteMany({"x": 100}),
        ]

        bw_u_result = await acol.bulk_write(bw_u_ops, ordered=False)

        assert bw_u_result.deleted_count == 0
        assert bw_u_result.inserted_count == 2
        assert bw_u_result.matched_count == 0
        assert bw_u_result.modified_count == 0
        assert bw_u_result.upserted_count == 1
        assert set(bw_u_result.upserted_ids.keys()) == {1}

        found_docs = [doc async for doc in acol.find({})]
        no_id_found_docs = [
            {k: v for k, v in doc.items() if k != "_id"} for doc in found_docs
        ]
        assert len(no_id_found_docs) == 2
        assert {"a": 1} in no_id_found_docs
        assert {"b": 1, "newfield": True} in no_id_found_docs
