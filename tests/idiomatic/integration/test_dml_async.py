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
        col = acol.to_sync()  # TODO: replace with async find once implemented

        ins_result1 = await acol.insert_many([{"_id": "a"}, {"_id": "b"}])
        assert set(ins_result1.inserted_ids) == {"a", "b"}
        assert {doc["_id"] for doc in col.find()} == {"a", "b"}

        with pytest.raises(APIRequestError):
            await acol.insert_many([{"_id": "a"}, {"_id": "c"}])
        assert {doc["_id"] for doc in col.find()} == {"a", "b"}

        with pytest.raises(APIRequestError):
            await acol.insert_many([{"_id": "c"}, {"_id": "a"}, {"_id": "d"}])
        assert {doc["_id"] for doc in col.find()} == {"a", "b", "c"}

        with pytest.raises(ValueError):
            await acol.insert_many(
                [{"_id": "c"}, {"_id": "d"}, {"_id": "e"}],
                ordered=False,
            )
        assert {doc["_id"] for doc in col.find()} == {"a", "b", "c", "d", "e"}
