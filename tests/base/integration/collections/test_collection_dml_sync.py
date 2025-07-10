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

from datetime import date, datetime, timezone
from typing import Any

import pytest

from astrapy.constants import DefaultDocumentType, ReturnDocument, SortMode
from astrapy.cursors import CursorState
from astrapy.data_types import (
    DataAPIDate,
    DataAPIMap,
    DataAPITimestamp,
    DataAPIVector,
)
from astrapy.exceptions import (
    CollectionInsertManyException,
    DataAPIResponseException,
    TooManyDocumentsToCountException,
)
from astrapy.ids import UUID, ObjectId
from astrapy.results import CollectionDeleteResult, CollectionInsertOneResult
from astrapy.utils.api_options import APIOptions, SerdesOptions

from ..conftest import DefaultCollection


class TestCollectionDMLSync:
    @pytest.mark.describe("test of collection count_documents, sync")
    def test_collection_count_documents_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        assert sync_empty_collection.count_documents(filter={}, upper_bound=100) == 0
        sync_empty_collection.insert_one({"doc": 1, "group": "A"})
        sync_empty_collection.insert_one({"doc": 2, "group": "B"})
        sync_empty_collection.insert_one({"doc": 3, "group": "A"})
        assert sync_empty_collection.count_documents(filter={}, upper_bound=100) == 3
        assert (
            sync_empty_collection.count_documents(
                filter={"group": "A"}, upper_bound=100
            )
            == 2
        )

    @pytest.mark.describe("test of collection estimated_document_count, sync")
    def test_collection_estimated_document_count_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        sync_empty_collection.insert_one({"doc": 1, "group": "A"})
        sync_empty_collection.insert_one({"doc": 2, "group": "B"})
        sync_empty_collection.insert_one({"doc": 3, "group": "A"})
        count = sync_empty_collection.estimated_document_count()
        # it's _estimated_, no precise expectation with such short sizes/times
        assert isinstance(count, int)
        assert count >= 0

    @pytest.mark.describe("test of overflowing collection count_documents, sync")
    def test_collection_overflowing_count_documents_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        sync_empty_collection.insert_many([{"a": i} for i in range(900)])
        assert sync_empty_collection.count_documents(filter={}, upper_bound=950) == 900
        assert sync_empty_collection.count_documents(filter={}, upper_bound=2000) == 900
        with pytest.raises(TooManyDocumentsToCountException):
            sync_empty_collection.count_documents(filter={}, upper_bound=100) == 900
        sync_empty_collection.insert_many([{"b": i} for i in range(200)])
        with pytest.raises(TooManyDocumentsToCountException):
            assert sync_empty_collection.count_documents(filter={}, upper_bound=100)
        with pytest.raises(TooManyDocumentsToCountException):
            assert sync_empty_collection.count_documents(filter={}, upper_bound=2000)

    @pytest.mark.describe("test of collection insert_one, sync")
    def test_collection_insert_one_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        io_result1 = sync_empty_collection.insert_one({"doc": 1, "group": "A"})
        assert isinstance(io_result1, CollectionInsertOneResult)
        io_result2 = sync_empty_collection.insert_one(
            {"_id": "xxx", "doc": 2, "group": "B"}
        )
        assert io_result2.inserted_id == "xxx"
        assert (
            sync_empty_collection.count_documents(
                filter={"group": "A"}, upper_bound=100
            )
            == 1
        )

    @pytest.mark.describe("test of collection insert_one with vector, sync")
    def test_collection_insert_one_vector_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        sync_empty_collection.insert_one({"tag": "v1", "$vector": [-1, -2]})
        retrieved1 = sync_empty_collection.find_one({"tag": "v1"}, projection={"*": 1})
        assert retrieved1 is not None
        assert retrieved1["$vector"] == DataAPIVector([-1, -2])

        sync_empty_collection.insert_one({"tag": "v2", "$vector": [-3, -4]})
        retrieved2 = sync_empty_collection.find_one({"tag": "v2"}, projection={"*": 1})
        assert retrieved2 is not None
        assert retrieved2["$vector"] == DataAPIVector([-3, -4])

        sync_empty_collection.insert_one({"tag": "v_null", "$vector": None})
        retrieved3 = sync_empty_collection.find_one(
            {"tag": "v_null"}, projection={"*": 1}
        )
        assert retrieved3 is not None
        assert retrieved3["$vector"] is None

        unroll_options = APIOptions(
            serdes_options=SerdesOptions(unroll_iterables_to_lists=True),
        )
        unroll_coll = sync_empty_collection.with_options(api_options=unroll_options)
        unroll_coll.insert_one({"tag": "v_null_u", "$vector": None})
        retrieved4 = unroll_coll.find_one({"tag": "v_null_u"}, projection={"*": 1})
        assert retrieved4 is not None
        assert retrieved4["$vector"] is None

    @pytest.mark.describe("test of collection vector insertion options, sync")
    def test_collection_vector_insertion_options_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        collection_Yb_Yc = sync_empty_collection.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    binary_encode_vectors=True,
                    unroll_iterables_to_lists=True,
                ),
            ),
        )
        collection_Nb_Yc = sync_empty_collection.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    binary_encode_vectors=False,
                    unroll_iterables_to_lists=True,
                ),
            ),
        )
        collection_Yb_Nc = sync_empty_collection.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    binary_encode_vectors=True,
                    unroll_iterables_to_lists=False,
                ),
            ),
        )
        collection_Nb_Nc = sync_empty_collection.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    binary_encode_vectors=False,
                    unroll_iterables_to_lists=False,
                ),
            ),
        )

        # writes w.r.t. options
        collection_Yb_Yc.insert_one(
            {"_id": "Yb_Yc_()", "$vector": (i for i in [0.1, 0.2])}
        )
        collection_Yb_Yc.insert_one({"_id": "Yb_Yc_[]", "$vector": [0.3, 0.4]})
        collection_Yb_Yc.insert_one(
            {"_id": "Yb_Yc_DV", "$vector": DataAPIVector([0.5, 0.6])}
        )

        collection_Nb_Yc.insert_one(
            {"_id": "Nb_Yc_()", "$vector": (i for i in [0.1, 0.2])}
        )
        collection_Nb_Yc.insert_one({"_id": "Nb_Yc_[]", "$vector": [0.3, 0.4]})
        collection_Nb_Yc.insert_one(
            {"_id": "Nb_Yc_DV", "$vector": DataAPIVector([0.5, 0.6])}
        )

        with pytest.raises(TypeError):
            collection_Yb_Nc.insert_one(
                {"_id": "Yb_Nc_()", "$vector": (i for i in [0.1, 0.2])}
            )
        collection_Yb_Nc.insert_one({"_id": "Yb_Nc_[]", "$vector": [0.3, 0.4]})
        collection_Yb_Nc.insert_one(
            {"_id": "Yb_Nc_DV", "$vector": DataAPIVector([0.5, 0.6])}
        )

        with pytest.raises(TypeError):
            collection_Nb_Nc.insert_one(
                {"_id": "Nb_Nc_()", "$vector": (i for i in [0.1, 0.2])}
            )
        collection_Nb_Nc.insert_one({"_id": "Nb_Nc_[]", "$vector": [0.3, 0.4]})
        collection_Nb_Nc.insert_one(
            {"_id": "Nb_Nc_DV", "$vector": DataAPIVector([0.5, 0.6])}
        )

        # check how the documents are stored
        expect_binaries = {
            "Yb_Yc_()": True,
            "Yb_Yc_[]": True,
            "Yb_Yc_DV": True,
            "Nb_Yc_()": False,
            "Nb_Yc_[]": False,
            "Nb_Yc_DV": False,
            #
            "Yb_Nc_[]": True,
            "Yb_Nc_DV": True,
            #
            "Nb_Nc_[]": False,
            "Nb_Nc_DV": False,
        }

        raw_find_response = sync_empty_collection.command(
            body={"find": {"projection": {"_id": True, "$vector": True}}},
        )
        raw_docs = raw_find_response["data"]["documents"]
        for raw_doc in raw_docs:
            expect_binary = expect_binaries[raw_doc["_id"]]
            has_binary = "$binary" in raw_doc["$vector"]
            assert expect_binary is has_binary

        collection_Ycc = sync_empty_collection.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    custom_datatypes_in_reading=True,
                ),
            ),
        )
        collection_Ncc = sync_empty_collection.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    custom_datatypes_in_reading=False,
                ),
            ),
        )
        docs_Ycc = list(collection_Ycc.find(projection={"$vector": True}))
        docs_Ncc = list(collection_Ncc.find(projection={"$vector": True}))
        assert all(isinstance(doc["$vector"], DataAPIVector) for doc in docs_Ycc)
        assert all(not isinstance(doc["$vector"], DataAPIVector) for doc in docs_Ncc)

    @pytest.mark.describe("test of collection delete_one, sync")
    def test_collection_delete_one_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        sync_empty_collection.insert_one({"doc": 1, "group": "A"})
        sync_empty_collection.insert_one({"doc": 2, "group": "B"})
        sync_empty_collection.insert_one({"doc": 3, "group": "A"})
        assert sync_empty_collection.count_documents(filter={}, upper_bound=100) == 3
        do_result1 = sync_empty_collection.delete_one({"group": "A"})
        assert isinstance(do_result1, CollectionDeleteResult)
        assert do_result1.deleted_count == 1
        assert sync_empty_collection.count_documents(filter={}, upper_bound=100) == 2

        # test of sort
        sync_empty_collection.insert_many([{"ts": 1, "seq": i} for i in [2, 0, 1]])
        sync_empty_collection.delete_one({"ts": 1}, sort={"seq": 1})
        assert set(sync_empty_collection.distinct("seq", filter={"ts": 1})) == {1, 2}

    @pytest.mark.describe("test of collection delete_many, sync")
    def test_collection_delete_many_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        sync_empty_collection.insert_one({"doc": 1, "group": "A"})
        sync_empty_collection.insert_one({"doc": 2, "group": "B"})
        sync_empty_collection.insert_one({"doc": 3, "group": "A"})
        assert sync_empty_collection.count_documents(filter={}, upper_bound=100) == 3
        do_result1 = sync_empty_collection.delete_many({"group": "A"})
        assert isinstance(do_result1, CollectionDeleteResult)
        assert do_result1.deleted_count == 2
        assert sync_empty_collection.count_documents(filter={}, upper_bound=100) == 1

        sync_empty_collection.delete_many({})
        sync_empty_collection.insert_many([{"a": 1} for _ in range(50)])
        do_result2 = sync_empty_collection.delete_many({"a": 1})
        assert do_result2.deleted_count == 50
        assert sync_empty_collection.count_documents({}, upper_bound=100) == 0

    @pytest.mark.describe("test of collection chunk-requiring delete_many, sync")
    def test_collection_chunked_delete_many_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        sync_empty_collection.insert_many([{"doc": i, "group": "A"} for i in range(50)])
        sync_empty_collection.insert_many([{"doc": i, "group": "B"} for i in range(10)])
        assert sync_empty_collection.count_documents(filter={}, upper_bound=100) == 60
        do_result1 = sync_empty_collection.delete_many({"group": "A"})
        assert isinstance(do_result1, CollectionDeleteResult)
        assert do_result1.deleted_count == 50
        assert sync_empty_collection.count_documents(filter={}, upper_bound=100) == 10

    @pytest.mark.describe("test of collection find, sync")
    def test_collection_find_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        sync_empty_collection.insert_many([{"seq": i} for i in range(30)])
        Nski = 1
        Nlim = 28
        Nsor = {"seq": SortMode.DESCENDING}
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
        with pytest.raises(DataAPIResponseException):
            len(
                list(
                    sync_empty_collection.find(
                        skip=Nski, limit=None, sort=None, filter=None
                    )
                )
            )

        # case 1001
        with pytest.raises(DataAPIResponseException):
            len(
                list(
                    sync_empty_collection.find(
                        skip=Nski, limit=None, sort=None, filter=Nfil
                    )
                )
            )

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
        with pytest.raises(DataAPIResponseException):
            len(
                list(
                    sync_empty_collection.find(
                        skip=Nski, limit=Nlim, sort=None, filter=None
                    )
                )
            )

        # case 1101
        with pytest.raises(DataAPIResponseException):
            len(
                list(
                    sync_empty_collection.find(
                        skip=Nski, limit=Nlim, sort=None, filter=Nfil
                    )
                )
            )

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
        sync_empty_collection: DefaultCollection,
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
        assert cursor0.consumed == 0
        document0 = cursor0.__next__()
        assert cursor0.consumed == 1
        assert "ternary" not in document0
        cursor0b = sync_empty_collection.find(projection={"ternary": True})
        document0b = cursor0b.__next__()
        assert "ternary" in document0b

        assert cursor0b.data_source == sync_empty_collection

        # rewinding, slicing and retrieved
        cursor1 = sync_empty_collection.find(sort={"seq": 1})
        cursor1.__next__()
        cursor1.__next__()
        items1 = list(cursor1)[:2]  # noqa: F841
        cursor1.rewind()
        assert list(cursor1) == list(sync_empty_collection.find(sort={"seq": 1}))
        cursor1.rewind()

        # address, cursor_id, collection
        assert isinstance(cursor1.cursor_id, int)
        assert cursor1.data_source == sync_empty_collection

        # clone
        cursor2 = sync_empty_collection.find()
        assert cursor2.state != CursorState.CLOSED
        for _ in range(8):
            cursor2.__next__()
        assert cursor2.state != CursorState.CLOSED  # type: ignore[comparison-overlap]
        cursor3 = cursor2.clone()
        assert len(list(cursor2)) == 2
        assert len(list(cursor3)) == 10
        assert cursor2.state == CursorState.CLOSED  # type: ignore[comparison-overlap]

        # close
        cursor4 = sync_empty_collection.find()
        for _ in range(8):
            cursor4.__next__()
        cursor4.close()
        assert cursor4.state == CursorState.CLOSED
        with pytest.raises(StopIteration):
            cursor4.__next__()

        # distinct from collections
        assert set(sync_empty_collection.distinct("ternary")) == {0, 1, 2}
        assert set(sync_empty_collection.distinct("nonfield")) == set()

    @pytest.mark.describe(
        "test of collective and mappings on cursors from collection.find, sync"
    )
    def test_collection_cursors_collective_maps_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        """
        Functionalities of cursors from find: map, tolist, foreach.
        """
        sync_empty_collection.insert_many(
            [{"seq": i, "ternary": (i % 3)} for i in range(75)]
        )

        cur0 = sync_empty_collection.find(filter={"seq": {"$gte": 10}})
        assert len(list(cur0)) == 65

        # map
        def mapper(doc: DefaultDocumentType) -> tuple[int, str]:
            return (doc["seq"], f"T{doc['ternary']}")

        cur1_map = sync_empty_collection.find(filter={"seq": {"$gte": 10}}).map(mapper)
        items1_map = list(cur1_map)
        assert len(items1_map) == 65
        assert (20, "T2") in items1_map
        assert all(isinstance(tup[0], int) for tup in items1_map)
        assert all(isinstance(tup[1], str) for tup in items1_map)

        # map composition

        def mapper_2(tup: tuple[int, str]) -> str:
            return f"{tup[1]}/{tup[0]}"

        cur2_maps = (
            sync_empty_collection.find(filter={"seq": {"$gte": 10}})
            .map(mapper)
            .map(mapper_2)
        )
        items2_maps = list(cur2_maps)
        assert len(items2_maps) == 65
        assert "T2/20" in items2_maps
        assert all(isinstance(itm, str) for itm in items2_maps)

        # clone (rewinding)
        cloned_2 = cur2_maps.clone()
        from_cl = next(cloned_2)
        assert isinstance(from_cl, str)
        assert "/" in from_cl

        # for each
        accum: list[int] = []

        def ingest(doc: DefaultDocumentType, acc: list[int] = accum) -> None:
            acc += [doc["ternary"]]

        (sync_empty_collection.find(filter={"seq": {"$gte": 10}}).for_each(ingest))
        assert len(accum) == 65
        assert set(accum) == {0, 1, 2}

        # to list
        cur3_tl = sync_empty_collection.find(
            filter={"seq": {"$gte": 10}},
            projection={"_id": False},
        )
        items3_tl = cur3_tl.to_list()
        assert len(items3_tl) == 65
        assert {"seq": 50, "ternary": 2} in items3_tl

    @pytest.mark.describe("test of distinct with non-hashable items, sync")
    def test_collection_distinct_nonhashable_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        col = sync_empty_collection
        the_datimestamp = DataAPITimestamp.from_string("2000-01-01T12:00:00.000Z")
        documents: list[dict[str, Any]] = [
            {},
            {"f": 1},
            {"f": "a"},
            {"f": {"subf": 99}},
            {"f": {"subf": 99, "another": {"subsubf": [True, False]}}},
            {"f": [10, 11]},
            {"f": [11, 10]},
            {"f": [10]},
            {"f": the_datimestamp},
            {"f": None},
        ]
        col.insert_many(documents * 2)

        d_items = col.distinct("f")
        assert len(d_items) == 8
        for doc in documents:
            if "f" in doc:
                if isinstance(doc["f"], list):
                    for item in doc["f"]:
                        assert item in d_items
                else:
                    assert doc["f"] in d_items

        d_items_noncustom = col.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    custom_datatypes_in_reading=False,
                )
            ),
        ).distinct("f")
        assert len(d_items_noncustom) == 8
        for doc in documents:
            if "f" in doc:
                if isinstance(doc["f"], list):
                    for item in doc["f"]:
                        assert item in d_items_noncustom
                elif isinstance(doc["f"], DataAPITimestamp):
                    assert doc["f"].to_datetime(tz=timezone.utc) in d_items_noncustom
                else:
                    assert doc["f"] in d_items_noncustom

    @pytest.mark.describe("test of distinct with key as list, sync")
    def test_collection_distinct_key_as_list_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        col = sync_empty_collection
        the_datimestamp = DataAPITimestamp.from_string("2000-01-01T12:00:00.000Z")
        documents: list[dict[str, Any]] = [
            {},
            {"f": 1},
            {"f": "a"},
            {"f": {"subf": 99}},
            {"f": {"subf": 99, "another": {"subsubf": [True, False]}}},
            {"f": [10, 11]},
            {"f": [11, 10]},
            {"f": [10]},
            {"f": the_datimestamp},
            {"f": None},
        ]
        col.insert_many(documents * 2)

        assert col.distinct("f") == col.distinct(["f"])

        col.insert_one({"x": [{"y": "Y", "0": "ZERO"}]})

        col.delete_many({})
        col.insert_one({"x": [{"y": "Y", "0": "ZERO"}]})

        assert col.distinct(["x", "y"]) == ["Y"]
        # these expose int-vs-str subtleties (listindex/mapkey issues)
        assert col.distinct(["x", "0"]) == ["ZERO"]
        assert col.distinct(["x", 0]) == [{"y": "Y", "0": "ZERO"}]
        assert col.distinct(["x", "0", "y"]) == []
        assert col.distinct(["x", 0, "y"]) == ["Y"]
        assert col.distinct(["x", "0", "0"]) == []
        assert col.distinct(["x", "0", 0]) == []
        assert col.distinct(["x", 0, "0"]) == ["ZERO"]
        assert col.distinct(["x", 0, 0]) == []

        with pytest.raises(ValueError):
            sync_empty_collection.distinct(["root", "1", "", "subf"])
        with pytest.raises(ValueError):
            sync_empty_collection.distinct(["root", "", "1", "subf"])
        with pytest.raises(ValueError):
            sync_empty_collection.distinct(["root", "", "subf", "subsubf"])
        with pytest.raises(ValueError):
            sync_empty_collection.distinct(["root", "subf", "", "subsubf"])

    @pytest.mark.describe("test of usage of projection in distinct, sync")
    def test_collection_projections_distinct_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        col = sync_empty_collection
        col.insert_one({"x": [{"y": "Y", "0": "ZERO"}]})

        assert col.distinct("x.y") == ["Y"]
        # the one below shows that if index-in-list, then browse-whole-list is off
        assert col.distinct("x.0") == [{"y": "Y", "0": "ZERO"}]
        assert col.distinct("x.0.y") == ["Y"]
        assert col.distinct("x.0.0") == ["ZERO"]

    @pytest.mark.describe("test of unacceptable paths for distinct, sync")
    def test_collection_wrong_paths_distinct_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        with pytest.raises(ValueError):
            sync_empty_collection.distinct("root.1..subf")
        with pytest.raises(ValueError):
            sync_empty_collection.distinct("root..1.subf")
        with pytest.raises(ValueError):
            sync_empty_collection.distinct("root..subf.subsubf")
        with pytest.raises(ValueError):
            sync_empty_collection.distinct("root.subf..subsubf")

    @pytest.mark.describe("test of collection find, find_one with vectors, sync")
    def test_collection_find_find_one_vectors_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        q_vector = [3, 3]
        sync_empty_collection.insert_many(
            [
                {"tag": "A", "$vector": [4, 5]},
                {"tag": "B", "$vector": [3, 4]},
                {"tag": "C", "$vector": [3, 2]},
                {"tag": "D", "$vector": [4, 1]},
                {"tag": "E", "$vector": [2, 5]},
            ]
        )

        hits = list(
            sync_empty_collection.find(
                {},
                sort={"$vector": q_vector},
                projection=["tag"],
                limit=3,
            )
        )
        assert [hit["tag"] for hit in hits] == ["A", "B", "C"]

        with pytest.raises(DataAPIResponseException):
            list(
                sync_empty_collection.find(
                    {},
                    projection=["tag"],
                    sort={"$vector": q_vector, "tag": SortMode.DESCENDING},
                    limit=3,
                )
            )

        top_doc = sync_empty_collection.find_one({}, sort={"$vector": [1, 0]})
        assert top_doc is not None
        assert top_doc["tag"] == "D"

        fdoc_no_s = sync_empty_collection.find(
            {}, sort={"$vector": [1, 1]}, include_similarity=False
        ).__next__()
        fdoc_wi_s = sync_empty_collection.find(
            {}, sort={"$vector": [1, 1]}, include_similarity=True
        ).__next__()
        assert fdoc_no_s is not None
        assert fdoc_wi_s is not None
        assert "$similarity" not in fdoc_no_s
        assert "$similarity" in fdoc_wi_s
        assert fdoc_wi_s["$similarity"] > 0.0

        f1doc_no_s = sync_empty_collection.find_one(
            {}, sort={"$vector": [1, 1]}, include_similarity=False
        )
        f1doc_wi_s = sync_empty_collection.find_one(
            {}, sort={"$vector": [1, 1]}, include_similarity=True
        )
        assert f1doc_no_s is not None
        assert f1doc_wi_s is not None
        assert "$similarity" not in f1doc_no_s
        assert "$similarity" in f1doc_wi_s
        assert f1doc_wi_s["$similarity"] > 0.0

    @pytest.mark.describe("test of include_sort_vector in collection find, sync")
    def test_collection_include_sort_vector_find_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        q_vector = DataAPIVector([10, 9])
        # with empty collection
        for include_sv in [False, True]:
            for sort_cl_label in ["reg", "vec"]:
                sort_cl_e: dict[str, Any] = (
                    {} if sort_cl_label == "reg" else {"$vector": q_vector}
                )
                vec_expected = include_sv and sort_cl_label == "vec"
                # pristine iterator
                this_ite_1 = sync_empty_collection.find(
                    {}, sort=sort_cl_e, include_sort_vector=include_sv
                )
                if vec_expected:
                    assert this_ite_1.get_sort_vector() == q_vector
                else:
                    assert this_ite_1.get_sort_vector() is None
                # after exhaustion with empty
                all_items_1 = list(this_ite_1)
                assert all_items_1 == []
                if vec_expected:
                    assert this_ite_1.get_sort_vector() == q_vector
                else:
                    assert this_ite_1.get_sort_vector() is None
                # directly exhausted before calling get_sort_vector
                this_ite_2 = sync_empty_collection.find(
                    {}, sort=sort_cl_e, include_sort_vector=include_sv
                )
                all_items_2 = list(this_ite_2)
                assert all_items_2 == []
                if vec_expected:
                    assert this_ite_2.get_sort_vector() == q_vector
                else:
                    assert this_ite_2.get_sort_vector() is None
        sync_empty_collection.insert_many(
            [{"seq": i, "$vector": [i, i + 1]} for i in range(10)]
        )
        # with non-empty collection
        for include_sv in [False, True]:
            for sort_cl_label in ["reg", "vec"]:
                sort_cl_f: dict[str, Any] = (
                    {} if sort_cl_label == "reg" else {"$vector": q_vector}
                )
                vec_expected = include_sv and sort_cl_label == "vec"
                # pristine iterator
                this_ite_1 = sync_empty_collection.find(
                    {}, sort=sort_cl_f, include_sort_vector=include_sv
                )
                if vec_expected:
                    assert this_ite_1.get_sort_vector() == q_vector
                else:
                    assert this_ite_1.get_sort_vector() is None
                # after consuming one item
                first_seqs = [
                    doc["seq"] for doc in [this_ite_1.__next__(), this_ite_1.__next__()]
                ]
                if vec_expected:
                    assert this_ite_1.get_sort_vector() == q_vector
                else:
                    assert this_ite_1.get_sort_vector() is None
                # after exhaustion with the rest
                last_seqs = [doc["seq"] for doc in list(this_ite_1)]
                assert len(set(last_seqs + first_seqs)) == 10
                assert len(last_seqs + first_seqs) == 10
                if vec_expected:
                    assert this_ite_1.get_sort_vector() == q_vector
                else:
                    assert this_ite_1.get_sort_vector() is None
                # directly exhausted before calling get_sort_vector
                this_ite_2 = sync_empty_collection.find(
                    {}, sort=sort_cl_f, include_sort_vector=include_sv
                )
                list(this_ite_2)
                if vec_expected:
                    assert this_ite_2.get_sort_vector() == q_vector
                else:
                    assert this_ite_2.get_sort_vector() is None

    @pytest.mark.describe("test of include_sort_vector with serdes options, sync")
    def test_collection_include_sort_vector_serdes_options_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        col_v0_d0 = sync_empty_collection.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    custom_datatypes_in_reading=False,
                    use_decimals_in_collections=False,
                ),
            ),
        )
        col_v0_d1 = sync_empty_collection.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    custom_datatypes_in_reading=False,
                    use_decimals_in_collections=True,
                ),
            ),
        )
        col_v1_d0 = sync_empty_collection.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    custom_datatypes_in_reading=True,
                    use_decimals_in_collections=False,
                ),
            ),
        )
        col_v1_d1 = sync_empty_collection.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    custom_datatypes_in_reading=True,
                    use_decimals_in_collections=True,
                ),
            ),
        )
        cur0_v0_d0 = col_v0_d0.find(sort={"$vector": [1, 2]})
        cur0_v0_d1 = col_v0_d1.find(sort={"$vector": [1, 2]})
        cur0_v1_d0 = col_v1_d0.find(sort={"$vector": [1, 2]})
        cur0_v1_d1 = col_v1_d1.find(sort={"$vector": [1, 2]})

        assert cur0_v0_d0.get_sort_vector() is None
        assert cur0_v0_d1.get_sort_vector() is None
        assert cur0_v1_d0.get_sort_vector() is None
        assert cur0_v1_d1.get_sort_vector() is None

        cur1_v0_d0 = col_v0_d0.find(sort={"$vector": [1, 2]}, include_sort_vector=True)
        cur1_v0_d1 = col_v0_d1.find(sort={"$vector": [1, 2]}, include_sort_vector=True)
        cur1_v1_d0 = col_v1_d0.find(sort={"$vector": [1, 2]}, include_sort_vector=True)
        cur1_v1_d1 = col_v1_d1.find(sort={"$vector": [1, 2]}, include_sort_vector=True)

        assert cur1_v0_d0.get_sort_vector() == [1, 2]
        assert cur1_v0_d1.get_sort_vector() == [1, 2]
        assert cur1_v1_d0.get_sort_vector() == DataAPIVector([1, 2])
        assert cur1_v1_d1.get_sort_vector() == DataAPIVector([1, 2])

    @pytest.mark.describe("test of projections in collection find with vectors, sync")
    def test_collection_find_projections_vectors_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        sync_empty_collection.insert_one(
            {
                "$vector": [1, 2],
                "otherfield": "OF",
                "anotherfield": "AF",
                "text": "T",
            }
        )
        req_projections = [
            None,
            {},
            {"text": True},
            {"$vector": True},
            {"text": True, "$vector": True},
        ]
        exp_fieldsets = [
            {"$vector", "_id", "otherfield", "anotherfield", "text"},
            {"$vector", "_id", "otherfield", "anotherfield", "text"},
            {"_id", "text"},
            {
                "$vector",
                "_id",
                "otherfield",
                "anotherfield",
                "text",
            },
            {"$vector", "_id", "text"},
        ]
        for include_similarity in [True, False]:
            for req_projection, exp_fields0 in zip(req_projections, exp_fieldsets):
                vdocs = list(
                    sync_empty_collection.find(
                        sort={"$vector": [11, 21]},
                        limit=1,
                        projection=req_projection,
                        include_similarity=include_similarity,
                    )
                )
                if include_similarity:
                    exp_fields = exp_fields0 | {"$similarity"}
                else:
                    exp_fields = exp_fields0
                # this test should not concern whether $vector is found or not
                # (abiding by the '$vector may or may not be returned' tenet)
                vkeys_novec = set(vdocs[0].keys()) - {"$vector"}
                expkeys_novec = exp_fields - {"$vector"}
                assert vkeys_novec == expkeys_novec
                # but in some cases $vector must be there:
                if "$vector" in (req_projection or set()):
                    assert "$vector" in vdocs[0]

    @pytest.mark.describe("test of collection insert_many with empty list, sync")
    def test_collection_insert_many_empty_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        col = sync_empty_collection
        col.insert_many([], ordered=True)
        col.insert_many([], ordered=False, concurrency=1)
        col.insert_many([], ordered=False, concurrency=5)

    @pytest.mark.describe("test of collection insert_many, sync")
    def test_collection_insert_many_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        col = sync_empty_collection

        ins_result1 = col.insert_many([{"_id": "a"}, {"_id": "b"}], ordered=True)
        assert set(ins_result1.inserted_ids) == {"a", "b"}
        assert {doc["_id"] for doc in col.find()} == {"a", "b"}

        with pytest.raises(CollectionInsertManyException):
            col.insert_many([{"_id": "a"}, {"_id": "c"}], ordered=True)
        assert {doc["_id"] for doc in col.find()} == {"a", "b"}

        with pytest.raises(CollectionInsertManyException):
            col.insert_many([{"_id": "c"}, {"_id": "a"}, {"_id": "d"}], ordered=True)
        assert {doc["_id"] for doc in col.find()} == {"a", "b", "c"}

        with pytest.raises(CollectionInsertManyException):
            col.insert_many(
                [{"_id": "c"}, {"_id": "d"}, {"_id": "e"}],
                ordered=False,
            )
        assert {doc["_id"] for doc in col.find()} == {"a", "b", "c", "d", "e"}

    @pytest.mark.describe("test of collection insert_many with vectors, sync")
    def test_collection_insert_many_vectors_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        col = sync_empty_collection
        col.insert_many(
            [
                {"t": 0, "$vector": [0, 1]},
                {"t": 1, "$vector": [1, 0]},
                {"t": 4, "$vector": [0, 3]},
                {"t": 5, "$vector": [3, 0]},
            ]
        )

        assert all(
            len(doc["$vector"]) == 2 for doc in col.find({}, projection={"*": 1})
        )

    @pytest.mark.describe("test of collection insert_many, failures, sync")
    def test_collection_insert_many_failures_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        # The main goal here is to keep the switch to returnDocumentResponses in check.
        N = 110
        ins_res0 = sync_empty_collection.insert_many(
            [{"_id": i} for i in range(N)],
            concurrency=1,
        )
        assert set(ins_res0.inserted_ids) == set(range(N))

        ins_res1 = sync_empty_collection.insert_many(
            [{"_id": N + i} for i in range(N)],
            concurrency=20,
        )
        assert set(ins_res1.inserted_ids) == {N + i for i in range(N)}

        # unordered insertion [good, bad]
        err2: CollectionInsertManyException | None = None
        try:
            sync_empty_collection.insert_many([{"_id": 2 * N}, {"_id": 0}])
        except CollectionInsertManyException as e:
            err2 = e
        assert err2 is not None
        assert len(err2.exceptions) == 1
        assert isinstance(err2.exceptions[0], DataAPIResponseException)
        assert len(err2.exceptions[0].error_descriptors) == 1
        assert err2.inserted_ids == [2 * N]

        # ordered insertion [good, bad, good_skipped]
        err3: CollectionInsertManyException | None = None
        try:
            sync_empty_collection.insert_many(
                [{"_id": 2 * N + 1}, {"_id": 0}, {"_id": 2 * N + 2}],
                ordered=True,
            )
        except CollectionInsertManyException as e:
            err3 = e
        assert err3 is not None
        assert len(err3.exceptions) == 1
        assert isinstance(err3.exceptions[0], DataAPIResponseException)
        assert len(err3.exceptions[0].error_descriptors) == 1
        assert err3.inserted_ids == [2 * N + 1]

    @pytest.mark.describe("test of collection find_one, sync")
    def test_collection_find_one_sync(
        self,
        sync_empty_collection: DefaultCollection,
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

        Nsor = {"seq": 1}
        Nfil = {"kind": "letter"}

        # case 00 of find-pattern matrix
        doc00 = col.find_one(sort=None, filter=None)
        assert doc00 is not None
        assert doc00["seq"] in {0, 1, 2}

        # case 01
        doc01 = col.find_one(sort=None, filter=Nfil)
        assert doc01 is not None
        assert doc01["seq"] in {1, 2}

        # case 10
        doc10 = col.find_one(sort=Nsor, filter=None)
        assert doc10 is not None
        assert doc10["seq"] == 0

        # case 11
        doc11 = col.find_one(sort=Nsor, filter=Nfil)
        assert doc11 is not None
        assert doc11["seq"] == 1

        # projection
        doc_full = col.find_one(sort=Nsor, filter=Nfil)
        doc_proj = col.find_one(sort=Nsor, filter=Nfil, projection={"kind": True})
        assert doc_proj == {"_id": "a", "kind": "letter"}
        assert doc_full == {"_id": "a", "seq": 1, "kind": "letter"}

    @pytest.mark.describe(
        "test of custom_datatypes_in_reading APIOptions setting, sync"
    )
    def test_custom_datatypes_in_reading_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        col_standard_dtypes = sync_empty_collection.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    custom_datatypes_in_reading=False,
                ),
            ),
        )
        col_custom_dtypes = sync_empty_collection.with_options(
            api_options=APIOptions(
                serdes_options=SerdesOptions(
                    custom_datatypes_in_reading=True,
                ),
            ),
        )
        the_dtime = datetime(2000, 1, 1, 10, 11, 12, 123000, tzinfo=timezone.utc)
        the_date = date(1998, 12, 31)

        # read path
        sync_empty_collection.insert_one(
            {
                "_id": "t0",
                "the_dtime": the_dtime,
                "the_date": the_date,
            },
        )
        doc_standard_dtypes = col_standard_dtypes.find_one({"_id": "t0"})
        doc_custom_dtypes = col_custom_dtypes.find_one({"_id": "t0"})

        assert doc_standard_dtypes is not None
        assert doc_custom_dtypes is not None
        dtime_standard_dtypes = doc_standard_dtypes["the_dtime"]
        dtime_custom_dtypes = doc_custom_dtypes["the_dtime"]
        date_standard_dtypes = doc_standard_dtypes["the_date"]
        date_custom_dtypes = doc_custom_dtypes["the_date"]

        assert isinstance(dtime_standard_dtypes, datetime)
        assert isinstance(dtime_custom_dtypes, DataAPITimestamp)
        assert (
            DataAPITimestamp.from_datetime(dtime_standard_dtypes) == dtime_custom_dtypes
        )
        assert dtime_custom_dtypes == DataAPITimestamp.from_datetime(
            dtime_standard_dtypes
        )
        assert isinstance(date_standard_dtypes, datetime)
        assert isinstance(date_custom_dtypes, DataAPITimestamp)
        assert (
            DataAPITimestamp.from_datetime(date_standard_dtypes) == date_custom_dtypes
        )
        assert date_custom_dtypes == DataAPITimestamp.from_datetime(
            date_standard_dtypes
        )

        # write path
        sync_empty_collection.delete_one({"_id": "t0"})
        col_standard_dtypes.insert_one(
            {"_id": "default_dtype_dt", "the_dtime": the_dtime}
        )
        # in write path, custom classes still accepted
        col_standard_dtypes.insert_one(
            {
                "_id": "default_dtype_ats",
                "the_dtime": DataAPITimestamp.from_datetime(the_dtime),
            }
        )
        col_custom_dtypes.insert_one({"_id": "custom_dtype_dt", "the_dtime": the_dtime})
        col_custom_dtypes.insert_one(
            {
                "_id": "custom_dtype_ats",
                "the_dtime": DataAPITimestamp.from_datetime(the_dtime),
            }
        )

        all_dates = [doc["the_dtime"] for doc in sync_empty_collection.find({})]
        assert len(all_dates) == 4
        assert len(set(all_dates)) == 1

    @pytest.mark.describe("test of find_one_and_replace, sync")
    def test_collection_find_one_and_replace_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        col = sync_empty_collection

        resp0000 = col.find_one_and_replace({"f": 0}, {"r": 1})
        assert resp0000 is None
        assert col.count_documents({}, upper_bound=100) == 0

        resp0001 = col.find_one_and_replace({"f": 0}, {"r": 1}, sort={"x": 1})
        assert resp0001 is None
        assert col.count_documents({}, upper_bound=100) == 0

        resp0010 = col.find_one_and_replace({"f": 0}, {"r": 1}, upsert=True)
        assert resp0010 is None
        assert col.count_documents({}, upper_bound=100) == 1
        col.delete_many({})

        resp0011 = col.find_one_and_replace(
            {"f": 0}, {"r": 1}, upsert=True, sort={"x": 1}
        )
        assert resp0011 is None
        assert col.count_documents({}, upper_bound=100) == 1
        col.delete_many({})

        col.insert_one({"f": 0})
        resp0100 = col.find_one_and_replace({"f": 0}, {"r": 1})
        assert resp0100 is not None
        assert resp0100["f"] == 0
        assert col.count_documents({}, upper_bound=100) == 1
        col.delete_many({})

        col.insert_one({"f": 0})
        resp0101 = col.find_one_and_replace({"f": 0}, {"r": 1}, sort={"x": 1})
        assert resp0101 is not None
        assert resp0101["f"] == 0
        assert col.count_documents({}, upper_bound=100) == 1
        col.delete_many({})

        col.insert_one({"f": 0})
        resp0110 = col.find_one_and_replace({"f": 0}, {"r": 1}, upsert=True)
        assert resp0110 is not None
        assert resp0110["f"] == 0
        assert col.count_documents({}, upper_bound=100) == 1
        col.delete_many({})

        col.insert_one({"f": 0})
        resp0111 = col.find_one_and_replace(
            {"f": 0}, {"r": 1}, upsert=True, sort={"x": 1}
        )
        assert resp0111 is not None
        assert resp0111["f"] == 0
        assert col.count_documents({}, upper_bound=100) == 1
        col.delete_many({})

        resp1000 = col.find_one_and_replace(
            {"f": 0}, {"r": 1}, return_document=ReturnDocument.AFTER
        )
        assert resp1000 is None
        assert col.count_documents({}, upper_bound=100) == 0

        resp1001 = col.find_one_and_replace(
            {"f": 0}, {"r": 1}, sort={"x": 1}, return_document=ReturnDocument.AFTER
        )
        assert resp1001 is None
        assert col.count_documents({}, upper_bound=100) == 0

        resp1010 = col.find_one_and_replace(
            {"f": 0}, {"r": 1}, upsert=True, return_document=ReturnDocument.AFTER
        )
        assert resp1010 is not None
        assert resp1010["r"] == 1
        assert col.count_documents({}, upper_bound=100) == 1
        col.delete_many({})

        resp1011 = col.find_one_and_replace(
            {"f": 0},
            {"r": 1},
            upsert=True,
            sort={"x": 1},
            return_document=ReturnDocument.AFTER,
        )
        assert resp1011 is not None
        assert resp1011["r"] == 1
        assert col.count_documents({}, upper_bound=100) == 1
        col.delete_many({})

        col.insert_one({"f": 0})
        resp1100 = col.find_one_and_replace(
            {"f": 0}, {"r": 1}, return_document=ReturnDocument.AFTER
        )
        assert resp1100 is not None
        assert resp1100["r"] == 1
        assert col.count_documents({}, upper_bound=100) == 1
        col.delete_many({})

        col.insert_one({"f": 0})
        resp1101 = col.find_one_and_replace(
            {"f": 0}, {"r": 1}, sort={"x": 1}, return_document=ReturnDocument.AFTER
        )
        assert resp1101 is not None
        assert resp1101["r"] == 1
        assert col.count_documents({}, upper_bound=100) == 1
        col.delete_many({})

        col.insert_one({"f": 0})
        resp1110 = col.find_one_and_replace(
            {"f": 0}, {"r": 1}, upsert=True, return_document=ReturnDocument.AFTER
        )
        assert resp1110 is not None
        assert resp1110["r"] == 1
        assert col.count_documents({}, upper_bound=100) == 1
        col.delete_many({})

        col.insert_one({"f": 0})
        resp1111 = col.find_one_and_replace(
            {"f": 0},
            {"r": 1},
            upsert=True,
            sort={"x": 1},
            return_document=ReturnDocument.AFTER,
        )
        assert resp1111 is not None
        assert resp1111["r"] == 1
        assert col.count_documents({}, upper_bound=100) == 1
        col.delete_many({})

        # projection
        col.insert_one({"f": 100, "name": "apple", "mode": "old"})
        resp_pr1 = col.find_one_and_replace(
            {"f": 100},
            {"f": 100, "name": "carrot", "mode": "replaced"},
            projection=["mode"],
            return_document=ReturnDocument.AFTER,
        )
        assert resp_pr1 is not None
        assert set(resp_pr1.keys()) == {"_id", "mode"}
        resp_pr2 = col.find_one_and_replace(
            {"f": 100},
            {"f": 100, "name": "turnip", "mode": "re-replaced"},
            projection={"name": False, "f": False, "_id": False},
            return_document=ReturnDocument.BEFORE,
        )
        assert resp_pr2 is not None
        assert set(resp_pr2.keys()) == {"mode"}
        col.delete_many({})

    @pytest.mark.describe("test of replace_one, sync")
    def test_collection_replace_one_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        col = sync_empty_collection

        result1 = col.replace_one(filter={"a": 1}, replacement={"b": 2})
        assert result1.update_info["n"] == 0
        assert result1.update_info["updatedExisting"] is False
        assert result1.update_info["nModified"] == 0
        assert "upserted" not in result1.update_info

        result2 = col.replace_one(filter={"a": 1}, replacement={"b": 2}, upsert=True)
        assert result2.update_info["n"] == 1
        assert result2.update_info["updatedExisting"] is False
        assert result2.update_info["nModified"] == 0
        assert "upserted" in result2.update_info

        result3 = col.replace_one(filter={"b": 2}, replacement={"c": 3})
        assert result3.update_info["n"] == 1
        assert result3.update_info["updatedExisting"] is True
        assert result3.update_info["nModified"] == 1
        assert "upserted" not in result3.update_info

        result4 = col.replace_one(filter={"c": 3}, replacement={"d": 4}, upsert=True)
        assert result4.update_info["n"] == 1
        assert result4.update_info["updatedExisting"] is True
        assert result4.update_info["nModified"] == 1
        assert "upserted" not in result4.update_info

        # test of sort
        sync_empty_collection.insert_many([{"ts": 1, "seq": i} for i in [2, 0, 1]])
        sync_empty_collection.replace_one(
            {"ts": 1}, {"ts": 1, "R": True}, sort={"seq": 1}
        )
        assert set(sync_empty_collection.distinct("seq", filter={"ts": 1})) == {1, 2}

    @pytest.mark.describe("test of replace_one with vectors, sync")
    def test_collection_replace_one_vector_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        col = sync_empty_collection
        col.insert_many(
            [
                {"tag": "h", "$vector": [10, 5]},
                {"tag": "v", "$vector": [2, 20]},
            ]
        )
        result = col.replace_one({}, {"new_doc": True}, sort={"$vector": [0, 1]})
        assert result.update_info["updatedExisting"]

        assert col.find_one({"tag": "h"}) is not None

    @pytest.mark.describe("test of update_one, sync")
    def test_collection_update_one_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        col = sync_empty_collection

        result1 = col.update_one(filter={"a": 1}, update={"$set": {"b": 2}})
        assert result1.update_info["n"] == 0
        assert result1.update_info["updatedExisting"] is False
        assert result1.update_info["nModified"] == 0
        assert "upserted" not in result1.update_info

        result2 = col.update_one(
            filter={"a": 1}, update={"$set": {"b": 2}}, upsert=True
        )
        assert result2.update_info["n"] == 1
        assert result2.update_info["updatedExisting"] is False
        assert result2.update_info["nModified"] == 0
        assert "upserted" in result2.update_info

        result3 = col.update_one(filter={"b": 2}, update={"$set": {"c": 3}})
        assert result3.update_info["n"] == 1
        assert result3.update_info["updatedExisting"] is True
        assert result3.update_info["nModified"] == 1
        assert "upserted" not in result3.update_info

        result4 = col.update_one(
            filter={"c": 3}, update={"$set": {"d": 4}}, upsert=True
        )
        assert result4.update_info["n"] == 1
        assert result4.update_info["updatedExisting"] is True
        assert result4.update_info["nModified"] == 1
        assert "upserted" not in result4.update_info

        # test of sort
        sync_empty_collection.insert_many([{"ts": 1, "seq": i} for i in [2, 0, 1]])
        sync_empty_collection.update_one(
            {"ts": 1}, {"$set": {"U": True}}, sort={"seq": 1}
        )
        updated = sync_empty_collection.find_one({"U": True})
        assert updated is not None
        assert updated["seq"] == 0

    @pytest.mark.describe("test of update_many, sync")
    def test_collection_update_many_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        col = sync_empty_collection
        col.insert_many([{"a": 1, "seq": i} for i in range(4)])
        col.insert_many([{"a": 2, "seq": i} for i in range(2)])

        resp1 = col.update_many({"a": 1}, {"$set": {"n": 1}})
        assert resp1.update_info["n"] == 4
        assert resp1.update_info["updatedExisting"] is True
        assert resp1.update_info["nModified"] == 4
        assert "upserted" not in resp1.update_info

        resp2 = col.update_many({"a": 1}, {"$set": {"n": 2}}, upsert=True)
        assert resp2.update_info["n"] == 4
        assert resp2.update_info["updatedExisting"] is True
        assert resp2.update_info["nModified"] == 4
        assert "upserted" not in resp2.update_info

        resp3 = col.update_many({"a": 3}, {"$set": {"n": 3}})
        assert resp3.update_info["n"] == 0
        assert resp3.update_info["updatedExisting"] is False
        assert resp3.update_info["nModified"] == 0
        assert "upserted" not in resp3.update_info

        resp4 = col.update_many({"a": 3}, {"$set": {"n": 4}}, upsert=True)
        assert resp4.update_info["n"] == 1
        assert resp4.update_info["updatedExisting"] is False
        assert resp4.update_info["nModified"] == 0
        assert "upserted" in resp4.update_info

    @pytest.mark.describe("test of update_many, sync")
    def test_collection_paginated_update_many_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        col = sync_empty_collection
        col.insert_many([{"a": 1} for _ in range(50)])
        col.insert_many([{"a": 10} for _ in range(10)])

        um_result = col.update_many({"a": 1}, {"$set": {"b": 2}})
        assert um_result.update_info["n"] == 50
        assert um_result.update_info["updatedExisting"] is True
        assert um_result.update_info["nModified"] == 50
        assert "upserted" not in um_result.update_info
        assert "upsertedd" not in um_result.update_info
        assert col.count_documents({"b": 2}, upper_bound=100) == 50
        assert col.count_documents({}, upper_bound=100) == 60

    @pytest.mark.describe("test of collection find_one_and_delete, sync")
    def test_collection_find_one_and_delete_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        sync_empty_collection.insert_one({"doc": 1, "group": "A"})
        sync_empty_collection.insert_one({"doc": 2, "group": "B"})
        sync_empty_collection.insert_one({"doc": 3, "group": "A"})
        assert sync_empty_collection.count_documents(filter={}, upper_bound=100) == 3

        fo_result1 = sync_empty_collection.find_one_and_delete({"group": "A"})
        assert fo_result1 is not None
        assert set(fo_result1.keys()) == {"_id", "doc", "group"}
        assert sync_empty_collection.count_documents(filter={}, upper_bound=100) == 2

        fo_result2 = sync_empty_collection.find_one_and_delete(
            {"group": "B"}, projection=["doc"]
        )
        assert fo_result2 is not None
        assert set(fo_result2.keys()) == {"_id", "doc"}
        assert sync_empty_collection.count_documents(filter={}, upper_bound=100) == 1

        fo_result3 = sync_empty_collection.find_one_and_delete(
            {"group": "A"}, projection={"_id": False, "group": False}
        )
        assert fo_result3 is not None
        assert set(fo_result3.keys()) == {"doc"}
        assert sync_empty_collection.count_documents(filter={}, upper_bound=100) == 0

        fo_result4 = sync_empty_collection.find_one_and_delete({}, sort={"f": 1})
        assert fo_result4 is None

    @pytest.mark.describe("test of collection find_one_and_delete with vectors, sync")
    def test_collection_find_one_and_delete_vectors_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        col = sync_empty_collection
        col.insert_many(
            [
                {"tag": "h", "$vector": [10, 5]},
                {"tag": "v", "$vector": [2, 20]},
            ]
        )
        deleted = col.find_one_and_delete({}, sort={"$vector": [0, 1]})
        assert deleted is not None
        assert deleted["tag"] == "v"

    @pytest.mark.describe("test of find_one_and_update, sync")
    def test_collection_find_one_and_update_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        col = sync_empty_collection

        resp0000 = col.find_one_and_update({"f": 0}, {"$set": {"n": 1}})
        assert resp0000 is None
        assert col.count_documents({}, upper_bound=100) == 0

        resp0001 = col.find_one_and_update({"f": 0}, {"$set": {"n": 1}}, sort={"x": 1})
        assert resp0001 is None
        assert col.count_documents({}, upper_bound=100) == 0

        resp0010 = col.find_one_and_update({"f": 0}, {"$set": {"n": 1}}, upsert=True)
        assert resp0010 is None
        assert col.count_documents({}, upper_bound=100) == 1
        col.delete_many({})

        resp0011 = col.find_one_and_update(
            {"f": 0}, {"$set": {"n": 1}}, upsert=True, sort={"x": 1}
        )
        assert resp0011 is None
        assert col.count_documents({}, upper_bound=100) == 1
        col.delete_many({})

        col.insert_one({"f": 0})
        resp0100 = col.find_one_and_update({"f": 0}, {"$set": {"n": 1}})
        assert resp0100 is not None
        assert resp0100["f"] == 0
        assert "n" not in resp0100
        assert col.count_documents({}, upper_bound=100) == 1
        col.delete_many({})

        col.insert_one({"f": 0})
        resp0101 = col.find_one_and_update({"f": 0}, {"$set": {"n": 1}}, sort={"x": 1})
        assert resp0101 is not None
        assert resp0101["f"] == 0
        assert "n" not in resp0101
        assert col.count_documents({}, upper_bound=100) == 1
        col.delete_many({})

        col.insert_one({"f": 0})
        resp0110 = col.find_one_and_update({"f": 0}, {"$set": {"n": 1}}, upsert=True)
        assert resp0110 is not None
        assert resp0110["f"] == 0
        assert "n" not in resp0110
        assert col.count_documents({}, upper_bound=100) == 1
        col.delete_many({})

        col.insert_one({"f": 0})
        resp0111 = col.find_one_and_update(
            {"f": 0}, {"$set": {"n": 1}}, upsert=True, sort={"x": 1}
        )
        assert resp0111 is not None
        assert resp0111["f"] == 0
        assert "n" not in resp0111
        assert col.count_documents({}, upper_bound=100) == 1
        col.delete_many({})

        resp1000 = col.find_one_and_update(
            {"f": 0}, {"$set": {"n": 1}}, return_document=ReturnDocument.AFTER
        )
        assert resp1000 is None
        assert col.count_documents({}, upper_bound=100) == 0

        resp1001 = col.find_one_and_update(
            {"f": 0},
            {"$set": {"n": 1}},
            sort={"x": 1},
            return_document=ReturnDocument.AFTER,
        )
        assert resp1001 is None
        assert col.count_documents({}, upper_bound=100) == 0

        resp1010 = col.find_one_and_update(
            {"f": 0},
            {"$set": {"n": 1}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        assert resp1010 is not None
        assert resp1010["n"] == 1
        assert col.count_documents({}, upper_bound=100) == 1
        col.delete_many({})

        resp1011 = col.find_one_and_update(
            {"f": 0},
            {"$set": {"n": 1}},
            upsert=True,
            sort={"x": 1},
            return_document=ReturnDocument.AFTER,
        )
        assert resp1011 is not None
        assert resp1011["n"] == 1
        assert col.count_documents({}, upper_bound=100) == 1
        col.delete_many({})

        col.insert_one({"f": 0})
        resp1100 = col.find_one_and_update(
            {"f": 0}, {"$set": {"n": 1}}, return_document=ReturnDocument.AFTER
        )
        assert resp1100 is not None
        assert resp1100["n"] == 1
        assert col.count_documents({}, upper_bound=100) == 1
        col.delete_many({})

        col.insert_one({"f": 0})
        resp1101 = col.find_one_and_update(
            {"f": 0},
            {"$set": {"n": 1}},
            sort={"x": 1},
            return_document=ReturnDocument.AFTER,
        )
        assert resp1101 is not None
        assert resp1101["n"] == 1
        assert col.count_documents({}, upper_bound=100) == 1
        col.delete_many({})

        col.insert_one({"f": 0})
        resp1110 = col.find_one_and_update(
            {"f": 0},
            {"$set": {"n": 1}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        assert resp1110 is not None
        assert resp1110["n"] == 1
        assert col.count_documents({}, upper_bound=100) == 1
        col.delete_many({})

        col.insert_one({"f": 0})
        resp1111 = col.find_one_and_update(
            {"f": 0},
            {"$set": {"n": 1}},
            upsert=True,
            sort={"x": 1},
            return_document=ReturnDocument.AFTER,
        )
        assert resp1111 is not None
        assert resp1111["n"] == 1
        assert col.count_documents({}, upper_bound=100) == 1
        col.delete_many({})

        # projection
        col.insert_one({"f": 100, "name": "apple", "mode": "old"})
        resp_pr1 = col.find_one_and_update(
            {"f": 100},
            {"$unset": {"mode": ""}},
            projection=["mode", "f"],
            return_document=ReturnDocument.AFTER,
        )
        assert resp_pr1 is not None
        assert set(resp_pr1.keys()) == {"_id", "f"}
        resp_pr2 = col.find_one_and_update(
            {"f": 100},
            {"$set": {"mode": "re-replaced"}},
            projection={"name": False, "_id": False},
            return_document=ReturnDocument.BEFORE,
        )
        assert resp_pr2 is not None
        assert set(resp_pr2.keys()) == {"f"}
        col.delete_many({})

    @pytest.mark.describe("test of the various ids in the document id field, sync")
    def test_collection_ids_as_doc_id_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        types_and_ids = {
            "uuid1": UUID("8ccd6ff8-e61b-11ee-a2fc-7df4a8c4164b"),
            "uuid3": UUID("6fa459ea-ee8a-3ca4-894e-db77e160355e"),
            "uuid4": UUID("4f16cba8-1115-43ab-aa39-3a9c29f37db5"),
            "uuid5": UUID("886313e1-3b8a-5372-9b90-0c9aee199e5d"),
            "uuid6": UUID("1eee61b9-8f2d-69ad-8ebb-5054d2a1a2c0"),
            "uuid7": UUID("018e57e5-f586-7ed6-be55-6b0de3041116"),
            "uuid8": UUID("018e57e5-fbcd-8bd4-b794-be914f2c4c85"),
            "objectid": ObjectId("65f9cfa0d7fabb3f255c25a1"),
        }

        sync_empty_collection.insert_many(
            [
                {"_id": t_id, "id_type": t_id_type}
                for t_id_type, t_id in types_and_ids.items()
            ]
        )

        for t_id_type, t_id in types_and_ids.items():
            this_doc = sync_empty_collection.find_one(
                {"_id": t_id},
                projection={"id_type": True},
            )
            assert this_doc is not None
            assert this_doc["id_type"] == t_id_type

    @pytest.mark.describe(
        "test of ids in various parameters of various DML methods, sync"
    )
    def test_collection_ids_throughout_dml_methods_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        types_and_ids = {
            "uuid1": UUID("8ccd6ff8-e61b-11ee-a2fc-7df4a8c4164b"),
            "uuid3": UUID("6fa459ea-ee8a-3ca4-894e-db77e160355e"),
            "uuid4": UUID("4f16cba8-1115-43ab-aa39-3a9c29f37db5"),
            "uuid5": UUID("886313e1-3b8a-5372-9b90-0c9aee199e5d"),
            "uuid6": UUID("1eee61b9-8f2d-69ad-8ebb-5054d2a1a2c0"),
            "uuid7": UUID("018e57e5-f586-7ed6-be55-6b0de3041116"),
            "uuid8": UUID("018e57e5-fbcd-8bd4-b794-be914f2c4c85"),
            "objectid": ObjectId("65f9cfa0d7fabb3f255c25a1"),
        }
        wide_document = {
            "all_ids": types_and_ids,
            "_id": 0,
            "name": "wide_document",
            "touched_times": 0,
        }
        sync_empty_collection.insert_one(wide_document)

        full_doc = sync_empty_collection.find_one({})
        assert full_doc == wide_document

        for t_id_type, t_id in types_and_ids.items():
            doc = sync_empty_collection.find_one(
                {f"all_ids.{t_id_type}": t_id}, projection={"name": True}
            )
            assert doc is not None
            assert doc["name"] == "wide_document"

        for upd_index, (t_id_type, t_id) in enumerate(types_and_ids.items()):
            updated_doc = sync_empty_collection.find_one_and_update(
                {f"all_ids.{t_id_type}": t_id},
                {"$inc": {"touched_times": 1}},
                return_document=ReturnDocument.AFTER,
            )
            assert updated_doc is not None
            assert updated_doc["touched_times"] == upd_index + 1

        sync_empty_collection.delete_one({"_id": 0})

        sync_empty_collection.insert_many(
            [{"_id": t_id} for t_id in types_and_ids.values()]
        )

        assert sync_empty_collection.count_documents({}, upper_bound=20) == len(
            types_and_ids
        )

        for del_index, t_id in enumerate(types_and_ids.values()):
            del_result = sync_empty_collection.delete_one({"_id": t_id})
            assert del_result.deleted_count == 1
            count = sync_empty_collection.count_documents({}, upper_bound=20)
            assert count == len(types_and_ids) - del_index - 1

    @pytest.mark.describe("test of inserting various data types in a collection, sync")
    def test_collection_datatype_insertability_sync(
        self,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        d_a_ts = DataAPITimestamp.from_string("1999-11-30T00:00:00.000Z")
        tz = timezone.utc
        at_document = {
            "_id": UUID("1f009012-ff61-646d-8c70-5d87cfbdee0b"),
            "int": 1,
            "string": "string",
            "float": 1.234,
            "dataapidate": DataAPIDate.from_string("1999-11-30"),
            "date": date(1999, 11, 30),
            "dataapitimestamp": d_a_ts,
            "datetime": d_a_ts.to_datetime(tz=tz),
            "dataapimap": DataAPIMap([("k", "v")]),
            "map": {"k": "v"},
        }
        at_expected = {
            **at_document,
            **{
                "dataapidate": d_a_ts,
                "date": d_a_ts,
                "datetime": d_a_ts,
                "dataapimap": {"k": "v"},
            },
        }

        ior = sync_empty_collection.insert_one(at_document)
        assert ior.inserted_id == at_document["_id"]

        read_document = sync_empty_collection.find_one({})
        assert read_document is not None
        date_checkfields = {"date", "dataapidate"}
        ok_read = {k: v for k, v in read_document.items() if k not in date_checkfields}
        ok_expected = {
            k: v for k, v in at_expected.items() if k not in date_checkfields
        }
        assert ok_read == ok_expected
        # irreducible approximate check on 'just-date' fields
        assert isinstance(read_document["date"], DataAPITimestamp)
        assert isinstance(read_document["dataapidate"], DataAPITimestamp)
        one_day_ms = 1000 * 86400
        assert (
            abs(read_document["date"].timestamp_ms - at_expected["date"].timestamp_ms)  # type: ignore[attr-defined]
            < one_day_ms
        )
        assert (
            abs(
                read_document["dataapidate"].timestamp_ms
                - at_expected["dataapidate"].timestamp_ms  # type: ignore[attr-defined]
            )
            < one_day_ms
        )
