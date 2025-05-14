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

import os
from typing import Any, TypedDict

import pytest

from astrapy import AsyncCollection, AsyncDatabase, Collection, Database
from astrapy.constants import VectorMetric
from astrapy.info import CollectionDefinition
from astrapy.utils.unset import _UNSET, UnsetType

from ..conftest import DefaultAsyncCollection, DefaultCollection


class TestDoc(TypedDict):
    p_bigint: int
    p_ascii: str


class TestMiniDoc(TypedDict):
    p_bigint: int


FIND_PROJECTION = {"_id": False, "p_bigint": True}
DOCUMENT = {"p_ascii": "abc", "p_bigint": 10000, "p_float": 0.123}
TYPED_DOCUMENT: TestDoc = {"p_ascii": "abc", "p_bigint": 10000}
FIND_FILTER = {"p_ascii": "abc", "p_bigint": 10000}
# find_and_rerank-related assets:
VLEX_DOCUMENT = {
    "p_ascii": "abc",
    "p_bigint": 10000,
    "p_float": 0.123,
    "$vector": [1, 2],
    "$lexical": "blo",
}
TYPED_VLEX_DOCUMENT: TestDoc = {
    "p_ascii": "abc",
    "p_bigint": 10000,
    "$vector": [1, 2],
    "$lexical": "blo",
}  # type: ignore[typeddict-unknown-key]


class TestCollectionTyping:
    @pytest.mark.describe("test of typing create_collection, sync")
    def test_create_collection_typing_sync(
        self,
        sync_database: Database,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        """Test of creating typed collections with generics (and not), sync."""

        # Untyped baseline
        c_co_untyped = sync_database.create_collection(
            sync_empty_collection.name,
            definition=(
                CollectionDefinition.builder()
                .set_vector_dimension(2)
                .set_vector_metric(VectorMetric.COSINE)
                .set_indexing("deny", ["not_indexed"])
                .build()
            ),
        )
        c_co_untyped.insert_one(DOCUMENT)
        cu_doc = c_co_untyped.find_one(FIND_FILTER)
        assert cu_doc is not None
        cu_a: str
        cu_b: int
        cu_a = cu_doc["p_ascii"]  # noqa: F841
        cu_b = cu_doc["p_bigint"]  # noqa: F841
        assert set(cu_doc.keys()) == {"_id", "p_ascii", "p_bigint", "p_float"}
        # untyped, these are all ok:
        cu_x: int
        cu_y: float
        cu_x = cu_doc["p_ascii"]  # noqa: F841
        with pytest.raises(KeyError):
            cu_y = cu_doc["c"]  # noqa: F841

        # untyped, cursors type inference on find
        c_co_untyped_cursor = c_co_untyped.find({}, projection=FIND_PROJECTION)
        cucur_doc = c_co_untyped_cursor.__next__()
        assert cucur_doc is not None
        cucur_b: int
        cucur_b = cucur_doc["p_bigint"]  # noqa: F841
        assert set(cucur_doc.keys()) == {"p_bigint"}
        # untyped, these are all ok:
        cucur_x: str
        cucur_y: float
        cucur_x = cucur_doc["p_bigint"]  # noqa: F841
        with pytest.raises(KeyError):
            cucur_y = cucur_doc["c"]  # noqa: F841

        c_co_untyped.delete_many({})

        # Typed
        c_co_typed: Collection[TestDoc] = sync_database.create_collection(
            sync_empty_collection.name,
            definition=(
                CollectionDefinition.builder()
                .set_vector_dimension(2)
                .set_vector_metric(VectorMetric.COSINE)
                .set_indexing("deny", ["not_indexed"])
                .build()
            ),
            document_type=TestDoc,
        )
        c_co_typed.insert_one(TYPED_DOCUMENT)
        ct_doc = c_co_typed.find_one(FIND_FILTER)
        assert ct_doc is not None
        ct_a: str
        ct_b: int
        ct_a = ct_doc["p_ascii"]  # noqa: F841
        ct_b = ct_doc["p_bigint"]  # noqa: F841
        assert set(ct_doc.keys()) == {"_id", "p_ascii", "p_bigint"}
        # these two SHOULD NOT typecheck (i.e. require the ignore directive)
        ct_x: int
        ct_y: float
        ct_x = ct_doc["p_ascii"]  # type: ignore[assignment]  # noqa: F841
        with pytest.raises(KeyError):
            ct_y = ct_doc["c"]  # type: ignore[typeddict-item]  # noqa: F841

        # typed, cursors type inference on find
        c_co_typed_cursor = c_co_typed.find(
            {}, projection=FIND_PROJECTION, document_type=TestMiniDoc
        )
        ctcur_doc = c_co_typed_cursor.__next__()
        assert ctcur_doc is not None
        ctcur_b: int
        ctcur_b = ctcur_doc["p_bigint"]  # noqa: F841
        assert set(ctcur_doc.keys()) == {"p_bigint"}
        # these two SHOULD NOT typecheck (i.e. require the ignore directive)
        ctcur_x: str
        ctcur_y: float
        ctcur_x = ctcur_doc["p_bigint"]  # type: ignore[assignment]  # noqa: F841
        with pytest.raises(KeyError):
            ctcur_y = ctcur_doc["c"]  # type: ignore[typeddict-item]  # noqa: F841

        c_co_typed.delete_many({})

    @pytest.mark.describe("test of typing get_collection, sync")
    def test_get_collection_typing_sync(
        self,
        sync_database: Database,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        """Test of getting typed collections with generics (and not), sync."""

        # Untyped baseline
        g_co_untyped = sync_database.get_collection(sync_empty_collection.name)
        g_co_untyped.insert_one(DOCUMENT)
        gu_doc = g_co_untyped.find_one(FIND_FILTER)
        assert gu_doc is not None
        gu_a: str
        gu_b: int
        gu_a = gu_doc["p_ascii"]  # noqa: F841
        gu_b = gu_doc["p_bigint"]  # noqa: F841
        assert set(gu_doc.keys()) == {"_id", "p_ascii", "p_bigint", "p_float"}
        # untyped, these are all ok:
        gu_x: int
        gu_y: float
        gu_x = gu_doc["p_ascii"]  # noqa: F841
        with pytest.raises(KeyError):
            gu_y = gu_doc["c"]  # noqa: F841

        g_co_untyped.delete_many({})

        # Typed
        g_co_typed: Collection[TestDoc] = sync_database.get_collection(
            sync_empty_collection.name,
            document_type=TestDoc,
        )
        g_co_typed.insert_one(TYPED_DOCUMENT)
        gt_doc = g_co_typed.find_one(FIND_FILTER)
        assert gt_doc is not None
        gt_a: str
        gt_b: int
        gt_a = gt_doc["p_ascii"]  # noqa: F841
        gt_b = gt_doc["p_bigint"]  # noqa: F841
        assert set(gt_doc.keys()) == {"_id", "p_ascii", "p_bigint"}
        # these two SHOULD NOT typecheck (i.e. require the ignore directive)
        gt_x: int
        gt_y: float
        gt_x = gt_doc["p_ascii"]  # type: ignore[assignment]  # noqa: F841
        with pytest.raises(KeyError):
            gt_y = gt_doc["c"]  # type: ignore[typeddict-item]  # noqa: F841

        g_co_typed.delete_many({})

    @pytest.mark.describe("test of typing collection cursors with map, sync")
    def test_collection_cursormap_typing_sync(
        self,
        sync_database: Database,
        sync_empty_collection: DefaultCollection,
    ) -> None:
        """Test of getting typed collections with generics (and not), sync."""

        g_co_untyped = sync_database.get_collection(sync_empty_collection.name)
        g_co_typed: Collection[TestDoc] = sync_database.get_collection(
            sync_empty_collection.name,
            document_type=TestDoc,
        )
        g_co_untyped.insert_many([DOCUMENT] * 30)

        # base cursors, read from them (un/typed)
        u_cur0 = g_co_untyped.find()
        t_cur0 = g_co_typed.find()
        gu_doc = next(u_cur0)
        gt_doc = next(t_cur0)
        assert gu_doc is not None
        assert gt_doc is not None
        gu_a: str
        gu_x: int
        gu_a = gu_doc["p_ascii"]  # noqa: F841
        gu_x = gu_doc["p_ascii"]  # noqa: F841
        gt_a: str
        gt_x: int
        gt_y: float
        gt_a = gt_doc["p_ascii"]  # noqa: F841
        gt_x = gt_doc["p_ascii"]  # type: ignore[assignment]  # noqa: F841
        with pytest.raises(KeyError):
            gt_y = gt_doc["c"]  # type: ignore[typeddict-item]  # noqa: F841

        # mapping

        def u_mapper(doc: dict[str, Any]) -> str:
            return f"{doc['p_bigint'] % 100}"

        def t_mapper(doc: TestDoc) -> str:
            return f"{doc['p_bigint'] % 100}"

        u_cur_1m = g_co_untyped.find().map(u_mapper)
        t_cur_1m = g_co_typed.find().map(t_mapper)
        gmu = next(u_cur_1m)
        gmt = next(t_cur_1m)
        assert gmu is not None
        assert gmt is not None
        gmu_a: str
        gmu_x: int
        gmu_a = gmu  # noqa: F841
        gmu_x = gmu  # type: ignore[assignment]  # noqa: F841
        gmt_a: str
        gmt_x: int
        gmt_a = gmu  # noqa: F841
        gmt_x = gmt  # type: ignore[assignment]  # noqa: F841

        # mapping composition

        def mapper_2(strint: str) -> float:
            return float(strint) / 10.0

        u_cur_2m = g_co_untyped.find().map(u_mapper).map(mapper_2)
        t_cur_2m = g_co_typed.find().map(t_mapper).map(mapper_2)
        gmu2 = next(u_cur_2m)
        gmt2 = next(t_cur_2m)
        assert gmu2 is not None
        assert gmt2 is not None
        gmu_a2: float
        gmu_x2: str
        gmu_a2 = gmu2  # noqa: F841
        gmu_x2 = gmu2  # type: ignore[assignment]  # noqa: F841
        gmt_a2: float
        gmt_x2: str
        gmt_a2 = gmu2  # noqa: F841
        gmt_x2 = gmt2  # type: ignore[assignment]  # noqa: F841

        # cloning a cursor
        u_cur_3c = g_co_untyped.find().map(u_mapper).clone()
        t_cur_3c = g_co_typed.find().map(t_mapper).clone()
        gu_doc3 = next(u_cur_3c)
        gt_doc3 = next(t_cur_3c)
        assert gu_doc3 is not None
        assert gt_doc3 is not None
        gu_a3: str
        gu_x3: int
        gu_a3 = gu_doc3  # noqa: F841
        gu_x3 = gu_doc3  # type: ignore[assignment] # noqa: F841
        gt_a3: str
        gt_x3: int
        gt_a3 = gt_doc3  # noqa: F841
        gt_x3 = gt_doc3  # type: ignore[assignment]  # noqa: F841

        # reading the buffer
        u_cur_4rb = g_co_untyped.find().map(u_mapper)
        t_cur_4rb = g_co_typed.find().map(t_mapper)
        next(u_cur_4rb)
        next(t_cur_4rb)
        u_doc_rbuf4 = u_cur_4rb.consume_buffer(3)[0]
        t_doc_rbuf4 = t_cur_4rb.consume_buffer(3)[0]
        assert u_doc_rbuf4 is not None
        assert t_doc_rbuf4 is not None
        gu_rbuf_a: str
        gu_rbuf_x: int
        gu_rbuf_a = u_doc_rbuf4["p_ascii"]  # noqa: F841
        gu_rbuf_x = u_doc_rbuf4["p_ascii"]  # noqa: F841
        gt_rbuf_a: str
        gt_rbuf_x: int
        gt_rbuf_y: float
        gt_rbuf_a = t_doc_rbuf4["p_ascii"]  # noqa: F841
        gt_rbuf_x = t_doc_rbuf4["p_ascii"]  # type: ignore[assignment]  # noqa: F841
        with pytest.raises(KeyError):
            gt_rbuf_y = t_doc_rbuf4["c"]  # type: ignore[typeddict-item]  # noqa: F841

        g_co_typed.delete_many({})

    @pytest.mark.describe("test of typing find_and_rerank, sync")
    def test_collection_find_and_rerank_typing_sync(
        self,
        sync_database: Database,
        sync_empty_collection: DefaultCollection,
        service_collection_parameters: dict[str, Any],
    ) -> None:
        """Test of typing in find_and_rerank, sync."""

        params = service_collection_parameters

        reranking_api_key: str | UnsetType
        if "ASTRAPY_FINDANDRERANK_USE_RERANKER_HEADER" in os.environ:
            assert params["reranking_api_key"] is not None
            reranking_api_key = params["reranking_api_key"]
        else:
            reranking_api_key = _UNSET

        # Untyped baseline
        f_co_untyped = sync_database.get_collection(
            sync_empty_collection.name, reranking_api_key=reranking_api_key
        )
        f_co_untyped.insert_one(VLEX_DOCUMENT)
        farr_u_hits = f_co_untyped.find_and_rerank(
            sort={"$hybrid": {"$vector": [2, 1], "$lexical": "bla"}},
            limit=1,
            rerank_on="p_ascii",
            rerank_query="bli",
        ).to_list()
        assert len(farr_u_hits) > 0
        farr_u_doc = farr_u_hits[0].document
        fu_a: str
        fu_b: int
        fu_a = farr_u_doc["p_ascii"]  # noqa: F841
        fu_b = farr_u_doc["p_bigint"]  # noqa: F841
        # untyped, these are all ok:
        fu_x: int
        fu_y: float
        fu_x = farr_u_doc["p_ascii"]  # noqa: F841
        with pytest.raises(KeyError):
            fu_y = farr_u_doc["c"]  # noqa: F841

        f_co_untyped.delete_many({})

        # Typed
        f_co_typed: Collection[TestDoc] = sync_database.get_collection(
            sync_empty_collection.name,
            document_type=TestDoc,
            reranking_api_key=reranking_api_key,
        )
        f_co_typed.insert_one(TYPED_VLEX_DOCUMENT)
        farr_t_hits = f_co_typed.find_and_rerank(
            sort={"$hybrid": {"$vector": [2, 1], "$lexical": "bla"}},
            limit=1,
            rerank_on="p_ascii",
            rerank_query="bli",
        ).to_list()
        assert len(farr_t_hits) > 0
        farr_t_doc = farr_t_hits[0].document
        ft_a: str
        ft_b: int
        ft_a = farr_t_doc["p_ascii"]  # noqa: F841
        ft_b = farr_t_doc["p_bigint"]  # noqa: F841
        # these two SHOULD NOT typecheck (i.e. require the ignore directive)
        ft_x: int
        ft_y: float
        ft_x = farr_t_doc["p_ascii"]  # type: ignore[assignment]  # noqa: F841
        with pytest.raises(KeyError):
            ft_y = farr_t_doc["c"]  # type: ignore[typeddict-item]  # noqa: F841

        farr_tm_hits = f_co_typed.find_and_rerank(
            sort={"$hybrid": {"$vector": [2, 1], "$lexical": "bla"}},
            limit=1,
            projection=FIND_PROJECTION,
            document_type=TestMiniDoc,
            rerank_on="p_ascii",
            rerank_query="bli",
        ).to_list()
        assert len(farr_tm_hits) > 0
        farr_tm_doc = farr_tm_hits[0].document
        ftm_a: str
        ftm_b: int
        # typechecks must detect that p_ascii is not there
        with pytest.raises(KeyError):
            ftm_a = farr_tm_doc["p_ascii"]  # type: ignore[typeddict-item]  # noqa: F841
        ftm_b = farr_tm_doc["p_bigint"]  # noqa: F841

        f_co_typed.delete_many({})

    @pytest.mark.describe("test of typing create_collection, async")
    async def test_create_collection_typing_async(
        self,
        async_database: AsyncDatabase,
        async_empty_collection: DefaultAsyncCollection,
    ) -> None:
        """Test of creating typed collections with generics (and not), async."""

        # Untyped baseline
        ac_co_untyped = await async_database.create_collection(
            async_empty_collection.name,
            definition=(
                CollectionDefinition.builder()
                .set_vector_dimension(2)
                .set_vector_metric(VectorMetric.COSINE)
                .set_indexing("deny", ["not_indexed"])
                .build()
            ),
        )
        await ac_co_untyped.insert_one(DOCUMENT)
        cu_doc = await ac_co_untyped.find_one(FIND_FILTER)
        assert cu_doc is not None
        cu_a: str
        cu_b: int
        cu_a = cu_doc["p_ascii"]  # noqa: F841
        cu_b = cu_doc["p_bigint"]  # noqa: F841
        assert set(cu_doc.keys()) == {"_id", "p_ascii", "p_bigint", "p_float"}
        # untyped, these are all ok:
        cu_x: int
        cu_y: float
        cu_x = cu_doc["p_ascii"]  # noqa: F841
        with pytest.raises(KeyError):
            cu_y = cu_doc["c"]  # noqa: F841

        # untyped, cursors type inference on find
        c_co_untyped_cursor = ac_co_untyped.find({}, projection=FIND_PROJECTION)
        cucur_doc = await c_co_untyped_cursor.__anext__()
        assert cucur_doc is not None
        cucur_b: int
        cucur_b = cucur_doc["p_bigint"]  # noqa: F841
        assert set(cucur_doc.keys()) == {"p_bigint"}
        # untyped, these are all ok:
        cucur_x: str
        cucur_y: float
        cucur_x = cucur_doc["p_bigint"]  # noqa: F841
        with pytest.raises(KeyError):
            cucur_y = cucur_doc["c"]  # noqa: F841

        await ac_co_untyped.delete_many({})

        # Typed
        ac_co_typed: AsyncCollection[TestDoc] = await async_database.create_collection(
            async_empty_collection.name,
            definition=(
                CollectionDefinition.builder()
                .set_vector_dimension(2)
                .set_vector_metric(VectorMetric.COSINE)
                .set_indexing("deny", ["not_indexed"])
                .build()
            ),
            document_type=TestDoc,
        )
        await ac_co_typed.insert_one(TYPED_DOCUMENT)
        ct_doc = await ac_co_typed.find_one(FIND_FILTER)
        assert ct_doc is not None
        ct_a: str
        ct_b: int
        ct_a = ct_doc["p_ascii"]  # noqa: F841
        ct_b = ct_doc["p_bigint"]  # noqa: F841
        assert set(ct_doc.keys()) == {"_id", "p_ascii", "p_bigint"}
        # these two SHOULD NOT typecheck (i.e. require the ignore directive)
        ct_x: int
        ct_y: float
        ct_x = ct_doc["p_ascii"]  # type: ignore[assignment]  # noqa: F841
        with pytest.raises(KeyError):
            ct_y = ct_doc["c"]  # type: ignore[typeddict-item]  # noqa: F841

        # typed, cursors type inference on find
        c_co_typed_cursor = ac_co_typed.find(
            {}, projection=FIND_PROJECTION, document_type=TestMiniDoc
        )
        ctcur_doc = await c_co_typed_cursor.__anext__()
        assert ctcur_doc is not None
        ctcur_b: int
        ctcur_b = ctcur_doc["p_bigint"]  # noqa: F841
        assert set(ctcur_doc.keys()) == {"p_bigint"}
        # these two SHOULD NOT typecheck (i.e. require the ignore directive)
        ctcur_x: str
        ctcur_y: float
        ctcur_x = ctcur_doc["p_bigint"]  # type: ignore[assignment]  # noqa: F841
        with pytest.raises(KeyError):
            ctcur_y = ctcur_doc["c"]  # type: ignore[typeddict-item]  # noqa: F841

        await ac_co_typed.delete_many({})

    @pytest.mark.describe("test of typing get_collection, async")
    async def test_get_collection_typing_async(
        self,
        async_database: AsyncDatabase,
        async_empty_collection: DefaultAsyncCollection,
    ) -> None:
        """Test of getting typed collections with generics (and not), async."""

        # Untyped baseline
        ag_co_untyped = async_database.get_collection(async_empty_collection.name)
        await ag_co_untyped.insert_one(DOCUMENT)
        gu_doc = await ag_co_untyped.find_one(FIND_FILTER)
        assert gu_doc is not None
        gu_a: str
        gu_b: int
        gu_a = gu_doc["p_ascii"]  # noqa: F841
        gu_b = gu_doc["p_bigint"]  # noqa: F841
        assert set(gu_doc.keys()) == {"_id", "p_ascii", "p_bigint", "p_float"}
        # untyped, these are all ok:
        gu_x: int
        gu_y: float
        gu_x = gu_doc["p_ascii"]  # noqa: F841
        with pytest.raises(KeyError):
            gu_y = gu_doc["c"]  # noqa: F841

        await ag_co_untyped.delete_many({})

        # Typed
        ag_co_typed: AsyncCollection[TestDoc] = async_database.get_collection(
            async_empty_collection.name,
            document_type=TestDoc,
        )
        await ag_co_typed.insert_one(TYPED_DOCUMENT)
        gt_doc = await ag_co_typed.find_one(FIND_FILTER)
        assert gt_doc is not None
        gt_a: str
        gt_b: int
        gt_a = gt_doc["p_ascii"]  # noqa: F841
        gt_b = gt_doc["p_bigint"]  # noqa: F841
        assert set(gt_doc.keys()) == {"_id", "p_ascii", "p_bigint"}
        # these two SHOULD NOT typecheck (i.e. require the ignore directive)
        gt_x: int
        gt_y: float
        gt_x = gt_doc["p_ascii"]  # type: ignore[assignment]  # noqa: F841
        with pytest.raises(KeyError):
            gt_y = gt_doc["c"]  # type: ignore[typeddict-item]  # noqa: F841

        await ag_co_typed.delete_many({})

    @pytest.mark.describe("test of typing collection cursors with map, async")
    async def test_collection_cursormap_typing_async(
        self,
        async_database: AsyncDatabase,
        async_empty_collection: DefaultAsyncCollection,
    ) -> None:
        """Test of getting typed collections with generics (and not), sync."""

        ag_co_untyped = async_database.get_collection(async_empty_collection.name)
        ag_co_typed: AsyncCollection[TestDoc] = async_database.get_collection(
            async_empty_collection.name,
            document_type=TestDoc,
        )
        await ag_co_untyped.insert_many([DOCUMENT] * 30)

        # base cursors, read from them (un/typed)
        u_cur0 = ag_co_untyped.find()
        t_cur0 = ag_co_typed.find()
        gu_doc = await u_cur0.__anext__()
        gt_doc = await t_cur0.__anext__()
        assert gu_doc is not None
        assert gt_doc is not None
        gu_a: str
        gu_x: int
        gu_a = gu_doc["p_ascii"]  # noqa: F841
        gu_x = gu_doc["p_ascii"]  # noqa: F841
        gt_a: str
        gt_x: int
        gt_y: float
        gt_a = gt_doc["p_ascii"]  # noqa: F841
        gt_x = gt_doc["p_ascii"]  # type: ignore[assignment]  # noqa: F841
        with pytest.raises(KeyError):
            gt_y = gt_doc["c"]  # type: ignore[typeddict-item]  # noqa: F841

        # mapping

        def u_mapper(doc: dict[str, Any]) -> str:
            return f"{doc['p_bigint'] % 100}"

        def t_mapper(doc: TestDoc) -> str:
            return f"{doc['p_bigint'] % 100}"

        u_cur_1m = ag_co_untyped.find().map(u_mapper)
        t_cur_1m = ag_co_typed.find().map(t_mapper)
        gmu = await u_cur_1m.__anext__()
        gmt = await t_cur_1m.__anext__()
        assert gmu is not None
        assert gmt is not None
        gmu_a: str
        gmu_x: int
        gmu_a = gmu  # noqa: F841
        gmu_x = gmu  # type: ignore[assignment]  # noqa: F841
        gmt_a: str
        gmt_x: int
        gmt_a = gmu  # noqa: F841
        gmt_x = gmt  # type: ignore[assignment]  # noqa: F841

        # mapping composition

        def mapper_2(strint: str) -> float:
            return float(strint) / 10.0

        u_cur_2m = ag_co_untyped.find().map(u_mapper).map(mapper_2)
        t_cur_2m = ag_co_typed.find().map(t_mapper).map(mapper_2)
        gmu2 = await u_cur_2m.__anext__()
        gmt2 = await t_cur_2m.__anext__()
        assert gmu2 is not None
        assert gmt2 is not None
        gmu_a2: float
        gmu_x2: str
        gmu_a2 = gmu2  # noqa: F841
        gmu_x2 = gmu2  # type: ignore[assignment]  # noqa: F841
        gmt_a2: float
        gmt_x2: str
        gmt_a2 = gmu2  # noqa: F841
        gmt_x2 = gmt2  # type: ignore[assignment]  # noqa: F841

        # cloning a cursor
        u_cur_3c = ag_co_untyped.find().map(u_mapper).clone()
        t_cur_3c = ag_co_typed.find().map(t_mapper).clone()
        gu_doc3 = await u_cur_3c.__anext__()
        gt_doc3 = await t_cur_3c.__anext__()
        assert gu_doc3 is not None
        assert gt_doc3 is not None
        gu_a3: str
        gu_x3: int
        gu_a3 = gu_doc3  # noqa: F841
        gu_x3 = gu_doc3  # type: ignore[assignment] # noqa: F841
        gt_a3: str
        gt_x3: int
        gt_a3 = gt_doc3  # noqa: F841
        gt_x3 = gt_doc3  # type: ignore[assignment]  # noqa: F841

        # reading the buffer
        u_cur_4rb = ag_co_untyped.find().map(u_mapper)
        t_cur_4rb = ag_co_typed.find().map(t_mapper)
        await u_cur_4rb.__anext__()
        await t_cur_4rb.__anext__()
        u_doc_rbuf4 = u_cur_4rb.consume_buffer(3)[0]
        t_doc_rbuf4 = t_cur_4rb.consume_buffer(3)[0]
        assert u_doc_rbuf4 is not None
        assert t_doc_rbuf4 is not None
        gu_rbuf_a: str
        gu_rbuf_x: int
        gu_rbuf_a = u_doc_rbuf4["p_ascii"]  # noqa: F841
        gu_rbuf_x = u_doc_rbuf4["p_ascii"]  # noqa: F841
        gt_rbuf_a: str
        gt_rbuf_x: int
        gt_rbuf_y: float
        gt_rbuf_a = t_doc_rbuf4["p_ascii"]  # noqa: F841
        gt_rbuf_x = t_doc_rbuf4["p_ascii"]  # type: ignore[assignment]  # noqa: F841
        with pytest.raises(KeyError):
            gt_rbuf_y = t_doc_rbuf4["c"]  # type: ignore[typeddict-item]  # noqa: F841

        await ag_co_typed.delete_many({})

    @pytest.mark.describe("test of typing find_and_rerank, async")
    async def test_collection_find_and_rerank_typing_async(
        self,
        async_database: AsyncDatabase,
        async_empty_collection: DefaultAsyncCollection,
        service_collection_parameters: dict[str, Any],
    ) -> None:
        """Test of typing in find_and_rerank, sync."""

        params = service_collection_parameters

        reranking_api_key: str | UnsetType
        if "ASTRAPY_FINDANDRERANK_USE_RERANKER_HEADER" in os.environ:
            assert params["reranking_api_key"] is not None
            reranking_api_key = params["reranking_api_key"]
        else:
            reranking_api_key = _UNSET

        # Untyped baseline
        f_co_untyped = async_database.get_collection(
            async_empty_collection.name,
            reranking_api_key=reranking_api_key,
        )
        await f_co_untyped.insert_one(VLEX_DOCUMENT)
        farr_u_hits = await f_co_untyped.find_and_rerank(
            sort={"$hybrid": {"$vector": [2, 1], "$lexical": "bla"}},
            limit=1,
            rerank_on="p_ascii",
            rerank_query="bli",
        ).to_list()
        assert len(farr_u_hits) > 0
        farr_u_doc = farr_u_hits[0].document
        fu_a: str
        fu_b: int
        fu_a = farr_u_doc["p_ascii"]  # noqa: F841
        fu_b = farr_u_doc["p_bigint"]  # noqa: F841
        # untyped, these are all ok:
        fu_x: int
        fu_y: float
        fu_x = farr_u_doc["p_ascii"]  # noqa: F841
        with pytest.raises(KeyError):
            fu_y = farr_u_doc["c"]  # noqa: F841

        await f_co_untyped.delete_many({})

        # Typed
        af_co_typed: AsyncCollection[TestDoc] = async_database.get_collection(
            async_empty_collection.name,
            document_type=TestDoc,
            reranking_api_key=reranking_api_key,
        )
        await af_co_typed.insert_one(TYPED_VLEX_DOCUMENT)
        farr_t_hits = await af_co_typed.find_and_rerank(
            sort={"$hybrid": {"$vector": [2, 1], "$lexical": "bla"}},
            limit=1,
            rerank_on="p_ascii",
            rerank_query="bli",
        ).to_list()
        assert len(farr_t_hits) > 0
        farr_t_doc = farr_t_hits[0].document
        ft_a: str
        ft_b: int
        ft_a = farr_t_doc["p_ascii"]  # noqa: F841
        ft_b = farr_t_doc["p_bigint"]  # noqa: F841
        # these two SHOULD NOT typecheck (i.e. require the ignore directive)
        ft_x: int
        ft_y: float
        ft_x = farr_t_doc["p_ascii"]  # type: ignore[assignment]  # noqa: F841
        with pytest.raises(KeyError):
            ft_y = farr_t_doc["c"]  # type: ignore[typeddict-item]  # noqa: F841

        farr_tm_hits = await af_co_typed.find_and_rerank(
            sort={"$hybrid": {"$vector": [2, 1], "$lexical": "bla"}},
            limit=1,
            projection=FIND_PROJECTION,
            document_type=TestMiniDoc,
            rerank_on="p_ascii",
            rerank_query="bli",
        ).to_list()
        assert len(farr_tm_hits) > 0
        farr_tm_doc = farr_tm_hits[0].document
        ftm_a: str
        ftm_b: int
        # typechecks must detect that p_ascii is not there
        with pytest.raises(KeyError):
            ftm_a = farr_tm_doc["p_ascii"]  # type: ignore[typeddict-item]  # noqa: F841
        ftm_b = farr_tm_doc["p_bigint"]  # noqa: F841

        await af_co_typed.delete_many({})
