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

import pytest

from astrapy.cursors import RerankedResult
from astrapy.data_types import DataAPIVector

from ..conftest import DefaultAsyncCollection
from .hybrid_sanitizer import _sanitize_dev_hybrid_clause


@pytest.mark.skipif(
    "ASTRAPY_TEST_FINDANDRERANK" not in os.environ,
    reason="No testing enabled on findAndRerank support",
)
class TestCollectionFindAndRerankAsync:
    @pytest.mark.describe("test of collection find-and-rerank vectorize, async")
    async def test_collection_farr_vectorize_async(
        self,
        async_empty_farr_vectorize_collection: DefaultAsyncCollection,
    ) -> None:
        # TODO: add insert modes
        acoll = async_empty_farr_vectorize_collection
        # insertions
        await acoll.insert_many(
            [
                {
                    "_id": "01",
                    "$vectorize": "this is a cat",
                    "$lexical": "a cat",
                    "tag": "test01",
                },
                {
                    "_id": "01b",
                    "$vectorize": "this is a lynx",
                    "$lexical": "a lynx",
                    "tag": "test01",
                },
                # {
                #     "_id": "02",
                #     "$hybrid": "this is a dog",
                #     "tag": "test01",
                # },
                # {
                #     "_id": "03",
                #     "$hybrid": {
                #         "$vectorize": "this is a Pucciniomycotina",
                #         "$lexical": "Pucciniomycotina, my dear rust fungus",
                #     },
                #     "tag": "test01",
                # },
            ]
        )
        # find-and-rerank functional test
        farr_cursor = acoll.find_and_rerank(
            {},
            sort=_sanitize_dev_hybrid_clause({"$hybrid": "bla"}),
            projection={"$vectorize": True},
            include_scores=True,
            limit=2,
        )

        hits = await farr_cursor.to_list()
        assert len(hits) == 2
        assert all(isinstance(hit, RerankedResult) for hit in hits)
        assert all(isinstance(hit.document, dict) for hit in hits)
        assert all(isinstance(hit.document["$vectorize"], str) for hit in hits)
        assert all(isinstance(hit.scores, dict) for hit in hits)
        assert all(len(hit.scores) > 0 for hit in hits)
        # some scores can get back as None or integer
        assert all(
            all(isinstance(sc, (float, int, type(None))) for sc in hit.scores.values())
            for hit in hits
        )
        assert all(
            any(isinstance(sc, float) for sc in hit.scores.values()) for hit in hits
        )

        # sort.$hybrid can be an object as well:
        farr_cursor_s_o = acoll.find_and_rerank(
            {},
            sort=_sanitize_dev_hybrid_clause(
                {"$hybrid": {"$vectorize": "bla", "$lexical": "bla"}}
            ),
            projection={"$vectorize": True},
            include_scores=True,
            limit=2,
        )
        s_o_hits = await farr_cursor_s_o.to_list()
        # scores may differ by epsilon
        assert [rr.document for rr in s_o_hits] == [rr.document for rr in hits]

        # hybrid_limits various forms, functional tests
        cur_no_hl = acoll.find_and_rerank(
            {},
            sort=_sanitize_dev_hybrid_clause({"$hybrid": "bla"}),
            limit=2,
        )
        cur_nu_hl = acoll.find_and_rerank(
            {},
            sort=_sanitize_dev_hybrid_clause({"$hybrid": "bla"}),
            limit=2,
            hybrid_limits=4,
        )
        cur_ob_hl = acoll.find_and_rerank(
            {},
            sort=_sanitize_dev_hybrid_clause({"$hybrid": "bla"}),
            limit=2,
            hybrid_limits={
                "$lexical": 4,
                "$vector": 3,
            },
        )
        assert len(await cur_no_hl.to_list()) == 2
        assert len(await cur_nu_hl.to_list()) == 2
        assert len(await cur_ob_hl.to_list()) == 2

    @pytest.mark.describe("test of collection find-and-rerank novectorize, async")
    async def test_collection_farr_novectorize_async(
        self,
        async_empty_farr_vector_collection: DefaultAsyncCollection,
    ) -> None:
        # TODO: add insert modes
        acoll = async_empty_farr_vector_collection
        # insertions
        await acoll.insert_many(
            [
                {
                    "_id": "01",
                    "text_content": "this is a cat",
                    "$vector": [1, 2],
                    "$lexical": "a cat",
                    "tag": "test01",
                },
                {
                    "_id": "01b",
                    "text_content": "this is a lynx",
                    "$vector": [2, 1],
                    "$lexical": "a lynx",
                    "tag": "test01",
                },
                # {
                #     "_id": "02",
                #     "text_content": "this is a Pucciniomycotina",
                #     "$hybrid": {
                #         "$vector": [3, 4],
                #         "$lexical": "Pucciniomycotina, my dear rust fungus",
                #     },
                #     "tag": "test01",
                # },
            ]
        )
        # find-and-rerank functional test
        farr_cursor = acoll.find_and_rerank(
            {},
            sort=_sanitize_dev_hybrid_clause(
                {"$hybrid": {"$vector": [0, 1], "$lexical": "bla"}}
            ),
            projection={"$vector": True},
            include_scores=True,
            limit=2,
            rerank_on="text_content",
            rerank_query="blaa",
        )

        hits = await farr_cursor.to_list()
        assert len(hits) == 2
        assert all(isinstance(hit, RerankedResult) for hit in hits)
        assert all(isinstance(hit.document, dict) for hit in hits)
        assert all(isinstance(hit.document["$vector"], DataAPIVector) for hit in hits)
        assert all(len(hit.document["$vector"]) == 2 for hit in hits)
        assert all(isinstance(hit.scores, dict) for hit in hits)
        assert all(len(hit.scores) > 0 for hit in hits)
        # some scores can get back as None or integer
        assert all(
            all(isinstance(sc, (float, int, type(None))) for sc in hit.scores.values())
            for hit in hits
        )
        assert all(
            any(isinstance(sc, float) for sc in hit.scores.values()) for hit in hits
        )

        hits_dav = await acoll.find_and_rerank(
            {},
            sort=_sanitize_dev_hybrid_clause(
                {"$hybrid": {"$vector": DataAPIVector([0, 1]), "$lexical": "bla"}}
            ),
            projection={"$vector": True},
            include_scores=True,
            limit=2,
            rerank_on="text_content",
            rerank_query="blaa",
        ).to_list()
        assert hits_dav == hits

        # hybrid_limits various forms, functional tests
        cur_no_hl = acoll.find_and_rerank(
            {},
            sort=_sanitize_dev_hybrid_clause(
                {"$hybrid": {"$vector": [0, 1], "$lexical": "bla"}}
            ),
            limit=2,
            rerank_on="text_content",
            rerank_query="blaa",
        )
        cur_nu_hl = acoll.find_and_rerank(
            {},
            sort=_sanitize_dev_hybrid_clause(
                {"$hybrid": {"$vector": [0, 1], "$lexical": "bla"}}
            ),
            limit=2,
            rerank_on="text_content",
            rerank_query="blaa",
            hybrid_limits=3,
        )
        cur_ob_hl = acoll.find_and_rerank(
            {},
            sort=_sanitize_dev_hybrid_clause(
                {"$hybrid": {"$vector": [0, 1], "$lexical": "bla"}}
            ),
            limit=2,
            rerank_on="text_content",
            rerank_query="blaa",
            hybrid_limits={
                "$lexical": 4,
                "$vector": 3,
            },
        )
        assert len(await cur_no_hl.to_list()) == 2
        assert len(await cur_nu_hl.to_list()) == 2
        assert len(await cur_ob_hl.to_list()) == 2

    @pytest.mark.describe(
        "test of collection find-and-rerank include_scores, vectorize, async"
    )
    async def test_collection_includescores_farr_vectorize_async(
        self,
        async_empty_farr_vectorize_collection: DefaultAsyncCollection,
    ) -> None:
        acoll = async_empty_farr_vectorize_collection
        await acoll.insert_one({"$vectorize": "text", "$lexical": "text"})

        cur_n = acoll.find_and_rerank(
            sort=_sanitize_dev_hybrid_clause({"$hybrid": "bla"})
        )
        cur_f = acoll.find_and_rerank(
            sort=_sanitize_dev_hybrid_clause({"$hybrid": "bla"}),
            include_scores=False,
        )
        cur_t = acoll.find_and_rerank(
            sort=_sanitize_dev_hybrid_clause({"$hybrid": "bla"}),
            include_scores=True,
        )
        itm_n = await cur_n.__anext__()
        itm_f = await cur_f.__anext__()
        itm_t = await cur_t.__anext__()

        assert itm_n.scores == {}
        assert itm_f.scores == {}
        assert itm_t.scores != {}
        assert all(
            isinstance(val, (float, int, type(None))) for val in itm_t.scores.values()
        )

    @pytest.mark.describe(
        "test of collection find-and-rerank include_scores, novectorize, async"
    )
    async def test_collection_includescores_farr_novectorize_async(
        self,
        async_empty_farr_vector_collection: DefaultAsyncCollection,
    ) -> None:
        acoll = async_empty_farr_vector_collection
        await acoll.insert_one(
            {"$vector": [11, 12], "$lexical": "text", "content": "text"}
        )

        cur_n = acoll.find_and_rerank(
            sort=_sanitize_dev_hybrid_clause(
                {"$hybrid": {"$vector": [0, 1], "$lexical": "bla"}}
            ),
            rerank_on="content",
            rerank_query="blaa",
        )
        cur_f = acoll.find_and_rerank(
            sort=_sanitize_dev_hybrid_clause(
                {"$hybrid": {"$vector": [0, 1], "$lexical": "bla"}}
            ),
            rerank_on="content",
            rerank_query="blaa",
            include_scores=False,
        )
        cur_t = acoll.find_and_rerank(
            sort=_sanitize_dev_hybrid_clause(
                {"$hybrid": {"$vector": [0, 1], "$lexical": "bla"}}
            ),
            rerank_on="content",
            rerank_query="blaa",
            include_scores=True,
        )
        itm_n = await cur_n.__anext__()
        itm_f = await cur_f.__anext__()
        itm_t = await cur_t.__anext__()

        assert itm_n.scores == {}
        assert itm_f.scores == {}
        assert itm_t.scores != {}
        assert all(
            isinstance(val, (float, int, type(None))) for val in itm_t.scores.values()
        )

    @pytest.mark.describe(
        "test of collection find-and-rerank get_sort_vector, vectorize, async"
    )
    async def test_collection_getsortvector_farr_vectorize_async(
        self,
        async_empty_farr_vectorize_collection: DefaultAsyncCollection,
    ) -> None:
        acoll = async_empty_farr_vectorize_collection
        await acoll.insert_one({"$vectorize": "text", "$lexical": "text"})

        cur_n0 = acoll.find_and_rerank(
            sort=_sanitize_dev_hybrid_clause({"$hybrid": "bla"})
        )
        cur_f0 = acoll.find_and_rerank(
            sort=_sanitize_dev_hybrid_clause({"$hybrid": "bla"}),
            include_sort_vector=False,
        )
        cur_t0 = acoll.find_and_rerank(
            sort=_sanitize_dev_hybrid_clause({"$hybrid": "bla"}),
            include_sort_vector=True,
        )

        assert await cur_n0.get_sort_vector() is None
        assert await cur_f0.get_sort_vector() is None
        gsv_t0 = await cur_t0.get_sort_vector()
        assert isinstance(gsv_t0, (list, DataAPIVector))
        assert isinstance(gsv_t0[0], float)

        cur_n1 = acoll.find_and_rerank(
            sort=_sanitize_dev_hybrid_clause({"$hybrid": "bla"})
        )
        cur_f1 = acoll.find_and_rerank(
            sort=_sanitize_dev_hybrid_clause({"$hybrid": "bla"}),
            include_sort_vector=False,
        )
        cur_t1 = acoll.find_and_rerank(
            sort=_sanitize_dev_hybrid_clause({"$hybrid": "bla"}),
            include_sort_vector=True,
        )
        await cur_n1.__anext__()
        await cur_f1.__anext__()
        await cur_t1.__anext__()

        assert await cur_n1.get_sort_vector() is None
        assert await cur_f1.get_sort_vector() is None
        gsv_t1 = await cur_t1.get_sort_vector()
        assert isinstance(gsv_t1, (list, DataAPIVector))
        assert isinstance(gsv_t1[0], float)

        cur_n2 = acoll.find_and_rerank(
            sort=_sanitize_dev_hybrid_clause({"$hybrid": "bla"})
        )
        cur_f2 = acoll.find_and_rerank(
            sort=_sanitize_dev_hybrid_clause({"$hybrid": "bla"}),
            include_sort_vector=False,
        )
        cur_t2 = acoll.find_and_rerank(
            sort=_sanitize_dev_hybrid_clause({"$hybrid": "bla"}),
            include_sort_vector=True,
        )
        await cur_n2.to_list()
        await cur_f2.to_list()
        await cur_t2.to_list()

        assert await cur_n2.get_sort_vector() is None
        assert await cur_f2.get_sort_vector() is None
        gsv_t2 = await cur_t2.get_sort_vector()
        assert isinstance(gsv_t2, (list, DataAPIVector))
        assert isinstance(gsv_t2[0], float)

    @pytest.mark.describe(
        "test of collection find-and-rerank get_sort_vector, novectorize, async"
    )
    async def test_collection_getsortvector_farr_novectorize_async(
        self,
        async_empty_farr_vector_collection: DefaultAsyncCollection,
    ) -> None:
        acoll = async_empty_farr_vector_collection
        await acoll.insert_one(
            {"$vector": [11, 12], "$lexical": "text", "content": "text"}
        )

        cur_n0 = acoll.find_and_rerank(
            sort=_sanitize_dev_hybrid_clause(
                {"$hybrid": {"$vector": [0, 1], "$lexical": "bla"}}
            ),
            rerank_on="content",
            rerank_query="blaa",
        )
        cur_f0 = acoll.find_and_rerank(
            sort=_sanitize_dev_hybrid_clause(
                {"$hybrid": {"$vector": [0, 1], "$lexical": "bla"}}
            ),
            rerank_on="content",
            rerank_query="blaa",
            include_sort_vector=False,
        )
        cur_t0 = acoll.find_and_rerank(
            sort=_sanitize_dev_hybrid_clause(
                {"$hybrid": {"$vector": [0, 1], "$lexical": "bla"}}
            ),
            rerank_on="content",
            rerank_query="blaa",
            include_sort_vector=True,
        )

        assert await cur_n0.get_sort_vector() is None
        assert await cur_f0.get_sort_vector() is None
        gsv_t0 = await cur_t0.get_sort_vector()
        assert isinstance(gsv_t0, (list, DataAPIVector))
        assert isinstance(gsv_t0[0], float)

        cur_n1 = acoll.find_and_rerank(
            sort=_sanitize_dev_hybrid_clause(
                {"$hybrid": {"$vector": [0, 1], "$lexical": "bla"}}
            ),
            rerank_on="content",
            rerank_query="blaa",
        )
        cur_f1 = acoll.find_and_rerank(
            sort=_sanitize_dev_hybrid_clause(
                {"$hybrid": {"$vector": [0, 1], "$lexical": "bla"}}
            ),
            rerank_on="content",
            rerank_query="blaa",
            include_sort_vector=False,
        )
        cur_t1 = acoll.find_and_rerank(
            sort=_sanitize_dev_hybrid_clause(
                {"$hybrid": {"$vector": [0, 1], "$lexical": "bla"}}
            ),
            rerank_on="content",
            rerank_query="blaa",
            include_sort_vector=True,
        )
        await cur_n1.__anext__()
        await cur_f1.__anext__()
        await cur_t1.__anext__()

        assert await cur_n1.get_sort_vector() is None
        assert await cur_f1.get_sort_vector() is None
        gsv_t1 = await cur_t1.get_sort_vector()
        assert isinstance(gsv_t1, (list, DataAPIVector))
        assert isinstance(gsv_t1[0], float)

        cur_n2 = acoll.find_and_rerank(
            sort=_sanitize_dev_hybrid_clause(
                {"$hybrid": {"$vector": [0, 1], "$lexical": "bla"}}
            ),
            rerank_on="content",
            rerank_query="blaa",
        )
        cur_f2 = acoll.find_and_rerank(
            sort=_sanitize_dev_hybrid_clause(
                {"$hybrid": {"$vector": [0, 1], "$lexical": "bla"}}
            ),
            rerank_on="content",
            rerank_query="blaa",
            include_sort_vector=False,
        )
        cur_t2 = acoll.find_and_rerank(
            sort=_sanitize_dev_hybrid_clause(
                {"$hybrid": {"$vector": [0, 1], "$lexical": "bla"}}
            ),
            rerank_on="content",
            rerank_query="blaa",
            include_sort_vector=True,
        )
        await cur_n2.to_list()
        await cur_f2.to_list()
        await cur_t2.to_list()

        assert await cur_n2.get_sort_vector() is None
        assert await cur_f2.get_sort_vector() is None
        gsv_t2 = await cur_t2.get_sort_vector()
        assert isinstance(gsv_t2, (list, DataAPIVector))
        assert isinstance(gsv_t2[0], float)
