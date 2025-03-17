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

from ..conftest import DefaultCollection


@pytest.mark.skipif(
    "ASTRAPY_TEST_FINDANDRERANK" not in os.environ,
    reason="No testing enabled on findAndRerank support",
)
class TestCollectionFindAndRerankSync:
    @pytest.mark.describe("test of collection find-and-rerank vectorize, sync")
    def test_collection_farr_vectorize_sync(
        self,
        sync_empty_farr_vectorize_collection: DefaultCollection,
    ) -> None:
        # TODO: (1) add insert modes, (2) test various params, (3) verify returned docs.
        coll = sync_empty_farr_vectorize_collection
        # insertions
        coll.insert_many(
            [
                {
                    "_id": "01",
                    "$vectorize": "this is a cat",
                    "$lexical": "a cat",
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
        farr_cursor = coll.find_and_rerank(
            {},
            sort={"$hybrid": "bla"},
            limit=2,
        )

        hits = farr_cursor.to_list()
        assert len(hits) == 2
        assert all(isinstance(hit, RerankedResult) for hit in hits)
        assert all(isinstance(hit.document, dict) for hit in hits)
        assert all(isinstance(hit.document["$vectorize"], str) for hit in hits)
        assert all(isinstance(hit.scores, dict) for hit in hits)
        assert all(
            all(isinstance(sc, float) for sc in hit.scores.values()) for hit in hits
        )

    @pytest.mark.describe("test of collection find-and-rerank novectorize, sync")
    def test_collection_farr_novectorize_sync(
        self,
        sync_empty_farr_vector_collection: DefaultCollection,
    ) -> None:
        # TODO: (1) add insert modes, (2) test various params, (3) verify returned docs.
        coll = sync_empty_farr_vector_collection
        # insertions
        coll.insert_many(
            [
                {
                    "_id": "01",
                    "text_content": "this is a cat",
                    "$vector": [1, 2],
                    "$lexical": "a cat",
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
        farr_cursor = coll.find_and_rerank(
            {},
            sort={"$hybrid": {"$vector": [0, 1], "$lexical": "bla"}},
            limit=2,
        )

        hits = farr_cursor.to_list()
        assert len(hits) == 2
        assert all(isinstance(hit, RerankedResult) for hit in hits)
        assert all(isinstance(hit.document, dict) for hit in hits)
        assert all(isinstance(hit.document["$vector"], list) for hit in hits)
        assert all(len(hit.document["$vector"]) == 2 for hit in hits)
        assert all(isinstance(hit.scores, dict) for hit in hits)
        assert all(
            all(isinstance(sc, float) for sc in hit.scores.values()) for hit in hits
        )
