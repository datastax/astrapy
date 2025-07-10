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

import pytest

from ..conftest import DefaultCollection

PJ = {"_id": True, "title": False}


class TestCollectionGeneralBM25Sync:
    @pytest.mark.describe("test of collection bm25 DML with vectorize, sync")
    def test_collection_bm25_dml_vectorize_sync(
        self,
        sync_empty_farr_vectorize_collection: DefaultCollection,
    ) -> None:
        coll = sync_empty_farr_vectorize_collection

        coll.insert_one(
            {
                "_id": "lx",
                "title": "Only lexical",
                "$lexical": "Water flows downward.",
            }
        )

        fol_doc = coll.find_one({"$lexical": {"$match": "water"}}, projection=PJ)
        assert fol_doc is not None
        assert fol_doc["_id"] == "lx"

        fcl_docs = coll.find({"$lexical": {"$match": "water"}}, projection=PJ).to_list()
        assert fcl_docs == [{"_id": "lx"}]

        # no lexical matching occurs
        fol_doc_n = coll.find_one({"$lexical": {"$match": "fire"}}, projection=PJ)
        assert fol_doc_n is None

        # cannot find a doc without vector
        fov_doc_n = coll.find_one(sort={"$vectorize": "water"}, projection=PJ)
        assert fov_doc_n is None

        coll.insert_many(
            [
                {
                    "_id": "hy",
                    "title": "Both, using hybrid",
                    "$hybrid": "Plants need water.",
                },
                {
                    "_id": "hy_0",
                    "title": "Both, using hybrid, irrelevant",
                    "$hybrid": "Sand forms dunes.",
                },
            ]
        )

        folv_doc = coll.find_one(
            {"$lexical": {"$match": "water"}},
            sort={"$vectorize": "water"},
            projection=PJ,
        )
        assert folv_doc is not None
        assert folv_doc["_id"] == "hy"

        # only one matches the lexical part and has a vector
        fcv_docs = coll.find(
            {"$lexical": {"$match": "water"}},
            sort={"$vectorize": "water"},
            projection=PJ,
        ).to_list()
        assert fcv_docs == [{"_id": "hy"}]

        coll.delete_many({})

        coll.insert_one(
            {
                "_id": "vl",
                "title": "Both, but different",
                "$lexical": "Birds fly.",
                "$vectorize": "Fish swim.",
            }
        )

        upd_one_result = coll.update_one(
            {"$lexical": {"$match": "birds"}},
            {"$set": {"updated": True}},
            sort={"$lexical": "fly"},
        )
        assert upd_one_result.update_info["n"] > 0

        upd_one_result_n = coll.update_one(
            {"$lexical": {"$match": "dogs"}},
            {"$set": {"updated": True}},
            sort={"$lexical": "fly"},
        )
        assert upd_one_result_n.update_info["n"] == 0

        upd_one_result_n2 = coll.update_one(
            {"$lexical": {"$match": "birds"}},
            {"$set": {"updated": True}},
            sort={"$lexical": "cats"},
        )
        assert upd_one_result_n2.update_info["n"] == 0

        f1u_result_doc = coll.find_one_and_update(
            {"$lexical": {"$match": "birds"}},
            {"$set": {"updated_n": 1}},
            sort={"$lexical": "fly"},
            projection=PJ,
        )
        assert f1u_result_doc is not None
        assert f1u_result_doc["_id"] == "vl"

        u1_result = coll.update_one(
            {"$lexical": {"$match": "birds"}},
            {"$set": {"updated_n": 2}},
            sort={"$lexical": "fly"},
        )
        assert u1_result.update_info["n"] > 0

        f1r_result_doc = coll.find_one_and_replace(
            {"$lexical": {"$match": "birds"}},
            {"name": "replaced 2"},
            sort={"$lexical": "fly"},
            projection=PJ,
        )
        assert f1r_result_doc is not None
        assert f1r_result_doc["_id"] == "vl"

        # Suspended until #2150 is understood/resolved
        # r1_result = coll.replace_one(
        #     {"$lexical": {"$match": "birds"}},
        #     {"name": "replaced 1"},
        #     sort={"$lexical": "fly"},
        # )
        # assert r1_result is not None
        # assert r1_result.update_info["n"] > 0

        coll.insert_many(
            [
                {
                    "_id": "vl2",
                    "title": "Again: both, but different",
                    "$lexical": "Ants tiptap.",
                    "$vectorize": "Snakes crawl.",
                },
                {
                    "_id": "vl3",
                    "title": "One more: Both, but different",
                    "$lexical": "Worms inch.",
                    "$vectorize": "Spiders climb.",
                },
            ]
        )

        f1d_result_doc = coll.find_one_and_delete(
            {"$lexical": {"$match": "ants"}},
            sort={"$lexical": "tiptap"},
            projection=PJ,
        )
        assert f1d_result_doc is not None
        assert f1d_result_doc["_id"] == "vl2"

        # Suspended until #2150 is understood/resolved
        # f1d_result_doc_n = coll.find_one_and_delete(
        #     {"$lexical": {"$match": "ants"}},
        #     sort={"$lexical": "tiptap"},
        #     projection=PJ,
        # )
        # assert f1d_result_doc is None

        d1_result = coll.delete_one(
            {"$lexical": {"$match": "worms"}},
            sort={"$lexical": "inch"},
        )
        assert d1_result.deleted_count > 0

        # Suspended until #2150 is understood/resolved
        # d1_result_n = coll.delete_one(
        #     {"$lexical": {"$match": "worms"}},
        #     sort={"$lexical": "inch"},
        # )
        # assert d1_result_n.deleted_count == 0

        coll.insert_many(
            [
                {
                    "_id": "hy2",
                    "title": "Document Alpha for *many test",
                    "$hybrid": "The King is dead.",
                },
                {
                    "_id": "hy3",
                    "title": "Document Omega for *many test",
                    "$hybrid": "Long live the king!",
                },
                {
                    "_id": "hy4_n",
                    "title": "Document Negative for *many test",
                    "$hybrid": "Vive la Republique!",
                },
            ]
        )

        um_result = coll.update_many(
            {"$lexical": {"$match": "king"}},
            {"$set": {"updated": True}},
        )
        assert um_result.update_info["n"] == 2
        assert coll.count_documents({"updated": True}, upper_bound=5) == 2

        dm_result = coll.delete_many(
            {"$lexical": {"$match": "king"}},
        )
        assert dm_result.deleted_count == 2
        assert (
            coll.count_documents(
                {"$lexical": {"$match": "king"}},
                upper_bound=5,
            )
            == 0
        )

        dm_result_n = coll.delete_many(
            {"$lexical": {"$match": "king"}},
        )
        assert dm_result_n.deleted_count == 0
        assert (
            coll.count_documents(
                {"$lexical": {"$match": "king"}},
                upper_bound=5,
            )
            == 0
        )
