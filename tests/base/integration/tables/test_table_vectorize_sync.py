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

from astrapy.api_options import APIOptions
from astrapy.data_types import DataAPIVector
from astrapy.exceptions import TableInsertManyException

from ..conftest import IS_ASTRA_DB, VECTORIZE_TEXTS, DefaultTable

HEADER_EMBEDDING_API_KEY_OPENAI = os.environ.get("HEADER_EMBEDDING_API_KEY_OPENAI")


class TestTableVectorizeSync:
    @pytest.mark.skipif(
        HEADER_EMBEDDING_API_KEY_OPENAI is None,
        reason="No HEADER_EMBEDDING_API_KEY_OPENAI detected",
    )
    @pytest.mark.describe("test of basic table vectorize with key via header, sync")
    def test_table_vectorize_header_sync(
        self,
        sync_empty_table_vectorize: DefaultTable,
    ) -> None:
        authenticated_table = sync_empty_table_vectorize.with_options(
            api_options=APIOptions(
                embedding_api_key=HEADER_EMBEDDING_API_KEY_OPENAI or "",
            ),
        )
        i_vector = DataAPIVector([0.01] * 64)
        authenticated_table.insert_one(
            {"p_text": "v", "p_vector": i_vector},
        )
        authenticated_table.insert_one(
            {"p_text": "t", "p_vector": "This is a text example."},
        )
        authenticated_table.insert_one(
            {"p_text": "u"},
        )
        authenticated_table.insert_one(
            {"p_text": "n", "p_vector": None},
        )
        v_row = authenticated_table.find_one(filter={"p_text": "v"})
        t_row = authenticated_table.find_one(filter={"p_text": "t"})
        u_row = authenticated_table.find_one(filter={"p_text": "u"})
        n_row = authenticated_table.find_one(filter={"p_text": "n"})
        assert v_row is not None
        assert t_row is not None
        assert u_row is not None
        assert n_row is not None
        assert isinstance(v_row["p_vector"], DataAPIVector)
        assert v_row["p_vector"] == i_vector
        assert u_row["p_vector"] is None
        assert n_row["p_vector"] is None

        vec_row = authenticated_table.find_one(
            sort={"p_vector": "This is one text example."}
        )
        assert vec_row == t_row

        authenticated_table.delete_many({})
        authenticated_table.insert_many(
            [
                {"p_text": f"t_{text_i}", "p_vector": text}
                for text_i, text in enumerate(VECTORIZE_TEXTS)
            ],
            chunk_size=min(20, 1 + len(VECTORIZE_TEXTS) // 3),
        )
        all_rows = authenticated_table.find({}).to_list()
        assert len(all_rows) == len(VECTORIZE_TEXTS)
        assert all(isinstance(row["p_vector"], DataAPIVector) for row in all_rows)

        # include sort vector and include similarity
        cur_isv = authenticated_table.find(
            sort={"p_vector": "This is a fact."},
            include_sort_vector=True,
            include_similarity=True,
        )
        # TODO: restore this check once #1949 is resolved
        # assert isinstance(cur_isv.get_sort_vector(), DataAPIVector)
        match0 = next(cur_isv)
        assert match0 is not None
        # TODO: restore this check once #1949 is resolved
        # assert isinstance(match0["$similarity"], float)

    @pytest.mark.skipif(
        HEADER_EMBEDDING_API_KEY_OPENAI is None,
        reason="No HEADER_EMBEDDING_API_KEY_OPENAI detected",
    )
    @pytest.mark.describe(
        "test of vectorize-based table insert many grand failures, sync"
    )
    def test_table_insertmany_grandfailure_vectorize_sync(
        self,
        sync_empty_table_vectorize: DefaultTable,
    ) -> None:
        authenticated_table = sync_empty_table_vectorize.with_options(
            api_options=APIOptions(
                embedding_api_key=HEADER_EMBEDDING_API_KEY_OPENAI or "",
            ),
        )
        authenticated_table.insert_many([{"p_text": "0", "p_vector": "Text."}])

        bad_table = authenticated_table.with_options(embedding_api_key="BadKey")

        err: TableInsertManyException | None = None
        try:
            bad_table.insert_many(
                [{"p_text": "1"}, {"p_text": "2", "p_vector": "Text."}]
            )
        except TableInsertManyException as e:
            err = e
        assert err is not None
        assert err.inserted_ids == []

    @pytest.mark.skipif(
        not IS_ASTRA_DB,
        reason="KMS vectorize is available only on Astra DB",
    )
    @pytest.mark.describe(
        "test of basic table vectorize with key via shared secret, sync"
    )
    def test_table_vectorize_shared_secret_sync(
        self,
        sync_empty_table_kms_vectorize: DefaultTable,
    ) -> None:
        i_vector = DataAPIVector([0.01] * 64)
        sync_empty_table_kms_vectorize.insert_one(
            {"p_text": "v", "p_vector": i_vector},
        )
        sync_empty_table_kms_vectorize.insert_one(
            {"p_text": "t", "p_vector": "This is a text example."},
        )
        sync_empty_table_kms_vectorize.insert_one(
            {"p_text": "u"},
        )
        sync_empty_table_kms_vectorize.insert_one(
            {"p_text": "n", "p_vector": None},
        )
        v_row = sync_empty_table_kms_vectorize.find_one(filter={"p_text": "v"})
        t_row = sync_empty_table_kms_vectorize.find_one(filter={"p_text": "t"})
        u_row = sync_empty_table_kms_vectorize.find_one(filter={"p_text": "u"})
        n_row = sync_empty_table_kms_vectorize.find_one(filter={"p_text": "n"})
        assert v_row is not None
        assert t_row is not None
        assert u_row is not None
        assert n_row is not None
        assert isinstance(v_row["p_vector"], DataAPIVector)
        assert v_row["p_vector"] == i_vector
        assert u_row["p_vector"] is None
        assert n_row["p_vector"] is None

        vec_row = sync_empty_table_kms_vectorize.find_one(
            sort={"p_vector": "This is one text example."}
        )
        assert vec_row == t_row

        sync_empty_table_kms_vectorize.delete_many({})
        sync_empty_table_kms_vectorize.insert_many(
            [
                {"p_text": f"t_{text_i}", "p_vector": text}
                for text_i, text in enumerate(VECTORIZE_TEXTS)
            ],
            chunk_size=min(20, 1 + len(VECTORIZE_TEXTS) // 3),
        )
        all_rows = sync_empty_table_kms_vectorize.find({}).to_list()
        assert len(all_rows) == len(VECTORIZE_TEXTS)
        assert all(isinstance(row["p_vector"], DataAPIVector) for row in all_rows)

        # include sort vector and include similarity
        cur_isv = sync_empty_table_kms_vectorize.find(
            sort={"p_vector": "This is a fact."},
            include_sort_vector=True,
            include_similarity=True,
        )

        # TODO: restore this check once #1949 is resolved
        # assert isinstance(cur_isv.get_sort_vector(), DataAPIVector)
        match0 = next(cur_isv)
        assert match0 is not None
        # TODO: restore this check once #1949 is resolved
        # assert isinstance(match0["$similarity"], float)

    @pytest.mark.skipif(
        HEADER_EMBEDDING_API_KEY_OPENAI is None,
        reason="No HEADER_EMBEDDING_API_KEY_OPENAI detected",
    )
    @pytest.mark.describe("test of multiple-vectorize table usage via header, sync")
    def test_table_multiplevectorize_header_sync(
        self,
        sync_empty_table_multiplevectorize: DefaultTable,
    ) -> None:
        table = sync_empty_table_multiplevectorize

        table.insert_one(
            {
                "id": "A",
                "p_text": "p_text of A",
                "p_vector_small": "a sentence on mountains",
                "p_vector_large": "telephones and electricity?",
            }
        )

        table.insert_many(
            [
                {
                    "id": "B",
                    "p_text": "p_text of B",
                    "p_vector_small": "rivers, and has only the bee",
                },
                {
                    "id": "C",
                    "p_text": "p_text of C",
                    "p_vector_large": "clouds, and has only the sea",
                },
            ]
        )

        reg_f_rows = table.find(projection={"id": True}).to_list()
        assert {row["id"] for row in reg_f_rows} == {"A", "B", "C"}

        ann_f_small = table.find(
            sort={"p_vector_small": "flowing water and honey-producing animals"},
            projection={"id": True},
        ).to_list()
        assert ann_f_small != []
        assert ann_f_small[0]["id"] == "B"

        ann_f1_small = table.find_one(
            sort={"p_vector_small": "the utterance about orography"},
            projection={"id": True},
        )
        assert ann_f1_small is not None
        assert ann_f1_small["id"] == "A"

        table.insert_one(
            {
                "id": "Z",
                "p_text": "p_text of Z",
                "p_vector_small": "telephones and electricity?",
                "p_vector_large": "a sentence on mountains",
            }
        )

        f1_ele_doc_sm = table.find_one(sort={"p_vector_small": "phones and wires"})
        f1_ele_doc_lg = table.find_one(sort={"p_vector_large": "phones and wires"})
        assert f1_ele_doc_sm is not None
        assert f1_ele_doc_sm["id"] == "Z"
        assert f1_ele_doc_lg is not None
        assert f1_ele_doc_lg["id"] == "A"

        table.update_one(
            {"id": "A"},
            update={
                "$set": {
                    "p_text": "the new p_text for A",
                    "p_vector_small": "glaciers among the woods.",
                },
            },
        )
