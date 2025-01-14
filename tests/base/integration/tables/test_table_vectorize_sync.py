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
        u_row = authenticated_table.find_one(
            filter={"p_text": "u"},
            projection={"p_vector": False},
        )
        n_row = authenticated_table.find_one(
            filter={"p_text": "n"},
            projection={"p_vector": False},
        )
        assert v_row is not None
        assert t_row is not None
        assert u_row is not None
        assert n_row is not None
        assert isinstance(v_row["p_vector"], DataAPIVector)
        assert v_row["p_vector"] == i_vector

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
        assert isinstance(cur_isv.get_sort_vector(), DataAPIVector)
        match0 = next(cur_isv)
        assert match0 is not None
        assert isinstance(match0["$similarity"], float)

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
        assert isinstance(cur_isv.get_sort_vector(), DataAPIVector)
        match0 = next(cur_isv)
        assert match0 is not None
        assert isinstance(match0["$similarity"], float)
