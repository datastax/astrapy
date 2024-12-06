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

from astrapy import AsyncDatabase
from astrapy.info import EmbeddingProvider, FindEmbeddingProvidersResult

from ..conftest import clean_nulls_from_dict


class TestVectorizeOpsAsync:
    @pytest.mark.describe("test of find_embedding_providers, async")
    async def test_collection_methods_vectorize_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        database_admin = async_database.get_database_admin()
        ep_result = database_admin.find_embedding_providers()

        assert isinstance(ep_result, FindEmbeddingProvidersResult)

        assert all(
            isinstance(emb_prov, EmbeddingProvider)
            for emb_prov in ep_result.embedding_providers.values()
        )

        reconstructed = {
            ep_name: EmbeddingProvider._from_dict(emb_prov.as_dict())
            for ep_name, emb_prov in ep_result.embedding_providers.items()
        }
        assert reconstructed == ep_result.embedding_providers
        dict_mapping = {
            ep_name: emb_prov.as_dict()
            for ep_name, emb_prov in ep_result.embedding_providers.items()
        }
        cleaned_dict_mapping = clean_nulls_from_dict(dict_mapping)
        cleaned_raw_info = clean_nulls_from_dict(
            ep_result.raw_info["embeddingProviders"]  # type: ignore[index]
        )
        assert cleaned_dict_mapping == cleaned_raw_info
