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

from ..conftest import IS_ASTRA_DB, clean_nulls_from_dict


def _count_models(fep_result: FindEmbeddingProvidersResult) -> int:
    return len(
        [
            model
            for prov_v in fep_result.embedding_providers.values()
            for model in prov_v.models
        ]
    )


class TestVectorizeOpsAsync:
    @pytest.mark.describe("test of find_embedding_providers, async")
    async def test_findembeddingproviders_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        database_admin = async_database.get_database_admin()
        ep_result = await database_admin.async_find_embedding_providers()

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
        if IS_ASTRA_DB:
            # TODO remove this (Astra-only) cleanup once apiModelSupport deployed
            raw_info_has_ams = any(
                "apiModelSupport" in model_dict
                for prov_v in cleaned_raw_info.values()
                for model_dict in prov_v["models"]
            )
            if not raw_info_has_ams:
                for emb_prov in cleaned_dict_mapping.values():
                    for model in emb_prov["models"]:
                        if "apiModelSupport" in model:
                            del model["apiModelSupport"]
        assert cleaned_dict_mapping == cleaned_raw_info

    # TODO: open to Astra DB once available
    @pytest.mark.skipif(IS_ASTRA_DB, reason="Filtering models not yet on Astra DB")
    @pytest.mark.describe("test of find_embedding_providers filtering, async")
    async def test_filtered_findembeddingproviders_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        database_admin = async_database.get_database_admin()
        default_count = _count_models(
            await database_admin.async_find_embedding_providers()
        )

        all_count = _count_models(
            await database_admin.async_find_embedding_providers(filter_model_status="")
        )

        sup_count = _count_models(
            await database_admin.async_find_embedding_providers(
                filter_model_status="SUPPORTED"
            )
        )
        dep_count = _count_models(
            await database_admin.async_find_embedding_providers(
                filter_model_status="DEPRECATED"
            )
        )
        eol_count = _count_models(
            await database_admin.async_find_embedding_providers(
                filter_model_status="END_OF_LIFE"
            )
        )

        assert sup_count + dep_count + eol_count == all_count
        assert sup_count == default_count
