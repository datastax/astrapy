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

from astrapy import Database
from astrapy.info import FindRerankingProvidersResult, RerankingProvider

from ..conftest import IS_ASTRA_DB


def _count_models(frp_result: FindRerankingProvidersResult) -> int:
    return len(
        [
            model
            for prov_v in frp_result.reranking_providers.values()
            for model in prov_v.models
        ]
    )


class TestRerankingOpsSync:
    @pytest.mark.describe("test of find_reranking_providers, sync")
    def test_findrerankingproviders_sync(
        self,
        sync_database: Database,
    ) -> None:
        database_admin = sync_database.get_database_admin()
        rp_result = database_admin.find_reranking_providers()

        assert isinstance(rp_result, FindRerankingProvidersResult)

        assert all(
            isinstance(rer_prov, RerankingProvider)
            for rer_prov in rp_result.reranking_providers.values()
        )

        # 'raw_info' not compared, for resiliency against newly-introduced fields
        assert (
            FindRerankingProvidersResult._from_dict(
                rp_result.as_dict()
            ).reranking_providers
            == rp_result.reranking_providers
        )

    @pytest.mark.skipif(
        "ASTRAPY_TEST_LATEST_MAIN" not in os.environ,
        reason="No 'latest main' tests required.",
    )
    @pytest.mark.skipif(IS_ASTRA_DB, reason="Filtering models not yet on Astra DB")
    @pytest.mark.describe("test of find_reranking_providers filtering, sync")
    def test_filtered_findrerankingproviders_sync(
        self,
        sync_database: Database,
    ) -> None:
        database_admin = sync_database.get_database_admin()
        default_count = _count_models(database_admin.find_reranking_providers())

        all_count = _count_models(
            database_admin.find_reranking_providers(filter_model_status="")
        )

        sup_count = _count_models(
            database_admin.find_reranking_providers(filter_model_status="SUPPORTED")
        )
        dep_count = _count_models(
            database_admin.find_reranking_providers(filter_model_status="DEPRECATED")
        )
        eol_count = _count_models(
            database_admin.find_reranking_providers(filter_model_status="END_OF_LIFE")
        )

        assert sup_count + dep_count + eol_count == all_count
        assert sup_count == default_count
