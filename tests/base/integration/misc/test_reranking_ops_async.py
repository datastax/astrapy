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
from astrapy.info import FindRerankingProvidersResult, RerankingProvider


class TestRerankingOpsAsync:
    @pytest.mark.describe("test of find_reranking_providers, async")
    async def test_findrerankingproviders_async(
        self,
        async_database: AsyncDatabase,
    ) -> None:
        database_admin = async_database.get_database_admin()
        rp_result = await database_admin.async_find_reranking_providers()

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
