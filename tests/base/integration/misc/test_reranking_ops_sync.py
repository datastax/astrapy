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


@pytest.mark.skipif(
    "ASTRAPY_TEST_FINDANDRERANK" not in os.environ,
    reason="Disabled except on bleeding-edge main so far",
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

        assert FindRerankingProvidersResult._from_dict(rp_result.as_dict()) == rp_result
