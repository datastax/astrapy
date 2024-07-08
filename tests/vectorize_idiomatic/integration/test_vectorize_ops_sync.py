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

from astrapy import Database
from astrapy.info import EmbeddingProvider


class TestVectorizeOpsSync:
    @pytest.mark.describe("test of find_embedding_providers, sync")
    def test_collection_methods_vectorize_sync(
        self,
        sync_database: Database,
    ) -> None:
        database_admin = sync_database.get_database_admin()
        ep_map = database_admin.find_embedding_providers()

        assert all(
            isinstance(emb_prov, EmbeddingProvider) for emb_prov in ep_map.values()
        )

        reconstructed = {
            ep_name: EmbeddingProvider.from_dict(emb_prov.as_dict())
            for ep_name, emb_prov in ep_map.items()
        }
        assert reconstructed == ep_map
