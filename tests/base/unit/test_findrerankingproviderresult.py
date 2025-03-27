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

from astrapy.info import FindRerankingProvidersResult

RESPONSE_DICT = {
    "rerankingProviders": {
        "provider": {
            "isDefault": True,
            "displayName": "TheProvider",
            "supportedAuthentication": {
                "NONE": {
                    "tokens": [],
                    "enabled": True,
                },
            },
            "models": [
                {
                    "name": "provider/<model>",
                    "isDefault": True,
                    "url": "https://<url>/ranking",
                    "properties": None,
                },
            ],
        },
    },
}


class TestFindRerankingProvidersResult:
    @pytest.mark.describe("test of FindRerankingProvidersResult parsing and back")
    def test_reranking_providers_result_parsing_and_back(self) -> None:
        parsed = FindRerankingProvidersResult._from_dict(RESPONSE_DICT)
        providers = parsed.reranking_providers
        assert len(providers) == 1
        assert providers["provider"].display_name is not None

        models = providers["provider"].models
        assert len(models) == 1
        assert models[0].is_default

        dumped = parsed.as_dict()
        assert dumped == RESPONSE_DICT
