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

from astrapy.authentication import (
    RerankingAPIKeyHeaderProvider,
    coerce_reranking_headers_provider,
)
from astrapy.settings.defaults import RERANKING_HEADER_API_KEY


class TestRerankingHeadersProvider:
    @pytest.mark.describe("test of headers from RerankingAPIKeyHeaderProvider")
    def test_rerankingheadersprovider_static(self) -> None:
        ehp = RerankingAPIKeyHeaderProvider("x")
        assert {k.lower(): v for k, v in ehp.get_headers().items()} == {
            RERANKING_HEADER_API_KEY.lower(): "x"
        }

    @pytest.mark.describe("test of headers from empty RerankingAPIKeyHeaderProvider")
    def test_rerankingheadersprovider_null(self) -> None:
        ehp = RerankingAPIKeyHeaderProvider(None)
        assert ehp.get_headers() == {}

    @pytest.mark.describe("test of reranking headers provider coercion")
    def test_rerankingheadersprovider_coercion(self) -> None:
        """This doubles as equality test."""
        ehp_s = RerankingAPIKeyHeaderProvider("x")
        ehp_n = RerankingAPIKeyHeaderProvider(None)
        assert coerce_reranking_headers_provider(ehp_s) == ehp_s
        assert coerce_reranking_headers_provider(ehp_n) == ehp_n

        assert coerce_reranking_headers_provider("x") == ehp_s
