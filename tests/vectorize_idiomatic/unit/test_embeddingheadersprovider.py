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

import pytest

from astrapy.authentication import (
    EMBEDDING_HEADER_API_KEY,
    EMBEDDING_HEADER_AWS_ACCESS_ID,
    EMBEDDING_HEADER_AWS_SECRET_ID,
    AWSEmbeddingHeadersProvider,
    StaticEmbeddingHeadersProvider,
    coerce_embedding_headers_provider,
)


class TestEmbeddingHeadersProvider:
    @pytest.mark.describe("test of headers from StaticEmbeddingHeadersProvider")
    def test_embeddingheadersprovider_static(self) -> None:
        ehp = StaticEmbeddingHeadersProvider("x")
        assert {k.lower(): v for k, v in ehp.get_headers().items()} == {
            EMBEDDING_HEADER_API_KEY.lower(): "x"
        }

    @pytest.mark.describe("test of headers from empty StaticEmbeddingHeadersProvider")
    def test_embeddingheadersprovider_null(self) -> None:
        ehp = StaticEmbeddingHeadersProvider(None)
        assert ehp.get_headers() == {}

    @pytest.mark.describe("test of headers from AWSEmbeddingHeadersProvider")
    def test_embeddingheadersprovider_aws(self) -> None:
        ehp = AWSEmbeddingHeadersProvider(
            embedding_access_id="x",
            embedding_secret_id="y",
        )
        gen_headers_lower = {k.lower(): v for k, v in ehp.get_headers().items()}
        exp_headers_lower = {
            EMBEDDING_HEADER_AWS_ACCESS_ID.lower(): "x",
            EMBEDDING_HEADER_AWS_SECRET_ID.lower(): "y",
        }
        assert gen_headers_lower == exp_headers_lower

    @pytest.mark.describe("test of embedding headers provider coercion")
    def test_embeddingheadersprovider_coercion(self) -> None:
        """This doubles as equality test."""
        ehp_s = StaticEmbeddingHeadersProvider("x")
        ehp_n = StaticEmbeddingHeadersProvider(None)
        ehp_a = AWSEmbeddingHeadersProvider(
            embedding_access_id="x",
            embedding_secret_id="y",
        )
        assert coerce_embedding_headers_provider(ehp_s) == ehp_s
        assert coerce_embedding_headers_provider(ehp_n) == ehp_n
        assert coerce_embedding_headers_provider(ehp_a) == ehp_a

        assert coerce_embedding_headers_provider("x") == ehp_s
        assert coerce_embedding_headers_provider(None) == ehp_n
