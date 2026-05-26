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

import json
from collections.abc import Callable
from typing import Any

import pytest
import werkzeug
from pytest_httpserver import HTTPServer

from astrapy import Collection, Database
from astrapy.data.cursors.query_engine import _CollectionFindAndRerankQueryEngine
from astrapy.info import RerankServiceOptions
from astrapy.utils.api_options import defaultAPIOptions
from astrapy.utils.request_tools import HttpMethod

from ..conftest import DefaultAsyncCollection, DefaultCollection

BASE_PATH = "v1"
KEYSPACE = "keyspace"
COLLECTION_NAME = "collection"
PATH_SUFFIX = f"{KEYSPACE}/{COLLECTION_NAME}"


@pytest.fixture
def mock_collection(httpserver: HTTPServer) -> DefaultCollection:
    base_endpoint = httpserver.url_for("/")
    coll: DefaultCollection = Collection(
        database=Database(
            api_endpoint=base_endpoint,
            keyspace=KEYSPACE,
            api_options=defaultAPIOptions(environment="other"),
        ),
        name=COLLECTION_NAME,
        keyspace=None,
        api_options=defaultAPIOptions(environment="other"),
    )
    return coll


@pytest.fixture
def mock_acollection(
    httpserver: HTTPServer, mock_collection: DefaultCollection
) -> DefaultAsyncCollection:
    return mock_collection.to_async()


def _capture_find_and_rerank_payloads(
    received_payloads: list[dict[str, Any]],
) -> Callable[[werkzeug.Request], werkzeug.Response]:
    def _handler(request: werkzeug.Request) -> werkzeug.Response:
        payload = json.loads(request.get_data(as_text=True))
        received_payloads.append(payload)
        return werkzeug.Response(
            json.dumps({"data": {"documents": [], "nextPageState": None}}),
            content_type="application/json",
        )

    return _handler


def _make_farr_query_engine(
    rerank: RerankServiceOptions | dict[str, Any] | None,
) -> _CollectionFindAndRerankQueryEngine[dict[str, Any]]:
    return _CollectionFindAndRerankQueryEngine(
        collection=None,
        async_collection=None,
        filter=None,
        projection=None,
        sort={"$hybrid": "query"},
        limit=None,
        hybrid_limits=None,
        include_scores=None,
        include_sort_vector=None,
        rerank_on=None,
        rerank_query=None,
        rerank=rerank,
    )


class TestFindAndRerankOptions:
    @pytest.mark.describe("test of find_and_rerank rerank option payload, sync")
    def test_find_and_rerank_rerank_option_payload_sync(
        self,
        httpserver: HTTPServer,
        mock_collection: DefaultCollection,
    ) -> None:
        received_payloads: list[dict[str, Any]] = []
        rerank = RerankServiceOptions(
            provider="nvidia",
            model_name="nv-rerankqa-mistral-4b-v3",
            authentication={"providerKey": "rk"},
            parameters={"truncate": "END"},
        )

        httpserver.expect_oneshot_request(
            f"/{BASE_PATH}/{PATH_SUFFIX}",
            method=HttpMethod.POST,
        ).respond_with_handler(_capture_find_and_rerank_payloads(received_payloads))

        results = mock_collection.find_and_rerank(
            sort={"$hybrid": "query"},
            limit=3,
            rerank=rerank,
        ).to_list()

        assert results == []
        assert received_payloads == [
            {
                "findAndRerank": {
                    "sort": {"$hybrid": "query"},
                    "options": {
                        "limit": 3,
                        "rerank": {
                            "provider": "nvidia",
                            "modelName": "nv-rerankqa-mistral-4b-v3",
                            "authentication": {"providerKey": "rk"},
                            "parameters": {"truncate": "END"},
                        },
                    },
                },
            },
        ]

    @pytest.mark.describe("test of find_and_rerank rerank option payload, async")
    async def test_find_and_rerank_rerank_option_payload_async(
        self,
        httpserver: HTTPServer,
        mock_acollection: DefaultAsyncCollection,
    ) -> None:
        received_payloads: list[dict[str, Any]] = []

        httpserver.expect_oneshot_request(
            f"/{BASE_PATH}/{PATH_SUFFIX}",
            method=HttpMethod.POST,
        ).respond_with_handler(_capture_find_and_rerank_payloads(received_payloads))

        results = await mock_acollection.find_and_rerank(
            sort={"$hybrid": {"$vectorize": "query", "$lexical": "lexical query"}},
            include_scores=True,
            rerank={
                "provider": "cohere",
                "modelName": "rerank-english-v3.0",
            },
        ).to_list()

        assert results == []
        assert received_payloads == [
            {
                "findAndRerank": {
                    "sort": {
                        "$hybrid": {
                            "$vectorize": "query",
                            "$lexical": "lexical query",
                        },
                    },
                    "options": {
                        "includeScores": True,
                        "rerank": {
                            "provider": "cohere",
                            "modelName": "rerank-english-v3.0",
                        },
                    },
                },
            },
        ]

    @pytest.mark.describe("test of find_and_rerank rerank empty options")
    def test_find_and_rerank_rerank_empty_options(self) -> None:
        assert "rerank" not in _make_farr_query_engine(None).f_options0
        assert _make_farr_query_engine({}).f_options0["rerank"] == {}
