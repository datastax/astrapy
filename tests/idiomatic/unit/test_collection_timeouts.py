from __future__ import annotations

import time

import pytest
import werkzeug
from pytest_httpserver import HTTPServer

from astrapy import AsyncCollection, Collection, Database
from astrapy.exceptions import DataAPITimeoutException
from astrapy.utils.api_options import defaultAPIOptions
from astrapy.utils.request_tools import HttpMethod

SLEEPER_TIME_MS = 100
TIMEOUT_PARAM_MS = 1

BASE_PATH = "v1"
KEYSPACE = "keyspace"
COLLECTION_NAME = "collection"
PATH_SUFFIX = f"{KEYSPACE}/{COLLECTION_NAME}"


@pytest.fixture
def mock_collection(httpserver: HTTPServer) -> Collection:
    base_endpoint = httpserver.url_for("/")
    coll = Collection(
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
    httpserver: HTTPServer, mock_collection: Collection
) -> AsyncCollection:
    return mock_collection.to_async()


def response_sleeper(request: werkzeug.Request) -> werkzeug.Response:
    time.sleep(SLEEPER_TIME_MS / 1000)
    return werkzeug.Response()


class TestCollectionTimeouts:
    @pytest.mark.describe("test of collection count_documents timeout, sync")
    def test_collection_count_documents_timeout_sync(
        self,
        httpserver: HTTPServer,
        mock_collection: Collection,
    ) -> None:
        httpserver.expect_oneshot_request(
            f"/{BASE_PATH}/{PATH_SUFFIX}",
            method=HttpMethod.POST,
        ).respond_with_json({"status": {"count": 500}})
        mock_collection.count_documents({}, upper_bound=800)

        httpserver.expect_oneshot_request(
            f"/{BASE_PATH}/{PATH_SUFFIX}",
            method=HttpMethod.POST,
        ).respond_with_handler(response_sleeper)
        with pytest.raises(DataAPITimeoutException):
            mock_collection.count_documents(
                {}, upper_bound=800, max_time_ms=TIMEOUT_PARAM_MS
            )

    @pytest.mark.describe("test of collection count_documents timeout, async")
    async def test_collection_count_documents_timeout_async(
        self,
        httpserver: HTTPServer,
        mock_acollection: AsyncCollection,
    ) -> None:
        httpserver.expect_oneshot_request(
            f"/{BASE_PATH}/{PATH_SUFFIX}",
            method=HttpMethod.POST,
        ).respond_with_json({"status": {"count": 500}})
        await mock_acollection.count_documents({}, upper_bound=800)

        httpserver.expect_oneshot_request(
            f"/{BASE_PATH}/{PATH_SUFFIX}",
            method=HttpMethod.POST,
        ).respond_with_handler(response_sleeper)
        with pytest.raises(DataAPITimeoutException):
            await mock_acollection.count_documents(
                {}, upper_bound=800, max_time_ms=TIMEOUT_PARAM_MS
            )

    @pytest.mark.describe("test of collection cursor-based timeouts, async")
    async def test_collection_cursor_timeouts_async(
        self,
        httpserver: HTTPServer,
        mock_acollection: AsyncCollection,
    ) -> None:
        httpserver.expect_oneshot_request(
            f"/{BASE_PATH}/{PATH_SUFFIX}",
            method=HttpMethod.POST,
        ).respond_with_json({"data": {"nextPageState": None, "documents": [{"a": 1}]}})
        cur0 = mock_acollection.find({})
        await cur0.__anext__()

        cur1 = mock_acollection.find({}, max_time_ms=1)
        httpserver.expect_oneshot_request(
            f"/{BASE_PATH}/{PATH_SUFFIX}",
            method=HttpMethod.POST,
        ).respond_with_handler(response_sleeper)
        with pytest.raises(DataAPITimeoutException):
            await cur1.__anext__()

        httpserver.expect_oneshot_request(
            f"/{BASE_PATH}/{PATH_SUFFIX}",
            method=HttpMethod.POST,
        ).respond_with_json({"data": {"nextPageState": None, "documents": [{"a": 1}]}})
        await mock_acollection.find_one({})

        httpserver.expect_oneshot_request(
            f"/{BASE_PATH}/{PATH_SUFFIX}",
            method=HttpMethod.POST,
        ).respond_with_handler(response_sleeper)
        with pytest.raises(DataAPITimeoutException):
            await mock_acollection.find_one({}, max_time_ms=1)

    @pytest.mark.describe("test of collection cursor-based timeouts, sync")
    def test_collection_cursor_timeouts_sync(
        self,
        httpserver: HTTPServer,
        mock_collection: Collection,
    ) -> None:
        httpserver.expect_oneshot_request(
            f"/{BASE_PATH}/{PATH_SUFFIX}",
            method=HttpMethod.POST,
        ).respond_with_json({"data": {"nextPageState": None, "documents": [{"a": 1}]}})
        cur0 = mock_collection.find({})
        cur0.__next__()

        cur1 = mock_collection.find({}, max_time_ms=1)
        httpserver.expect_oneshot_request(
            f"/{BASE_PATH}/{PATH_SUFFIX}",
            method=HttpMethod.POST,
        ).respond_with_handler(response_sleeper)
        with pytest.raises(DataAPITimeoutException):
            cur1.__next__()

        httpserver.expect_oneshot_request(
            f"/{BASE_PATH}/{PATH_SUFFIX}",
            method=HttpMethod.POST,
        ).respond_with_json({"data": {"nextPageState": None, "documents": [{"a": 1}]}})
        mock_collection.find_one({})

        httpserver.expect_oneshot_request(
            f"/{BASE_PATH}/{PATH_SUFFIX}",
            method=HttpMethod.POST,
        ).respond_with_handler(response_sleeper)
        with pytest.raises(DataAPITimeoutException):
            mock_collection.find_one({}, max_time_ms=1)
