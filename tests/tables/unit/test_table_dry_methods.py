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
from pytest_httpserver import HTTPServer

from astrapy import Database, Table
from astrapy.exceptions import TooManyRowsToCountException
from astrapy.utils.api_options import defaultAPIOptions
from astrapy.utils.request_tools import HttpMethod

from ..conftest import DefaultAsyncTable, DefaultTable

BASE_PATH = "v1"
KEYSPACE = "keyspace"
TABLE_NAME = "table"
PATH_SUFFIX = f"{KEYSPACE}/{TABLE_NAME}"


@pytest.fixture
def mock_table(httpserver: HTTPServer) -> DefaultTable:
    base_endpoint = httpserver.url_for("/")
    table: DefaultTable = Table(
        database=Database(
            api_endpoint=base_endpoint,
            keyspace=KEYSPACE,
            api_options=defaultAPIOptions(environment="other"),
        ),
        name=TABLE_NAME,
        keyspace=None,
        api_options=defaultAPIOptions(environment="other"),
    )
    return table


@pytest.fixture
def mock_atable(httpserver: HTTPServer, mock_table: DefaultTable) -> DefaultAsyncTable:
    return mock_table.to_async()


class TestTableDryMethods:
    @pytest.mark.describe("test of table estimated_document_count and alias, sync")
    def test_table_estimated_document_count_sync(
        self,
        httpserver: HTTPServer,
        mock_table: DefaultTable,
    ) -> None:
        httpserver.expect_oneshot_request(
            f"/{BASE_PATH}/{PATH_SUFFIX}",
            method=HttpMethod.POST,
        ).respond_with_json({"status": {"count": 500}})
        assert mock_table.estimated_document_count() == 500

    @pytest.mark.describe("test of table estimated_document_count and alias, async")
    async def test_table_estimated_document_count_async(
        self,
        httpserver: HTTPServer,
        mock_atable: DefaultAsyncTable,
    ) -> None:
        httpserver.expect_oneshot_request(
            f"/{BASE_PATH}/{PATH_SUFFIX}",
            method=HttpMethod.POST,
        ).respond_with_json({"status": {"count": 500}})
        assert await mock_atable.estimated_document_count() == 500

    @pytest.mark.describe("test of table count_documents and alias, sync")
    def test_table_count_documents_sync(
        self,
        httpserver: HTTPServer,
        mock_table: DefaultTable,
    ) -> None:
        httpserver.expect_oneshot_request(
            f"/{BASE_PATH}/{PATH_SUFFIX}",
            method=HttpMethod.POST,
        ).respond_with_json({"status": {"count": 500}})
        assert mock_table.count_documents({}, upper_bound=650) == 500

        httpserver.expect_oneshot_request(
            f"/{BASE_PATH}/{PATH_SUFFIX}",
            method=HttpMethod.POST,
        ).respond_with_json({"status": {"count": 1000, "moreData": True}})
        with pytest.raises(TooManyRowsToCountException):
            mock_table.count_documents({}, upper_bound=1000)

        httpserver.expect_oneshot_request(
            f"/{BASE_PATH}/{PATH_SUFFIX}",
            method=HttpMethod.POST,
        ).respond_with_json({"status": {"count": 50}})
        with pytest.raises(TooManyRowsToCountException):
            mock_table.count_documents({}, upper_bound=20)

    @pytest.mark.describe("test of table count_documents and alias, async")
    async def test_table_count_documents_async(
        self,
        httpserver: HTTPServer,
        mock_atable: DefaultAsyncTable,
    ) -> None:
        httpserver.expect_oneshot_request(
            f"/{BASE_PATH}/{PATH_SUFFIX}",
            method=HttpMethod.POST,
        ).respond_with_json({"status": {"count": 500}})
        assert await mock_atable.count_documents({}, upper_bound=650) == 500

        httpserver.expect_oneshot_request(
            f"/{BASE_PATH}/{PATH_SUFFIX}",
            method=HttpMethod.POST,
        ).respond_with_json({"status": {"count": 1000, "moreData": True}})
        with pytest.raises(TooManyRowsToCountException):
            await mock_atable.count_documents({}, upper_bound=1000)

        httpserver.expect_oneshot_request(
            f"/{BASE_PATH}/{PATH_SUFFIX}",
            method=HttpMethod.POST,
        ).respond_with_json({"status": {"count": 50}})
        with pytest.raises(TooManyRowsToCountException):
            await mock_atable.count_documents({}, upper_bound=20)

    @pytest.mark.describe("test of table update_one, sync")
    def test_table_update_one_sync(
        self,
        httpserver: HTTPServer,
        mock_table: DefaultTable,
    ) -> None:
        httpserver.expect_oneshot_request(
            f"/{BASE_PATH}/{PATH_SUFFIX}",
            method=HttpMethod.POST,
        ).respond_with_json({"status": {}})
        mock_table.update_one({"pk": "v"}, {"$set": {"x": {1, 2, 3}}})

    @pytest.mark.describe("test of table update_one, async")
    async def test_table_update_one_async(
        self,
        httpserver: HTTPServer,
        mock_atable: DefaultAsyncTable,
    ) -> None:
        httpserver.expect_oneshot_request(
            f"/{BASE_PATH}/{PATH_SUFFIX}",
            method=HttpMethod.POST,
        ).respond_with_json({"status": {}})
        await mock_atable.update_one({"pk": "v"}, {"$set": {"x": {1, 2, 3}}})
