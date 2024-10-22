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

"""
Unit tests for the parsing of API endpoints and related
"""

from __future__ import annotations

import time

import pytest
import werkzeug
from pytest_httpserver import HTTPServer

from astrapy.exceptions import DataAPITimeoutException, DevOpsAPITimeoutException
from astrapy.utils.api_commander import APICommander
from astrapy.utils.request_tools import HttpMethod

SLEEPER_TIME_MS = 500
TIMEOUT_PARAM_MS = 100


def response_sleeper(request: werkzeug.Request) -> werkzeug.Response:
    time.sleep(SLEEPER_TIME_MS / 1000)
    return werkzeug.Response()


class TestTimeouts:
    @pytest.mark.describe("test of APICommander timeout, sync")
    def test_apicommander_timeout_sync(self, httpserver: HTTPServer) -> None:
        base_endpoint = httpserver.url_for("/")
        base_path = "/base"
        cmd = APICommander(
            api_endpoint=base_endpoint,
            path=base_path,
        )

        httpserver.expect_oneshot_request(
            base_path,
            method=HttpMethod.POST,
        ).respond_with_handler(response_sleeper)
        with pytest.raises(DataAPITimeoutException):
            cmd.request(timeout_ms=TIMEOUT_PARAM_MS)

    @pytest.mark.describe("test of APICommander timeout, async")
    async def test_apicommander_timeout_async(self, httpserver: HTTPServer) -> None:
        base_endpoint = httpserver.url_for("/")
        base_path = "/base"
        cmd = APICommander(
            api_endpoint=base_endpoint,
            path=base_path,
        )

        httpserver.expect_oneshot_request(
            base_path,
            method=HttpMethod.POST,
        ).respond_with_handler(response_sleeper)
        with pytest.raises(DataAPITimeoutException):
            await cmd.async_request(timeout_ms=TIMEOUT_PARAM_MS)

    @pytest.mark.describe("test of APICommander timeout DevOps, sync")
    def test_apicommander_timeout_devops_sync(self, httpserver: HTTPServer) -> None:
        base_endpoint = httpserver.url_for("/")
        base_path = "/base"
        cmd = APICommander(
            api_endpoint=base_endpoint,
            path=base_path,
            dev_ops_api=True,
        )

        httpserver.expect_oneshot_request(
            base_path,
            method=HttpMethod.POST,
        ).respond_with_handler(response_sleeper)
        with pytest.raises(DevOpsAPITimeoutException):
            cmd.request(timeout_ms=TIMEOUT_PARAM_MS)

    @pytest.mark.describe("test of APICommander timeout DevOps, async")
    async def test_apicommander_timeout_devops_async(
        self, httpserver: HTTPServer
    ) -> None:
        base_endpoint = httpserver.url_for("/")
        base_path = "/base"
        cmd = APICommander(
            api_endpoint=base_endpoint,
            path=base_path,
            dev_ops_api=True,
        )

        httpserver.expect_oneshot_request(
            base_path,
            method=HttpMethod.POST,
        ).respond_with_handler(response_sleeper)
        with pytest.raises(DevOpsAPITimeoutException):
            await cmd.async_request(timeout_ms=TIMEOUT_PARAM_MS)
