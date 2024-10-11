from __future__ import annotations

import time

import pytest
import werkzeug
from pytest_httpserver import HTTPServer

from astrapy.exceptions import DataAPITimeoutException, DevOpsAPITimeoutException
from astrapy.utils.api_commander import APICommander
from astrapy.utils.request_tools import HttpMethod

SLEEPER_TIME_S = 0.5
TIMEOUT_PARAM_S = 0.1


def response_sleeper(request: werkzeug.Request) -> werkzeug.Response:
    time.sleep(SLEEPER_TIME_S)
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
            cmd.request(timeout_info=TIMEOUT_PARAM_S)

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
            await cmd.async_request(timeout_info=TIMEOUT_PARAM_S)

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
            cmd.request(timeout_info=TIMEOUT_PARAM_S)

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
            await cmd.async_request(timeout_info=TIMEOUT_PARAM_S)
