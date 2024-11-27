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

import json
import time
from typing import Any, Callable

import pytest
import werkzeug
from pytest_httpserver import HTTPServer

from astrapy import DataAPIClient
from astrapy.api_options import APIOptions, TimeoutOptions
from astrapy.exceptions import (
    DataAPITimeoutException,
    DevOpsAPITimeoutException,
    _TimeoutContext,
)
from astrapy.utils.api_commander import APICommander
from astrapy.utils.request_tools import HttpMethod

SLEEPER_TIME_MS = 500
TIMEOUT_PARAM_MS = 100


def response_sleeper(request: werkzeug.Request) -> werkzeug.Response:
    time.sleep(SLEEPER_TIME_MS / 1000)
    return werkzeug.Response()


def delayed_response_handler(
    delay_ms: int, response_json: dict[str, Any]
) -> Callable[[werkzeug.Request], werkzeug.Response]:
    def _response_sleeper(request: werkzeug.Request) -> werkzeug.Response:
        time.sleep(delay_ms / 1000)
        return werkzeug.Response(json.dumps(response_json))

    return _response_sleeper


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
            cmd.request(timeout_context=_TimeoutContext(request_ms=TIMEOUT_PARAM_MS))

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
            await cmd.async_request(
                timeout_context=_TimeoutContext(request_ms=TIMEOUT_PARAM_MS)
            )

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
            cmd.request(timeout_context=_TimeoutContext(request_ms=TIMEOUT_PARAM_MS))

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
            await cmd.async_request(
                timeout_context=_TimeoutContext(request_ms=TIMEOUT_PARAM_MS)
            )

    @pytest.mark.describe(
        "test of timeout occurring, zero and nonzero, for Collection class, sync"
    )
    def test_collection_timeout_occurring_sync(self, httpserver: HTTPServer) -> None:
        root_endpoint = httpserver.url_for("/")
        client = DataAPIClient(environment="other")
        database = client.get_database(root_endpoint, keyspace="xkeyspace")
        # S_L = short timeout for request; long timeout for general-method
        collection_S_L = database.get_collection(
            "xcollt",
            spawn_api_options=APIOptions(
                timeout_options=TimeoutOptions(
                    request_timeout_ms=1,
                    general_method_timeout_ms=10000,
                ),
            ),
        )
        collection_L_S = database.get_collection(
            "xcollt",
            spawn_api_options=APIOptions(
                timeout_options=TimeoutOptions(
                    request_timeout_ms=10000,
                    general_method_timeout_ms=1,
                ),
            ),
        )
        expected_url = "/v1/xkeyspace/xcollt"

        # no method arg, the short request_ms makes it timeout
        httpserver.expect_oneshot_request(
            expected_url,
            method="POST",
        ).respond_with_handler(
            delayed_response_handler(
                delay_ms=1200,
                response_json={
                    "status": {
                        "deletedCount": 12,
                        "moreData": False,
                    },
                },
            )
        )
        with pytest.raises(DataAPITimeoutException):
            collection_S_L.delete_many({})

        # no method arg, the short generalmethod makes it timeout
        httpserver.expect_oneshot_request(
            expected_url,
            method="POST",
        ).respond_with_handler(
            delayed_response_handler(
                delay_ms=1200,
                response_json={
                    "status": {
                        "deletedCount": 12,
                        "moreData": False,
                    },
                },
            )
        )
        with pytest.raises(DataAPITimeoutException):
            collection_L_S.delete_many({})

        # this avoids dangling timing-out response to pollute next test:
        httpserver.stop()  # type: ignore[no-untyped-call]
        httpserver.start()  # type: ignore[no-untyped-call]

    @pytest.mark.describe(
        "test of timeout suppression, zero and nonzero, for Collection class, sync"
    )
    def test_collection_timeout_suppression_sync(self, httpserver: HTTPServer) -> None:
        # various ways to raise or disable timeouts and have requests succeed on time
        root_endpoint = httpserver.url_for("/")
        client = DataAPIClient(environment="other")
        database = client.get_database(root_endpoint, keyspace="xkeyspace")
        # S_L = short timeout for request; long timeout for general-method
        collection_S_L = database.get_collection(
            "xcoll",
            spawn_api_options=APIOptions(
                timeout_options=TimeoutOptions(
                    request_timeout_ms=1,
                    general_method_timeout_ms=10000,
                ),
            ),
        )
        collection_L_S = database.get_collection(
            "xcoll",
            spawn_api_options=APIOptions(
                timeout_options=TimeoutOptions(
                    request_timeout_ms=10000,
                    general_method_timeout_ms=1,
                ),
            ),
        )
        expected_url = "/v1/xkeyspace/xcoll"

        # remove the timeout with method reqtimeout override
        httpserver.expect_oneshot_request(
            expected_url,
            method="POST",
        ).respond_with_handler(
            delayed_response_handler(
                delay_ms=1200,
                response_json={
                    "status": {
                        "deletedCount": 12,
                        "moreData": False,
                    },
                },
            )
        )
        dmr_SL_rq = collection_S_L.delete_many({}, request_timeout_ms=1500)
        assert dmr_SL_rq.deleted_count == 12

        # remove the timeout with method genmeth-timeout override
        httpserver.expect_oneshot_request(
            expected_url,
            method="POST",
        ).respond_with_handler(
            delayed_response_handler(
                delay_ms=1200,
                response_json={
                    "status": {
                        "deletedCount": 12,
                        "moreData": False,
                    },
                },
            )
        )
        dmr_LS_rq = collection_L_S.delete_many({}, general_method_timeout_ms=1500)
        assert dmr_LS_rq.deleted_count == 12

        # remove the timeout completely with a zero per-method per-req timeout
        httpserver.expect_oneshot_request(
            expected_url,
            method="POST",
        ).respond_with_handler(
            delayed_response_handler(
                delay_ms=1200,
                response_json={
                    "status": {
                        "deletedCount": 12,
                        "moreData": False,
                    },
                },
            )
        )
        dmr_SL_zrq = collection_S_L.delete_many({}, request_timeout_ms=0)
        assert dmr_SL_zrq.deleted_count == 12

        # remove the timeout completely with a zero per-method genmeth-timeout override
        httpserver.expect_oneshot_request(
            expected_url,
            method="POST",
        ).respond_with_handler(
            delayed_response_handler(
                delay_ms=1200,
                response_json={
                    "status": {
                        "deletedCount": 12,
                        "moreData": False,
                    },
                },
            )
        )
        dmr_LS_zrq = collection_L_S.delete_many({}, general_method_timeout_ms=0)
        assert dmr_LS_zrq.deleted_count == 12

        # remove the timeout completely with a zero per-method 'timeout_ms' shorthand
        httpserver.expect_oneshot_request(
            expected_url,
            method="POST",
        ).respond_with_handler(
            delayed_response_handler(
                delay_ms=1200,
                response_json={
                    "status": {
                        "deletedCount": 12,
                        "moreData": False,
                    },
                },
            )
        )
        dmr_LS_zgrq = collection_L_S.delete_many({}, timeout_ms=0)
        assert dmr_LS_zgrq.deleted_count == 12

    @pytest.mark.describe(
        "test of timeout occurring, zero and nonzero, for Collection class, async"
    )
    async def test_collection_timeout_occurring_async(
        self, httpserver: HTTPServer
    ) -> None:
        root_endpoint = httpserver.url_for("/")
        client = DataAPIClient(environment="other")
        database = client.get_database(root_endpoint, keyspace="xkeyspace")
        # S_L = short timeout for request; long timeout for general-method
        acollection_S_L = await database.to_async().get_collection(
            "xcollt",
            spawn_api_options=APIOptions(
                timeout_options=TimeoutOptions(
                    request_timeout_ms=1,
                    general_method_timeout_ms=10000,
                ),
            ),
        )
        acollection_L_S = await database.to_async().get_collection(
            "xcollt",
            spawn_api_options=APIOptions(
                timeout_options=TimeoutOptions(
                    request_timeout_ms=10000,
                    general_method_timeout_ms=1,
                ),
            ),
        )
        expected_url = "/v1/xkeyspace/xcollt"

        # no method arg, the short request_ms makes it timeout
        httpserver.expect_oneshot_request(
            expected_url,
            method="POST",
        ).respond_with_handler(
            delayed_response_handler(
                delay_ms=1200,
                response_json={
                    "status": {
                        "deletedCount": 12,
                        "moreData": False,
                    },
                },
            )
        )
        with pytest.raises(DataAPITimeoutException):
            await acollection_S_L.delete_many({})

        # no method arg, the short generalmethod makes it timeout
        httpserver.expect_oneshot_request(
            expected_url,
            method="POST",
        ).respond_with_handler(
            delayed_response_handler(
                delay_ms=1200,
                response_json={
                    "status": {
                        "deletedCount": 12,
                        "moreData": False,
                    },
                },
            )
        )
        with pytest.raises(DataAPITimeoutException):
            await acollection_L_S.delete_many({})

        # this avoids dangling timing-out response to pollute next test:
        httpserver.stop()  # type: ignore[no-untyped-call]
        httpserver.start()  # type: ignore[no-untyped-call]

    @pytest.mark.describe(
        "test of timeout suppression, zero and nonzero, for Collection class, async"
    )
    async def test_collection_timeout_suppression_async(
        self, httpserver: HTTPServer
    ) -> None:
        # various ways to raise or disable timeouts and have requests succeed on time
        root_endpoint = httpserver.url_for("/")
        client = DataAPIClient(environment="other")
        database = client.get_database(root_endpoint, keyspace="xkeyspace")
        # S_L = short timeout for request; long timeout for general-method
        acollection_S_L = await database.to_async().get_collection(
            "xcoll",
            spawn_api_options=APIOptions(
                timeout_options=TimeoutOptions(
                    request_timeout_ms=1,
                    general_method_timeout_ms=10000,
                ),
            ),
        )
        acollection_L_S = await database.to_async().get_collection(
            "xcoll",
            spawn_api_options=APIOptions(
                timeout_options=TimeoutOptions(
                    request_timeout_ms=10000,
                    general_method_timeout_ms=1,
                ),
            ),
        )
        expected_url = "/v1/xkeyspace/xcoll"

        # remove the timeout with method reqtimeout override
        httpserver.expect_oneshot_request(
            expected_url,
            method="POST",
        ).respond_with_handler(
            delayed_response_handler(
                delay_ms=1200,
                response_json={
                    "status": {
                        "deletedCount": 12,
                        "moreData": False,
                    },
                },
            )
        )
        dmr_SL_rq = await acollection_S_L.delete_many({}, request_timeout_ms=1500)
        assert dmr_SL_rq.deleted_count == 12

        # remove the timeout with method genmeth-timeout override
        httpserver.expect_oneshot_request(
            expected_url,
            method="POST",
        ).respond_with_handler(
            delayed_response_handler(
                delay_ms=1200,
                response_json={
                    "status": {
                        "deletedCount": 12,
                        "moreData": False,
                    },
                },
            )
        )
        dmr_LS_rq = await acollection_L_S.delete_many(
            {}, general_method_timeout_ms=1500
        )
        assert dmr_LS_rq.deleted_count == 12

        # remove the timeout completely with a zero per-method per-req timeout
        httpserver.expect_oneshot_request(
            expected_url,
            method="POST",
        ).respond_with_handler(
            delayed_response_handler(
                delay_ms=1200,
                response_json={
                    "status": {
                        "deletedCount": 12,
                        "moreData": False,
                    },
                },
            )
        )
        dmr_SL_zrq = await acollection_S_L.delete_many({}, request_timeout_ms=0)
        assert dmr_SL_zrq.deleted_count == 12

        # remove the timeout completely with a zero per-method genmeth-timeout override
        httpserver.expect_oneshot_request(
            expected_url,
            method="POST",
        ).respond_with_handler(
            delayed_response_handler(
                delay_ms=1200,
                response_json={
                    "status": {
                        "deletedCount": 12,
                        "moreData": False,
                    },
                },
            )
        )
        dmr_LS_zrq = await acollection_L_S.delete_many({}, general_method_timeout_ms=0)
        assert dmr_LS_zrq.deleted_count == 12

        # remove the timeout completely with a zero per-method 'timeout_ms' shorthand
        httpserver.expect_oneshot_request(
            expected_url,
            method="POST",
        ).respond_with_handler(
            delayed_response_handler(
                delay_ms=1200,
                response_json={
                    "status": {
                        "deletedCount": 12,
                        "moreData": False,
                    },
                },
            )
        )
        dmr_LS_zgrq = await acollection_L_S.delete_many({}, timeout_ms=0)
        assert dmr_LS_zgrq.deleted_count == 12
