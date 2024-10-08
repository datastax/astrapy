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
import logging

import pytest
from pytest_httpserver import HTTPServer

from astrapy.api_commander import APICommander
from astrapy.exceptions import (
    DataAPIFaultyResponseException,
    DataAPIHttpException,
    DataAPIResponseException,
    DevOpsAPIFaultyResponseException,
    DevOpsAPIHttpException,
    DevOpsAPIResponseException,
)
from astrapy.request_tools import HttpMethod


class TestAPICommander:
    @pytest.mark.describe("test of APICommander conversion methods")
    def test_apicommander_conversions(self) -> None:
        cmd1 = APICommander(
            api_endpoint="api_endpoint1",
            path="path1",
            headers={"h": "headers1"},
            callers=[("c", "v")],
            redacted_header_names=["redacted_header_names1"],
            dev_ops_api=True,
        )
        cmd2 = APICommander(
            api_endpoint="api_endpoint1",
            path="path1",
            headers={"h": "headers1"},
            callers=[("c", "v")],
            redacted_header_names=["redacted_header_names1"],
            dev_ops_api=True,
        )
        assert cmd1 == cmd2

        assert cmd1 != cmd1._copy(api_endpoint="x")
        assert cmd1 != cmd1._copy(path="x")
        assert cmd1 != cmd1._copy(headers={})
        assert cmd1 != cmd1._copy(callers=[])
        assert cmd1 != cmd1._copy(redacted_header_names=[])
        assert cmd1 != cmd1._copy(dev_ops_api=False)

        assert cmd1 == cmd1._copy(api_endpoint="x")._copy(api_endpoint="api_endpoint1")
        assert cmd1 == cmd1._copy(path="x")._copy(path="path1")
        assert cmd1 == cmd1._copy(headers={})._copy(headers={"h": "headers1"})
        assert cmd1 == cmd1._copy(callers=[])._copy(callers=[("c", "v")])
        assert cmd1 == cmd1._copy(redacted_header_names=[])._copy(
            redacted_header_names=["redacted_header_names1"]
        )
        assert cmd1 == cmd1._copy(dev_ops_api=False)._copy(dev_ops_api=True)

    @pytest.mark.describe("test of APICommander request, sync")
    def test_apicommander_request_sync(self, httpserver: HTTPServer) -> None:
        base_endpoint = httpserver.url_for("/")
        base_path = "/base"
        extra_path = "extra/path"
        cmd = APICommander(
            api_endpoint=base_endpoint,
            path=base_path,
            headers={"h": "v"},
            callers=[("cn0", "cv0"), ("cn1", "cv1")],
        )

        def hv_matcher(hk: str, hv: str | None, ev: str) -> bool:
            if hk == "v":
                return hv == ev
            elif hk.lower() == "user-agent":
                return hv is not None and hv.startswith(ev)
            else:
                return True

        httpserver.expect_oneshot_request(
            base_path,
            method=HttpMethod.PUT,
            headers={
                "h": "v",
                "User-Agent": "cn0/cv0 cn1/cv1",
            },
            header_value_matcher=hv_matcher,
            data="{}",
        ).respond_with_json({"r": 1})
        resp_b = cmd.request(
            http_method=HttpMethod.PUT,
            payload={},
        )
        assert resp_b == {"r": 1}

        httpserver.expect_oneshot_request(
            "/".join([base_path, extra_path]),
            method=HttpMethod.DELETE,
            data="{}",
        ).respond_with_json({"r": 2})
        resp_e = cmd.request(
            http_method=HttpMethod.DELETE,
            payload={},
            additional_path=extra_path,
        )
        assert resp_e == {"r": 2}

    @pytest.mark.describe("test of APICommander request, async")
    async def test_apicommander_request_async(self, httpserver: HTTPServer) -> None:
        base_endpoint = httpserver.url_for("/")
        base_path = "/base"
        extra_path = "extra/path"
        cmd = APICommander(
            api_endpoint=base_endpoint,
            path=base_path,
            headers={"h": "v"},
            callers=[("cn0", "cv0"), ("cn1", "cv1")],
        )

        def hv_matcher(hk: str, hv: str | None, ev: str) -> bool:
            if hk == "v":
                return hv == ev
            elif hk.lower() == "user-agent":
                return hv is not None and hv.startswith(ev)
            else:
                return True

        httpserver.expect_oneshot_request(
            base_path,
            method=HttpMethod.PUT,
            data="{}",
            headers={
                "h": "v",
                "User-Agent": "cn0/cv0 cn1/cv1",
            },
            header_value_matcher=hv_matcher,
        ).respond_with_json({"r": 1})
        resp_b = await cmd.async_request(
            http_method=HttpMethod.PUT,
            payload={},
        )
        assert resp_b == {"r": 1}

        httpserver.expect_oneshot_request(
            "/".join([base_path, extra_path]),
            method=HttpMethod.DELETE,
            data="{}",
        ).respond_with_json({"r": 2})
        resp_e = await cmd.async_request(
            http_method=HttpMethod.DELETE,
            payload={},
            additional_path=extra_path,
        )
        assert resp_e == {"r": 2}

    @pytest.mark.describe("test of APICommander exceptions, sync")
    def test_apicommander_exceptions_sync(self, httpserver: HTTPServer) -> None:
        base_endpoint = httpserver.url_for("/")
        base_path = "/base"
        cmd = APICommander(
            api_endpoint=base_endpoint,
            path=base_path,
            dev_ops_api=False,
        )

        httpserver.expect_oneshot_request(
            base_path,
        ).respond_with_data("{unparseable")
        with pytest.raises(DataAPIFaultyResponseException):
            cmd.request()

        httpserver.expect_oneshot_request(
            base_path,
        ).respond_with_data(
            json.dumps(
                {
                    "errors": [
                        {"title": "Error", "errorCode": "E_C"},
                    ]
                }
            )
        )
        with pytest.raises(DataAPIResponseException):
            cmd.request()

        httpserver.expect_oneshot_request(
            base_path,
        ).respond_with_data(
            json.dumps(
                {
                    "errors": [
                        {"title": "Error", "errorCode": "E_C"},
                    ]
                }
            )
        )
        cmd.request(raise_api_errors=False)

        httpserver.expect_oneshot_request(
            base_path,
        ).respond_with_data(
            json.dumps(
                {
                    "errors": [
                        {"title": "Error", "errorCode": "E_C"},
                    ]
                }
            ),
            status=500,
        )
        with pytest.raises(DataAPIHttpException):
            cmd.request()

    @pytest.mark.describe("test of APICommander exceptions, async")
    async def test_apicommander_exceptions_async(self, httpserver: HTTPServer) -> None:
        base_endpoint = httpserver.url_for("/")
        base_path = "/base"
        cmd = APICommander(
            api_endpoint=base_endpoint,
            path=base_path,
            dev_ops_api=False,
        )

        httpserver.expect_oneshot_request(
            base_path,
        ).respond_with_data("{unparseable")
        with pytest.raises(DataAPIFaultyResponseException):
            await cmd.async_request()

        httpserver.expect_oneshot_request(
            base_path,
        ).respond_with_data(
            json.dumps(
                {
                    "errors": [
                        {"title": "Error", "errorCode": "E_C"},
                    ]
                }
            )
        )
        with pytest.raises(DataAPIResponseException):
            await cmd.async_request()

        httpserver.expect_oneshot_request(
            base_path,
        ).respond_with_data(
            json.dumps(
                {
                    "errors": [
                        {"title": "Error", "errorCode": "E_C"},
                    ]
                }
            )
        )
        await cmd.async_request(raise_api_errors=False)

        httpserver.expect_oneshot_request(
            base_path,
        ).respond_with_data(
            json.dumps(
                {
                    "errors": [
                        {"title": "Error", "errorCode": "E_C"},
                    ]
                }
            ),
            status=500,
        )
        with pytest.raises(DataAPIHttpException):
            await cmd.async_request()

    @pytest.mark.describe("test of APICommander DevOps exceptions, sync")
    def test_apicommander_devops_exceptions_sync(self, httpserver: HTTPServer) -> None:
        base_endpoint = httpserver.url_for("/")
        base_path = "/base"
        cmd = APICommander(
            api_endpoint=base_endpoint,
            path=base_path,
            dev_ops_api=True,
        )

        httpserver.expect_oneshot_request(
            base_path,
        ).respond_with_data("{unparseable")
        with pytest.raises(DevOpsAPIFaultyResponseException):
            cmd.request()

        httpserver.expect_oneshot_request(
            base_path,
        ).respond_with_data(
            json.dumps(
                {
                    "errors": [
                        {"title": "Error", "errorCode": "E_C"},
                    ]
                }
            )
        )
        with pytest.raises(DevOpsAPIResponseException):
            cmd.request()

        httpserver.expect_oneshot_request(
            base_path,
        ).respond_with_data(
            json.dumps(
                {
                    "errors": [
                        {"title": "Error", "errorCode": "E_C"},
                    ]
                }
            )
        )
        cmd.request(raise_api_errors=False)

        httpserver.expect_oneshot_request(
            base_path,
        ).respond_with_data(
            json.dumps(
                {
                    "errors": [
                        {"title": "Error", "errorCode": "E_C"},
                    ]
                }
            ),
            status=500,
        )
        with pytest.raises(DevOpsAPIHttpException):
            cmd.request()

    @pytest.mark.describe("test of APICommander DevOps exceptions, async")
    async def test_apicommander_devops_exceptions_async(
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
        ).respond_with_data("{unparseable")
        with pytest.raises(DevOpsAPIFaultyResponseException):
            await cmd.async_request()

        httpserver.expect_oneshot_request(
            base_path,
        ).respond_with_data(
            json.dumps(
                {
                    "errors": [
                        {"title": "Error", "errorCode": "E_C"},
                    ]
                }
            )
        )
        with pytest.raises(DevOpsAPIResponseException):
            await cmd.async_request()

        httpserver.expect_oneshot_request(
            base_path,
        ).respond_with_data(
            json.dumps(
                {
                    "errors": [
                        {"title": "Error", "errorCode": "E_C"},
                    ]
                }
            )
        )
        await cmd.async_request(raise_api_errors=False)

        httpserver.expect_oneshot_request(
            base_path,
        ).respond_with_data(
            json.dumps(
                {
                    "errors": [
                        {"title": "Error", "errorCode": "E_C"},
                    ]
                }
            ),
            status=500,
        )
        with pytest.raises(DevOpsAPIHttpException):
            await cmd.async_request()

    @pytest.mark.describe("test of APICommander server warnings, sync")
    def test_apicommander_server_warnings_sync(
        self,
        caplog: pytest.LogCaptureFixture,
        httpserver: HTTPServer,
    ) -> None:
        base_endpoint = httpserver.url_for("/")
        base_path = "/base"
        cmd = APICommander(
            api_endpoint=base_endpoint,
            path=base_path,
            dev_ops_api=False,
        )
        httpserver.expect_oneshot_request(
            base_path,
        ).respond_with_json({"status": {"warnings": ["THE_WARNING", "THE_WARNING_2"]}})

        with caplog.at_level(logging.WARNING):
            cmd.request()
            w_records = [
                record
                for record in caplog.records
                if record.levelno == logging.WARNING
                if "THE_WARNING" in record.msg
            ]
            assert len(w_records) == 2
        caplog.clear()

        ops_base_path = "/base_ops"
        devops_cmd = APICommander(
            api_endpoint=base_endpoint,
            path=ops_base_path,
            dev_ops_api=True,
        )
        httpserver.expect_oneshot_request(
            ops_base_path,
        ).respond_with_json({"status": {"warnings": ["THE_WARNING"]}})

        with caplog.at_level(logging.WARNING):
            devops_cmd.request()
            w_records = [
                record
                for record in caplog.records
                if record.levelno == logging.WARNING
                if "THE_WARNING" in record.msg
            ]
            assert len(w_records) == 0
        caplog.clear()

    @pytest.mark.describe("test of APICommander server warnings, async")
    async def test_apicommander_server_warnings_async(
        self,
        caplog: pytest.LogCaptureFixture,
        httpserver: HTTPServer,
    ) -> None:
        base_endpoint = httpserver.url_for("/")
        base_path = "/base"
        cmd = APICommander(
            api_endpoint=base_endpoint,
            path=base_path,
            dev_ops_api=False,
        )
        httpserver.expect_oneshot_request(
            base_path,
        ).respond_with_json({"status": {"warnings": ["THE_WARNING", "THE_WARNING_2"]}})

        with caplog.at_level(logging.WARNING):
            await cmd.async_request()
            w_records = [
                record
                for record in caplog.records
                if record.levelno == logging.WARNING
                if "THE_WARNING" in record.msg
            ]
            assert len(w_records) == 2
        caplog.clear()

        ops_base_path = "/base_ops"
        devops_cmd = APICommander(
            api_endpoint=base_endpoint,
            path=ops_base_path,
            dev_ops_api=True,
        )
        httpserver.expect_oneshot_request(
            ops_base_path,
        ).respond_with_json({"status": {"warnings": ["THE_WARNING"]}})

        with caplog.at_level(logging.WARNING):
            await devops_cmd.async_request()
            w_records = [
                record
                for record in caplog.records
                if record.levelno == logging.WARNING
                if "THE_WARNING" in record.msg
            ]
            assert len(w_records) == 0
        caplog.clear()
