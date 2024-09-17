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

from astrapy.api_commander import APICommander
from astrapy.request_tools import HttpMethod


class TestAPICommander:
    @pytest.mark.describe("test of APICommander")
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

    @pytest.mark.describe("")
    def test_apicommander_request_sync(self, httpserver: HTTPServer) -> None:
        base_endpoint = httpserver.url_for("/")
        base_path = "/base"
        extra_path = "extra/path/"
        cmd = APICommander(
            api_endpoint=base_endpoint,
            path=base_path,
            headers={"h": "v"},
            callers=[("cn", "cv")],
        )

        httpserver.expect_oneshot_request(
            base_path,
            method=HttpMethod.PUT,
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

    @pytest.mark.describe("")
    async def test_apicommander_request_async(self, httpserver: HTTPServer) -> None:
        base_endpoint = httpserver.url_for("/")
        base_path = "/base"
        extra_path = "extra/path/"
        cmd = APICommander(
            api_endpoint=base_endpoint,
            path=base_path,
            headers={"h": "v"},
            callers=[("cn", "cv")],
        )

        httpserver.expect_oneshot_request(
            base_path,
            method=HttpMethod.PUT,
            data="{}",
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
