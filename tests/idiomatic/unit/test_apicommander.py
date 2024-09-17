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

from astrapy.api_commander import APICommander


class TestAPICommander:
    @pytest.mark.describe("test of APICommander")
    def test_apicommander(self) -> None:
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
