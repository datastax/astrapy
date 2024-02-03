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
Tests for the User-Agent customization logic
"""

import logging
import pytest
from pytest_httpserver import HTTPServer

from astrapy import __version__
from astrapy.utils import compose_user_agent, package_name
from astrapy.db import AstraDB


logger = logging.getLogger(__name__)


@pytest.mark.describe("compose_user_agent")
def test_compose_user_agent() -> None:
    assert (
        compose_user_agent(caller_name=None, caller_version=None)
        == f"{package_name}/{__version__}"
    )
    assert (
        compose_user_agent(caller_name="N", caller_version=None)
        == f"N {package_name}/{__version__}"
    )
    assert (
        compose_user_agent(caller_name=None, caller_version="V")
        == f"{package_name}/{__version__}"
    )
    assert (
        compose_user_agent(caller_name="N", caller_version="V")
        == f"N/V {package_name}/{__version__}"
    )


@pytest.mark.describe("test user-agent in the standard way for AstraDB")
def test_useragent_astradb_standard(httpserver: HTTPServer) -> None:
    root_endpoint = httpserver.url_for("/")
    my_db = AstraDB(
        token="token",
        api_endpoint=root_endpoint,
        namespace="ns",
    )
    expected_url = f"{my_db.base_path}"

    httpserver.expect_request(
        expected_url,
        method="POST",
        headers={
            "user-agent": compose_user_agent(caller_name=None, caller_version=None)
        },
    ).respond_with_data("{}")

    my_db.get_collections()
