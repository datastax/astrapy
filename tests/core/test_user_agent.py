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

from __future__ import annotations

import logging

import pytest
from pytest_httpserver import HTTPServer

from astrapy import __version__
from astrapy.core.db import (
    AstraDB,
    AstraDBCollection,
    AsyncAstraDB,
    AsyncAstraDBCollection,
)
from astrapy.core.ops import AstraDBOps
from astrapy.core.utils import compose_user_agent, package_name

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


@pytest.mark.describe("test user-agent for AstraDB")
def test_useragent_astradb(httpserver: HTTPServer) -> None:
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

    my_db.set_caller(caller_name="CN", caller_version="CV")
    httpserver.expect_request(
        expected_url,
        method="POST",
        headers={
            "user-agent": compose_user_agent(caller_name="CN", caller_version="CV")
        },
    ).respond_with_data("{}")
    my_db.get_collections()


@pytest.mark.describe("test user-agent for AstraDBCollection")
def test_useragent_astradbcollection(httpserver: HTTPServer) -> None:
    root_endpoint = httpserver.url_for("/")
    my_coll = AstraDBCollection(
        "c1",
        token="token",
        api_endpoint=root_endpoint,
        namespace="ns",
    )
    expected_url = f"{my_coll.base_path}"
    httpserver.expect_request(
        expected_url,
        method="POST",
        headers={
            "user-agent": compose_user_agent(caller_name=None, caller_version=None)
        },
    ).respond_with_data("{}")
    my_coll.find()

    my_coll.set_caller(caller_name="CN", caller_version="CV")
    httpserver.expect_request(
        expected_url,
        method="POST",
        headers={
            "user-agent": compose_user_agent(caller_name="CN", caller_version="CV")
        },
    ).respond_with_data("{}")
    my_coll.find()


@pytest.mark.describe("test user-agent for AstraDB (async)")
async def test_useragent_astradb_async(httpserver: HTTPServer) -> None:
    root_endpoint = httpserver.url_for("/")
    my_db = AsyncAstraDB(
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
    await my_db.get_collections()

    my_db.set_caller(caller_name="CN", caller_version="CV")
    httpserver.expect_request(
        expected_url,
        method="POST",
        headers={
            "user-agent": compose_user_agent(caller_name="CN", caller_version="CV")
        },
    ).respond_with_data("{}")
    await my_db.get_collections()


@pytest.mark.describe("test user-agent for AstraDBCollection (async)")
async def test_useragent_astradbcollection_async(httpserver: HTTPServer) -> None:
    root_endpoint = httpserver.url_for("/")
    my_coll = AsyncAstraDBCollection(
        "c1",
        token="token",
        api_endpoint=root_endpoint,
        namespace="ns",
    )
    expected_url = f"{my_coll.base_path}"
    httpserver.expect_request(
        expected_url,
        method="POST",
        headers={
            "user-agent": compose_user_agent(caller_name=None, caller_version=None)
        },
    ).respond_with_data("{}")
    await my_coll.find()

    my_coll.set_caller(caller_name="CN", caller_version="CV")
    httpserver.expect_request(
        expected_url,
        method="POST",
        headers={
            "user-agent": compose_user_agent(caller_name="CN", caller_version="CV")
        },
    ).respond_with_data("{}")
    await my_coll.find()


@pytest.mark.describe("test user-agent for AstraDBOps")
def test_useragent_astradbops(httpserver: HTTPServer) -> None:
    root_endpoint = httpserver.url_for("/")
    my_ops = AstraDBOps(
        token="token",
    )
    # A TWEAK (pending resolution of #197)
    my_ops.base_url = my_ops.base_url.replace(
        "https://api.astra.datastax.com/",
        root_endpoint,  # such as "http://localhost:44385/"
    )

    # this is to have the right relative path for httpserver ("/v2/databases")
    expected_url = (
        my_ops.base_url.replace(
            root_endpoint,
            "/",
        )
        + "/databases"
    )
    httpserver.expect_request(
        expected_url,
        method="GET",
        headers={
            "user-agent": compose_user_agent(caller_name=None, caller_version=None)
        },
    ).respond_with_data("{}")
    my_ops.get_databases()

    my_ops.set_caller(caller_name="CN", caller_version="CV")
    httpserver.expect_request(
        expected_url,
        method="GET",
        headers={
            "user-agent": compose_user_agent(caller_name="CN", caller_version="CV")
        },
    ).respond_with_data("{}")
    my_ops.get_databases()
