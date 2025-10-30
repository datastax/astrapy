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
Unit tests for the event-observing API
"""

from __future__ import annotations

import json

import pytest
from pytest_httpserver import HTTPServer

from astrapy import DataAPIClient
from astrapy.api_options import APIOptions
from astrapy.event_observers import (
    ObservableEvent,
    ObservableEventType,
    ObservableRequest,
    Observer,
)
from astrapy.utils.request_tools import HttpMethod


class TestObservingEvents:
    @pytest.mark.describe("test of attached event observers, sync")
    def test_attached_eventobservers_sync(self, httpserver: HTTPServer) -> None:
        """
        Attachment test, i.e. that each of the (sync) requesting classes
        hooks to observers.
        """
        root_endpoint = httpserver.url_for("/")

        ev_list: list[ObservableEvent] = []
        my_obs = Observer.from_event_list(
            ev_list, event_types=[ObservableEventType.REQUEST]
        )
        api_options = APIOptions(event_observers={"test": my_obs})

        client = DataAPIClient(environment="other", api_options=api_options)
        database = client.get_database(root_endpoint, keyspace="xkeyspace")
        collection = database.get_collection("xcollt")
        table = database.get_table("xtablet")

        expected_c_url = "/v1/xkeyspace/xcollt"
        httpserver.expect_oneshot_request(
            expected_c_url,
            method=HttpMethod.POST,
        ).respond_with_json({"data": {"document": None}, "status": {}})
        collection.find_one()

        assert ev_list != []
        assert isinstance(ev_list[-1], ObservableRequest)
        assert json.loads(ev_list[-1].payload or "") == {"findOne": {}}

        expected_t_url = "/v1/xkeyspace/xtablet"
        httpserver.expect_oneshot_request(
            expected_t_url,
            method=HttpMethod.POST,
        ).respond_with_json(
            {"data": {"document": None}, "status": {"projectionSchema": {"x": "text"}}}
        )
        table.find_one()

        assert len(ev_list) == 2
        assert isinstance(ev_list[-1], ObservableRequest)
        assert json.loads(ev_list[-1].payload or "") == {"findOne": {}}

        expected_d_url = "/v1/xkeyspace"
        httpserver.expect_oneshot_request(
            expected_d_url,
            method=HttpMethod.POST,
        ).respond_with_json({"status": {"tables": ["x"]}})
        database.list_table_names()

        assert len(ev_list) == 3
        assert isinstance(ev_list[-1], ObservableRequest)
        assert json.loads(ev_list[-1].payload or "") == {"listTables": {}}

        expected_dc_url = "/v1"
        httpserver.expect_oneshot_request(
            expected_dc_url,
            method=HttpMethod.POST,
        ).respond_with_json({"a": 1})
        database.command({"z": -1}, keyspace=None)

        assert len(ev_list) == 4
        assert isinstance(ev_list[-1], ObservableRequest)
        assert json.loads(ev_list[-1].payload or "") == {"z": -1}
