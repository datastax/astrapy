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
from typing import Any

import httpx
import pytest
from pytest_httpserver import HTTPServer

from astrapy import DataAPIClient
from astrapy.admin.admin import (
    async_fetch_raw_database_info_from_id_token,
    fetch_raw_database_info_from_id_token,
)
from astrapy.api_options import APIOptions, DevOpsAPIURLOptions
from astrapy.event_observers import (
    ObservableError,
    ObservableEvent,
    ObservableEventType,
    ObservableRequest,
    ObservableResponse,
    ObservableWarning,
    Observer,
)
from astrapy.exceptions import DataAPIResponseException
from astrapy.exceptions.error_descriptors import (
    DataAPIErrorDescriptor,
    DataAPIWarningDescriptor,
)
from astrapy.settings.defaults import DEV_OPS_RESPONSE_HTTP_CREATED
from astrapy.utils.request_tools import HttpMethod


class TestObservingEvents:
    @pytest.mark.describe("test of regular class attached event observers, sync")
    def test_regularclass_attached_eventobservers_sync(
        self, httpserver: HTTPServer
    ) -> None:
        """
        Attachment test, i.e. that each of the (sync) requesting classes
        hooks to observers. Admin excluded. Sync classes.
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

    @pytest.mark.describe("test of regular class attached event observers, async")
    async def test_regularclass_attached_eventobservers_async(
        self, httpserver: HTTPServer
    ) -> None:
        """
        Attachment test, i.e. that each of the (sync) requesting classes
        hooks to observers. Admin excluded. Async classes.
        """
        root_endpoint = httpserver.url_for("/")

        ev_list: list[ObservableEvent] = []
        my_obs = Observer.from_event_list(
            ev_list, event_types=[ObservableEventType.REQUEST]
        )
        api_options = APIOptions(event_observers={"test": my_obs})

        client = DataAPIClient(environment="other", api_options=api_options)
        adatabase = client.get_async_database(root_endpoint, keyspace="xkeyspace")
        acollection = adatabase.get_collection("xcollt")
        atable = adatabase.get_table("xtablet")

        expected_c_url = "/v1/xkeyspace/xcollt"
        httpserver.expect_oneshot_request(
            expected_c_url,
            method=HttpMethod.POST,
        ).respond_with_json({"data": {"document": None}, "status": {}})
        await acollection.find_one()

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
        await atable.find_one()

        assert len(ev_list) == 2
        assert isinstance(ev_list[-1], ObservableRequest)
        assert json.loads(ev_list[-1].payload or "") == {"findOne": {}}

        expected_d_url = "/v1/xkeyspace"
        httpserver.expect_oneshot_request(
            expected_d_url,
            method=HttpMethod.POST,
        ).respond_with_json({"status": {"tables": ["x"]}})
        await adatabase.list_table_names()

        assert len(ev_list) == 3
        assert isinstance(ev_list[-1], ObservableRequest)
        assert json.loads(ev_list[-1].payload or "") == {"listTables": {}}

        expected_dc_url = "/v1"
        httpserver.expect_oneshot_request(
            expected_dc_url,
            method=HttpMethod.POST,
        ).respond_with_json({"a": 1})
        await adatabase.command({"z": -1}, keyspace=None)

        assert len(ev_list) == 4
        assert isinstance(ev_list[-1], ObservableRequest)
        assert json.loads(ev_list[-1].payload or "") == {"z": -1}

    @pytest.mark.describe("test of event types being emitted, sync")
    def test_eventobservers_event_types_emitted(self, httpserver: HTTPServer) -> None:
        """
        Test about all kinds of events being emitted properly.
        Uses just a collection.
        """
        root_endpoint = httpserver.url_for("/")

        ev_list: list[ObservableEvent] = []
        my_obs = Observer.from_event_list(ev_list)
        api_options = APIOptions(event_observers={"test": my_obs})

        client = DataAPIClient(environment="other", api_options=api_options)
        database = client.get_database(root_endpoint, keyspace="xkeyspace")
        collection = database.get_collection("xcollt")
        expected_url = "/v1/xkeyspace/xcollt"

        response_dict = {
            "data": {"document": None},
            "status": {
                "warnings": [
                    {"title": "Warning!"},
                ],
            },
            "errors": [
                {"title": "Error!"},
            ],
        }

        httpserver.expect_oneshot_request(
            expected_url,
            method=HttpMethod.POST,
        ).respond_with_json(response_dict)
        with pytest.raises(DataAPIResponseException):
            collection.find_one()

        assert len(ev_list) == 4
        assert isinstance(ev_list[0], ObservableRequest)
        assert json.loads(ev_list[0].payload or "") == {"findOne": {}}
        assert isinstance(ev_list[1], ObservableResponse)
        assert json.loads(ev_list[1].body or "") == response_dict
        assert isinstance(ev_list[2], ObservableWarning)
        assert ev_list[2].warning == DataAPIWarningDescriptor({"title": "Warning!"})
        assert isinstance(ev_list[3], ObservableError)
        assert ev_list[3].error == DataAPIErrorDescriptor({"title": "Error!"})

    @pytest.mark.describe("test of admin classes attached event observers, sync")
    def test_adminclasses_attached_eventobservers_sync(
        self, httpserver: HTTPServer
    ) -> None:
        """
        Attachment test, i.e. that each of the (sync) requesting classes
        hooks to observers. Admin classes. Sync calls.

        Note: it's a single class with a single commander: no need to test async calls.
        """
        root_endpoint = httpserver.url_for("/")
        db_id = "00000000-0000-0000-0000-000000000000"

        ev_list: list[ObservableEvent] = []
        my_obs = Observer.from_event_list(
            ev_list, event_types=[ObservableEventType.REQUEST]
        )

        # Astra DB admin classes require tweaking the devops API options
        dbadmin_apioptions = APIOptions(
            event_observers={"test": my_obs},
            dev_ops_api_url_options=DevOpsAPIURLOptions(
                dev_ops_api_version="v2",
                dev_ops_url=root_endpoint,
            ),
        )
        astra_client = DataAPIClient(api_options=dbadmin_apioptions)
        astra_admin = astra_client.get_admin()
        astra_db_admin = astra_admin.get_database_admin(
            f"https://{db_id}-reg.apps.astra.datastax.com"
        )

        # AstraDBAdmin's main commander
        expected_c_url = "/v2/databases"
        httpserver.expect_oneshot_request(
            expected_c_url,
            method=HttpMethod.GET,
        ).respond_with_json(
            [
                {
                    "id": "012",
                    "status": "bla",
                    "info": {"name": "blo", "datacenters": [], "cloudProvider": "cp"},
                    "orgId": "234",
                    "ownerId": "678",
                }
            ]
        )
        astra_admin.list_databases()

        assert ev_list != []
        assert isinstance(ev_list[-1], ObservableRequest)
        assert ev_list[-1].payload is None

        # AstraDBAdmin's region-specific commander
        # this uses another APICommander --> another test
        expected_c_url = "/v2/regions/serverless"
        httpserver.expect_oneshot_request(
            expected_c_url,
            method=HttpMethod.GET,
        ).respond_with_json([])
        astra_admin.find_available_regions()

        assert len(ev_list) == 2
        assert isinstance(ev_list[-1], ObservableRequest)
        assert ev_list[-1].payload is None

        # AstraDBDatabaseAdmin's Data API commander
        # this will badly fail because the URL must be an Astra URL for this class
        with pytest.raises(httpx.ConnectError):
            astra_db_admin.find_embedding_providers()

        assert len(ev_list) == 3
        assert isinstance(ev_list[-1], ObservableRequest)
        assert json.loads(ev_list[-1].payload or "") == {"findEmbeddingProviders": {}}

        # AstraDBDatabaseAdmin's DevOps API commander
        expected_c_url = f"/v2/databases/{db_id}/keyspaces/xnewkeyspace"
        httpserver.expect_oneshot_request(
            expected_c_url,
            method=HttpMethod.POST,
        ).respond_with_data(status=DEV_OPS_RESPONSE_HTTP_CREATED)
        astra_db_admin.create_keyspace("xnewkeyspace", wait_until_active=False)

        assert len(ev_list) == 4
        assert isinstance(ev_list[-1], ObservableRequest)
        assert ev_list[-1].payload is None

        # DataAPIDatabaseAdmin's (only) commander
        dapi_client = DataAPIClient(environment="other", api_options=dbadmin_apioptions)
        dapi_db_admin = dapi_client.get_database(root_endpoint).get_database_admin()
        expected_c_url = "/v1"
        httpserver.expect_oneshot_request(
            expected_c_url,
            method=HttpMethod.POST,
        ).respond_with_json({"status": {"keyspaces": []}})
        dapi_db_admin.list_keyspaces()

        assert len(ev_list) == 5
        assert isinstance(ev_list[-1], ObservableRequest)
        assert json.loads(ev_list[-1].payload or "") == {"findKeyspaces": {}}

    @pytest.mark.describe("test of admin functions attached event observers, sync")
    def test_adminfunctions_attached_eventobservers_sync(
        self, httpserver: HTTPServer
    ) -> None:
        """
        Attachment test, i.e. that each of the requesting admin functions
        hook to observers. Sync calls.
        """
        root_endpoint = httpserver.url_for("/")
        db_id = "012"

        ev_list: list[ObservableEvent] = []
        my_obs = Observer.from_event_list(
            ev_list, event_types=[ObservableEventType.REQUEST]
        )

        dbadmin_apioptions = APIOptions(
            event_observers={"test": my_obs},
            dev_ops_api_url_options=DevOpsAPIURLOptions(
                dev_ops_api_version="v2",
                dev_ops_url=root_endpoint,
            ),
        )

        expected_c_url = f"/v2/databases/{db_id}"
        httpserver.expect_oneshot_request(
            expected_c_url,
            method=HttpMethod.GET,
        ).respond_with_json({})
        fetch_raw_database_info_from_id_token(db_id, api_options=dbadmin_apioptions)

        assert ev_list != []
        assert isinstance(ev_list[-1], ObservableRequest)
        assert ev_list[-1].payload is None

    @pytest.mark.describe("test of admin functions attached event observers, async")
    async def test_adminfunctions_attached_eventobservers_async(
        self, httpserver: HTTPServer
    ) -> None:
        """
        Attachment test, i.e. that each of the requesting admin functions
        hook to observers. Sync calls.
        """
        root_endpoint = httpserver.url_for("/")
        db_id = "012"

        ev_list: list[ObservableEvent] = []
        my_obs = Observer.from_event_list(
            ev_list, event_types=[ObservableEventType.REQUEST]
        )

        dbadmin_apioptions = APIOptions(
            event_observers={"test": my_obs},
            dev_ops_api_url_options=DevOpsAPIURLOptions(
                dev_ops_api_version="v2",
                dev_ops_url=root_endpoint,
            ),
        )

        expected_c_url = f"/v2/databases/{db_id}"
        httpserver.expect_oneshot_request(
            expected_c_url,
            method=HttpMethod.GET,
        ).respond_with_json({})
        await async_fetch_raw_database_info_from_id_token(
            db_id, api_options=dbadmin_apioptions
        )

        assert ev_list != []
        assert isinstance(ev_list[-1], ObservableRequest)
        assert ev_list[-1].payload is None

    @pytest.mark.describe(
        "test of metadata coming with an emitted event, sampled, sync"
    )
    def test_eventobservers_eventmetadata_sampled_sync(
        self, httpserver: HTTPServer
    ) -> None:
        """
        Testing the metadata attached to an emitted event (sender, method name etc).
        This is just sampled in one case (a method of the sync collection).
        """
        root_endpoint = httpserver.url_for("/")

        recv_list: list[dict[str, Any]] = []

        class MyRichObserver(Observer):
            def __init__(
                self,
                rich_list: list[dict[str, Any]],
            ) -> None:
                self.evt_list = rich_list

            def receive(
                self,
                event: ObservableEvent,
                sender: Any = None,
                function_name: str | None = None,
                request_id: str | None = None,
            ) -> None:
                if event.event_type in {
                    ObservableEventType.REQUEST,
                    ObservableEventType.RESPONSE,
                }:
                    self.evt_list += [
                        {
                            "event": event,
                            "sender": sender,
                            "function_name": function_name,
                            "request_id": request_id,
                        }
                    ]

        my_r_obs = MyRichObserver(recv_list)
        api_options = APIOptions(
            event_observers={"test": my_r_obs},
        )

        client = DataAPIClient(environment="other", api_options=api_options)
        database = client.get_database(root_endpoint, keyspace="xkeyspace")
        collection = database.get_collection("xcollt")
        expected_url = "/v1/xkeyspace/xcollt"
        response_dict = {
            "data": {"document": None},
            "status": {
                "warnings": [
                    {"title": "Warning!"},
                ],
            },
            "errors": [
                {"title": "Error!"},
            ],
        }

        httpserver.expect_oneshot_request(
            expected_url,
            method=HttpMethod.POST,
        ).respond_with_json(response_dict)
        with pytest.raises(DataAPIResponseException):
            collection.find_one()

        assert len(recv_list) == 2

        rq_evt = recv_list[0]
        assert rq_evt["sender"] == collection
        assert rq_evt["function_name"] == "find_one"
        assert isinstance(rq_evt["event"], ObservableRequest)
        assert rq_evt["event"].http_method == HttpMethod.POST
        assert rq_evt["event"].url == root_endpoint.rstrip("/") + expected_url
        assert rq_evt["event"].query_parameters == {}
        expected_header_keys = {"Content-Type", "Accept", "User-Agent"}
        found_header_keys = (rq_evt["event"].redacted_headers or {}).keys()
        assert found_header_keys - expected_header_keys == set()

        rs_evt = recv_list[1]
        assert rs_evt["sender"] == collection
        assert rs_evt["function_name"] == "find_one"
        assert isinstance(rs_evt["event"], ObservableResponse)
        assert rs_evt["event"].status_code == 200

    @pytest.mark.describe("test of request_id for emitted events, sampled, sync")
    def test_eventobservers_requestid_sampled_sync(
        self, httpserver: HTTPServer
    ) -> None:
        """
        Testing the request_id attached to emitted events. These must be
        the same for all events from a request, and differ between requests.
        This is sampled (a method of the sync collection + one admin "raw-req" call).
        """
        root_endpoint = httpserver.url_for("/")
        db_id = "00000000-0000-0000-0000-000000000000"

        req_ids: list[str | None] = []

        class MyReqIDObserver(Observer):
            def __init__(
                self,
                id_list: list[str | None],
            ) -> None:
                self.id_list = id_list

            def receive(
                self,
                event: ObservableEvent,
                sender: Any = None,
                function_name: str | None = None,
                request_id: str | None = None,
            ) -> None:
                self.id_list += [request_id]

        my_id_obs = MyReqIDObserver(req_ids)

        api_options = APIOptions(
            event_observers={"test_ids": my_id_obs},
        )
        client = DataAPIClient(environment="other", api_options=api_options)
        database = client.get_database(root_endpoint, keyspace="xkeyspace")
        collection = database.get_collection("xcollt")

        astra_api_options = APIOptions(
            event_observers={"test_ids": my_id_obs},
            dev_ops_api_url_options=DevOpsAPIURLOptions(
                dev_ops_api_version="v2",
                dev_ops_url=root_endpoint,
            ),
        )
        astra_client = DataAPIClient(api_options=astra_api_options)
        astra_admin = astra_client.get_admin()
        astra_db_admin = astra_admin.get_database_admin(
            f"https://{db_id}-reg.apps.astra.datastax.com"
        )

        expected_url = "/v1/xkeyspace/xcollt"
        response_dict = {
            "data": {"document": None},
            "status": {
                "warnings": [
                    {"title": "Warning!"},
                ],
            },
            "errors": [
                {"title": "Error!"},
            ],
        }
        httpserver.expect_oneshot_request(
            expected_url,
            method=HttpMethod.POST,
        ).respond_with_json(response_dict)
        with pytest.raises(DataAPIResponseException):
            collection.find_one()

        response_dict = {
            "data": {"document": None},
        }
        httpserver.expect_oneshot_request(
            expected_url,
            method=HttpMethod.POST,
        ).respond_with_json(response_dict)
        collection.find_one()

        expected_c_url = f"/v2/databases/{db_id}/keyspaces/xnewkeyspace"
        httpserver.expect_oneshot_request(
            expected_c_url,
            method=HttpMethod.POST,
        ).respond_with_data(status=DEV_OPS_RESPONSE_HTTP_CREATED)
        astra_db_admin.create_keyspace("xnewkeyspace", wait_until_active=False)

        assert len(req_ids) == 8
        assert len(set(req_ids[:4])) == 1
        assert len(set(req_ids[4:6])) == 1
        assert len(set(req_ids[6:8])) == 1
        assert len(set(req_ids)) == 3
