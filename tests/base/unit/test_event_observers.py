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

from typing import Any

import pytest

from astrapy.event_observers import (
    ObservableError,
    ObservableEvent,
    ObservableEventType,
    ObservableRequest,
    ObservableResponse,
    ObservableWarning,
    Observer,
)
from astrapy.exceptions import (
    DataAPIErrorDescriptor,
    DataAPIWarningDescriptor,
)

OBS_ERR = ObservableError(
    error=DataAPIErrorDescriptor(
        {
            "title": "error_title",
            "errorCode": "error_errorCode",
            "message": "error_message",
        }
    ),
)
OBS_WRN = ObservableWarning(
    warning=DataAPIWarningDescriptor(
        {
            "title": "warning_title",
            "errorCode": "warning_errorCode",
            "message": "warning_message",
        }
    ),
)
OBS_RSP_1 = ObservableResponse(body='{"k_resp":"v1"}')
OBS_RSP_2 = ObservableResponse(body='{"k_resp":"v2"}')
OBS_REQ = ObservableRequest(payload='{"k_req":"v"}')


class TestEventObservers:
    @pytest.mark.describe("test of custom observer receiving events")
    def test_custom_observer(self) -> None:
        class MyObserver(Observer):
            def __init__(
                self, evt_map: dict[ObservableEventType, list[ObservableEvent]]
            ) -> None:
                self.evt_map = evt_map

            def receive(
                self,
                event: ObservableEvent,
                sender: Any = None,
                function_name: str | None = None,
            ) -> None:
                self.evt_map[event.event_type] = self.evt_map.get(
                    event.event_type, []
                ) + [event]

        received_events: dict[ObservableEventType, list[ObservableEvent]] = {}
        my_obs = MyObserver(received_events)

        my_obs.receive(OBS_RSP_1)
        my_obs.receive(OBS_ERR)
        my_obs.receive(OBS_WRN)
        my_obs.receive(OBS_RSP_2)
        my_obs.receive(OBS_REQ)

        assert received_events[ObservableEventType.ERROR] == [OBS_ERR]
        assert received_events[ObservableEventType.WARNING] == [OBS_WRN]
        assert received_events[ObservableEventType.RESPONSE] == [OBS_RSP_1, OBS_RSP_2]
        assert received_events[ObservableEventType.REQUEST] == [OBS_REQ]
        assert len(received_events) == 4

    @pytest.mark.describe("test of observer from event dict")
    def test_observer_from_evdict(self) -> None:
        received_events: dict[ObservableEventType, list[ObservableEvent]] = {}
        my_obs = Observer.from_event_dict(received_events)

        my_obs.receive(OBS_RSP_1)
        my_obs.receive(OBS_ERR)
        my_obs.receive(OBS_WRN)
        my_obs.receive(OBS_RSP_2)
        my_obs.receive(OBS_REQ)

        assert received_events[ObservableEventType.ERROR] == [OBS_ERR]
        assert received_events[ObservableEventType.WARNING] == [OBS_WRN]
        assert received_events[ObservableEventType.RESPONSE] == [OBS_RSP_1, OBS_RSP_2]
        assert received_events[ObservableEventType.REQUEST] == [OBS_REQ]
        assert len(received_events) == 4

        # now with filtering by event type
        received_events_f: dict[ObservableEventType, list[ObservableEvent]] = {}
        my_obs_f = Observer.from_event_dict(
            received_events_f,
            event_types=(ObservableEventType.RESPONSE, ObservableEventType.WARNING),
        )

        my_obs_f.receive(OBS_RSP_1)
        my_obs_f.receive(OBS_ERR)
        my_obs_f.receive(OBS_WRN)
        my_obs_f.receive(OBS_RSP_2)
        my_obs_f.receive(OBS_REQ)

        assert received_events_f[ObservableEventType.RESPONSE] == [
            OBS_RSP_1,
            OBS_RSP_2,
        ]
        assert received_events_f[ObservableEventType.WARNING] == [OBS_WRN]
        assert len(received_events_f) == 2

    @pytest.mark.describe("test of observer from event list")
    def test_observer_from_evlist(self) -> None:
        ev_list: list[ObservableEvent] = []
        my_obs = Observer.from_event_list(ev_list)

        my_obs.receive(OBS_RSP_1)
        my_obs.receive(OBS_ERR)
        my_obs.receive(OBS_WRN)
        my_obs.receive(OBS_RSP_2)
        my_obs.receive(OBS_REQ)

        assert ev_list == [
            OBS_RSP_1,
            OBS_ERR,
            OBS_WRN,
            OBS_RSP_2,
            OBS_REQ,
        ]

        # now with filtering by event type
        ev_list_f: list[ObservableEvent] = []
        my_obs_f = Observer.from_event_list(
            ev_list_f,
            event_types=(ObservableEventType.RESPONSE, ObservableEventType.WARNING),
        )

        my_obs_f.receive(OBS_RSP_1)
        my_obs_f.receive(OBS_ERR)
        my_obs_f.receive(OBS_WRN)
        my_obs_f.receive(OBS_RSP_2)
        my_obs_f.receive(OBS_REQ)

        assert ev_list_f == [
            OBS_RSP_1,
            OBS_WRN,
            OBS_RSP_2,
        ]
