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

import logging
from typing import Any

import pytest

from astrapy.event_observers import (
    ObservableError,
    ObservableEvent,
    ObservableEventType,
    ObservableLog,
    ObservableWarning,
    Observer,
)
from astrapy.exceptions import (
    DataAPIErrorDescriptor,
    DataAPIWarningDescriptor,
)

LOG_MESSAGE_1 = "The log 1"
LOG_MESSAGE_2 = "The log 2"
ERR_DESC = DataAPIErrorDescriptor(
    {
        "title": "error_title",
        "errorCode": "error_errorCode",
        "message": "error_message",
    }
)
WRN_DESC = DataAPIWarningDescriptor(
    {
        "title": "warning_title",
        "errorCode": "warning_errorCode",
        "message": "warning_message",
    }
)


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

        my_obs.receive(ObservableLog(level=logging.DEBUG, message=LOG_MESSAGE_1))
        my_obs.receive(ObservableError(error=ERR_DESC))
        my_obs.receive(ObservableWarning(warning=WRN_DESC))
        my_obs.receive(ObservableLog(level=logging.ERROR, message=LOG_MESSAGE_2))

        assert received_events[ObservableEventType.LOG] == [
            ObservableLog(level=logging.DEBUG, message=LOG_MESSAGE_1),
            ObservableLog(level=logging.ERROR, message=LOG_MESSAGE_2),
        ]
        assert received_events[ObservableEventType.ERROR] == [
            ObservableError(error=ERR_DESC)
        ]
        assert received_events[ObservableEventType.WARNING] == [
            ObservableWarning(warning=WRN_DESC)
        ]
        assert len(received_events) == 3

    @pytest.mark.describe("test of observer from event dict")
    def test_observer_from_evdict(self) -> None:
        received_events: dict[ObservableEventType, list[ObservableEvent]] = {}
        my_obs = Observer.from_event_dict(received_events)

        my_obs.receive(ObservableLog(level=logging.DEBUG, message=LOG_MESSAGE_1))
        my_obs.receive(ObservableError(error=ERR_DESC))
        my_obs.receive(ObservableWarning(warning=WRN_DESC))
        my_obs.receive(ObservableLog(level=logging.ERROR, message=LOG_MESSAGE_2))

        assert received_events[ObservableEventType.LOG] == [
            ObservableLog(level=logging.DEBUG, message=LOG_MESSAGE_1),
            ObservableLog(level=logging.ERROR, message=LOG_MESSAGE_2),
        ]
        assert received_events[ObservableEventType.ERROR] == [
            ObservableError(error=ERR_DESC)
        ]
        assert received_events[ObservableEventType.WARNING] == [
            ObservableWarning(warning=WRN_DESC)
        ]
        assert len(received_events) == 3

        # now with filtering by event type
        received_events_f: dict[ObservableEventType, list[ObservableEvent]] = {}
        my_obs_f = Observer.from_event_dict(
            received_events_f,
            event_types=(ObservableEventType.LOG, ObservableEventType.WARNING),
        )

        my_obs_f.receive(ObservableLog(level=logging.DEBUG, message=LOG_MESSAGE_1))
        my_obs_f.receive(ObservableError(error=ERR_DESC))
        my_obs_f.receive(ObservableWarning(warning=WRN_DESC))
        my_obs_f.receive(ObservableLog(level=logging.ERROR, message=LOG_MESSAGE_2))

        assert received_events_f[ObservableEventType.LOG] == [
            ObservableLog(level=logging.DEBUG, message=LOG_MESSAGE_1),
            ObservableLog(level=logging.ERROR, message=LOG_MESSAGE_2),
        ]
        assert received_events_f[ObservableEventType.WARNING] == [
            ObservableWarning(warning=WRN_DESC)
        ]
        assert len(received_events_f) == 2

    @pytest.mark.describe("test of observer from event list")
    def test_observer_from_evlist(self) -> None:
        ev_list: list[ObservableEvent] = []
        my_obs = Observer.from_event_list(ev_list)

        my_obs.receive(ObservableLog(level=logging.DEBUG, message=LOG_MESSAGE_1))
        my_obs.receive(ObservableError(error=ERR_DESC))
        my_obs.receive(ObservableWarning(warning=WRN_DESC))
        my_obs.receive(ObservableLog(level=logging.ERROR, message=LOG_MESSAGE_2))

        assert ev_list == [
            ObservableLog(level=logging.DEBUG, message=LOG_MESSAGE_1),
            ObservableError(error=ERR_DESC),
            ObservableWarning(warning=WRN_DESC),
            ObservableLog(level=logging.ERROR, message=LOG_MESSAGE_2),
        ]

        # now with filtering by event type
        ev_list_f: list[ObservableEvent] = []
        my_obs_f = Observer.from_event_list(
            ev_list_f,
            event_types=(ObservableEventType.LOG, ObservableEventType.WARNING),
        )

        my_obs_f.receive(ObservableLog(level=logging.DEBUG, message=LOG_MESSAGE_1))
        my_obs_f.receive(ObservableError(error=ERR_DESC))
        my_obs_f.receive(ObservableWarning(warning=WRN_DESC))
        my_obs_f.receive(ObservableLog(level=logging.ERROR, message=LOG_MESSAGE_2))

        assert ev_list_f == [
            ObservableLog(level=logging.DEBUG, message=LOG_MESSAGE_1),
            ObservableWarning(warning=WRN_DESC),
            ObservableLog(level=logging.ERROR, message=LOG_MESSAGE_2),
        ]
