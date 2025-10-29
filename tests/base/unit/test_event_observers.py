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
            def __init__(self, evt_map: dict[str, list[ObservableEvent]]) -> None:
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

        received_events: dict[str, list[ObservableEvent]] = {}
        my_obs = MyObserver(received_events)

        my_obs.receive(ObservableLog(level=logging.DEBUG, message=LOG_MESSAGE_1))
        my_obs.receive(ObservableError(error=ERR_DESC))
        my_obs.receive(ObservableWarning(warning=WRN_DESC))
        my_obs.receive(ObservableLog(level=logging.ERROR, message=LOG_MESSAGE_2))

        assert received_events["log"] == [
            ObservableLog(level=logging.DEBUG, message=LOG_MESSAGE_1),
            ObservableLog(level=logging.ERROR, message=LOG_MESSAGE_2),
        ]
        assert received_events["warning"] == [ObservableWarning(warning=WRN_DESC)]
        assert received_events["error"] == [ObservableError(error=ERR_DESC)]
