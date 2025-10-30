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

from astrapy.event_observers import ObservableEvent, Observer
from astrapy.utils.api_options import APIOptions, defaultAPIOptions


class TestAPIOptions:
    @pytest.mark.describe("test of header inheritance in APIOptions")
    def test_apioptions_headers(self) -> None:
        opts_d = defaultAPIOptions(environment="dev")
        opts_1 = opts_d.with_override(
            APIOptions(
                database_additional_headers={"d": "y", "D": None},
                admin_additional_headers={"a": "y", "A": None},
                redacted_header_names={"x", "y"},
            )
        )
        opts_2 = opts_d.with_override(
            APIOptions(
                database_additional_headers={"D": "y"},
                admin_additional_headers={"A": "y"},
                redacted_header_names={"x"},
            )
        ).with_override(
            APIOptions(
                database_additional_headers={"d": "y", "D": None},
                admin_additional_headers={"a": "y", "A": None},
                redacted_header_names={"y"},
            )
        )

        assert opts_1 == opts_2

    @pytest.mark.describe("test of event observer inheritance in APIOptions")
    def test_apioptions_eventobservers(self) -> None:
        lst_a: list[ObservableEvent] = []
        lst_b: list[ObservableEvent] = []
        obs_a = Observer.from_event_list(lst_a)
        obs_b = Observer.from_event_list(lst_b)
        opts_d = defaultAPIOptions(environment="hcd")
        opts_1 = opts_d.with_override(
            APIOptions(
                event_observers={"a": obs_a, "b": obs_b},
            )
        )
        opts_2 = opts_d.with_override(
            APIOptions(
                event_observers={"a": obs_a},
            )
        ).with_override(
            APIOptions(
                event_observers={"b": obs_b},
            )
        )
        opts_3n = opts_d.with_override(
            APIOptions(
                event_observers={"a": obs_a, "b": None},
            )
        )
        opts_4n = opts_d.with_override(
            APIOptions(
                event_observers={"a": obs_a, "b": obs_b},
            )
        ).with_override(
            APIOptions(
                event_observers={"b": None},
            )
        )

        assert opts_1 == opts_2
        assert opts_3n == opts_4n
