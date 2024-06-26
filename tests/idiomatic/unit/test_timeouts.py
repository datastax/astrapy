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

import time

import pytest

from astrapy.exceptions import DataAPITimeoutException, MultiCallTimeoutManager


@pytest.mark.describe("test MultiCallTimeoutManager")
def test_multicalltimeoutmanager() -> None:
    mgr_n = MultiCallTimeoutManager(overall_max_time_ms=None)
    assert mgr_n.remaining_timeout_info() is None
    time.sleep(0.5)
    assert mgr_n.remaining_timeout_info() is None

    mgr_1 = MultiCallTimeoutManager(overall_max_time_ms=1000)
    crt_1 = mgr_1.remaining_timeout_info()
    assert crt_1 is not None
    assert crt_1["base"] > 0
    time.sleep(0.6)
    crt_2 = mgr_1.remaining_timeout_info()
    assert crt_2 is not None
    assert crt_2["base"] > 0
    time.sleep(0.6)
    with pytest.raises(DataAPITimeoutException):
        mgr_1.remaining_timeout_info()
