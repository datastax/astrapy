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
Tests for the "TRACE" custom logging level
"""

import logging

import pytest

from ..conftest import DataAPICredentials
from astrapy.core.db import AstraDB


logger = logging.getLogger(__name__)


@pytest.mark.describe("should obey the 'TRACE' logging level when requested")
def test_trace_logging_trace(
    caplog: pytest.LogCaptureFixture,
    data_api_credentials_kwargs: DataAPICredentials,
) -> None:
    astra_db = AstraDB(**data_api_credentials_kwargs)
    with caplog.at_level(10):
        astra_db.get_collections()
        for record in caplog.records:
            assert record.levelname != "TRACE"
    caplog.clear()
    # TRACE is level 5:
    with caplog.at_level(5):
        astra_db.get_collections()
        trace_records = [
            record for record in caplog.records if record.levelname == "TRACE"
        ]
        assert len(trace_records) == 1
        assert "findCollections" in trace_records[0].msg
