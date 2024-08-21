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
Tests for the `db.py` parts on pagination primitives
"""

from __future__ import annotations

import logging
import time
from typing import Optional

import pytest

from astrapy.core.db import AstraDBCollection

logger = logging.getLogger(__name__)


FIND_LIMIT = 183  # Keep this > 20 and <= N to actually put pagination to test
PREFETCHED = 42  # Keep this > 20 and <= FIND_LIMIT to actually trigger prefetching


@pytest.mark.describe(
    "should retrieve the required amount of documents, all different, through pagination"
)
@pytest.mark.parametrize(
    "prefetched",
    [
        pytest.param(None, id="without pre-fetching"),
        pytest.param(PREFETCHED, id="with pre-fetching"),
    ],
)
def test_find_paginated(
    prefetched: Optional[int],
    pagination_v_collection: AstraDBCollection,
    caplog: pytest.LogCaptureFixture,
) -> None:
    options = {"limit": FIND_LIMIT}
    projection = {"$vector": 0}
    caplog.set_level(logging.INFO)

    paginated_documents_it = pagination_v_collection.paginated_find(
        projection=projection, options=options, prefetched=prefetched
    )

    time.sleep(1)
    if prefetched:
        # If prefetched is set requests are performed eagerly
        assert caplog.text.count("HTTP Request: POST") == 3
    else:
        assert caplog.text.count("HTTP Request: POST") == 0

    paginated_documents = list(paginated_documents_it)
    paginated_ids = [doc["_id"] for doc in paginated_documents]
    assert all(["$vector" not in doc for doc in paginated_documents])
    assert len(paginated_ids) == FIND_LIMIT
    assert len(paginated_ids) == len(set(paginated_ids))


def test_prefetched_thread_terminated(
    pagination_v_collection: AstraDBCollection, caplog: pytest.LogCaptureFixture
) -> None:
    options = {"limit": FIND_LIMIT}
    projection = {"$vector": 0}
    caplog.set_level(logging.DEBUG)

    paginated_documents_it = pagination_v_collection.paginated_find(
        projection=projection, options=options, prefetched=PREFETCHED
    )

    assert next(paginated_documents_it) is not None
    del paginated_documents_it

    time.sleep(1)

    assert caplog.text.count("queued_paginate terminated") == 1
