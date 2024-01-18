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

import logging
from typing import Optional
import pytest

from astrapy.db import AsyncAstraDBCollection

logger = logging.getLogger(__name__)


FIND_LIMIT = 183  # Keep this > 20 and <= N to actually put pagination to test
PREFETCHED = 42  # Keep this > 20 and <= FIND_LIMIT to actually trigger prefetching


@pytest.mark.describe(
    "should retrieve the required amount of documents, all different, through pagination (async)"
)
@pytest.mark.parametrize(
    "prefetched",
    [
        pytest.param(None, id="without pre-fetching"),
        pytest.param(PREFETCHED, id="with pre-fetching"),
    ],
)
async def test_find_paginated(
    prefetched: Optional[int], async_pagination_v_collection: AsyncAstraDBCollection
) -> None:
    options = {"limit": FIND_LIMIT}
    projection = {"$vector": 0}

    paginated_documents_agen = async_pagination_v_collection.paginated_find(
        projection=projection, options=options, prefetched=prefetched
    )
    paginated_documents = [doc async for doc in paginated_documents_agen]
    paginated_ids = [doc["_id"] for doc in paginated_documents]
    assert all(["$vector" not in doc for doc in paginated_documents])
    assert len(paginated_ids) == FIND_LIMIT
    assert len(paginated_ids) == len(set(paginated_ids))
