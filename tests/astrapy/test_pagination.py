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

import math
import os
import logging
from typing import Iterable, TypeVar

from astrapy.db import AstraDB
from astrapy.defaults import DEFAULT_KEYSPACE_NAME

from dotenv import load_dotenv
import pytest

logger = logging.getLogger(__name__)


load_dotenv()


ASTRA_DB_APPLICATION_TOKEN = os.environ.get("ASTRA_DB_APPLICATION_TOKEN")
ASTRA_DB_API_ENDPOINT = os.environ.get("ASTRA_DB_API_ENDPOINT")
ASTRA_DB_KEYSPACE = os.environ.get("ASTRA_DB_KEYSPACE", DEFAULT_KEYSPACE_NAME)


TEST_COLLECTION_NAME = "test_collection"
INSERT_BATCH_SIZE = 20  # max 20, fixed by API constraints
N = 200  # must be EVEN
FIND_LIMIT = 183  # Keep this > 20 and <= N to actually put pagination to test

T = TypeVar("T")


def mk_vector(i, N):
    angle = 2 * math.pi * i / N
    return [math.cos(angle), math.sin(angle)]


def _batch_iterable(iterable: Iterable[T], batch_size: int) -> Iterable[Iterable[T]]:
    this_batch = []
    for entry in iterable:
        this_batch.append(entry)
        if len(this_batch) == batch_size:
            yield this_batch
            this_batch = []
    if this_batch:
        yield this_batch


@pytest.fixture(scope="module")
def test_collection():
    astra_db = AstraDB(
        token=ASTRA_DB_APPLICATION_TOKEN,
        api_endpoint=ASTRA_DB_API_ENDPOINT,
        namespace=ASTRA_DB_KEYSPACE,
    )

    astra_db_collection = astra_db.create_collection(
        collection_name=TEST_COLLECTION_NAME, dimension=2
    )

    if int(os.getenv("TEST_PAGINATION_SKIP_INSERTION", "0")) == 0:
        inserted_ids = set()
        for i_batch in _batch_iterable(range(N), INSERT_BATCH_SIZE):
            batch_ids = astra_db_collection.insert_many(
                documents=[{"_id": str(i), "$vector": mk_vector(i, N)} for i in i_batch]
            )["status"]["insertedIds"]
            inserted_ids = inserted_ids | set(batch_ids)
        assert inserted_ids == {str(i) for i in range(N)}
    yield astra_db_collection
    if int(os.getenv("TEST_PAGINATION_SKIP_DELETE_COLLECTION", "0")) == 0:
        _ = astra_db.delete_collection(collection_name=TEST_COLLECTION_NAME)


@pytest.mark.describe(
    "should retrieve the required amount of documents, all different, through pagination"
)
def test_find_paginated(test_collection):
    options = {"limit": FIND_LIMIT}
    projection = {"$vector": 0}

    paginated_documents = test_collection.paginated_find(
        projection=projection,
        options=options,
    )
    paginated_ids = [doc["_id"] for doc in paginated_documents]
    assert len(paginated_ids) == FIND_LIMIT
    assert len(paginated_ids) == len(set(paginated_ids))
