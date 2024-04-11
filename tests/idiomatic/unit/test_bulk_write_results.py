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

import pytest

from astrapy.results import BulkWriteResult
from astrapy.operations import reduce_bulk_write_results


class TestBulkWriteResults:
    @pytest.mark.describe("test of reduction of bulk write results")
    def test_reduce_bulk_write_results(self) -> None:
        bwr1 = BulkWriteResult(
            bulk_api_results={1: [{"seq1": 1}]},
            deleted_count=100,
            inserted_count=200,
            matched_count=300,
            modified_count=400,
            upserted_count=500,
            upserted_ids={1: {"useq1": 1}},
        )
        bwr2 = BulkWriteResult(
            bulk_api_results={},
            deleted_count=10,
            inserted_count=20,
            matched_count=30,
            modified_count=40,
            upserted_count=50,
            upserted_ids={2: {"useq2": 2}},
        )
        bwr3 = BulkWriteResult(
            bulk_api_results={3: [{"seq3": 3}]},
            deleted_count=1,
            inserted_count=2,
            matched_count=3,
            modified_count=4,
            upserted_count=5,
            upserted_ids={},
        )

        reduced_a = reduce_bulk_write_results([bwr1, bwr2, bwr3])
        expected_a = BulkWriteResult(
            bulk_api_results={1: [{"seq1": 1}], 3: [{"seq3": 3}]},
            deleted_count=111,
            inserted_count=222,
            matched_count=333,
            modified_count=444,
            upserted_count=555,
            upserted_ids={1: {"useq1": 1}, 2: {"useq2": 2}},
        )
        assert reduced_a == expected_a

        bwr_n = BulkWriteResult(
            bulk_api_results={},
            deleted_count=None,
            inserted_count=0,
            matched_count=0,
            modified_count=0,
            upserted_count=0,
            upserted_ids={},
        )
        bwr_1 = BulkWriteResult(
            bulk_api_results={},
            deleted_count=1,
            inserted_count=1,
            matched_count=1,
            modified_count=1,
            upserted_count=1,
            upserted_ids={},
        )

        reduced_n = reduce_bulk_write_results([bwr_1, bwr_n, bwr_1])
        expected_n = BulkWriteResult(
            bulk_api_results={},
            deleted_count=None,
            inserted_count=2,
            matched_count=2,
            modified_count=2,
            upserted_count=2,
            upserted_ids={},
        )
        assert reduced_n == expected_n
