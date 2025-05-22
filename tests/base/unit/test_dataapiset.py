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

import pickle

import pytest

from astrapy.data_types import DataAPISet


class TestDataAPISet:
    @pytest.mark.describe("test of table set usage with hashables")
    def test_dataapiset_hashables(self) -> None:
        ts0: DataAPISet[int] = DataAPISet()
        assert ts0 == DataAPISet()
        assert set(ts0) == set()
        # identity/equality
        ts1 = DataAPISet([1, 2, 3])
        assert ts1 == DataAPISet([1, 2, 3])
        assert ts1 == DataAPISet([1, 2, 3, 2])
        assert ts1 != DataAPISet([1, 3, 2])
        assert set(ts1) == {1, 2, 3}
        # set operations
        assert ts1 - {2} == DataAPISet([1, 3])
        assert ts1 | {2} == DataAPISet([1, 2, 3])
        assert ts1 | {4} == DataAPISet([1, 2, 3, 4])
        assert ts1 | DataAPISet([4, 2, 5]) == DataAPISet([1, 2, 3, 4, 5])
        assert ts1 ^ DataAPISet([4, 2, 5]) == DataAPISet([1, 3, 4, 5])
        assert ts1 & DataAPISet([4, 2, 5]) == DataAPISet([2])

    @pytest.mark.describe("test of table set usage with non-hashables")
    def test_dataapiset_nonhashables(self) -> None:
        ts0: DataAPISet[list[int]] = DataAPISet()
        assert ts0 == DataAPISet()
        # identity/equality
        ts1 = DataAPISet([[1], [2], [3]])
        assert ts1 == DataAPISet([[1], [2], [3]])
        assert ts1 == DataAPISet([[1], [2], [3], [2]])
        assert ts1 != DataAPISet([[1], [3], [2]])
        # set operations
        assert ts1 - DataAPISet([[2]]) == DataAPISet([[1], [3]])
        assert ts1 | DataAPISet([[2]]) == DataAPISet([[1], [2], [3]])
        assert ts1 | DataAPISet([[4]]) == DataAPISet([[1], [2], [3], [4]])
        assert ts1 | DataAPISet([[4], [2], [5]]) == DataAPISet(
            [[1], [2], [3], [4], [5]]
        )
        assert ts1 ^ DataAPISet([[4], [2], [5]]) == DataAPISet([[1], [3], [4], [5]])
        assert ts1 & DataAPISet([[4], [2], [5]]) == DataAPISet([[2]])

    @pytest.mark.describe("test pickling of DataAPISet")
    def test_dataapiset_pickle(self) -> None:
        the_set = DataAPISet([1, 2, 3])
        assert pickle.loads(pickle.dumps(the_set)) == the_set
