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

from astrapy.data_types import DataAPIMap


class TestDataAPIMap:
    @pytest.mark.describe("test of table map usage with hashables")
    def test_dataapimap_hashables(self) -> None:
        mp0: DataAPIMap[int, int] = DataAPIMap()
        assert mp0 == DataAPIMap()
        assert dict(mp0) == {}
        assert mp0 == {}
        # identity/equality
        items1 = [(1, "a"), (2, "b"), (3, "c")]
        mp1 = DataAPIMap(items1)
        assert mp1 == DataAPIMap(items1)
        assert mp1 == DataAPIMap(items1 + [(2, "b")])
        assert mp1 == DataAPIMap(items1[2:] + items1[:2])
        assert mp1 != DataAPIMap(items1[1:] + [(1, "z")])
        assert mp1 != DataAPIMap(items1[1:] + [(9, "z")])
        assert dict(items1) == mp1
        assert mp1 == dict(items1)
        assert mp1 == dict(items1 + [(2, "b")])
        assert mp1 == dict(items1[2:] + items1[:2])
        assert mp1 != dict(items1[1:] + [(1, "z")])
        assert mp1 != dict(items1[1:] + [(9, "z")])
        # map operations
        assert list(mp1.keys()) == [1, 2, 3]
        assert list(mp1.values()) == ["a", "b", "c"]
        assert mp1[2] == "b"
        with pytest.raises(KeyError):
            mp1[4]
        assert list(mp1.items()) == items1
        assert len(mp1) == len(items1)
        assert list(mp1) == [1, 2, 3]

    @pytest.mark.describe("test of table map usage with non-hashables")
    def test_dataapimap_nonhashables(self) -> None:
        mp0: DataAPIMap[list[int], int] = DataAPIMap()
        assert mp0 == DataAPIMap()
        assert dict(mp0) == {}
        # identity/equality
        items1 = [([1], "a"), ([2], "b"), ([3], "c")]
        mp1 = DataAPIMap(items1)
        assert mp1 == DataAPIMap(items1)
        assert mp1 == DataAPIMap(items1 + [([2], "b")])
        assert mp1 == DataAPIMap(items1[2:] + items1[:2])
        assert mp1 != DataAPIMap(items1[1:] + [([1], "z")])
        assert mp1 != DataAPIMap(items1[1:] + [([9], "z")])
        # map operations
        assert list(mp1.keys()) == [[1], [2], [3]]
        assert list(mp1.values()) == ["a", "b", "c"]
        assert mp1[[2]] == "b"
        with pytest.raises(KeyError):
            mp1[[4]]
        assert list(mp1.items()) == items1
        assert len(mp1) == len(items1)
        assert list(mp1) == [[1], [2], [3]]

    @pytest.mark.describe("test pickling of DataAPIMap")
    def test_dataapimap_pickle(self) -> None:
        the_map = DataAPIMap([("key1", 1), (None, 2), ("key3", {"a": 1})])
        assert pickle.loads(pickle.dumps(the_map)) == the_map
