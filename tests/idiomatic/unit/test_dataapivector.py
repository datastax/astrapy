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

from astrapy.data_types import DataAPIVector
from astrapy.data_types.data_api_vector import bytes_to_floats, floats_to_bytes
from astrapy.utils.transform_payload import (
    convert_ejson_binary_object_to_bytes,
    convert_to_ejson_bytes,
)

COMPARE_EPSILON = 0.00001

SAMPLE_FLOAT_LISTS: list[list[float]] = [
    [10, 100, 1000, 0.1],
    [0.0043, 0.123, 1.332],
    [],
    [103.1, 104.5, 105.6],
    [(-1 if i % 2 == 0 else +1) * i / 5000 for i in range(4096)],
]


def _nearly_equal_lists(list1: list[float], list2: list[float]) -> bool:
    if len(list1) != len(list2):
        return False
    if len(list1) == 0:
        return True
    return max(abs(x - y) for x, y in zip(list1, list2)) < COMPARE_EPSILON


def _nearly_equal_vectors(vec1: DataAPIVector, vec2: DataAPIVector) -> bool:
    return _nearly_equal_lists(vec1.data, vec2.data)


class TestDataAPIVector:
    @pytest.mark.describe("test of float-binary conversions")
    def test_dataapivector_byteconversions(self) -> None:
        for test_list0 in SAMPLE_FLOAT_LISTS:
            test_bytes0 = floats_to_bytes(test_list0)
            test_list1 = bytes_to_floats(test_bytes0)
            assert _nearly_equal_lists(test_list0, test_list1)

    @pytest.mark.describe("test of float-string conversions")
    def test_dataapivector_stringconversions(self) -> None:
        # known expectation
        obj_1 = {"$binary": "PczMzT5MzM0+mZma"}
        vec = DataAPIVector.from_bytes(convert_ejson_binary_object_to_bytes(obj_1))
        vec_exp = DataAPIVector([0.1, 0.2, 0.3])
        assert _nearly_equal_vectors(vec, vec_exp)
        # some full-round conversions
        for test_list0 in SAMPLE_FLOAT_LISTS:
            test_vec0 = DataAPIVector(test_list0)
            test_ejson = convert_to_ejson_bytes(test_vec0.to_bytes())
            test_vec1 = DataAPIVector.from_bytes(
                convert_ejson_binary_object_to_bytes(test_ejson)
            )
            assert _nearly_equal_vectors(test_vec0, test_vec1)

    @pytest.mark.describe("test of DataAPIVector lifecycle")
    def test_dataapivector_lifecycle(self) -> None:
        v0 = DataAPIVector([])
        v1 = DataAPIVector([1.1, 2.2, 3.3])
        for i in v0:
            pass
        for i in v1:
            pass
        assert list(v0) == []
        assert list(v1) == [1.1, 2.2, 3.3]
        assert v0 != v1
        assert v1 == DataAPIVector([1.1, 2.2, 3.3])

        # list-likeness
        assert v1[1:2] == DataAPIVector([2.2])
        assert len(v1) == 3
