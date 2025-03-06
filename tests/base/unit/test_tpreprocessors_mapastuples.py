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

from astrapy.data.table import (
    MAP2TUPLE_PATHS_INSERT_MANY,
    MAP2TUPLE_PATHS_INSERT_ONE,
    MAP2TUPLE_PATHS_UPDATE_ONE,
)
from astrapy.data.utils.table_converters import preprocess_table_payload
from astrapy.utils.api_options import SerdesOptions, defaultSerdesOptions

MAP2TUPLE_OPTIONS = defaultSerdesOptions.with_override(
    SerdesOptions(encode_maps_as_lists_in_tables=True)
)


class TestTPreprocessorsMapsAsTuples:
    @pytest.mark.describe("test of tuple conversion as in insert_one")
    def test_map2tuple_conversion_insertone(self) -> None:
        payload = {"insertOne": {"document": {"a": {1: "x"}}}}
        expected = {"insertOne": {"document": {"a": [[1, "x"]]}}}
        converted = preprocess_table_payload(
            payload,
            MAP2TUPLE_OPTIONS,
            map2tuple_paths=MAP2TUPLE_PATHS_INSERT_ONE,
        )
        assert expected == converted

    @pytest.mark.describe("test of tuple conversion as in insert_many")
    def test_map2tuple_conversion_insertmany(self) -> None:
        payload = {"insertMany": {"documents": [{"a": {1: "x"}}]}}
        expected = {"insertMany": {"documents": [{"a": [[1, "x"]]}]}}
        converted = preprocess_table_payload(
            payload,
            MAP2TUPLE_OPTIONS,
            map2tuple_paths=MAP2TUPLE_PATHS_INSERT_MANY,
        )
        assert expected == converted

    @pytest.mark.describe("test of tuple conversion as in update_one")
    def test_map2tuple_conversion_updateone(self) -> None:
        payload = {
            "updateOne": {
                "filter": {"f": {1: "g"}},
                "update": {
                    "$set": {"s": {10: "t"}},
                    "$unset": {"u": {10: "v"}},
                },
            }
        }
        expected = {
            "updateOne": {
                "filter": {"f": {1: "g"}},
                "update": {
                    "$set": {"s": [[10, "t"]]},
                    "$unset": {"u": {10: "v"}},
                },
            }
        }
        converted = preprocess_table_payload(
            payload,
            MAP2TUPLE_OPTIONS,
            map2tuple_paths=MAP2TUPLE_PATHS_UPDATE_ONE,
        )
        assert expected == converted
