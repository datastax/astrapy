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

from typing import Any, Dict, List

import pytest

from astrapy.cursors import _create_document_key_extractor


class TestDocumentExtractors:
    @pytest.mark.describe("test of plain fieldname document extractor")
    def test_plain_fieldname_document_extractor(self) -> None:
        f_extractor = _create_document_key_extractor("f")

        assert list(f_extractor({})) == []
        assert list(f_extractor({"e": "E", "f": "F", "g": "G"})) == ["F"]
        assert list(f_extractor({"f": {"name": "F"}})) == [{"name": "F"}]
        assert list(f_extractor({"f": None})) == [None]

    @pytest.mark.describe("test of dotted fieldname document extractor")
    def test_dotted_fieldname_document_extractor(self) -> None:
        # various patterns mixing dict and list indexed access
        document1 = {
            "l": ["L0", "L1", {"name": "L2"}],
            "d": {"0": "D0", "1": "D1", "2": {"name": "D2"}},
            "ll": [
                ["LL00", "LL01"],
                ["LL10", {"name": "LL11"}],
            ],
            "ld": [
                {"0": "LD00", "1": "LD01"},
                {"0": "LD10", "1": {"name": "LD11"}},
            ],
            "dl": {
                "0": ["DL090", "DL01"],
                "1": ["DL10", {"name": "DL11"}],
            },
            "dd": {
                "0": {"0": "DL00", "1": "DL01"},
                "1": {"0": "DL10", "1": {"name": "DL11"}},
            },
            "non_index": {"00": "NI00", "01": "NI01"},
        }

        def assert_extracts(
            document: Dict[str, Any], key: str, expected: List[Any]
        ) -> None:
            _extractor = _create_document_key_extractor(key)
            _extracted = list(_extractor(document))
            assert len(_extracted) == len(expected)
            for item in expected:
                assert item in _extracted

        assert_extracts(document1, "l", ["L0", "L1", {"name": "L2"}])
        assert_extracts(document1, "l.1", ["L1"])
        assert_extracts(document1, "l.2", [{"name": "L2"}])
        assert_extracts(document1, "d", [{"0": "D0", "1": "D1", "2": {"name": "D2"}}])
        assert_extracts(document1, "d.1", ["D1"])
        assert_extracts(document1, "d.2", [{"name": "D2"}])
        assert_extracts(document1, "ll", [["LL00", "LL01"], ["LL10", {"name": "LL11"}]])
        assert_extracts(document1, "ll.1", ["LL10", {"name": "LL11"}])
        assert_extracts(document1, "ll.1.1", [{"name": "LL11"}])
        assert_extracts(
            document1,
            "ld",
            [{"0": "LD00", "1": "LD01"}, {"0": "LD10", "1": {"name": "LD11"}}],
        )
        assert_extracts(document1, "ld.1", [{"0": "LD10", "1": {"name": "LD11"}}])
        assert_extracts(document1, "ld.1.1", [{"name": "LD11"}])
        assert_extracts(
            document1, "dl", [{"0": ["DL090", "DL01"], "1": ["DL10", {"name": "DL11"}]}]
        )
        assert_extracts(document1, "dl.1", ["DL10", {"name": "DL11"}])
        assert_extracts(document1, "dl.1.1", [{"name": "DL11"}])
        assert_extracts(
            document1,
            "dd",
            [
                {
                    "0": {"0": "DL00", "1": "DL01"},
                    "1": {"0": "DL10", "1": {"name": "DL11"}},
                }
            ],
        )
        assert_extracts(document1, "dd.1", [{"0": "DL10", "1": {"name": "DL11"}}])
        assert_extracts(document1, "dd.1.1", [{"name": "DL11"}])

        # test about omitting list indices
        document2a = {"x": [{"y": 10}, {"y": 11}, {"y": 12}]}
        document2b = {"x": [{"y": 100}]}
        document2c = {"x": ["X0", "X1"]}

        assert_extracts(document2a, "x.y", [10, 11, 12])
        assert_extracts(document2a, "x.0.y", [10])
        assert_extracts(document2a, "x.1.y", [11])

        assert_extracts(document2b, "x.y", [100])
        assert_extracts(document2b, "x.0.y", [100])
        assert_extracts(document2b, "x.1.y", [])

        assert_extracts(document2c, "x", ["X0", "X1"])
        assert_extracts(document2c, "x.0", ["X0"])
        assert_extracts(document2c, "x.1", ["X1"])

        # nonexistent index should block list auto-unrolling
        document_3 = {"a": [1, 2, ["a", "b", "c", "d", "e", "f"]]}

        assert_extracts(document_3, "a.5", [])
