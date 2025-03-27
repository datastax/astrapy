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

from typing import Any

import pytest

from astrapy.data.utils.distinct_extractors import (
    _create_document_key_extractor,
    _reduce_distinct_key_to_safe,
    _reduce_distinct_key_to_shallow_safe,
)

DOCUMENT_1 = {
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
DOCUMENT_2A = {"x": [{"y": 10}, {"y": 11}, {"y": 12}]}
DOCUMENT_2B = {"x": [{"y": 100}]}
DOCUMENT_2C = {"x": ["X0", "X1"]}
DOCUMENT_3 = {"a": [1, 2, ["a", "b", "c", "d", "e", "f"]]}
DOCUMENT_ESC_1 = {
    "a.b": {"..": "the-dots"},
    "c&d": [
        {"&&": "the-ampersands"},
        {"..": "amp-then-dots"},
    ],
    ".": {"0": "dot-zero"},
    "&": ["amp-zero"],
}


def _assert_extracts(
    document: dict[str, Any], key: str | list[str | int], expected: list[Any]
) -> None:
    _extractor = _create_document_key_extractor(key)
    _extracted = list(_extractor(document))
    assert len(_extracted) == len(expected)
    for item in expected:
        assert item in _extracted


class TestDocumentExtractors:
    @pytest.mark.describe("test of key reduction to safe, from str")
    def test_reduce_key_to_safe_str(self) -> None:
        with pytest.raises(ValueError):
            _reduce_distinct_key_to_safe("")
        with pytest.raises(ValueError):
            _reduce_distinct_key_to_safe("0.a")
        assert _reduce_distinct_key_to_safe("a.b.0.d") == "a.b"
        assert _reduce_distinct_key_to_safe("a.b") == "a.b"
        assert _reduce_distinct_key_to_safe("a") == "a"

    @pytest.mark.describe("test of key reduction to safe, from list")
    def test_reduce_key_to_safe_list(self) -> None:
        with pytest.raises(ValueError):
            _reduce_distinct_key_to_safe([])
        with pytest.raises(ValueError):
            _reduce_distinct_key_to_safe([0, "a"])
        assert _reduce_distinct_key_to_safe(["a", "b", 0, "d"]) == "a.b"
        assert _reduce_distinct_key_to_safe(["a", "b"]) == "a.b"
        assert _reduce_distinct_key_to_safe(["a"]) == "a"

    @pytest.mark.describe("test of key reduction to shallow safe, from str")
    def test_reduce_key_to_shallow_safe_str(self) -> None:
        with pytest.raises(ValueError):
            _reduce_distinct_key_to_shallow_safe("")
        with pytest.raises(ValueError):
            _reduce_distinct_key_to_shallow_safe("0.a")
        assert _reduce_distinct_key_to_shallow_safe("a.b.0.d") == "a"
        assert _reduce_distinct_key_to_shallow_safe("a.b") == "a"
        assert _reduce_distinct_key_to_shallow_safe("a") == "a"

    @pytest.mark.describe("test of key reduction to shallow safe, from list")
    def test_reduce_key_to_shallow_safe_list(self) -> None:
        with pytest.raises(ValueError):
            _reduce_distinct_key_to_shallow_safe([])
        with pytest.raises(ValueError):
            _reduce_distinct_key_to_shallow_safe([0, "a"])
        assert _reduce_distinct_key_to_shallow_safe(["a", "b", 0, "d"]) == "a"
        assert _reduce_distinct_key_to_shallow_safe(["a", "b"]) == "a"
        assert _reduce_distinct_key_to_shallow_safe(["a"]) == "a"

    @pytest.mark.describe("test of plain fieldname document extractor, from str")
    def test_plain_fieldname_document_extractor_str(self) -> None:
        f_extractor = _create_document_key_extractor("f")

        assert list(f_extractor({})) == []
        assert list(f_extractor({"e": "E", "f": "F", "g": "G"})) == ["F"]
        assert list(f_extractor({"f": {"name": "F"}})) == [{"name": "F"}]
        assert list(f_extractor({"f": None})) == [None]

    @pytest.mark.describe("test of dotted fieldname document extractor, from str")
    def test_dotted_fieldname_document_extractor_str(self) -> None:
        # various patterns mixing dict and list indexed access, with string input

        _assert_extracts(DOCUMENT_1, "l", ["L0", "L1", {"name": "L2"}])
        _assert_extracts(DOCUMENT_1, "l.1", ["L1"])
        _assert_extracts(DOCUMENT_1, "l.2", [{"name": "L2"}])
        _assert_extracts(DOCUMENT_1, "d", [{"0": "D0", "1": "D1", "2": {"name": "D2"}}])
        _assert_extracts(DOCUMENT_1, "d.1", ["D1"])
        _assert_extracts(DOCUMENT_1, "d.2", [{"name": "D2"}])
        _assert_extracts(
            DOCUMENT_1, "ll", [["LL00", "LL01"], ["LL10", {"name": "LL11"}]]
        )
        _assert_extracts(DOCUMENT_1, "ll.1", ["LL10", {"name": "LL11"}])
        _assert_extracts(DOCUMENT_1, "ll.1.1", [{"name": "LL11"}])
        _assert_extracts(
            DOCUMENT_1,
            "ld",
            [{"0": "LD00", "1": "LD01"}, {"0": "LD10", "1": {"name": "LD11"}}],
        )
        _assert_extracts(DOCUMENT_1, "ld.1", [{"0": "LD10", "1": {"name": "LD11"}}])
        _assert_extracts(DOCUMENT_1, "ld.1.1", [{"name": "LD11"}])
        _assert_extracts(
            DOCUMENT_1,
            "dl",
            [{"0": ["DL090", "DL01"], "1": ["DL10", {"name": "DL11"}]}],
        )
        _assert_extracts(DOCUMENT_1, "dl.1", ["DL10", {"name": "DL11"}])
        _assert_extracts(DOCUMENT_1, "dl.1.1", [{"name": "DL11"}])
        _assert_extracts(
            DOCUMENT_1,
            "dd",
            [
                {
                    "0": {"0": "DL00", "1": "DL01"},
                    "1": {"0": "DL10", "1": {"name": "DL11"}},
                }
            ],
        )
        _assert_extracts(DOCUMENT_1, "dd.1", [{"0": "DL10", "1": {"name": "DL11"}}])
        _assert_extracts(DOCUMENT_1, "dd.1.1", [{"name": "DL11"}])

        # test about omitting list indices
        _assert_extracts(DOCUMENT_2A, "x.y", [10, 11, 12])
        _assert_extracts(DOCUMENT_2A, "x.0.y", [10])
        _assert_extracts(DOCUMENT_2A, "x.1.y", [11])

        _assert_extracts(DOCUMENT_2B, "x.y", [100])
        _assert_extracts(DOCUMENT_2B, "x.0.y", [100])
        _assert_extracts(DOCUMENT_2B, "x.1.y", [])

        _assert_extracts(DOCUMENT_2C, "x", ["X0", "X1"])
        _assert_extracts(DOCUMENT_2C, "x.0", ["X0"])
        _assert_extracts(DOCUMENT_2C, "x.1", ["X1"])

        # nonexistent index should block list auto-unrolling
        _assert_extracts(DOCUMENT_3, "a.5", [])

    @pytest.mark.describe("test of dotted fieldname document extractor, from list")
    def test_dotted_fieldname_document_extractor_list(self) -> None:
        # various patterns mixing dict and list indexed access, with list input

        _assert_extracts(DOCUMENT_1, ["l"], ["L0", "L1", {"name": "L2"}])
        _assert_extracts(DOCUMENT_1, ["l", 1], ["L1"])
        _assert_extracts(DOCUMENT_1, ["l", "1"], [])
        _assert_extracts(DOCUMENT_1, ["l", 2], [{"name": "L2"}])
        _assert_extracts(DOCUMENT_1, ["l", "2"], [])
        _assert_extracts(
            DOCUMENT_1, ["d"], [{"0": "D0", "1": "D1", "2": {"name": "D2"}}]
        )
        _assert_extracts(DOCUMENT_1, ["d", "1"], ["D1"])
        _assert_extracts(DOCUMENT_1, ["d", 1], [])
        _assert_extracts(DOCUMENT_1, ["d", "2"], [{"name": "D2"}])
        _assert_extracts(DOCUMENT_1, ["d", 2], [])
        _assert_extracts(
            DOCUMENT_1, ["ll"], [["LL00", "LL01"], ["LL10", {"name": "LL11"}]]
        )
        _assert_extracts(DOCUMENT_1, ["ll", 1], ["LL10", {"name": "LL11"}])
        _assert_extracts(DOCUMENT_1, ["ll", 1, 1], [{"name": "LL11"}])
        _assert_extracts(
            DOCUMENT_1,
            ["ld"],
            [{"0": "LD00", "1": "LD01"}, {"0": "LD10", "1": {"name": "LD11"}}],
        )
        _assert_extracts(DOCUMENT_1, ["ld", 1], [{"0": "LD10", "1": {"name": "LD11"}}])
        _assert_extracts(DOCUMENT_1, ["ld", "1"], ["LD01", {"name": "LD11"}])
        _assert_extracts(DOCUMENT_1, ["ld", 1, "1"], [{"name": "LD11"}])
        _assert_extracts(DOCUMENT_1, ["ld", 1, 1], [])
        _assert_extracts(DOCUMENT_1, ["ld", "1", 1], [])
        _assert_extracts(DOCUMENT_1, ["ld", "1", "1"], [])
        _assert_extracts(
            DOCUMENT_1,
            ["dl"],
            [{"0": ["DL090", "DL01"], "1": ["DL10", {"name": "DL11"}]}],
        )
        _assert_extracts(DOCUMENT_1, ["dl", "1"], ["DL10", {"name": "DL11"}])
        _assert_extracts(DOCUMENT_1, ["dl", 1], [])
        _assert_extracts(DOCUMENT_1, ["dl", "1", 1], [{"name": "DL11"}])
        _assert_extracts(DOCUMENT_1, ["dl", "1", "1"], [])
        _assert_extracts(DOCUMENT_1, ["dl", 1, 1], [])
        _assert_extracts(DOCUMENT_1, ["dl", 1, "1"], [])
        _assert_extracts(
            DOCUMENT_1,
            ["dd"],
            [
                {
                    "0": {"0": "DL00", "1": "DL01"},
                    "1": {"0": "DL10", "1": {"name": "DL11"}},
                }
            ],
        )
        _assert_extracts(
            DOCUMENT_1, ["dd", "1"], [{"0": "DL10", "1": {"name": "DL11"}}]
        )
        _assert_extracts(DOCUMENT_1, ["dd", 1], [])
        _assert_extracts(DOCUMENT_1, ["dd", "1", "1"], [{"name": "DL11"}])
        _assert_extracts(DOCUMENT_1, ["dd", "1", 1], [])
        _assert_extracts(DOCUMENT_1, ["dd", 1, "1"], [])
        _assert_extracts(DOCUMENT_1, ["dd", 1, 1], [])

        # test about omitting list indices
        _assert_extracts(DOCUMENT_2A, ["x", "y"], [10, 11, 12])
        _assert_extracts(DOCUMENT_2A, ["x", 0, "y"], [10])
        _assert_extracts(DOCUMENT_2A, ["x", 1, "y"], [11])

        _assert_extracts(DOCUMENT_2B, ["x", "y"], [100])
        _assert_extracts(DOCUMENT_2B, ["x", 0, "y"], [100])
        _assert_extracts(DOCUMENT_2B, ["x", 1, "y"], [])

        _assert_extracts(DOCUMENT_2C, ["x"], ["X0", "X1"])
        _assert_extracts(DOCUMENT_2C, ["x", 0], ["X0"])
        _assert_extracts(DOCUMENT_2C, ["x", 1], ["X1"])

        # nonexistent index should block list auto-unrolling
        _assert_extracts(DOCUMENT_3, ["a", 5], [])

    @pytest.mark.describe("test of escapee fieldname document extractor, from str")
    def test_escapee_fieldname_document_extractor_str(self) -> None:
        # field names that ought to be escaped, str (escaped) specification
        _assert_extracts(DOCUMENT_ESC_1, "a&.b.&.&.", ["the-dots"])
        _assert_extracts(DOCUMENT_ESC_1, "c&&d.&&&&", ["the-ampersands"])
        _assert_extracts(DOCUMENT_ESC_1, "c&&d.0.&&&&", ["the-ampersands"])
        _assert_extracts(DOCUMENT_ESC_1, "c&&d.&.&.", ["amp-then-dots"])
        _assert_extracts(DOCUMENT_ESC_1, "c&&d.1.&.&.", ["amp-then-dots"])
        _assert_extracts(DOCUMENT_ESC_1, "c&&d.0.&.&.", [])
        _assert_extracts(DOCUMENT_ESC_1, "&..0", ["dot-zero"])
        _assert_extracts(DOCUMENT_ESC_1, "&&.0", ["amp-zero"])

    @pytest.mark.describe("test of escapee fieldname document extractor, from list")
    def test_escapee_fieldname_document_extractor_list(self) -> None:
        # field names that ought to be escaped, literals specification
        _assert_extracts(DOCUMENT_ESC_1, ["a.b", ".."], ["the-dots"])
        _assert_extracts(DOCUMENT_ESC_1, ["c&d", "&&"], ["the-ampersands"])
        _assert_extracts(DOCUMENT_ESC_1, ["c&d", 0, "&&"], ["the-ampersands"])
        _assert_extracts(DOCUMENT_ESC_1, ["c&d", ".."], ["amp-then-dots"])
        _assert_extracts(DOCUMENT_ESC_1, ["c&d", 1, ".."], ["amp-then-dots"])
        _assert_extracts(DOCUMENT_ESC_1, ["c&d", 0, ".."], [])
        _assert_extracts(DOCUMENT_ESC_1, [".", "0"], ["dot-zero"])
        _assert_extracts(DOCUMENT_ESC_1, [".", 0], [])
        _assert_extracts(DOCUMENT_ESC_1, ["&", "0"], [])
        _assert_extracts(DOCUMENT_ESC_1, ["&", 0], ["amp-zero"])

    @pytest.mark.describe("test of failure modes in creating document extractors")
    def test_failure_document_extractors(self) -> None:
        _create_document_key_extractor([0, 1, "two"])
        _create_document_key_extractor("0.1.two")

        with pytest.raises(ValueError):
            _create_document_key_extractor(".xyz")
        with pytest.raises(ValueError):
            _create_document_key_extractor("")
        with pytest.raises(ValueError):
            _create_document_key_extractor("a.b..d")
        with pytest.raises(ValueError):
            _create_document_key_extractor(["", "xyz"])
        with pytest.raises(ValueError):
            _create_document_key_extractor([])
        with pytest.raises(ValueError):
            _create_document_key_extractor(["a", "b", "", "d"])
