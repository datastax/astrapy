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

from astrapy.utils.document_paths import (
    ILLEGAL_ESCAPE_ERROR_MESSAGE_TEMPLATE,
    UNTERMINATED_ESCAPE_ERROR_MESSAGE_TEMPLATE,
    escape_field_name,
    escape_field_names,
    field_names_to_path,
    unescape_field_path,
)

ESCAPE_TEST_FIELDS = {
    "a": "a",
    "a.b": "a&.b",
    "a&c": "a&&c",
    "": "",
    "xyz": "xyz",
    ".": "&.",
    "...": "&.&.&.",
    "&": "&&",
    "&&": "&&&&",
    "&.&.&..": "&&&.&&&.&&&.&.",
    "🇦🇨": "🇦🇨",
    "q🇦🇨w": "q🇦🇨w",
    ".🇦🇨&": "&.🇦🇨&&",
    "a&b.c.&.d": "a&&b&.c&.&&&.d",
    ".env": "&.env",
    "&quot": "&&quot",
    "ݤ": "ݤ",
    "Aݤ": "Aݤ",
    "ݤZ": "ݤZ",
    "🚾 🆒 🆓 🆕 🆖 🆗 🆙 🏧": "🚾 🆒 🆓 🆕 🆖 🆗 🆙 🏧",
    "✋🏿 💪🏿 👐🏿 🙌🏿 👏🏿 🙏🏿": "✋🏿 💪🏿 👐🏿 🙌🏿 👏🏿 🙏🏿",
    "👨‍👩‍👦 👨‍👩‍👧‍👦 👨‍👨‍👦 👩‍👩‍👧 👨‍👦 👨‍👧‍👦 👩‍👦 👩‍👧‍👦": "👨‍👩‍👦 👨‍👩‍👧‍👦 👨‍👨‍👦 👩‍👩‍👧 👨‍👦 👨‍👧‍👦 👩‍👦 👩‍👧‍👦",
}


class TestDocumentPaths:
    @pytest.mark.describe("test of escape_field_name")
    def test_escape_field_name(self) -> None:
        for lit_fn, esc_fn in ESCAPE_TEST_FIELDS.items():
            assert escape_field_name(lit_fn) == esc_fn

    @pytest.mark.describe("test of escape_field_names")
    def test_escape_field_names(self) -> None:
        all_lits, all_escs = list(zip(*ESCAPE_TEST_FIELDS.items()))
        assert escape_field_names(all_lits) == list(all_escs)
        assert escape_field_names(all_lits[:0]) == list(all_escs[:0])
        assert escape_field_names(all_lits[:3]) == list(all_escs[:3])
        assert escape_field_names(all_lits[3:6]) == list(all_escs[3:6])

    @pytest.mark.describe("test of field_names_to_path")
    def test_field_names_to_path(self) -> None:
        all_lits, all_escs = list(zip(*ESCAPE_TEST_FIELDS.items()))
        assert field_names_to_path(all_lits) == ".".join(all_escs)
        assert field_names_to_path(all_lits[:3]) == ".".join(all_escs[:3])
        assert field_names_to_path(all_lits[3:6]) == ".".join(all_escs[3:6])

        assert field_names_to_path([]) == ""

    @pytest.mark.describe("test of unescape_field_path")
    def test_unescape_field_path(self) -> None:
        all_lits, all_escs = list(zip(*ESCAPE_TEST_FIELDS.items()))
        assert unescape_field_path(".".join(all_escs)) == list(all_lits)
        assert unescape_field_path(".".join(all_escs[:3])) == list(all_lits[:3])
        assert unescape_field_path(".".join(all_escs[3:6])) == list(all_lits[3:6])

        assert unescape_field_path("") == [""]

        with pytest.raises(
            ValueError, match=ILLEGAL_ESCAPE_ERROR_MESSAGE_TEMPLATE[:24]
        ):
            unescape_field_path("a.b&?c.d")

        with pytest.raises(
            ValueError, match=UNTERMINATED_ESCAPE_ERROR_MESSAGE_TEMPLATE[:24]
        ):
            unescape_field_path("a.b.c&")
