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
    escape_field_names,
    unescape_field_path,
)

ESCAPE_TEST_FIELDS = {
    "a": "a",
    "a.b": "a&.b",
    "a&c": "a&&c",
    "a..": "a&.&.",
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
    @pytest.mark.describe("test of escape_field_names, one arg")
    def test_escape_field_names_onearg(self) -> None:
        for lit_fn, esc_fn in ESCAPE_TEST_FIELDS.items():
            assert escape_field_names(lit_fn) == esc_fn
            assert escape_field_names([lit_fn]) == esc_fn
        for num in [0, 12, 130099]:
            assert escape_field_names(num) == str(num)
            assert escape_field_names([num]) == str(num)

    @pytest.mark.describe("test of escape_field_names, multiple args")
    def test_escape_field_names_multiargs(self) -> None:
        all_lits, all_escs = list(zip(*ESCAPE_TEST_FIELDS.items()))
        assert escape_field_names(all_lits) == ".".join(all_escs)
        assert escape_field_names(*all_lits) == ".".join(all_escs)
        assert escape_field_names(all_lits[:0]) == ".".join(all_escs[:0])
        assert escape_field_names(*all_lits[:0]) == ".".join(all_escs[:0])
        assert escape_field_names(all_lits[:3]) == ".".join(all_escs[:3])
        assert escape_field_names(*all_lits[:3]) == ".".join(all_escs[:3])
        assert escape_field_names(all_lits[3:6]) == ".".join(all_escs[3:6])
        assert escape_field_names(*all_lits[3:6]) == ".".join(all_escs[3:6])

        assert escape_field_names(["first", 12, "last&!."]) == "first.12.last&&!&."
        assert escape_field_names("first", 12, "last&!.") == "first.12.last&&!&."

    @pytest.mark.describe("test of unescape_field_path")
    def test_unescape_field_path(self) -> None:
        assert unescape_field_path("a&.b") == ["a.b"]

        all_lits, all_escs = list(zip(*ESCAPE_TEST_FIELDS.items()))
        assert unescape_field_path(".".join(all_escs)) == list(all_lits)
        assert unescape_field_path(".".join(all_escs[:3])) == list(all_lits[:3])
        assert unescape_field_path(".".join(all_escs[3:6])) == list(all_lits[3:6])

        assert unescape_field_path("") == []

        with pytest.raises(
            ValueError, match=ILLEGAL_ESCAPE_ERROR_MESSAGE_TEMPLATE[:24]
        ):
            unescape_field_path("a.b&?c.d")

        with pytest.raises(
            ValueError, match=UNTERMINATED_ESCAPE_ERROR_MESSAGE_TEMPLATE[:24]
        ):
            unescape_field_path("a.b.c&")
