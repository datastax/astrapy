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
    # ILLEGAL_ESCAPE_ERROR_MESSAGE_TEMPLATE,
    # UNTERMINATED_ESCAPE_ERROR_MESSAGE_TEMPLATE,
    escape_field_name,
    # escape_field_names,
    # field_names_to_path,
    # unescape_field_path,
)


class TestDocumentPaths:
    @pytest.mark.describe("test of escape_field_name")
    def test_escape_field_name(self) -> None:
        # TODO tests
        assert escape_field_name("a") == "a"
        assert escape_field_name("a.b") == "a&.b"
        assert escape_field_name("a&c") == "a&&c"
