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

from astrapy.info import ListTypeDescriptor


class TestListTypeDescriptor:
    @pytest.mark.describe("test of ListTypeDescriptor parsing and back")
    def test_listtypedescriptor_parsing(self) -> None:
        types_json = [
            {
                "type": "userDefined",
                "udtName": "tame_type",
                "definition": {
                    "fields": {
                        "t": {
                            "type": "text",
                        },
                        "i": {
                            "type": "int",
                        },
                    },
                },
                "apiSupport": {
                    "createTable": True,
                    "insert": True,
                    "read": True,
                    "filter": False,
                    "cqlDefinition": "default_keyspace.tame_type",
                },
            },
            {
                "type": "userDefined",
                "udtName": "lazy_type",
                "definition": {
                    "fields": {
                        "b": {
                            "type": "blob",
                        },
                        "a": {
                            "type": "ascii",
                        },
                    },
                },
            },
        ]

        for type_json in types_json:
            t_fd = ListTypeDescriptor._from_dict(type_json)
            t_co = ListTypeDescriptor.coerce(type_json)

            assert t_fd == t_co

            assert t_fd.as_dict() == type_json
