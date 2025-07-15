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

    @pytest.mark.describe("test of ListTypeDescriptor, faulty and unsupported UDTs")
    def test_listtypedescriptor_weird_udts(self) -> None:
        # good; empty; unsupported
        good_dict = {
            "type": "userDefined",
            "udtName": "bla",
            "definition": {"fields": {"a": {"type": "int"}}},
            "apiSupport": {
                "createTable": True,
                "insert": True,
                "read": True,
                "filter": False,
                "cqlDefinition": "default_keyspace.bla",
            },
        }
        empty_dict: dict[str, Any] = {}
        unsupported_dict = {
            "type": "UNSUPPORTED",
            "apiSupport": {
                "createTable": False,
                "insert": False,
                "read": False,
                "filter": False,
                "cqlDefinition": "default_keyspace.blu",
            },
        }

        assert ListTypeDescriptor._is_valid_dict(good_dict)
        assert not ListTypeDescriptor._is_valid_dict(empty_dict)
        assert ListTypeDescriptor._is_valid_dict(unsupported_dict)

        good_parsed = ListTypeDescriptor._from_dict(good_dict)
        assert good_parsed.as_dict() == good_dict

        with pytest.raises(KeyError):
            ListTypeDescriptor._from_dict(empty_dict)

        unsupported_parsed = ListTypeDescriptor._from_dict(unsupported_dict)
        assert unsupported_parsed.as_dict() == unsupported_dict
