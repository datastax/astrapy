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

import warnings
from dataclasses import dataclass
from typing import Any

from astrapy.data.info.table_descriptor.table_columns import TableAPISupportDescriptor
from astrapy.data.info.table_descriptor.type_creation import CreateTypeDefinition
from astrapy.data.utils.table_types import TableUDTColumnType
from astrapy.utils.parsing import _warn_residual_keys


@dataclass
class ListTypeDescriptor:
    """
    A structure describing a user-defined type (UDT) stored on the database.

    This description provides all information, including the UDT name as found
    on the database and possibly a sub-object detailing the allowed operations
    with the UDT. `ListTypeDescriptor` objects are used for the return
    type of the database `list_types` method.

    Attributes:
        udt_name: the name of the UDT as is stored in the database (and in a keyspace).
        definition: the definition of the type, i.e. its fields and their types.
        api_support: a structure detailing what operations the type supports.
    """

    udt_name: str
    definition: CreateTypeDefinition
    api_support: TableAPISupportDescriptor | None

    def __init__(
        self,
        *,
        udt_name: str,
        definition: CreateTypeDefinition,
        api_support: TableAPISupportDescriptor | None,
    ) -> None:
        self.udt_name = udt_name
        self.definition = definition
        self.api_support = api_support

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.udt_name}: {self.definition})"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "type": TableUDTColumnType.USERDEFINED.value,
            "udtName": self.udt_name,
            "definition": self.definition.as_dict(),
            **(
                {"apiSupport": self.api_support.as_dict()}
                if self.api_support is not None
                else {}
            ),
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> ListTypeDescriptor:
        """
        Create an instance of ListTypeDescriptor from a dictionary
        such as one from the Data API.
        """

        _warn_residual_keys(
            cls, raw_dict, {"type", "udtName", "definition", "apiSupport"}
        )
        if "type" in raw_dict:
            if raw_dict["type"] != TableUDTColumnType.USERDEFINED.value:
                warnings.warn(
                    "Unexpected 'type' found in a UDT description from the Data API: "
                    f"{repr(raw_dict['type'])} "
                    f"(for user-defined type '{raw_dict.get('udtName')}')."
                )
        return ListTypeDescriptor(
            udt_name=raw_dict["udtName"],
            definition=CreateTypeDefinition._from_dict(raw_dict["definition"]),
            api_support=TableAPISupportDescriptor._from_dict(raw_dict["apiSupport"])
            if "apiSupport" in raw_dict
            else None,
        )

    @classmethod
    def coerce(
        cls, raw_input: ListTypeDescriptor | dict[str, Any]
    ) -> ListTypeDescriptor:
        """
        Normalize the input, whether an object already or a plain dictionary
        of the right structure, into a ListTypeDescriptor.
        """

        if isinstance(raw_input, ListTypeDescriptor):
            return raw_input
        else:
            return cls._from_dict(raw_input)
