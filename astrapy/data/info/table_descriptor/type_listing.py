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

from dataclasses import dataclass
from typing import Any

from astrapy.data.info.table_descriptor.table_columns import TableAPISupportDescriptor
from astrapy.data.info.table_descriptor.type_creation import CreateTypeDefinition
from astrapy.data.utils.table_types import (
    TableUDTColumnType,
    TableUnsupportedColumnType,
)
from astrapy.utils.parsing import _warn_residual_keys


@dataclass
class ListTypeDescriptor:
    """
    A structure describing a user-defined type (UDT) stored on the database.

    This object is used for the items returned by the database `list_types` method.
    `ListTypeDescriptor` expresses all information received by the Data API, including
    (when provided) the UDT name as found on the database, the UDT name and possibly a
    sub-object detailing the allowed operations with the UDT.

    This object must be able to describe any item returned from the Data API:
    this means it can describe "unsupported" UDTs as well (i.e. those which have been
    created outside of the Data API). Unsupported UDTs lack some attributes compared
    to the fully-supported ones.

    Attributes:
        udt_type: a value of either the TableUDTColumnType or
            the TableUnsupportedColumnType enum, depending on the UDT support status.
        udt_name: the name of the UDT as is stored in the database (and in a keyspace).
        definition: the definition of the type, i.e. its fields and their types.
        api_support: a structure detailing what operations the type supports.
    """

    udt_type: TableUDTColumnType | TableUnsupportedColumnType
    udt_name: str | None
    definition: CreateTypeDefinition | None
    api_support: TableAPISupportDescriptor | None

    def __init__(
        self,
        *,
        udt_type: TableUDTColumnType | TableUnsupportedColumnType,
        udt_name: str | None,
        definition: CreateTypeDefinition | None,
        api_support: TableAPISupportDescriptor | None,
    ) -> None:
        self.udt_type = udt_type
        self.udt_name = udt_name
        self.definition = definition
        self.api_support = api_support

    def __repr__(self) -> str:
        if isinstance(self.udt_type, TableUnsupportedColumnType):
            return f"{self.__class__.__name__}({self.udt_type.value})"
        else:
            return f"{self.__class__.__name__}({self.udt_name}: {self.definition})"

    @staticmethod
    def _is_valid_dict(raw_dict: dict[str, Any]) -> bool:
        """
        Assess whether a dictionary can be converted into a ListTypeDescriptor.

        This can be used by e.g. the database `list_types` method to filter
        offending responses and issue warnings if needed.

        Returns:
            True if and only if the dict is valid, otherwise False.
        """

        return all(fld in raw_dict for fld in {"type", "apiSupport"})

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            k: v
            for k, v in {
                "type": self.udt_type.value,
                "udtName": self.udt_name,
                "definition": self.definition.as_dict()
                if self.definition is not None
                else None,
                "apiSupport": self.api_support.as_dict()
                if self.api_support is not None
                else None,
            }.items()
            if v is not None
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
        _udt_type: TableUDTColumnType | TableUnsupportedColumnType
        if raw_dict["type"] in TableUDTColumnType:
            _udt_type = TableUDTColumnType.coerce(raw_dict["type"])
        else:
            _udt_type = TableUnsupportedColumnType.coerce(raw_dict["type"])
        return ListTypeDescriptor(
            udt_type=_udt_type,
            udt_name=raw_dict.get("udtName"),
            definition=CreateTypeDefinition._from_dict(raw_dict["definition"])
            if "definition" in raw_dict
            else None,
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
