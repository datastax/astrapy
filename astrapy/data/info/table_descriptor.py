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

from astrapy.data.info.database_info import DatabaseInfo


@dataclass
class TableInfo:
    """
    Represents the identifying information for a table,
    including the information about the database the table belongs to.

    Attributes:
        database_info: a DatabaseInfo instance for the underlying database.
        keyspace: the keyspace where the table is located.
        name: table name. Unique within a keyspace (across tables/collections).
        full_name: identifier for the table within the database,
            in the form "keyspace.table_name".
    """

    database_info: DatabaseInfo
    keyspace: str
    name: str
    full_name: str


@dataclass
class TableOptions:
    """
    A structure expressing the options ("schema") of a table.
    See the Data API specifications for detailed specification and allowed values.

    Attributes:
        raw_options: the raw response from the Data API for the table configuration.
    """

    raw_options: dict[str, Any] | None

    def __repr__(self) -> str:
        not_null_pieces = [
            pc
            for pc in [
                None if self.raw_options is None else "raw_options=...",
            ]
            if pc is not None
        ]
        return f"{self.__class__.__name__}({', '.join(not_null_pieces)})"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            k: v
            for k, v in {
                "temp": "temp"
            }.items()
            if v is not None
        }

    @staticmethod
    def from_dict(raw_dict: dict[str, Any]) -> TableOptions:
        """
        Create an instance of TableOptions from a dictionary
        such as one from the Data API.
        """

        return TableOptions(
            raw_options=raw_dict,
        )


@dataclass
class TableDescriptor:
    """
    A structure expressing full description of a table as the Data API
    returns it, i.e. its name and its `options` sub-structure.

    Attributes:
        name: the name of the table.
        options: a TableOptions instance.
        raw_descriptor: the raw response from the Data API.
    """

    name: str
    options: TableOptions
    raw_descriptor: dict[str, Any] | None

    def __repr__(self) -> str:
        not_null_pieces = [
            pc
            for pc in [
                f"name={self.name.__repr__()}",
                f"options={self.options.__repr__()}",
                None if self.raw_descriptor is None else "raw_descriptor=...",
            ]
            if pc is not None
        ]
        return f"{self.__class__.__name__}({', '.join(not_null_pieces)})"

    def as_dict(self) -> dict[str, Any]:
        """
        Recast this object into a dictionary.
        Empty `options` will not be returned at all.
        """

        return {
            k: v
            for k, v in {
                "name": self.name,
                "options": self.options.as_dict(),
            }.items()
            if v
        }

    @staticmethod
    def from_dict(raw_dict: dict[str, Any]) -> TableDescriptor:
        """
        Create an instance of TableDescriptor from a dictionary
        such as one from the Data API.
        """

        return TableDescriptor(
            name=raw_dict["name"],
            options=TableOptions.from_dict(raw_dict.get("options") or {}),
            raw_descriptor=raw_dict,
        )
