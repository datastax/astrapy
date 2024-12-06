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

from astrapy.data.info.database_info import AstraDBDatabaseInfo
from astrapy.data.info.table_descriptor.table_columns import (
    TableColumnTypeDescriptor,
    TablePrimaryKeyDescriptor,
)
from astrapy.utils.parsing import _warn_residual_keys


@dataclass
class TableInfo:
    """
    Represents the identifying information for a table,
    including the information about the database the table belongs to.

    Attributes:
        database_info: an AstraDBDatabaseInfo instance for the underlying database.
        keyspace: the keyspace where the table is located.
        name: table name. Unique within a keyspace (across tables/collections).
        full_name: identifier for the table within the database,
            in the form "keyspace.table_name".
    """

    database_info: AstraDBDatabaseInfo
    keyspace: str
    name: str
    full_name: str


@dataclass
class ListTableDefinition:
    """
    A structure expressing the definition ("schema") of a table the way the Data API
    describes it. This is the returned object when querying the Data API about
    table metadata.

    This class differs from `CreateTableDefinition`, used when creating tables:
    this one can also describe tables with unsupported features, which could not
    be created through the Data API.

    Attributes:
        columns: a map from column names to their type definition object.
        primary_key: a specification of the primary key for the table.
    """

    columns: dict[str, TableColumnTypeDescriptor]
    primary_key: TablePrimaryKeyDescriptor

    def __repr__(self) -> str:
        not_null_pieces = [
            pc
            for pc in [
                f"columns=[{','.join(self.columns.keys())}]",
                f"primary_key={self.primary_key}",
            ]
            if pc is not None
        ]
        return f"{self.__class__.__name__}({', '.join(not_null_pieces)})"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            k: v
            for k, v in {
                "columns": {
                    col_n: col_v.as_dict() for col_n, col_v in self.columns.items()
                },
                "primaryKey": self.primary_key.as_dict(),
            }.items()
            if v is not None
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> ListTableDefinition:
        """
        Create an instance of ListTableDefinition from a dictionary
        such as one from the Data API.
        """

        _warn_residual_keys(cls, raw_dict, {"columns", "primaryKey"})
        return ListTableDefinition(
            columns={
                col_n: TableColumnTypeDescriptor.coerce(col_v)
                for col_n, col_v in raw_dict["columns"].items()
            },
            primary_key=TablePrimaryKeyDescriptor.coerce(raw_dict["primaryKey"]),
        )

    @classmethod
    def coerce(
        cls, raw_input: ListTableDefinition | dict[str, Any]
    ) -> ListTableDefinition:
        """
        Normalize the input, whether an object already or a plain dictionary
        of the right structure, into a ListTableDefinition.
        """

        if isinstance(raw_input, ListTableDefinition):
            return raw_input
        else:
            return cls._from_dict(raw_input)


@dataclass
class ListTableDescriptor:
    """
    A structure expressing full description of a table as the Data API
    returns it, i.e. its name and its `definition` sub-structure.

    Attributes:
        name: the name of the table.
        definition: a ListTableDefinition instance.
        raw_descriptor: the raw response from the Data API.
    """

    name: str
    definition: ListTableDefinition
    raw_descriptor: dict[str, Any] | None

    def __repr__(self) -> str:
        not_null_pieces = [
            pc
            for pc in [
                f"name={self.name.__repr__()}",
                f"definition={self.definition.__repr__()}",
                None if self.raw_descriptor is None else "raw_descriptor=...",
            ]
            if pc is not None
        ]
        return f"{self.__class__.__name__}({', '.join(not_null_pieces)})"

    def as_dict(self) -> dict[str, Any]:
        """
        Recast this object into a dictionary.
        Empty `definition` will not be returned at all.
        """

        return {
            k: v
            for k, v in {
                "name": self.name,
                "definition": self.definition.as_dict(),
            }.items()
            if v
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> ListTableDescriptor:
        """
        Create an instance of ListTableDescriptor from a dictionary
        such as one from the Data API.
        """

        _warn_residual_keys(cls, raw_dict, {"name", "definition"})
        return ListTableDescriptor(
            name=raw_dict["name"],
            definition=ListTableDefinition.coerce(raw_dict.get("definition") or {}),
            raw_descriptor=raw_dict,
        )

    @classmethod
    def coerce(
        cls, raw_input: ListTableDescriptor | dict[str, Any]
    ) -> ListTableDescriptor:
        """
        Normalize the input, whether an object already or a plain dictionary
        of the right structure, into a ListTableDescriptor.
        """

        if isinstance(raw_input, ListTableDescriptor):
            return raw_input
        else:
            return cls._from_dict(raw_input)
