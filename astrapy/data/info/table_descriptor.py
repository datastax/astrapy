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
from astrapy.data.info.vectorize import VectorServiceOptions


def warn_residual_keys(
    klass: type, raw_dict: dict[str, Any], known_keys: set[str]
) -> None:
    residual_keys = raw_dict.keys() - known_keys
    if residual_keys:
        warnings.warn(
            "Unexpected key(s) encountered parsing a dictionary into "
            f"a `{klass.__name__}`: '{','.join(sorted(residual_keys))}'"
        )


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
class TableColumnTypeDescriptor:
    """
    TODO
    """

    column_type: str

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.column_type})"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "type": self.column_type,
        }

    @classmethod
    def from_dict(cls, raw_dict: dict[str, Any]) -> TableColumnTypeDescriptor:
        """
        Create an instance of TableColumnTypeDescriptor from a dictionary
        such as one from the Data API.

        This method switches to the proper subclass depending on the input.
        """

        if "keyType" in raw_dict:
            return TableKeyValuedColumnTypeDescriptor.from_dict(raw_dict)
        elif "valueType" in raw_dict:
            return TableValuedColumnTypeDescriptor.from_dict(raw_dict)
        elif raw_dict["type"] == "vector":
            return TableVectorColumnTypeDescriptor.from_dict(raw_dict)
        else:
            warn_residual_keys(cls, raw_dict, {"type"})
            return TableColumnTypeDescriptor(
                column_type=raw_dict["type"],
            )


@dataclass
class TableVectorColumnTypeDescriptor(TableColumnTypeDescriptor):
    """
    TODO
    """

    dimension: int
    service: VectorServiceOptions | None

    def __repr__(self) -> str:
        not_null_pieces = [
            pc
            for pc in [
                f"dimension={self.dimension}",
                None if self.service is None else f"service={self.service}",
            ]
            if pc is not None
        ]
        inner_desc = ", ".join(not_null_pieces)

        return f"{self.__class__.__name__}({self.column_type}[{inner_desc}])"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            k: v
            for k, v in {
                "type": self.column_type,
                "dimension": self.dimension,
                "service": None if self.service is None else self.service.as_dict(),
            }.items()
            if v is not None
        }

    @classmethod
    def from_dict(cls, raw_dict: dict[str, Any]) -> TableVectorColumnTypeDescriptor:
        """
        Create an instance of TableVectorColumnTypeDescriptor from a dictionary
        such as one from the Data API.
        """

        warn_residual_keys(cls, raw_dict, {"type", "dimension"})
        return TableVectorColumnTypeDescriptor(
            column_type=raw_dict["type"],
            dimension=raw_dict["dimension"],
            service=VectorServiceOptions.from_dict(raw_dict.get("service")),
        )


@dataclass
class TableValuedColumnTypeDescriptor(TableColumnTypeDescriptor):
    """
    TODO
    """

    value_type: str

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.column_type}<{self.value_type}>)"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "type": self.column_type,
            "valueType": self.value_type,
        }

    @classmethod
    def from_dict(cls, raw_dict: dict[str, Any]) -> TableValuedColumnTypeDescriptor:
        """
        Create an instance of TableValuedColumnTypeDescriptor from a dictionary
        such as one from the Data API.
        """

        warn_residual_keys(cls, raw_dict, {"type", "valueType"})
        return TableValuedColumnTypeDescriptor(
            column_type=raw_dict["type"],
            value_type=raw_dict["valueType"],
        )


@dataclass
class TableKeyValuedColumnTypeDescriptor(TableValuedColumnTypeDescriptor):
    """
    TODO
    """

    key_type: str

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.column_type}<{self.key_type},{self.value_type}>)"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "type": self.column_type,
            "keyType": self.key_type,
            "valueType": self.value_type,
        }

    @classmethod
    def from_dict(cls, raw_dict: dict[str, Any]) -> TableKeyValuedColumnTypeDescriptor:
        """
        Create an instance of TableKeyValuedColumnTypeDescriptor from a dictionary
        such as one from the Data API.
        """

        warn_residual_keys(cls, raw_dict, {"type", "keyType", "valueType"})
        return TableKeyValuedColumnTypeDescriptor(
            column_type=raw_dict["type"],
            key_type=raw_dict["keyType"],
            value_type=raw_dict["valueType"],
        )


@dataclass
class TablePrimaryKeyDescriptor:
    """
    TODO
    """

    partition_by: list[str]
    partition_sort: dict[str, int]

    def __repr__(self) -> str:
        partition_key_block = ",".join(self.partition_by)
        clustering_block = ",".join(
            f"{clu_col_name}:{'a' if clu_col_sort > 0 else 'd'}"
            for clu_col_name, clu_col_sort in self.partition_sort.items()
        )
        pk_block = f"({partition_key_block}){clustering_block}"
        return f"{self.__class__.__name__}[{pk_block}]"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            k: v
            for k, v in {
                "partitionBy": self.partition_by,
                "partitionSort": dict(self.partition_sort.items()),
            }.items()
            if v is not None
        }

    @classmethod
    def from_dict(cls, raw_dict: dict[str, Any]) -> TablePrimaryKeyDescriptor:
        """
        Create an instance of TablePrimaryKeyDescriptor from a dictionary
        such as one from the Data API.
        """

        warn_residual_keys(cls, raw_dict, {"partitionBy", "partitionSort"})
        return TablePrimaryKeyDescriptor(
            partition_by=raw_dict["partitionBy"],
            partition_sort=raw_dict["partitionSort"],
        )


@dataclass
class TableDefinition:
    """
    A structure expressing the definition ("schema") of a table.
    See the Data API specifications for detailed specification and allowed values.

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
    def from_dict(cls, raw_dict: dict[str, Any]) -> TableDefinition:
        """
        Create an instance of TableDefinition from a dictionary
        such as one from the Data API.
        """

        warn_residual_keys(cls, raw_dict, {"columns", "primaryKey"})
        return TableDefinition(
            columns={
                col_n: TableColumnTypeDescriptor.from_dict(col_v)
                for col_n, col_v in raw_dict["columns"].items()
            },
            primary_key=TablePrimaryKeyDescriptor.from_dict(raw_dict["primaryKey"]),
        )


@dataclass
class TableDescriptor:
    """
    A structure expressing full description of a table as the Data API
    returns it, i.e. its name and its `definition` sub-structure.

    Attributes:
        name: the name of the table.
        definition: a TableDefinition instance.
        raw_descriptor: the raw response from the Data API.
    """

    name: str
    definition: TableDefinition
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
    def from_dict(cls, raw_dict: dict[str, Any]) -> TableDescriptor:
        """
        Create an instance of TableDescriptor from a dictionary
        such as one from the Data API.
        """

        warn_residual_keys(cls, raw_dict, {"name", "definition"})
        return TableDescriptor(
            name=raw_dict["name"],
            definition=TableDefinition.from_dict(raw_dict.get("definition") or {}),
            raw_descriptor=raw_dict,
        )
