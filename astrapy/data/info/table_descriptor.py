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
from astrapy.utils.str_enum import StrEnum
from astrapy.utils.unset import _UNSET, UnsetType


def warn_residual_keys(
    klass: type, raw_dict: dict[str, Any], known_keys: set[str]
) -> None:
    residual_keys = raw_dict.keys() - known_keys
    if residual_keys:
        warnings.warn(
            "Unexpected key(s) encountered parsing a dictionary into "
            f"a `{klass.__name__}`: '{','.join(sorted(residual_keys))}'"
        )


class TableScalarType(StrEnum):
    ASCII = "ASCII"
    BIGINT = "BIGINT"
    BLOB = "BLOB"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    DECIMAL = "DECIMAL"
    DOUBLE = "DOUBLE"
    DURATION = "DURATION"
    FLOAT = "FLOAT"
    INET = "INET"
    INT = "INT"
    SMALLINT = "SMALLINT"
    TEXT = "TEXT"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    TINYINT = "TINYINT"
    UUID = "UUID"
    VARINT = "VARINT"


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
    def _from_dict(cls, raw_dict: dict[str, Any]) -> TableColumnTypeDescriptor:
        """
        Create an instance of TableColumnTypeDescriptor from a dictionary
        such as one from the Data API.

        This method switches to the proper subclass depending on the input.
        """

        if "keyType" in raw_dict:
            return TableKeyValuedColumnTypeDescriptor._from_dict(raw_dict)
        elif "valueType" in raw_dict:
            return TableValuedColumnTypeDescriptor._from_dict(raw_dict)
        elif raw_dict["type"] == "vector":
            return TableVectorColumnTypeDescriptor._from_dict(raw_dict)
        elif raw_dict["type"] == "UNSUPPORTED":
            return TableUnsupportedColumnTypeDescriptor._from_dict(raw_dict)
        else:
            warn_residual_keys(cls, raw_dict, {"type"})
            return TableColumnTypeDescriptor(
                column_type=raw_dict["type"],
            )

    @classmethod
    def coerce(
        cls, raw_input: TableColumnTypeDescriptor | dict[str, Any] | str
    ) -> TableColumnTypeDescriptor:
        if isinstance(raw_input, TableColumnTypeDescriptor):
            return raw_input
        elif isinstance(raw_input, str):
            return cls._from_dict({"type": raw_input})
        else:
            return cls._from_dict(raw_input)


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
    def _from_dict(cls, raw_dict: dict[str, Any]) -> TableVectorColumnTypeDescriptor:
        """
        Create an instance of TableVectorColumnTypeDescriptor from a dictionary
        such as one from the Data API.
        """

        warn_residual_keys(cls, raw_dict, {"type", "dimension", "service"})
        return TableVectorColumnTypeDescriptor(
            column_type=raw_dict["type"],
            dimension=raw_dict["dimension"],
            service=VectorServiceOptions.coerce(raw_dict.get("service")),
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
    def _from_dict(cls, raw_dict: dict[str, Any]) -> TableValuedColumnTypeDescriptor:
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
    def _from_dict(cls, raw_dict: dict[str, Any]) -> TableKeyValuedColumnTypeDescriptor:
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
class TableAPISupportDescriptor:
    """
    TODO
    """

    cql_definition: str
    create_table: bool
    insert: bool
    read: bool

    def __repr__(self) -> str:
        desc = ", ".join(
            [
                f'"{self.cql_definition}"',
                f"create_table={self.create_table}",
                f"insert={self.insert}",
                f"read={self.read}",
            ]
        )
        return f"{self.__class__.__name__}({desc})"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "cqlDefinition": self.cql_definition,
            "createTable": self.create_table,
            "insert": self.insert,
            "read": self.read,
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> TableAPISupportDescriptor:
        """
        Create an instance of TableAPISupportDescriptor from a dictionary
        such as one from the Data API.
        """

        warn_residual_keys(
            cls,
            raw_dict,
            {"cqlDefinition", "createTable", "insert", "read"},
        )
        return TableAPISupportDescriptor(
            cql_definition=raw_dict["cqlDefinition"],
            create_table=raw_dict["createTable"],
            insert=raw_dict["insert"],
            read=raw_dict["read"],
        )


@dataclass
class TableUnsupportedColumnTypeDescriptor(TableColumnTypeDescriptor):
    """
    TODO
    This has no coerce since it is always only found in API responses
    """

    api_support: TableAPISupportDescriptor

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.api_support.cql_definition})"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "type": self.column_type,
            "apiSupport": self.api_support.as_dict(),
        }

    @classmethod
    def _from_dict(
        cls, raw_dict: dict[str, Any]
    ) -> TableUnsupportedColumnTypeDescriptor:
        """
        Create an instance of TableUnsupportedColumnTypeDescriptor from a dictionary
        such as one from the Data API.
        """

        warn_residual_keys(cls, raw_dict, {"type", "apiSupport"})
        return TableUnsupportedColumnTypeDescriptor(
            column_type=raw_dict["type"],
            api_support=TableAPISupportDescriptor._from_dict(raw_dict["apiSupport"]),
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
    def _from_dict(cls, raw_dict: dict[str, Any]) -> TablePrimaryKeyDescriptor:
        """
        Create an instance of TablePrimaryKeyDescriptor from a dictionary
        such as one from the Data API.
        """

        warn_residual_keys(cls, raw_dict, {"partitionBy", "partitionSort"})
        return TablePrimaryKeyDescriptor(
            partition_by=raw_dict["partitionBy"],
            partition_sort=raw_dict["partitionSort"],
        )

    @classmethod
    def coerce(
        cls, raw_input: TablePrimaryKeyDescriptor | dict[str, Any] | str
    ) -> TablePrimaryKeyDescriptor:
        if isinstance(raw_input, TablePrimaryKeyDescriptor):
            return raw_input
        elif isinstance(raw_input, str):
            return cls._from_dict({"partitionBy": [raw_input], "partitionSort": {}})
        else:
            return cls._from_dict(raw_input)


@dataclass
class TableDefinition:
    """
    A structure expressing the definition ("schema") of a table.
    See the Data API specifications for detailed specification and allowed values.

    Attributes:
        columns: a map from column names to their type definition object.
        primary_key: a specification of the primary key for the table.

    TODO ways to build one, examples (here and throughout)
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
    def _from_dict(cls, raw_dict: dict[str, Any]) -> TableDefinition:
        """
        Create an instance of TableDefinition from a dictionary
        such as one from the Data API.
        """

        warn_residual_keys(cls, raw_dict, {"columns", "primaryKey"})
        return TableDefinition(
            columns={
                col_n: TableColumnTypeDescriptor.coerce(col_v)
                for col_n, col_v in raw_dict["columns"].items()
            },
            primary_key=TablePrimaryKeyDescriptor.coerce(raw_dict["primaryKey"]),
        )

    @classmethod
    def coerce(cls, raw_input: TableDefinition | dict[str, Any]) -> TableDefinition:
        if isinstance(raw_input, TableDefinition):
            return raw_input
        else:
            return cls._from_dict(raw_input)

    @staticmethod
    def zero() -> TableDefinition:
        return TableDefinition(
            columns={},
            primary_key=TablePrimaryKeyDescriptor(
                partition_by=[],
                partition_sort={},
            ),
        )

    def add_primitive_column(
        self, column_name: str, column_type: str
    ) -> TableDefinition:
        return TableDefinition(
            columns={
                **self.columns,
                **{column_name: TableColumnTypeDescriptor(column_type=column_type)},
            },
            primary_key=self.primary_key,
        )

    def add_column(self, column_name: str, column_type: str) -> TableDefinition:
        return self.add_primitive_column(
            column_name=column_name, column_type=column_type
        )

    def add_set_column(self, column_name: str, column_type: str) -> TableDefinition:
        return TableDefinition(
            columns={
                **self.columns,
                **{
                    column_name: TableValuedColumnTypeDescriptor(
                        column_type="set", value_type=column_type
                    )
                },
            },
            primary_key=self.primary_key,
        )

    def add_list_column(self, column_name: str, value_type: str) -> TableDefinition:
        return TableDefinition(
            columns={
                **self.columns,
                **{
                    column_name: TableValuedColumnTypeDescriptor(
                        column_type="list", value_type=value_type
                    )
                },
            },
            primary_key=self.primary_key,
        )

    def add_map_column(
        self, column_name: str, key_type: str, value_type: str
    ) -> TableDefinition:
        return TableDefinition(
            columns={
                **self.columns,
                **{
                    column_name: TableKeyValuedColumnTypeDescriptor(
                        column_type="map", key_type=key_type, value_type=value_type
                    )
                },
            },
            primary_key=self.primary_key,
        )

    def add_vector_column(
        self,
        column_name: str,
        *,
        dimension: int,
        service: VectorServiceOptions | dict[str, Any] | None = None,
    ) -> TableDefinition:
        return TableDefinition(
            columns={
                **self.columns,
                **{
                    column_name: TableVectorColumnTypeDescriptor(
                        column_type="vector",
                        dimension=dimension,
                        service=VectorServiceOptions.coerce(service),
                    )
                },
            },
            primary_key=self.primary_key,
        )

    def add_partition_by(self, partition_columns: list[str]) -> TableDefinition:
        return TableDefinition(
            columns=self.columns,
            primary_key=TablePrimaryKeyDescriptor(
                partition_by=self.primary_key.partition_by + partition_columns,
                partition_sort=self.primary_key.partition_sort,
            ),
        )

    def add_partition_sort(self, partition_sort: dict[str, int]) -> TableDefinition:
        return TableDefinition(
            columns=self.columns,
            primary_key=TablePrimaryKeyDescriptor(
                partition_by=self.primary_key.partition_by,
                partition_sort={**self.primary_key.partition_sort, **partition_sort},
            ),
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
    def _from_dict(cls, raw_dict: dict[str, Any]) -> TableDescriptor:
        """
        Create an instance of TableDescriptor from a dictionary
        such as one from the Data API.
        """

        warn_residual_keys(cls, raw_dict, {"name", "definition"})
        return TableDescriptor(
            name=raw_dict["name"],
            definition=TableDefinition.coerce(raw_dict.get("definition") or {}),
            raw_descriptor=raw_dict,
        )

    @classmethod
    def coerce(cls, raw_input: TableDescriptor | dict[str, Any]) -> TableDescriptor:
        if isinstance(raw_input, TableDescriptor):
            return raw_input
        else:
            return cls._from_dict(raw_input)


@dataclass
class TableIndexOptions:
    """
    TODO
    """

    ascii: bool | UnsetType = _UNSET
    normalize: bool | UnsetType = _UNSET
    case_sensitive: bool | UnsetType = _UNSET

    def __repr__(self) -> str:
        not_null_pieces = [
            pc
            for pc in (
                None if isinstance(self.ascii, UnsetType) else f"ascii={self.ascii}",
                None
                if isinstance(self.ascii, UnsetType)
                else f"normalize={self.normalize}",
                None
                if isinstance(self.ascii, UnsetType)
                else f"case_sensitive={self.case_sensitive}",
            )
            if pc is not None
        ]
        inner_desc = ", ".join(not_null_pieces)
        return f"{self.__class__.__name__}({inner_desc})"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            k: v
            for k, v in {
                "ascii": None if isinstance(self.ascii, UnsetType) else self.ascii,
                "normalize": None
                if isinstance(self.normalize, UnsetType)
                else self.normalize,
                "caseSensitive": None
                if isinstance(self.case_sensitive, UnsetType)
                else self.case_sensitive,
            }.items()
            if v is not None
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> TableIndexOptions:
        """
        Create an instance of TableIndexOptions from a dictionary
        such as one from the Data API.
        """

        warn_residual_keys(cls, raw_dict, {"ascii", "normalize", "caseSensitive"})
        return TableIndexOptions(
            ascii=raw_dict["ascii"] if raw_dict.get("ascii") is not None else _UNSET,
            normalize=raw_dict["normalize"]
            if raw_dict.get("normalize") is not None
            else _UNSET,
            case_sensitive=raw_dict["caseSensitive"]
            if raw_dict.get("caseSensitive") is not None
            else _UNSET,
        )

    @classmethod
    def coerce(cls, raw_input: TableIndexOptions | dict[str, Any]) -> TableIndexOptions:
        if isinstance(raw_input, TableIndexOptions):
            return raw_input
        else:
            return cls._from_dict(raw_input)


@dataclass
class TableVectorIndexOptions:
    """
    TODO
    """

    metric: str | UnsetType = _UNSET

    def __repr__(self) -> str:
        not_null_pieces = [
            pc
            for pc in (
                None if isinstance(self.metric, UnsetType) else f"metric={self.metric}",
            )
            if pc is not None
        ]
        inner_desc = ", ".join(not_null_pieces)
        return f"{self.__class__.__name__}({inner_desc})"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            k: v
            for k, v in {
                "metric": None if isinstance(self.metric, UnsetType) else self.metric,
            }.items()
            if v is not None
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> TableVectorIndexOptions:
        """
        Create an instance of TableIndexOptions from a dictionary
        such as one from the Data API.
        """

        warn_residual_keys(cls, raw_dict, {"metric"})
        return TableVectorIndexOptions(
            metric=raw_dict["metric"] if raw_dict.get("metric") is not None else _UNSET,
        )

    @classmethod
    def coerce(
        cls, raw_input: TableVectorIndexOptions | dict[str, Any]
    ) -> TableVectorIndexOptions:
        if isinstance(raw_input, TableVectorIndexOptions):
            return raw_input
        else:
            return cls._from_dict(raw_input)


@dataclass
class TableIndexDefinition:
    """
    TODO
    """

    column: str
    options: TableIndexOptions

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.column}, options={self.options})"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "column": self.column,
            "options": self.options.as_dict(),
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> TableIndexDefinition:
        """
        Create an instance of TableIndexDefinition from a dictionary
        such as one from the Data API.
        """

        warn_residual_keys(cls, raw_dict, {"column", "options"})
        return TableIndexDefinition(
            column=raw_dict["column"],
            options=TableIndexOptions.coerce(raw_dict["options"]),
        )

    @classmethod
    def coerce(
        cls, raw_input: TableIndexDefinition | dict[str, Any]
    ) -> TableIndexDefinition:
        if isinstance(raw_input, TableIndexDefinition):
            return raw_input
        else:
            _filled_raw_input = {**{"options": {}}, **raw_input}
            return cls._from_dict(_filled_raw_input)


@dataclass
class TableVectorIndexDefinition:
    """
    TODO
    """

    column: str
    options: TableVectorIndexOptions

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.column}, options={self.options})"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "column": self.column,
            "options": self.options.as_dict(),
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> TableVectorIndexDefinition:
        """
        Create an instance of TableIndexDefinition from a dictionary
        such as one from the Data API.
        """

        warn_residual_keys(cls, raw_dict, {"column", "options"})
        return TableVectorIndexDefinition(
            column=raw_dict["column"],
            options=TableVectorIndexOptions.coerce(raw_dict["options"]),
        )

    @classmethod
    def coerce(
        cls, raw_input: TableVectorIndexDefinition | dict[str, Any]
    ) -> TableVectorIndexDefinition:
        if isinstance(raw_input, TableVectorIndexDefinition):
            return raw_input
        else:
            _filled_raw_input = {**{"options": {}}, **raw_input}
            return cls._from_dict(_filled_raw_input)
