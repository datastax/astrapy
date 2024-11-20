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
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, cast

from astrapy.data.info.database_info import AstraDBDatabaseInfo
from astrapy.data.info.vectorize import VectorServiceOptions
from astrapy.data.utils.table_types import (
    TableKeyValuedColumnType,
    TableScalarColumnType,
    TableUnsupportedColumnType,
    TableValuedColumnType,
    TableVectorColumnType,
)
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
class TableColumnTypeDescriptor(ABC):
    """
    TODO
    """

    column_type: (
        TableScalarColumnType
        | TableValuedColumnType
        | TableKeyValuedColumnType
        | TableVectorColumnType
        | TableUnsupportedColumnType
    )

    @abstractmethod
    def as_dict(self) -> dict[str, Any]: ...

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
            return TableScalarColumnTypeDescriptor(
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
class TableScalarColumnTypeDescriptor(TableColumnTypeDescriptor):
    """
    TODO
    """

    column_type: TableScalarColumnType

    def __init__(self, *, column_type: str | TableScalarColumnType) -> None:
        self.column_type = TableScalarColumnType.coerce(column_type)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.column_type})"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "type": self.column_type.value,
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> TableScalarColumnTypeDescriptor:
        """
        TODO
        """

        warn_residual_keys(cls, raw_dict, {"type"})
        return TableScalarColumnTypeDescriptor(
            column_type=raw_dict["type"],
        )


@dataclass
class TableVectorColumnTypeDescriptor(TableColumnTypeDescriptor):
    """
    TODO
    """

    column_type: TableVectorColumnType
    dimension: int | None
    service: VectorServiceOptions | None

    def __init__(
        self,
        *,
        column_type: str | TableVectorColumnType,
        dimension: int | None,
        service: VectorServiceOptions | None,
    ) -> None:
        self.dimension = dimension
        self.service = service
        super().__init__(column_type=TableVectorColumnType.coerce(column_type))

    def __repr__(self) -> str:
        not_null_pieces = [
            pc
            for pc in [
                f"dimension={self.dimension}" if self.dimension is not None else None,
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
                "type": self.column_type.value,
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
            dimension=raw_dict.get("dimension"),
            service=VectorServiceOptions.coerce(raw_dict.get("service")),
        )


@dataclass
class TableValuedColumnTypeDescriptor(TableColumnTypeDescriptor):
    """
    TODO
    """

    column_type: TableValuedColumnType
    value_type: TableScalarColumnType

    def __init__(
        self,
        *,
        column_type: str | TableValuedColumnType,
        value_type: str | TableScalarColumnType,
    ) -> None:
        self.value_type = TableScalarColumnType.coerce(value_type)
        super().__init__(column_type=TableValuedColumnType.coerce(column_type))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.column_type}<{self.value_type}>)"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "type": self.column_type.value,
            "valueType": self.value_type.value,
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
class TableKeyValuedColumnTypeDescriptor(TableColumnTypeDescriptor):
    """
    TODO
    """

    column_type: TableKeyValuedColumnType
    key_type: TableScalarColumnType
    value_type: TableScalarColumnType

    def __init__(
        self,
        *,
        column_type: str | TableKeyValuedColumnType,
        value_type: str | TableScalarColumnType,
        key_type: str | TableScalarColumnType,
    ) -> None:
        self.key_type = TableScalarColumnType.coerce(key_type)
        self.value_type = TableScalarColumnType.coerce(value_type)
        super().__init__(column_type=TableKeyValuedColumnType.coerce(column_type))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.column_type}<{self.key_type},{self.value_type}>)"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "type": self.column_type.value,
            "keyType": self.key_type.value,
            "valueType": self.value_type.value,
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

    column_type: TableUnsupportedColumnType
    api_support: TableAPISupportDescriptor

    def __init__(
        self,
        *,
        column_type: TableUnsupportedColumnType | str,
        api_support: TableAPISupportDescriptor,
    ) -> None:
        self.api_support = api_support
        super().__init__(column_type=TableUnsupportedColumnType.coerce(column_type))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.api_support.cql_definition})"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "type": self.column_type.value,
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
class BaseTableDefinition:
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
    def _from_dict(cls, raw_dict: dict[str, Any]) -> BaseTableDefinition:
        """
        Create an instance of BaseTableDefinition from a dictionary
        such as one from the Data API.
        """

        warn_residual_keys(cls, raw_dict, {"columns", "primaryKey"})
        return BaseTableDefinition(
            columns={
                col_n: TableColumnTypeDescriptor.coerce(col_v)
                for col_n, col_v in raw_dict["columns"].items()
            },
            primary_key=TablePrimaryKeyDescriptor.coerce(raw_dict["primaryKey"]),
        )

    @classmethod
    def coerce(
        cls, raw_input: BaseTableDefinition | dict[str, Any]
    ) -> BaseTableDefinition:
        if isinstance(raw_input, BaseTableDefinition):
            return raw_input
        else:
            return cls._from_dict(raw_input)

    @staticmethod
    def zero() -> BaseTableDefinition:
        return BaseTableDefinition(
            columns={},
            primary_key=TablePrimaryKeyDescriptor(
                partition_by=[],
                partition_sort={},
            ),
        )

    def add_primitive_column(
        self, column_name: str, column_type: str
    ) -> BaseTableDefinition:
        return BaseTableDefinition(
            columns={
                **self.columns,
                **{
                    column_name: TableScalarColumnTypeDescriptor(
                        column_type=TableScalarColumnType.coerce(column_type)
                    )
                },
            },
            primary_key=self.primary_key,
        )

    def add_column(self, column_name: str, column_type: str) -> BaseTableDefinition:
        return self.add_primitive_column(
            column_name=column_name, column_type=column_type
        )

    def add_set_column(self, column_name: str, column_type: str) -> BaseTableDefinition:
        return BaseTableDefinition(
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

    def add_list_column(self, column_name: str, value_type: str) -> BaseTableDefinition:
        return BaseTableDefinition(
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
    ) -> BaseTableDefinition:
        return BaseTableDefinition(
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
    ) -> BaseTableDefinition:
        return BaseTableDefinition(
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

    def add_partition_by(self, partition_columns: list[str]) -> BaseTableDefinition:
        return BaseTableDefinition(
            columns=self.columns,
            primary_key=TablePrimaryKeyDescriptor(
                partition_by=self.primary_key.partition_by + partition_columns,
                partition_sort=self.primary_key.partition_sort,
            ),
        )

    def add_partition_sort(self, partition_sort: dict[str, int]) -> BaseTableDefinition:
        return BaseTableDefinition(
            columns=self.columns,
            primary_key=TablePrimaryKeyDescriptor(
                partition_by=self.primary_key.partition_by,
                partition_sort={**self.primary_key.partition_sort, **partition_sort},
            ),
        )


@dataclass
class BaseTableDescriptor:
    """
    A structure expressing full description of a table as the Data API
    returns it, i.e. its name and its `definition` sub-structure.

    Attributes:
        name: the name of the table.
        definition: a BaseTableDefinition instance.
        raw_descriptor: the raw response from the Data API.
    """

    name: str
    definition: BaseTableDefinition
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
    def _from_dict(cls, raw_dict: dict[str, Any]) -> BaseTableDescriptor:
        """
        Create an instance of BaseTableDescriptor from a dictionary
        such as one from the Data API.
        """

        warn_residual_keys(cls, raw_dict, {"name", "definition"})
        return BaseTableDescriptor(
            name=raw_dict["name"],
            definition=BaseTableDefinition.coerce(raw_dict.get("definition") or {}),
            raw_descriptor=raw_dict,
        )

    @classmethod
    def coerce(
        cls, raw_input: BaseTableDescriptor | dict[str, Any]
    ) -> BaseTableDescriptor:
        if isinstance(raw_input, BaseTableDescriptor):
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
    source_model: str | UnsetType = _UNSET

    def __repr__(self) -> str:
        not_null_pieces = [
            pc
            for pc in (
                None if isinstance(self.metric, UnsetType) else f"metric={self.metric}",
                None
                if isinstance(self.source_model, UnsetType)
                else f"source_model={self.source_model}",
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
                "sourceModel": None
                if isinstance(self.source_model, UnsetType)
                else self.source_model,
            }.items()
            if v is not None
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> TableVectorIndexOptions:
        """
        Create an instance of TableIndexOptions from a dictionary
        such as one from the Data API.
        """

        warn_residual_keys(cls, raw_dict, {"metric", "sourceModel"})
        return TableVectorIndexOptions(
            metric=raw_dict["metric"] if raw_dict.get("metric") is not None else _UNSET,
            source_model=raw_dict["sourceModel"]
            if raw_dict.get("sourceModel") is not None
            else _UNSET,
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
class TableBaseIndexDefinition:
    """
    TODO
    """

    column: str

    @classmethod
    def from_dict(cls, raw_input: dict[str, Any]) -> TableBaseIndexDefinition:
        if "options" not in raw_input:
            return TableIndexDefinition.coerce(raw_input)
        else:
            if "metric" in raw_input["options"]:
                return TableVectorIndexDefinition.coerce(raw_input)
            else:
                return TableIndexDefinition.coerce(raw_input)


@dataclass
class TableIndexDefinition(TableBaseIndexDefinition):
    """
    TODO
    """

    options: TableIndexOptions

    def __init__(
        self,
        column: str,
        options: TableIndexOptions | UnsetType = _UNSET,
    ) -> None:
        self.column = column
        self.options = (
            TableIndexOptions() if isinstance(options, UnsetType) else options
        )

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
class TableVectorIndexDefinition(TableBaseIndexDefinition):
    """
    TODO
    """

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


@dataclass
class AlterTableOperation(ABC):
    """
    TODO
    """

    _name: str

    @abstractmethod
    def as_dict(self) -> dict[str, Any]: ...

    @staticmethod
    def from_full_dict(operation_dict: dict[str, Any]) -> AlterTableOperation:
        key_set = set(operation_dict.keys())
        if key_set == {"add"}:
            return AlterTableAddColumns.coerce(operation_dict["add"])
        elif key_set == {"drop"}:
            return AlterTableDropColumns.coerce(operation_dict["drop"])
        elif key_set == {"addVectorize"}:
            return AlterTableAddVectorize.coerce(operation_dict["addVectorize"])
        elif key_set == {"dropVectorize"}:
            return AlterTableDropVectorize.coerce(operation_dict["dropVectorize"])
        else:
            raise ValueError(
                f"Cannot parse a dict with keys {', '.join(sorted(key_set))} "
                "into an AlterTableOperation"
            )


@dataclass
class AlterTableAddColumns(AlterTableOperation):
    """
    TODO
    """

    columns: dict[str, TableColumnTypeDescriptor]

    def __init__(self, *, columns: dict[str, TableColumnTypeDescriptor]) -> None:
        self._name = "add"
        self.columns = columns

    def __repr__(self) -> str:
        _col_desc = f"columns=[{','.join(self.columns.keys())}]"
        return f"{self.__class__.__name__}({_col_desc})"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""
        return {
            "columns": {col_n: col_v.as_dict() for col_n, col_v in self.columns.items()}
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> AlterTableAddColumns:
        """
        Create an instance of AlterTableAddColumns from a dictionary
        such as one suitable as (partial) command payload.
        """

        warn_residual_keys(cls, raw_dict, {"columns"})
        return AlterTableAddColumns(
            columns={
                col_n: TableColumnTypeDescriptor.coerce(col_v)
                for col_n, col_v in raw_dict["columns"].items()
            },
        )

    @classmethod
    def coerce(
        cls, raw_input: AlterTableAddColumns | dict[str, Any]
    ) -> AlterTableAddColumns:
        if isinstance(raw_input, AlterTableAddColumns):
            return raw_input
        else:
            return cls._from_dict(raw_input)


@dataclass
class AlterTableDropColumns(AlterTableOperation):
    """
    TODO
    """

    columns: list[str]

    def __init__(self, *, columns: list[str]) -> None:
        self._name = "drop"
        self.columns = columns

    def __repr__(self) -> str:
        _col_desc = f"columns=[{','.join(self.columns)}]"
        return f"{self.__class__.__name__}({_col_desc})"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""
        return {
            "columns": self.columns,
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> AlterTableDropColumns:
        """
        Create an instance of AlterTableDropColumns from a dictionary
        such as one suitable as (partial) command payload.
        """
        warn_residual_keys(cls, raw_dict, {"columns"})
        return AlterTableDropColumns(
            columns=raw_dict["columns"],
        )

    @classmethod
    def coerce(
        cls, raw_input: AlterTableDropColumns | dict[str, Any]
    ) -> AlterTableDropColumns:
        if isinstance(raw_input, AlterTableDropColumns):
            return raw_input
        else:
            return cls._from_dict(raw_input)


@dataclass
class AlterTableAddVectorize(AlterTableOperation):
    """
    TODO
    """

    columns: dict[str, VectorServiceOptions]

    def __init__(self, *, columns: dict[str, VectorServiceOptions]) -> None:
        self._name = "addVectorize"
        self.columns = columns

    def __repr__(self) -> str:
        _cols_desc = [
            f"{col_n}({col_svc.provider}/{col_svc.model_name})"
            for col_n, col_svc in self.columns.items()
        ]
        return f"{self.__class__.__name__}(columns={', '.join(_cols_desc)})"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""
        return {
            "columns": {
                col_n: col_svc.as_dict() for col_n, col_svc in self.columns.items()
            }
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> AlterTableAddVectorize:
        """
        Create an instance of AlterTableAddVectorize from a dictionary
        such as one suitable as (partial) command payload.
        """
        warn_residual_keys(cls, raw_dict, {"columns"})
        _columns: dict[str, VectorServiceOptions | None] = {
            col_n: VectorServiceOptions.coerce(col_v)
            for col_n, col_v in raw_dict["columns"].items()
        }
        if any(_col_svc is None for _col_svc in _columns.values()):
            raise ValueError(
                "Vector service definition cannot be None for AlterTableAddVectorize"
            )
        return AlterTableAddVectorize(
            columns=cast(
                dict[str, VectorServiceOptions],
                _columns,
            )
        )

    @classmethod
    def coerce(
        cls, raw_input: AlterTableAddVectorize | dict[str, Any]
    ) -> AlterTableAddVectorize:
        if isinstance(raw_input, AlterTableAddVectorize):
            return raw_input
        else:
            return cls._from_dict(raw_input)


@dataclass
class AlterTableDropVectorize(AlterTableOperation):
    """
    TODO
    """

    columns: list[str]

    def __init__(self, *, columns: list[str]) -> None:
        self._name = "dropVectorize"
        self.columns = columns

    def __repr__(self) -> str:
        _col_desc = f"columns=[{','.join(self.columns)}]"
        return f"{self.__class__.__name__}({_col_desc})"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""
        return {
            "columns": self.columns,
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> AlterTableDropVectorize:
        """
        Create an instance of AlterTableDropVectorize from a dictionary
        such as one suitable as (partial) command payload.
        """
        warn_residual_keys(cls, raw_dict, {"columns"})
        return AlterTableDropVectorize(
            columns=raw_dict["columns"],
        )

    @classmethod
    def coerce(
        cls, raw_input: AlterTableDropVectorize | dict[str, Any]
    ) -> AlterTableDropVectorize:
        if isinstance(raw_input, AlterTableDropVectorize):
            return raw_input
        else:
            return cls._from_dict(raw_input)


# aliases
ListTableDescriptor = BaseTableDescriptor
CreateTableDescriptor = BaseTableDescriptor
ListTableDefinition = BaseTableDefinition
CreateTableDefinition = BaseTableDefinition
