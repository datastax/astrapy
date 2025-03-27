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

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from astrapy.data.info.vectorize import VectorServiceOptions
from astrapy.data.utils.table_types import (
    ColumnType,
    TableKeyValuedColumnType,
    TableUnsupportedColumnType,
    TableValuedColumnType,
    TableVectorColumnType,
)
from astrapy.utils.parsing import _warn_residual_keys


@dataclass
class TableAPISupportDescriptor:
    """
    Represents the additional support information returned by the Data API when
    describing columns of a table. Some columns indeed require a detailed description
    of what operations are supported on them - this includes, but is not limited to,
    columns created by means other than the Data API (e.g. CQL direct interaction
    with the database).

    When the Data API reports these columns (in listing the tables and their metadata),
    it provides the information marshaled in this object to detail which level
    of support the column has (for instance, it can be a partial support whereby the
    column is readable by the API but not writable).

    Attributes:
        cql_definition: a free-form string containing the CQL definition for the column.
        create_table: whether a column of this nature can be used in API table creation.
        insert: whether a column of this nature can be written through the API.
        filter: whether a column of this nature can be used for filtering with API find.
        read: whether a column of this nature can be read through the API.
    """

    cql_definition: str
    create_table: bool
    insert: bool
    filter: bool
    read: bool

    def __repr__(self) -> str:
        desc = ", ".join(
            [
                f'"{self.cql_definition}"',
                f"create_table={self.create_table}",
                f"insert={self.insert}",
                f"filter={self.filter}",
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
            "filter": self.filter,
            "read": self.read,
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> TableAPISupportDescriptor:
        """
        Create an instance of TableAPISupportDescriptor from a dictionary
        such as one from the Data API.
        """

        _warn_residual_keys(
            cls,
            raw_dict,
            {"cqlDefinition", "createTable", "insert", "filter", "read"},
        )
        return TableAPISupportDescriptor(
            cql_definition=raw_dict["cqlDefinition"],
            create_table=raw_dict["createTable"],
            insert=raw_dict["insert"],
            filter=raw_dict["filter"],
            read=raw_dict["read"],
        )


@dataclass
class TableColumnTypeDescriptor(ABC):
    """
    Represents and describes a column in a Table, with its type and any
    additional property.

    This is an abstract class, whose concrete implementation are the various
    kinds of column descriptors such as `TableScalarColumnTypeDescriptor`,
    `TableVectorColumnTypeDescriptor`, `TableValuedColumnTypeDescriptor`, and so on.

    Attributes:
        column_type: an instance of one of the various column-type classes, according
            to the type of the column. In other words, each subclass of
            `TableColumnTypeDescriptor` has an appropriate object as its
            `column_type` attributes.
            For example the `column_type` of `TableValuedColumnTypeDescriptor`
            is a `TableValuedColumnType`.
        api_support: a `TableAPISupportDescriptor` object giving more details.
    """

    column_type: (
        ColumnType
        | TableValuedColumnType
        | TableKeyValuedColumnType
        | TableVectorColumnType
        | TableUnsupportedColumnType
    )
    api_support: TableAPISupportDescriptor | None

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
            return TableScalarColumnTypeDescriptor._from_dict(raw_dict)

    @classmethod
    def coerce(
        cls, raw_input: TableColumnTypeDescriptor | dict[str, Any] | str
    ) -> TableColumnTypeDescriptor:
        """
        Normalize the input, whether an object already or a plain dictionary
        of the right structure, into a TableColumnTypeDescriptor.
        """

        if isinstance(raw_input, TableColumnTypeDescriptor):
            return raw_input
        elif isinstance(raw_input, str):
            return cls._from_dict({"type": raw_input})
        else:
            return cls._from_dict(raw_input)


@dataclass
class TableScalarColumnTypeDescriptor(TableColumnTypeDescriptor):
    """
    Represents and describes a column in a Table, of scalar type, i.e. which contains
    a single simple value.

    Attributes:
        column_type: a `ColumnType` value. When creating the object,
            simple strings such as "TEXT" or "UUID" are also accepted.
        api_support: a `TableAPISupportDescriptor` object giving more details.
    """

    column_type: ColumnType

    def __init__(
        self,
        column_type: str | ColumnType,
        api_support: TableAPISupportDescriptor | None = None,
    ) -> None:
        super().__init__(
            column_type=ColumnType.coerce(column_type),
            api_support=api_support,
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.column_type.value})"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            k: v
            for k, v in {
                "type": self.column_type.value,
                "apiSupport": self.api_support.as_dict() if self.api_support else None,
            }.items()
            if v is not None
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> TableScalarColumnTypeDescriptor:
        """
        Create an instance of TableScalarColumnTypeDescriptor from a dictionary
        such as one from the Data API.
        """

        _warn_residual_keys(cls, raw_dict, {"type", "apiSupport"})
        return TableScalarColumnTypeDescriptor(
            column_type=raw_dict["type"],
            api_support=TableAPISupportDescriptor._from_dict(raw_dict["apiSupport"])
            if raw_dict.get("apiSupport")
            else None,
        )


@dataclass
class TableVectorColumnTypeDescriptor(TableColumnTypeDescriptor):
    """
    Represents and describes a column in a Table, of vector type, i.e. which contains
    a list of `dimension` floats that is treated specially as a "vector".

    Attributes:
        column_type: a `TableVectorColumnType` value. This can be omitted when
            creating the object. It only ever assumes the "VECTOR" value.
        dimension: an integer, the number of components (numbers) in the vectors.
            This can be left unspecified in some cases of vectorize-enabled columns.
        service: an optional `VectorServiceOptions` object defining the vectorize
            settings (i.e. server-side embedding computation) for the column.
        api_support: a `TableAPISupportDescriptor` object giving more details.
    """

    column_type: TableVectorColumnType
    dimension: int | None
    service: VectorServiceOptions | None

    def __init__(
        self,
        *,
        column_type: str | TableVectorColumnType = TableVectorColumnType.VECTOR,
        dimension: int | None,
        service: VectorServiceOptions | None = None,
        api_support: TableAPISupportDescriptor | None = None,
    ) -> None:
        self.dimension = dimension
        self.service = service
        super().__init__(
            column_type=TableVectorColumnType.coerce(column_type),
            api_support=api_support,
        )

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

        return f"{self.__class__.__name__}({self.column_type.value}[{inner_desc}])"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            k: v
            for k, v in {
                "type": self.column_type.value,
                "dimension": self.dimension,
                "service": None if self.service is None else self.service.as_dict(),
                "apiSupport": self.api_support.as_dict() if self.api_support else None,
            }.items()
            if v is not None
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> TableVectorColumnTypeDescriptor:
        """
        Create an instance of TableVectorColumnTypeDescriptor from a dictionary
        such as one from the Data API.
        """

        _warn_residual_keys(
            cls,
            raw_dict,
            {"type", "dimension", "service", "apiSupport"},
        )
        return TableVectorColumnTypeDescriptor(
            column_type=raw_dict["type"],
            dimension=raw_dict.get("dimension"),
            service=VectorServiceOptions.coerce(raw_dict.get("service")),
            api_support=TableAPISupportDescriptor._from_dict(raw_dict["apiSupport"])
            if raw_dict.get("apiSupport")
            else None,
        )


@dataclass
class TableValuedColumnTypeDescriptor(TableColumnTypeDescriptor):
    """
    Represents and describes a column in a Table, of a 'valued' type that stores
    multiple values. This means either a list or a set of homogeneous items.

    Attributes:
        column_type: an instance of `TableValuedColumnType`. When creating the
            object, simple strings such as "list" or "set" are also accepted.
        value_type: the type of the individual items stored in the column.
            This is a `ColumnType`, but when creating the object,
            strings such as "TEXT" or "UUID" are also accepted.
        api_support: a `TableAPISupportDescriptor` object giving more details.
    """

    column_type: TableValuedColumnType
    value_type: ColumnType

    def __init__(
        self,
        *,
        column_type: str | TableValuedColumnType,
        value_type: str | ColumnType,
        api_support: TableAPISupportDescriptor | None = None,
    ) -> None:
        self.value_type = ColumnType.coerce(value_type)
        super().__init__(
            column_type=TableValuedColumnType.coerce(column_type),
            api_support=api_support,
        )

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}({self.column_type.value}"
            f"<{self.value_type.value}>)"
        )

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            k: v
            for k, v in {
                "type": self.column_type.value,
                "valueType": self.value_type.value,
                "apiSupport": self.api_support.as_dict() if self.api_support else None,
            }.items()
            if v is not None
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> TableValuedColumnTypeDescriptor:
        """
        Create an instance of TableValuedColumnTypeDescriptor from a dictionary
        such as one from the Data API.
        """

        _warn_residual_keys(cls, raw_dict, {"type", "valueType", "apiSupport"})
        return TableValuedColumnTypeDescriptor(
            column_type=raw_dict["type"],
            value_type=raw_dict["valueType"],
            api_support=TableAPISupportDescriptor._from_dict(raw_dict["apiSupport"])
            if raw_dict.get("apiSupport")
            else None,
        )


@dataclass
class TableKeyValuedColumnTypeDescriptor(TableColumnTypeDescriptor):
    """
    Represents and describes a column in a Table, of a 'key-value' type, that stores
    an associative map (essentially a dict) between keys of a certain scalar type and
    values of a certain scalar type. The only such kind of column is a "map".

    Attributes:
        column_type: an instance of `TableKeyValuedColumnType`. When creating the
            object, this can be omitted as it only ever assumes the "MAP" value.
        key_type: the type of the individual keys in the map column.
            This is a `ColumnType`, but when creating the object,
            strings such as "TEXT" or "UUID" are also accepted.
        value_type: the type of the individual values stored in the map for a single key.
            This is a `ColumnType`, but when creating the object,
            strings such as "TEXT" or "UUID" are also accepted.
        api_support: a `TableAPISupportDescriptor` object giving more details.
    """

    column_type: TableKeyValuedColumnType
    key_type: ColumnType
    value_type: ColumnType

    def __init__(
        self,
        *,
        value_type: str | ColumnType,
        key_type: str | ColumnType,
        column_type: str | TableKeyValuedColumnType = TableKeyValuedColumnType.MAP,
        api_support: TableAPISupportDescriptor | None = None,
    ) -> None:
        self.key_type = ColumnType.coerce(key_type)
        self.value_type = ColumnType.coerce(value_type)
        super().__init__(
            column_type=TableKeyValuedColumnType.coerce(column_type),
            api_support=api_support,
        )

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}({self.column_type.value}"
            f"<{self.key_type.value},{self.value_type.value}>)"
        )

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            k: v
            for k, v in {
                "type": self.column_type.value,
                "keyType": self.key_type.value,
                "valueType": self.value_type.value,
                "apiSupport": self.api_support.as_dict() if self.api_support else None,
            }.items()
            if v is not None
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> TableKeyValuedColumnTypeDescriptor:
        """
        Create an instance of TableKeyValuedColumnTypeDescriptor from a dictionary
        such as one from the Data API.
        """

        _warn_residual_keys(
            cls, raw_dict, {"type", "keyType", "valueType", "apiSupport"}
        )
        return TableKeyValuedColumnTypeDescriptor(
            column_type=raw_dict["type"],
            key_type=raw_dict["keyType"],
            value_type=raw_dict["valueType"],
            api_support=TableAPISupportDescriptor._from_dict(raw_dict["apiSupport"])
            if raw_dict.get("apiSupport")
            else None,
        )


@dataclass
class TableUnsupportedColumnTypeDescriptor(TableColumnTypeDescriptor):
    """
    Represents and describes a column in a Table, of unsupported type.

    Note that this column type descriptor cannot be used in table creation,
    rather it can only be returned when listing the tables or getting their
    metadata by the API.

    Attributes:
        column_type: an instance of `TableUnsupportedColumnType`.
        api_support: a `TableAPISupportDescriptor` object giving more details.

    This class has no `coerce` method, since it is always only found in API responses.
    """

    column_type: TableUnsupportedColumnType
    api_support: TableAPISupportDescriptor

    def __init__(
        self,
        *,
        column_type: TableUnsupportedColumnType | str,
        api_support: TableAPISupportDescriptor,
    ) -> None:
        super().__init__(
            column_type=TableUnsupportedColumnType.coerce(column_type),
            api_support=api_support,
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.api_support.cql_definition})"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            k: v
            for k, v in {
                "type": self.column_type.value,
                "apiSupport": self.api_support.as_dict(),
            }.items()
            if v is not None
        }

    @classmethod
    def _from_dict(
        cls, raw_dict: dict[str, Any]
    ) -> TableUnsupportedColumnTypeDescriptor:
        """
        Create an instance of TableUnsupportedColumnTypeDescriptor from a dictionary
        such as one from the Data API.
        """

        _warn_residual_keys(cls, raw_dict, {"type", "apiSupport"})
        return TableUnsupportedColumnTypeDescriptor(
            column_type=raw_dict["type"],
            api_support=TableAPISupportDescriptor._from_dict(raw_dict["apiSupport"]),
        )


@dataclass
class TablePrimaryKeyDescriptor:
    """
    Represents the part of a table definition that describes the primary key.

    Attributes:
        partition_by: a list of column names forming the partition key, i.e.
            the portion of primary key that determines physical grouping and storage
            of rows on the database. Rows with the same values for the partition_by
            columns are guaranteed to be stored next to each other. This list
            cannot be empty.
        partition_sort: this defines how rows are to be sorted within a partition.
            It is a dictionary that specifies, for each column of the primary key
            not in the `partition_by` field, whether the sorting is ascending
            or descending (see the values in the `SortMode` constant).
            The sorting within a partition considers all columns in this dictionary,
            in a hierarchical way: hence, ordering in this dictionary is relevant.
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

        _warn_residual_keys(cls, raw_dict, {"partitionBy", "partitionSort"})
        return TablePrimaryKeyDescriptor(
            partition_by=raw_dict["partitionBy"],
            partition_sort=raw_dict["partitionSort"],
        )

    @classmethod
    def coerce(
        cls, raw_input: TablePrimaryKeyDescriptor | dict[str, Any] | str
    ) -> TablePrimaryKeyDescriptor:
        """
        Normalize the input, whether an object already or a plain dictionary
        of the right structure, into a TablePrimaryKeyDescriptor.
        """

        if isinstance(raw_input, TablePrimaryKeyDescriptor):
            return raw_input
        elif isinstance(raw_input, str):
            return cls._from_dict({"partitionBy": [raw_input], "partitionSort": {}})
        else:
            return cls._from_dict(raw_input)
