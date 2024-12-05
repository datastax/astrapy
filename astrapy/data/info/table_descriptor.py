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
from astrapy.utils.parsing import _warn_residual_keys
from astrapy.utils.unset import _UNSET, UnsetType


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
            _warn_residual_keys(cls, raw_dict, {"type"})
            return TableScalarColumnTypeDescriptor(
                column_type=raw_dict["type"],
            )

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
        column_type: a `TableScalarColumnType` value. When creating the object,
            simple strings such as "TEXT" or "UUID" are also accepted.
    """

    column_type: TableScalarColumnType

    def __init__(self, column_type: str | TableScalarColumnType) -> None:
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
        Create an instance of TableScalarColumnTypeDescriptor from a dictionary
        such as one from the Data API.
        """

        _warn_residual_keys(cls, raw_dict, {"type"})
        return TableScalarColumnTypeDescriptor(
            column_type=raw_dict["type"],
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

        _warn_residual_keys(cls, raw_dict, {"type", "dimension", "service"})
        return TableVectorColumnTypeDescriptor(
            column_type=raw_dict["type"],
            dimension=raw_dict.get("dimension"),
            service=VectorServiceOptions.coerce(raw_dict.get("service")),
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
            This is a `TableScalarColumnType`, but when creating the object,
            strings such as "TEXT" or "UUID" are also accepted.
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

        _warn_residual_keys(cls, raw_dict, {"type", "valueType"})
        return TableValuedColumnTypeDescriptor(
            column_type=raw_dict["type"],
            value_type=raw_dict["valueType"],
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
            This is a `TableScalarColumnType`, but when creating the object,
            strings such as "TEXT" or "UUID" are also accepted.
        value_type: the type of the individual values stored in the map for a single key.
            This is a `TableScalarColumnType`, but when creating the object,
            strings such as "TEXT" or "UUID" are also accepted.
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

        _warn_residual_keys(cls, raw_dict, {"type", "keyType", "valueType"})
        return TableKeyValuedColumnTypeDescriptor(
            column_type=raw_dict["type"],
            key_type=raw_dict["keyType"],
            value_type=raw_dict["valueType"],
        )


@dataclass
class TableAPISupportDescriptor:
    """
    Represents the additional information returned by the Data API when describing
    a table with unsupported columns. Unsupported columns may have been created by
    means other than the Data API (e.g. CQL direct interaction with the database).

    The Data API reports these columns when listing the tables and their metadata,
    and provides the information marshaled in this object to detail which level
    of support the column has (for instance, it can be a partial support where the
    column is readable by the API but not writable).

    Attributes:
        cql_definition: a free-form string containing the CQL definition for the column.
        create_table: whether a column of this nature can be used in API table creation.
        insert: whether a column of this nature can be written through the API.
        read: whether a column of this nature can be read through the API.
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

        _warn_residual_keys(
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


@dataclass
class BaseTableDefinition:
    """
    A structure expressing the definition ("schema") of a table.
    See the Data API specifications for detailed specification and allowed values.

    Instances of this object can be created in three ways: using a fluent interface,
    passing a fully-formed definition to the class constructor, or coercing an
    appropriately-shaped plain dictionary into this class.
    For the practical purpose of creating tables, it is recommended
    to import and use the `CreateTableDefinition` alias to this class.
    See the examples below and the Table documentation for more details.

    Attributes:
        columns: a map from column names to their type definition object.
        primary_key: a specification of the primary key for the table.

    Example:
        >>> from astrapy.constants import SortMode
        >>> from astrapy.info import (
        ...     CreateTableDefinition,
        ...     TablePrimaryKeyDescriptor,
        ...     TableScalarColumnType,
        ...     TableScalarColumnTypeDescriptor,
        ...     TableValuedColumnType,
        ...     TableValuedColumnTypeDescriptor,
        ...     TableVectorColumnTypeDescriptor,
        ... )
        >>>
        >>> # Create a table definition with the fluent interface:
        >>> table_definition = (
        ...     CreateTableDefinition.zero()
        ...     .add_column("match_id", TableScalarColumnType.TEXT)
        ...     .add_column("round", TableScalarColumnType.INT)
        ...     .add_vector_column("m_vector", dimension=3)
        ...     .add_column("score", TableScalarColumnType.INT)
        ...     .add_column("when", TableScalarColumnType.TIMESTAMP)
        ...     .add_column("winner", TableScalarColumnType.TEXT)
        ...     .add_set_column("fighters", TableScalarColumnType.UUID)
        ...     .add_partition_by(["match_id"])
        ...     .add_partition_sort({"round": SortMode.ASCENDING})
        ...     .build()
        ... )
        >>>
        >>> # Create a table definition passing everything to the constructor:
        >>> table_definition_1 = CreateTableDefinition(
        ...     columns={
        ...         "match_id": TableScalarColumnTypeDescriptor(
        ...             TableScalarColumnType.TEXT,
        ...         ),
        ...         "round": TableScalarColumnTypeDescriptor(
        ...             TableScalarColumnType.INT,
        ...         ),
        ...         "m_vector": TableVectorColumnTypeDescriptor(
        ...             column_type="vector", dimension=3
        ...         ),
        ...         "score": TableScalarColumnTypeDescriptor(
        ...             TableScalarColumnType.INT,
        ...         ),
        ...         "when": TableScalarColumnTypeDescriptor(
        ...             TableScalarColumnType.TIMESTAMP,
        ...         ),
        ...         "winner": TableScalarColumnTypeDescriptor(
        ...             TableScalarColumnType.TEXT,
        ...         ),
        ...         "fighters": TableValuedColumnTypeDescriptor(
        ...             column_type=TableValuedColumnType.SET,
        ...             value_type=TableScalarColumnType.UUID,
        ...         ),
        ...     },
        ...     primary_key=TablePrimaryKeyDescriptor(
        ...         partition_by=["match_id"],
        ...         partition_sort={"round": SortMode.ASCENDING},
        ...     ),
        ... )
        >>>
        >>> # Coerce a dictionary into a table definition:
        >>> table_definition_2_dict = {
        ...     "columns": {
        ...         "match_id": {"type": "text"},
        ...         "round": {"type": "int"},
        ...         "m_vector": {"type": "vector", "dimension": 3},
        ...         "score": {"type": "int"},
        ...         "when": {"type": "timestamp"},
        ...         "winner": {"type": "text"},
        ...         "fighters": {"type": "set", "valueType": "uuid"},
        ...     },
        ...     "primaryKey": {
        ...         "partitionBy": ["match_id"],
        ...         "partitionSort": {"round": 1},
        ...     },
        ... }
        >>> table_definition_2 = CreateTableDefinition.coerce(
        ...     table_definition_2_dict
        ... )
        >>>
        >>> # The three created objects are exactly identical:
        >>> table_definition_2 == table_definition_1
        True
        >>> table_definition_2 == table_definition
        True
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

        _warn_residual_keys(cls, raw_dict, {"columns", "primaryKey"})
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
        """
        Normalize the input, whether an object already or a plain dictionary
        of the right structure, into a BaseTableDefinition.
        """

        if isinstance(raw_input, BaseTableDefinition):
            return raw_input
        else:
            return cls._from_dict(raw_input)

    @staticmethod
    def zero() -> BaseTableDefinition:
        """
        Create an "empty" builder for constructing a table definition through
        a fluent interface. The resulting object has no columns and no primary key,
        traits that are to be added progressively with the corresponding methods.

        Since it describes a "table with no columns at all", the result of
        this method alone is not an acceptable table definition for running a table
        creation method on a Database.

        See the class docstring for a full example on using the fluent interface.

        Returns:
            a BaseTableDefinition formally describing a table with no columns.
        """

        return BaseTableDefinition(
            columns={},
            primary_key=TablePrimaryKeyDescriptor(
                partition_by=[],
                partition_sort={},
            ),
        )

    def add_scalar_column(
        self, column_name: str, column_type: str | TableScalarColumnType
    ) -> BaseTableDefinition:
        """
        Return a new table definition object with an added column
        of a scalar type (i.e. not a list, set or other composite type).
        This method is for use within the fluent interface for progressively
        building a complete table definition.

        See the class docstring for a full example on using the fluent interface.

        Args:
            column_name: the name of the new column to add to the definition.
            column_type: a string, or a `TableScalarColumnType` value, defining
                the scalar type for the column.

        Returns:
            a BaseTableDefinition obtained by adding (or replacing) the desired
            column to this table definition.
        """

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

    def add_column(
        self, column_name: str, column_type: str | TableScalarColumnType
    ) -> BaseTableDefinition:
        """
        Return a new table definition object with an added column
        of a scalar type (i.e. not a list, set or other composite type).
        This method is for use within the fluent interface for progressively
        building a complete table definition.

        This method is an alias for `add_scalar_column`.

        See the class docstring for a full example on using the fluent interface.

        Args:
            column_name: the name of the new column to add to the definition.
            column_type: a string, or a `TableScalarColumnType` value, defining
                the scalar type for the column.

        Returns:
            a BaseTableDefinition obtained by adding (or replacing) the desired
            column to this table definition.
        """

        return self.add_scalar_column(column_name=column_name, column_type=column_type)

    def add_set_column(
        self, column_name: str, value_type: str | TableScalarColumnType
    ) -> BaseTableDefinition:
        """
        Return a new table definition object with an added column
        of 'set' type. This method is for use within the
        fluent interface for progressively building a complete table definition.

        See the class docstring for a full example on using the fluent interface.

        Args:
            column_name: the name of the new column to add to the definition.
            value_type: a string, or a `TableScalarColumnType` value, defining
                the data type for the items in the set.

        Returns:
            a BaseTableDefinition obtained by adding (or replacing) the desired
            column to this table definition.
        """

        return BaseTableDefinition(
            columns={
                **self.columns,
                **{
                    column_name: TableValuedColumnTypeDescriptor(
                        column_type="set", value_type=value_type
                    )
                },
            },
            primary_key=self.primary_key,
        )

    def add_list_column(
        self, column_name: str, value_type: str | TableScalarColumnType
    ) -> BaseTableDefinition:
        """
        Return a new table definition object with an added column
        of 'list' type. This method is for use within the
        fluent interface for progressively building a complete table definition.

        See the class docstring for a full example on using the fluent interface.

        Args:
            column_name: the name of the new column to add to the definition.
            value_type: a string, or a `TableScalarColumnType` value, defining
                the data type for the items in the list.

        Returns:
            a BaseTableDefinition obtained by adding (or replacing) the desired
            column to this table definition.
        """

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
        self,
        column_name: str,
        key_type: str | TableScalarColumnType,
        value_type: str | TableScalarColumnType,
    ) -> BaseTableDefinition:
        """
        Return a new table definition object with an added column
        of 'map' type. This method is for use within the
        fluent interface for progressively building a complete table definition.

        See the class docstring for a full example on using the fluent interface.

        Args:
            column_name: the name of the new column to add to the definition.
            key_type: a string, or a `TableScalarColumnType` value, defining
                the data type for the keys in the map.
            value_type: a string, or a `TableScalarColumnType` value, defining
                the data type for the values in the map.

        Returns:
            a BaseTableDefinition obtained by adding (or replacing) the desired
            column to this table definition.
        """

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
        dimension: int | None = None,
        service: VectorServiceOptions | dict[str, Any] | None = None,
    ) -> BaseTableDefinition:
        """
        Return a new table definition object with an added column
        of 'vector' type. This method is for use within the
        fluent interface for progressively building a complete table definition.

        See the class docstring for a full example on using the fluent interface.

        Args:
            column_name: the name of the new column to add to the definition.
            dimension: the dimensionality of the vector, i.e. the number of components
                each vector in this column will have. If a `service` parameter is
                supplied and the vectorize model allows for it, the dimension may be
                left unspecified to have the API set a default value.
                The Data API will raise an error if a table creation is attempted with
                a vector column for which neither a service nor the dimension are given.
            service: a `VectorServiceOptions` object, or an equivalent plain dictionary,
                defining the server-side embedding service associated to the column,
                if desired.

        Returns:
            a BaseTableDefinition obtained by adding (or replacing) the desired
            column to this table definition.
        """

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

    def add_partition_by(
        self, partition_columns: list[str] | str
    ) -> BaseTableDefinition:
        """
        Return a new table definition object with one or more added `partition_by`
        columns. This method is for use within the
        fluent interface for progressively building a complete table definition.

        See the class docstring for a full example on using the fluent interface.

        Successive calls append the requested columns at the end of the pre-existing
        `partition_by` list. In other words, these two patterns are equivalent:
        (1) X.add_partition_by(["col1", "col2"])
        (2) X.add_partition_by(["col1"]).add_partition_by("col2")

        Note that no deduplication is applied to the overall
        result: the caller should take care of not supplying the same column name
        more than once.

        Args:
            partition_columns: a list of column names (strings) to be added to the
                full table partition key. A single string (not a list) is also accepted.

        Returns:
            a BaseTableDefinition obtained by enriching the `partition_by`
            of this table definition as requested.
        """

        _partition_columns = (
            partition_columns
            if isinstance(partition_columns, list)
            else [partition_columns]
        )

        return BaseTableDefinition(
            columns=self.columns,
            primary_key=TablePrimaryKeyDescriptor(
                partition_by=self.primary_key.partition_by + _partition_columns,
                partition_sort=self.primary_key.partition_sort,
            ),
        )

    def add_partition_sort(self, partition_sort: dict[str, int]) -> BaseTableDefinition:
        """
        Return a new table definition object with one or more added `partition_sort`
        column specifications. This method is for use within the
        fluent interface for progressively building a complete table definition.

        See the class docstring for a full example on using the fluent interface.

        Successive calls append (or replace) the requested columns at the end of
        the pre-existing `partition_sort` dictionary. In other words, these two
        patterns are equivalent:
        (1) X.add_partition_sort({"c1": 1, "c2": -1})
        (2) X.add_partition_sort({"c1": 1}).add_partition_sort({"c2": -1})

        Args:
            partition_sort: a dictoinary mapping column names to their sort mode
            (ascending/descending, i.e 1/-1. See also `astrapy.constants.SortMode`).

        Returns:
            a BaseTableDefinition obtained by enriching the `partition_sort`
            of this table definition as requested.
        """

        return BaseTableDefinition(
            columns=self.columns,
            primary_key=TablePrimaryKeyDescriptor(
                partition_by=self.primary_key.partition_by,
                partition_sort={**self.primary_key.partition_sort, **partition_sort},
            ),
        )

    def build(self) -> BaseTableDefinition:
        """
        The final step in the fluent (builder) interface. Calling this method
        finalizes the definition that has been built so far and makes it into a
        table definition ready for use in e.g. table creation.

        Note that this step may be automatically invoked by the receiving methods:
        however it is a good practice - and also adds to the readability of the code -
        to call it explicitly.

        See the class docstring for a full example on using the fluent interface.

        Returns:
            a BaseTableDefinition obtained by finalizing the definition being
                built so far.
        """

        return self


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

        _warn_residual_keys(cls, raw_dict, {"name", "definition"})
        return BaseTableDescriptor(
            name=raw_dict["name"],
            definition=BaseTableDefinition.coerce(raw_dict.get("definition") or {}),
            raw_descriptor=raw_dict,
        )

    @classmethod
    def coerce(
        cls, raw_input: BaseTableDescriptor | dict[str, Any]
    ) -> BaseTableDescriptor:
        """
        Normalize the input, whether an object already or a plain dictionary
        of the right structure, into a BaseTableDescriptor.
        """

        if isinstance(raw_input, BaseTableDescriptor):
            return raw_input
        else:
            return cls._from_dict(raw_input)


@dataclass
class TableIndexOptions:
    """
    An object describing the options for a table regular (non-vector) index.

    Both when creating indexes and retrieving index metadata from the API, instances
    of TableIndexOptions are used to express the corresponding index settings.

    Attributes:
        ascii: whether the index should convert to US-ASCII before indexing.
            It can be passed only for indexes on a TEXT or ASCII column.
        normalize: whether the index should normalize Unicode and diacritics before
            indexing. It can be passed only for indexes on a TEXT or ASCII column.
        case_sensitive: whether the index should index the input in a case-sensitive
            manner. It can be passed only for indexes on a TEXT or ASCII column.
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

        _warn_residual_keys(cls, raw_dict, {"ascii", "normalize", "caseSensitive"})
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
        """
        Normalize the input, whether an object already or a plain dictionary
        of the right structure, into a TableIndexOptions.
        """

        if isinstance(raw_input, TableIndexOptions):
            return raw_input
        else:
            return cls._from_dict(raw_input)


@dataclass
class TableVectorIndexOptions:
    """
    An object describing the options for a table vector index, which is the index
    that enables vector (ANN) search on a column.

    Both when creating indexes and retrieving index metadata from the API, instances
    of TableIndexOptions are used to express the corresponding index settings.

    Attributes:
        metric: the similarity metric used in the index. It must be one of the strings
            defined in `astrapy.constants.VectorMetric` (such as "dot_product").
        source_model: an optional parameter to help the index pick the set of
            parameters best suited to a specific embedding model. If omitted, the Data
            API will use its defaults. See the Data API documentation for more details.
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

        _warn_residual_keys(cls, raw_dict, {"metric", "sourceModel"})
        return TableVectorIndexOptions(
            metric=raw_dict["metric"] if raw_dict.get("metric") is not None else _UNSET,
            source_model=raw_dict["sourceModel"]
            if raw_dict.get("sourceModel") is not None
            else _UNSET,
        )

    @classmethod
    def coerce(
        cls, raw_input: TableVectorIndexOptions | dict[str, Any] | None
    ) -> TableVectorIndexOptions:
        """
        Normalize the input, whether an object already or a plain dictionary
        of the right structure, into a TableVectorIndexOptions.
        """

        if isinstance(raw_input, TableVectorIndexOptions):
            return raw_input
        elif raw_input is None:
            return cls(metric=_UNSET, source_model=_UNSET)
        else:
            return cls._from_dict(raw_input)


@dataclass
class TableBaseIndexDefinition(ABC):
    """
    An object describing an index definition, including the name of the indexed column
    and the index options if there are any.
    This is an abstract class common to the various types of index:
    see the appropriate subclass for more details.

    Attributes:
        column: the name of the indexed column.
    """

    column: str

    @abstractmethod
    def as_dict(self) -> dict[str, Any]: ...

    @classmethod
    def _from_dict(cls, raw_input: dict[str, Any]) -> TableBaseIndexDefinition:
        """
        Create an instance of TableBaseIndexDefinition from a dictionary
        such as one from the Data API. This method inspects the input dictionary
        to select the right class to use so as to represent the index definition.
        """

        if "options" not in raw_input:
            if raw_input["column"] == "UNKNOWN" and "apiSupport" in raw_input:
                return TableUnsupportedIndexDefinition.coerce(raw_input)
            else:
                return TableIndexDefinition.coerce(raw_input)
        else:
            if "metric" in raw_input["options"]:
                return TableVectorIndexDefinition.coerce(raw_input)
            else:
                return TableIndexDefinition.coerce(raw_input)


@dataclass
class TableIndexDefinition(TableBaseIndexDefinition):
    """
    An object describing a regular (non-vector) index definition,
    including the name of the indexed column and the index options.

    Attributes:
        column: the name of the indexed column.
        options: a `TableIndexOptions` detailing the index configuration.
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

        _warn_residual_keys(cls, raw_dict, {"column", "options"})
        return TableIndexDefinition(
            column=raw_dict["column"],
            options=TableIndexOptions.coerce(raw_dict["options"]),
        )

    @classmethod
    def coerce(
        cls, raw_input: TableIndexDefinition | dict[str, Any]
    ) -> TableIndexDefinition:
        """
        Normalize the input, whether an object already or a plain dictionary
        of the right structure, into a TableIndexDefinition.
        """

        if isinstance(raw_input, TableIndexDefinition):
            return raw_input
        else:
            _filled_raw_input = {**{"options": {}}, **raw_input}
            return cls._from_dict(_filled_raw_input)


@dataclass
class TableVectorIndexDefinition(TableBaseIndexDefinition):
    """
    An object describing a vector index definition,
    including the name of the indexed column and the index options.

    Attributes:
        column: the name of the indexed column.
        options: a `TableVectorIndexOptions` detailing the index configuration.
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

        _warn_residual_keys(cls, raw_dict, {"column", "options"})
        return TableVectorIndexDefinition(
            column=raw_dict["column"],
            options=TableVectorIndexOptions.coerce(raw_dict["options"]),
        )

    @classmethod
    def coerce(
        cls, raw_input: TableVectorIndexDefinition | dict[str, Any]
    ) -> TableVectorIndexDefinition:
        """
        Normalize the input, whether an object already or a plain dictionary
        of the right structure, into a TableVectorIndexDefinition.
        """

        if isinstance(raw_input, TableVectorIndexDefinition):
            return raw_input
        else:
            _filled_raw_input = {**{"options": {}}, **raw_input}
            return cls._from_dict(_filled_raw_input)


@dataclass
class TableAPIIndexSupportDescriptor:
    """
    Represents the additional information returned by the Data API when describing
    an index that has 'unsupported' status. Unsupported indexes may have been created by
    means other than the Data API (e.g. CQL direct interaction with the database).

    The Data API reports these indexes along with the others when listing the indexes,
    and provides the information marshaled in this object to detail which level
    of support the index has (for instance, it can be a partial support where the
    index can still be used to filter reads).

    Attributes:
        cql_definition: a free-form string containing the CQL definition for the index.
        create_index: whether such an index can be created through the Data API.
        filter: whether the index can be involved in a Data API filter clause.
    """

    cql_definition: str
    create_index: bool
    filter: bool

    def __repr__(self) -> str:
        desc = ", ".join(
            [
                f'"{self.cql_definition}"',
                f"create_index={self.create_index}",
                f"filter={self.filter}",
            ]
        )
        return f"{self.__class__.__name__}({desc})"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "cqlDefinition": self.cql_definition,
            "createIndex": self.create_index,
            "filter": self.filter,
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> TableAPIIndexSupportDescriptor:
        """
        Create an instance of TableAPIIndexSupportDescriptor from a dictionary
        such as one from the Data API.
        """

        _warn_residual_keys(
            cls,
            raw_dict,
            {"cqlDefinition", "createIndex", "filter"},
        )
        return TableAPIIndexSupportDescriptor(
            cql_definition=raw_dict["cqlDefinition"],
            create_index=raw_dict["createIndex"],
            filter=raw_dict["filter"],
        )


@dataclass
class TableUnsupportedIndexDefinition(TableBaseIndexDefinition):
    """
    An object describing the definition of an unsupported index found on a table,
    including the name of the indexed column and the index support status.

    Attributes:
        column: the name of the indexed column.
        api_support: a `TableAPIIndexSupportDescriptor` detailing the level of support
            for the index by the Data API.
    """

    api_support: TableAPIIndexSupportDescriptor

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.api_support.cql_definition})"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "column": self.column,
            "apiSupport": self.api_support.as_dict(),
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> TableUnsupportedIndexDefinition:
        """
        Create an instance of TableIndexDefinition from a dictionary
        such as one from the Data API.
        """

        _warn_residual_keys(cls, raw_dict, {"column", "apiSupport"})
        return TableUnsupportedIndexDefinition(
            column=raw_dict["column"],
            api_support=TableAPIIndexSupportDescriptor._from_dict(
                raw_dict["apiSupport"]
            ),
        )

    @classmethod
    def coerce(
        cls, raw_input: TableUnsupportedIndexDefinition | dict[str, Any]
    ) -> TableUnsupportedIndexDefinition:
        """
        Normalize the input, whether an object already or a plain dictionary
        of the right structure, into a TableUnsupportedIndexDefinition.
        """

        if isinstance(raw_input, TableUnsupportedIndexDefinition):
            return raw_input
        else:
            return cls._from_dict(raw_input)


@dataclass
class TableIndexDescriptor:
    """
    The top-level object describing a table index on a column.

    The hierarchical arrangement of `TableIndexDescriptor`, which contains a
    `TableBaseIndexDefinition` (plus possibly index options within the latter),
    is designed to mirror the shape of payloads and response about indexes in the
    Data API.

    Attributes:
        name: the name of the index. Index names are unique within a keyspace: hence,
            two tables in the same keyspace cannot use the same name for their indexes.
        definition: an appropriate concrete subclass of `TableBaseIndexDefinition`
            providing the detailed definition of the index.
    """

    name: str
    definition: TableBaseIndexDefinition

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "name": self.name,
            "definition": self.definition.as_dict(),
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> TableIndexDescriptor:
        """
        Create an instance of TableIndexDescriptor from a dictionary
        such as one from the Data API.
        """

        _warn_residual_keys(cls, raw_dict, {"name", "definition"})
        return TableIndexDescriptor(
            name=raw_dict["name"],
            definition=TableBaseIndexDefinition._from_dict(raw_dict["definition"]),
        )

    def coerce(
        raw_input: TableIndexDescriptor | dict[str, Any],
    ) -> TableIndexDescriptor:
        """
        Normalize the input, whether an object already or a plain dictionary
        of the right structure, into a TableIndexDescriptor.
        """

        if isinstance(raw_input, TableIndexDescriptor):
            return raw_input
        else:
            return TableIndexDescriptor._from_dict(raw_input)


@dataclass
class AlterTableOperation(ABC):
    """
    An abstract class representing a generic "alter table" operation. Concrete
    implementations are used to represent operations such as adding/dropping columns
    and adding/dropping the vectorize service (server-side embedding computations).

    `AlterTableOperation` objects are the parameter to the Table's `alter` method.

    Please consult the documentation of the concrete subclasses for more info.
    """

    _name: str

    @abstractmethod
    def as_dict(self) -> dict[str, Any]: ...

    @staticmethod
    def from_full_dict(operation_dict: dict[str, Any]) -> AlterTableOperation:
        """
        Inspect a provided dictionary and make it into the correct concrete subclass
        of AlterTableOperation depending on its contents.

        Note: while the nature of the operation must be the top-level single key of
        the (nested) dictionary parameter to this method (such as "add" or
        "dropVectorize"), the resulting `AlterTableOperation` object encodes the content
        of the corresponding value. Likewise, the calling the `as_dict()` method of
        the result from this method does not return the whole original input, rather
        the "one level in" part (see the example provided here).

        Args:
            operation_dict: a dictionary such as `{"add": ...}`, whose outermost *value*
            corresponds to the desired operation.

        Returns:
            an `AlterTableOperation` object chosen after inspection of
                the provided input.

        Example:
            >>> full_dict = {"drop": {"columns": ["col1", "col2"]}}
            >>> alter_op = AlterTableOperation.from_full_dict(full_dict)
            >>> alter_op
            AlterTableDropColumns(columns=[col1,col2])
            >>> alter_op.as_dict()
            {'columns': ['col1', 'col2']}
            >>> alter_op.as_dict() == full_dict["drop"]
            True
        """

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
    An object representing the alter-table operation of adding column(s),
    for use as argument to the table's `alter()` method.

    Attributes:
        columns: a mapping between the names of the columns to add and
            `TableColumnTypeDescriptor` objects, formatted in the same way as
            the `columns` attribute of `CreateTableDefinition`.
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

        _warn_residual_keys(cls, raw_dict, {"columns"})
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
        """
        Normalize the input, whether an object already or a plain dictionary
        of the right structure, into an AlterTableAddColumns.
        """

        if isinstance(raw_input, AlterTableAddColumns):
            return raw_input
        else:
            return cls._from_dict(raw_input)


@dataclass
class AlterTableDropColumns(AlterTableOperation):
    """
    An object representing the alter-table operation of dropping column(s),
    for use as argument to the table's `alter()` method.

    Attributes:
        columns: a list of the column names to drop.
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
        _warn_residual_keys(cls, raw_dict, {"columns"})
        return AlterTableDropColumns(
            columns=raw_dict["columns"],
        )

    @classmethod
    def coerce(
        cls, raw_input: AlterTableDropColumns | dict[str, Any]
    ) -> AlterTableDropColumns:
        """
        Normalize the input, whether an object already or a plain dictionary
        of the right structure, into an AlterTableDropColumns.
        """

        if isinstance(raw_input, AlterTableDropColumns):
            return raw_input
        else:
            return cls._from_dict(raw_input)


@dataclass
class AlterTableAddVectorize(AlterTableOperation):
    """
    An object representing the alter-table operation of enabling the vectorize service
    (i.e. server-side embedding computation) on one or more columns,
    for use as argument to the table's `alter()` method.

    Attributes:
        columns: a mapping between column names and the corresponding
            `VectorServiceOptions` objects describing the settings for the
            desired vectorize service.
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
        _warn_residual_keys(cls, raw_dict, {"columns"})
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
        """
        Normalize the input, whether an object already or a plain dictionary
        of the right structure, into an AlterTableAddVectorize.
        """

        if isinstance(raw_input, AlterTableAddVectorize):
            return raw_input
        else:
            return cls._from_dict(raw_input)


@dataclass
class AlterTableDropVectorize(AlterTableOperation):
    """
    An object representing the alter-table operation of removing the vectorize
    service (i.e. the server-side embedding computation) from one or more columns,
    for use as argument to the table's `alter()` method.

    Note: this operation does not drop the column, simply unsets its vectorize
    service. Existing embedding vectors, stored in the table, are retained.

    Attributes:
        columns: a list of the column names whose vectorize service is to be removed.
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
        _warn_residual_keys(cls, raw_dict, {"columns"})
        return AlterTableDropVectorize(
            columns=raw_dict["columns"],
        )

    @classmethod
    def coerce(
        cls, raw_input: AlterTableDropVectorize | dict[str, Any]
    ) -> AlterTableDropVectorize:
        """
        Normalize the input, whether an object already or a plain dictionary
        of the right structure, into an AlterTableDropVectorize.
        """

        if isinstance(raw_input, AlterTableDropVectorize):
            return raw_input
        else:
            return cls._from_dict(raw_input)


# aliases
ListTableDescriptor = BaseTableDescriptor
CreateTableDescriptor = BaseTableDescriptor
ListTableDefinition = BaseTableDefinition
CreateTableDefinition = BaseTableDefinition
