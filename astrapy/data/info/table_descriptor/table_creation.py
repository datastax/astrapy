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

from astrapy.data.info.table_descriptor.table_columns import (
    TableColumnTypeDescriptor,
    TableKeyValuedColumnTypeDescriptor,
    TablePrimaryKeyDescriptor,
    TableScalarColumnTypeDescriptor,
    TableValuedColumnTypeDescriptor,
    TableVectorColumnTypeDescriptor,
)
from astrapy.data.info.vectorize import VectorServiceOptions
from astrapy.data.utils.table_types import (
    ColumnType,
)
from astrapy.utils.parsing import _warn_residual_keys


@dataclass
class CreateTableDefinition:
    """
    A structure expressing the definition ("schema") of a table to be created through
    the Data API. This object is passed as the `definition` parameter to the database
    `create_table` method.

    See the Data API specifications for detailed specification and allowed values.

    Instances of this object can be created in three ways: using a fluent interface,
    passing a fully-formed definition to the class constructor, or coercing an
    appropriately-shaped plain dictionary into this class.

    Attributes:
        columns: a map from column names to their type definition object.
        primary_key: a specification of the primary key for the table.

    Example:
        >>> from astrapy.constants import SortMode
        >>> from astrapy.info import (
        ...     CreateTableDefinition,
        ...     TablePrimaryKeyDescriptor,
        ...     ColumnType,
        ...     TableScalarColumnTypeDescriptor,
        ...     TableValuedColumnType,
        ...     TableValuedColumnTypeDescriptor,
        ...     TableVectorColumnTypeDescriptor,
        ... )
        >>>
        >>> # Create a table definition with the fluent interface:
        >>> table_definition = (
        ...     CreateTableDefinition.builder()
        ...     .add_column("match_id", ColumnType.TEXT)
        ...     .add_column("round", ColumnType.INT)
        ...     .add_vector_column("m_vector", dimension=3)
        ...     .add_column("score", ColumnType.INT)
        ...     .add_column("when", ColumnType.TIMESTAMP)
        ...     .add_column("winner", ColumnType.TEXT)
        ...     .add_set_column("fighters", ColumnType.UUID)
        ...     .add_partition_by(["match_id"])
        ...     .add_partition_sort({"round": SortMode.ASCENDING})
        ...     .build()
        ... )
        >>>
        >>> # Create a table definition passing everything to the constructor:
        >>> table_definition_1 = CreateTableDefinition(
        ...     columns={
        ...         "match_id": TableScalarColumnTypeDescriptor(
        ...             ColumnType.TEXT,
        ...         ),
        ...         "round": TableScalarColumnTypeDescriptor(
        ...             ColumnType.INT,
        ...         ),
        ...         "m_vector": TableVectorColumnTypeDescriptor(
        ...             column_type="vector", dimension=3
        ...         ),
        ...         "score": TableScalarColumnTypeDescriptor(
        ...             ColumnType.INT,
        ...         ),
        ...         "when": TableScalarColumnTypeDescriptor(
        ...             ColumnType.TIMESTAMP,
        ...         ),
        ...         "winner": TableScalarColumnTypeDescriptor(
        ...             ColumnType.TEXT,
        ...         ),
        ...         "fighters": TableValuedColumnTypeDescriptor(
        ...             column_type=TableValuedColumnType.SET,
        ...             value_type=ColumnType.UUID,
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
    def _from_dict(cls, raw_dict: dict[str, Any]) -> CreateTableDefinition:
        """
        Create an instance of CreateTableDefinition from a dictionary
        such as one from the Data API.
        """

        _warn_residual_keys(cls, raw_dict, {"columns", "primaryKey"})
        return CreateTableDefinition(
            columns={
                col_n: TableColumnTypeDescriptor.coerce(col_v)
                for col_n, col_v in raw_dict["columns"].items()
            },
            primary_key=TablePrimaryKeyDescriptor.coerce(raw_dict["primaryKey"]),
        )

    @classmethod
    def coerce(
        cls, raw_input: CreateTableDefinition | dict[str, Any]
    ) -> CreateTableDefinition:
        """
        Normalize the input, whether an object already or a plain dictionary
        of the right structure, into a CreateTableDefinition.
        """

        if isinstance(raw_input, CreateTableDefinition):
            return raw_input
        else:
            return cls._from_dict(raw_input)

    @staticmethod
    def builder() -> CreateTableDefinition:
        """
        Create an "empty" builder for constructing a table definition through
        a fluent interface. The resulting object has no columns and no primary key,
        traits that are to be added progressively with the corresponding methods.

        Since it describes a "table with no columns at all", the result of
        this method alone is not an acceptable table definition for running a table
        creation method on a Database.

        See the class docstring for a full example on using the fluent interface.

        Returns:
            a CreateTableDefinition formally describing a table with no columns.
        """

        return CreateTableDefinition(
            columns={},
            primary_key=TablePrimaryKeyDescriptor(
                partition_by=[],
                partition_sort={},
            ),
        )

    def add_scalar_column(
        self, column_name: str, column_type: str | ColumnType
    ) -> CreateTableDefinition:
        """
        Return a new table definition object with an added column
        of a scalar type (i.e. not a list, set or other composite type).
        This method is for use within the fluent interface for progressively
        building a complete table definition.

        See the class docstring for a full example on using the fluent interface.

        Args:
            column_name: the name of the new column to add to the definition.
            column_type: a string, or a `ColumnType` value, defining
                the scalar type for the column.

        Returns:
            a CreateTableDefinition obtained by adding (or replacing) the desired
            column to this table definition.
        """

        return CreateTableDefinition(
            columns={
                **self.columns,
                **{
                    column_name: TableScalarColumnTypeDescriptor(
                        column_type=ColumnType.coerce(column_type)
                    )
                },
            },
            primary_key=self.primary_key,
        )

    def add_column(
        self, column_name: str, column_type: str | ColumnType
    ) -> CreateTableDefinition:
        """
        Return a new table definition object with an added column
        of a scalar type (i.e. not a list, set or other composite type).
        This method is for use within the fluent interface for progressively
        building a complete table definition.

        This method is an alias for `add_scalar_column`.

        See the class docstring for a full example on using the fluent interface.

        Args:
            column_name: the name of the new column to add to the definition.
            column_type: a string, or a `ColumnType` value, defining
                the scalar type for the column.

        Returns:
            a CreateTableDefinition obtained by adding (or replacing) the desired
            column to this table definition.
        """

        return self.add_scalar_column(column_name=column_name, column_type=column_type)

    def add_set_column(
        self, column_name: str, value_type: str | ColumnType
    ) -> CreateTableDefinition:
        """
        Return a new table definition object with an added column
        of 'set' type. This method is for use within the
        fluent interface for progressively building a complete table definition.

        See the class docstring for a full example on using the fluent interface.

        Args:
            column_name: the name of the new column to add to the definition.
            value_type: a string, or a `ColumnType` value, defining
                the data type for the items in the set.

        Returns:
            a CreateTableDefinition obtained by adding (or replacing) the desired
            column to this table definition.
        """

        return CreateTableDefinition(
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
        self, column_name: str, value_type: str | ColumnType
    ) -> CreateTableDefinition:
        """
        Return a new table definition object with an added column
        of 'list' type. This method is for use within the
        fluent interface for progressively building a complete table definition.

        See the class docstring for a full example on using the fluent interface.

        Args:
            column_name: the name of the new column to add to the definition.
            value_type: a string, or a `ColumnType` value, defining
                the data type for the items in the list.

        Returns:
            a CreateTableDefinition obtained by adding (or replacing) the desired
            column to this table definition.
        """

        return CreateTableDefinition(
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
        key_type: str | ColumnType,
        value_type: str | ColumnType,
    ) -> CreateTableDefinition:
        """
        Return a new table definition object with an added column
        of 'map' type. This method is for use within the
        fluent interface for progressively building a complete table definition.

        See the class docstring for a full example on using the fluent interface.

        Args:
            column_name: the name of the new column to add to the definition.
            key_type: a string, or a `ColumnType` value, defining
                the data type for the keys in the map.
            value_type: a string, or a `ColumnType` value, defining
                the data type for the values in the map.

        Returns:
            a CreateTableDefinition obtained by adding (or replacing) the desired
            column to this table definition.
        """

        return CreateTableDefinition(
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
    ) -> CreateTableDefinition:
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
            a CreateTableDefinition obtained by adding (or replacing) the desired
            column to this table definition.
        """

        return CreateTableDefinition(
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
    ) -> CreateTableDefinition:
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
            a CreateTableDefinition obtained by enriching the `partition_by`
            of this table definition as requested.
        """

        _partition_columns = (
            partition_columns
            if isinstance(partition_columns, list)
            else [partition_columns]
        )

        return CreateTableDefinition(
            columns=self.columns,
            primary_key=TablePrimaryKeyDescriptor(
                partition_by=self.primary_key.partition_by + _partition_columns,
                partition_sort=self.primary_key.partition_sort,
            ),
        )

    def add_partition_sort(
        self, partition_sort: dict[str, int]
    ) -> CreateTableDefinition:
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
            a CreateTableDefinition obtained by enriching the `partition_sort`
            of this table definition as requested.
        """

        return CreateTableDefinition(
            columns=self.columns,
            primary_key=TablePrimaryKeyDescriptor(
                partition_by=self.primary_key.partition_by,
                partition_sort={**self.primary_key.partition_sort, **partition_sort},
            ),
        )

    def build(self) -> CreateTableDefinition:
        """
        The final step in the fluent (builder) interface. Calling this method
        finalizes the definition that has been built so far and makes it into a
        table definition ready for use in e.g. table creation.

        Note that this step may be automatically invoked by the receiving methods:
        however it is a good practice - and also adds to the readability of the code -
        to call it explicitly.

        See the class docstring for a full example on using the fluent interface.

        Returns:
            a CreateTableDefinition obtained by finalizing the definition being
                built so far.
        """

        return self
