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
    TableScalarColumnTypeDescriptor,
)
from astrapy.data.utils.table_types import ColumnType
from astrapy.utils.parsing import _warn_residual_keys


@dataclass
class CreateTypeDefinition:
    """
    A structure expressing the definition of a user-defined type( UDT) to be created
    through the Data API. This object is passed as the `definition` parameter to
    the database `create_type` method.

    See the Data API specifications for detailed specification and allowed values.

    Instances of this object can be created in three ways: using a fluent interface,
    passing a fully-formed definition to the class constructor, or coercing an
    appropriately-shaped plain dictionary into this class.

    Attributes:
        fields: a map from field names to their type definition object. This follows
            the same structure as the `columns` attribute of `CreateTableDefinition`.

    Example:
            >>> from astrapy.info import CreateTypeDefinition
            >>> from astrapy.info import ColumnType, TableScalarColumnTypeDescriptor
            >>>
            >>> type_definition_0 = (
            ...     CreateTypeDefinition.builder()
            ...     .add_field("tagline", ColumnType.TEXT)
            ...     .add_field("score", ColumnType.INT)
            ...     .add_field("height", "float")  # plain strings accepted for field types
            ...     .build()
            ... )
            >>>
            >>> type_definition_1 = CreateTypeDefinition(fields={
            ...     "tagline": TableScalarColumnTypeDescriptor(ColumnType.TEXT),
            ...     "score": TableScalarColumnTypeDescriptor(ColumnType.INT),
            ...     "height": TableScalarColumnTypeDescriptor(ColumnType.FLOAT),
            ... })
            >>>
            >>> fields_dict_2 = {
            ...     "fields": {
            ...         "tagline": "text",
            ...         "score": "int",
            ...         "height": "float",
            ...     },
            ... }
            >>> type_definition_2 = CreateTypeDefinition.coerce(fields_dict_2)
            >>>
            >>> fields_dict_3 = {
            ...     "fields": {
            ...         "tagline": "text",
            ...         "score": {"type": "int"},
            ...         "height": TableScalarColumnTypeDescriptor(ColumnType.FLOAT),
            ...     },
            ... }
            >>> type_definition_3_mixed = CreateTypeDefinition.coerce(fields_dict_3)
            >>> type_definition_0 == type_definition_1
            True
            >>> type_definition_1 == type_definition_2
            True
            >>> type_definition_2 == type_definition_3_mixed
            True
    """

    fields: dict[str, TableColumnTypeDescriptor]

    def __init__(
        self,
        *,
        fields: dict[str, TableColumnTypeDescriptor | dict[str, Any] | str],
    ) -> None:
        self.fields = {
            fld_n: TableColumnTypeDescriptor.coerce(fld_v)
            for fld_n, fld_v in fields.items()
        }

    def __repr__(self) -> str:
        fld_desc = f"fields=[{','.join(sorted(self.fields.keys()))}]"
        return f"{self.__class__.__name__}({fld_desc})"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "fields": {col_n: col_v.as_dict() for col_n, col_v in self.fields.items()},
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> CreateTypeDefinition:
        """
        Create an instance of CreateTypeDefinition from a dictionary
        such as one from the Data API.
        """

        _warn_residual_keys(cls, raw_dict, {"fields"})
        return CreateTypeDefinition(
            fields={
                fld_n: TableColumnTypeDescriptor.coerce(fld_v)
                for fld_n, fld_v in raw_dict["fields"].items()
            },
        )

    @classmethod
    def coerce(
        cls, raw_input: CreateTypeDefinition | dict[str, Any]
    ) -> CreateTypeDefinition:
        """
        Normalize the input, whether an object already or a plain dictionary
        of the right structure, into a CreateTypeDefinition.
        """

        if isinstance(raw_input, CreateTypeDefinition):
            return raw_input
        else:
            return cls._from_dict(raw_input)

    @staticmethod
    def builder() -> CreateTypeDefinition:
        """
        Create an "empty" builder for constructing a type definition through
        a fluent interface. The resulting object has no fields yet: those are to
        be added progressively with the `add_field` method.

        Being a "type without fields", the type definition returned by this method
        cannot be directly used to create a type.

        See the class docstring for a full example on using the fluent interface.

        Returns:
            a CreateTypeDefinition formally describing a type without fields.
        """

        return CreateTypeDefinition(fields={})

    def add_field(
        self, field_name: str, field_type: str | ColumnType
    ) -> CreateTypeDefinition:
        """
        Return a new type definition object with an added field
        of a scalar type (i.e. not a list, set or other composite type).
        This method is for use within the fluent interface for progressively
        building the final type definition.

        See the class docstring for a full example on using the fluent interface.

        Args:
            field_name: the name of the new field to add to the type.
            field_type: a string, or a `ColumnType` value, defining
                the scalar type for the field.

        Returns:
            a CreateTypeDefinition obtained by adding (or replacing) the desired
            field to this type definition.
        """

        return CreateTypeDefinition(
            fields={
                **self.fields,
                **{
                    field_name: TableScalarColumnTypeDescriptor(
                        column_type=ColumnType.coerce(field_type)
                    )
                },
            },
        )

    def build(self) -> CreateTypeDefinition:
        """
        The final step in the fluent (builder) interface. Calling this method
        finalizes the definition that has been built so far and makes it into a
        type definition ready for use e.g. with the database's `create_type` method.

        See the class docstring for a full example on using the fluent interface.

        Returns:
            a CreateTypeDefinition obtained by finalizing the definition being
                built so far.
        """

        return self
