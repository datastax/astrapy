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
from typing import Any, TypeVar

from astrapy.data.info.table_descriptor.table_columns import (
    TableColumnTypeDescriptor,
)
from astrapy.utils.parsing import _warn_residual_keys

AYO = TypeVar("AYO", bound="AlterTypeOperation")


@dataclass
class AlterTypeOperation(ABC):
    """
    An abstract class representing a generic "alter type" operation, i.e. a change
    to be applied to a user-defined type (UDT) stored on the database. Concrete
    implementations are used to represent operations such as adding/renaming fields.

    `AlterTypeOperation` objects are used in the Database's `alter_type` method.

    Please consult the documentation of the concrete subclasses for more info.
    """

    _name: str

    @abstractmethod
    def as_dict(self) -> dict[str, Any]: ...

    @staticmethod
    def from_full_dict(operation_dict: dict[str, Any]) -> AlterTypeOperation:
        """
        Inspect a provided dictionary and make it into the correct concrete subclass
        of AlterTypeOperation depending on its contents.

        Note: while the nature of the operation must be the top-level single key of
        the (nested) dictionary parameter to this method (such as "add" or
        "rename"), the resulting `AlterTypeOperation` object encodes the content
        of the corresponding value. Likewise, calling the `as_dict()` method of
        the result from this method does not return the whole original input, rather
        the "one level in" part (see the example provided here).

        Args:
            operation_dict: a dictionary such as `{"add": ...}`, whose outermost *value*
            corresponds to the desired operation.

        Returns:
            an `AlterTypeOperation` object chosen after inspection of
                the provided input.

        Example:
            >>> full_dict = {"add": {"fields": {
            ...     "fld1": "text", "fld2": {"type": "int"}
            ... }}}
            >>> alter_op = AlterTypeOperation.from_full_dict(full_dict)
            >>> alter_op
            AlterTypeAddFields(fields=[fld1,fld2])
            >>> alter_op.as_dict()
            {'fields': {'fld1': {'type': 'text'}, 'fld2': {'type': 'int'}}}
        """

        key_set = set(operation_dict.keys())
        if key_set == {"add"}:
            return AlterTypeAddFields.coerce(operation_dict["add"])
        elif key_set == {"rename"}:
            return AlterTypeRenameFields.coerce(operation_dict["rename"])
        else:
            raise ValueError(
                f"Cannot parse a dict with keys {', '.join(sorted(key_set))} "
                "into an AlterTypeOperation"
            )

    @classmethod
    @abstractmethod
    def coerce(cls: type[AYO], raw_input: AYO | dict[str, Any]) -> AYO: ...

    @classmethod
    @abstractmethod
    def stack(cls: type[AYO], operations: list[AYO]) -> AYO: ...

    @classmethod
    def stack_by_name(
        cls, operations: list[AlterTypeOperation]
    ) -> dict[str, AlterTypeOperation]:
        grouped_ops: dict[str, list[AlterTypeOperation]] = {}
        for op in operations:
            grouped_ops[op._name] = grouped_ops.get(op._name, []) + [op]

        stacked_op_map: dict[str, AlterTypeOperation] = {
            op_name: op_list[0].stack(op_list)
            for op_name, op_list in grouped_ops.items()
        }
        return stacked_op_map


@dataclass
class AlterTypeAddFields(AlterTypeOperation):
    """
    An object representing the alter-type operation of adding field(s),
    for use in the argument to the database's `alter_type()` method.

    Attributes:
        fields: a mapping between the names of the fields to add and
            `TableColumnTypeDescriptor` objects, formatted in the same way as
            the `columns` attribute of `CreateTableDefinition`.
    """

    fields: dict[str, TableColumnTypeDescriptor]

    def __init__(
        self,
        *,
        fields: dict[str, TableColumnTypeDescriptor | dict[str, Any] | str],
    ) -> None:
        self._name = "add"
        self.fields = {
            fld_n: TableColumnTypeDescriptor.coerce(fld_v)
            for fld_n, fld_v in fields.items()
        }

    def __repr__(self) -> str:
        _fld_desc = f"fields=[{','.join(sorted(self.fields.keys()))}]"
        return f"{self.__class__.__name__}({_fld_desc})"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "fields": {fld_n: fld_v.as_dict() for fld_n, fld_v in self.fields.items()}
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> AlterTypeAddFields:
        """
        Create an instance of AlterTypeAddFields from a dictionary
        such as one suitable as (partial) command payload.
        """

        _warn_residual_keys(cls, raw_dict, {"fields"})
        return AlterTypeAddFields(fields=raw_dict["fields"])

    @classmethod
    def coerce(
        cls, raw_input: AlterTypeAddFields | dict[str, Any]
    ) -> AlterTypeAddFields:
        """
        Normalize the input, whether an object already or a plain dictionary
        of the right structure, into an AlterTypeAddFields.
        """

        if isinstance(raw_input, AlterTypeAddFields):
            return raw_input
        else:
            return cls._from_dict(raw_input)

    @classmethod
    def stack(cls, operations: list[AlterTypeAddFields]) -> AlterTypeAddFields:
        fields_dict: dict[str, TableColumnTypeDescriptor | dict[str, Any] | str] = {}
        for ataf in operations:
            fields_dict = {**fields_dict, **ataf.fields}
        return AlterTypeAddFields(fields=fields_dict)


@dataclass
class AlterTypeRenameFields(AlterTypeOperation):
    """
    An object representing the alter-type operation of renaming field(s),
    for use in the argument to the database's `alter_type()` method.

    Attributes:
        fields: a mapping from current to new names for the fields to rename.
    """

    fields: dict[str, str]

    def __init__(self, *, fields: dict[str, str]) -> None:
        self._name = "rename"
        self.fields = fields

    def __repr__(self) -> str:
        _renames = "; ".join(
            [f"'{old_n}' -> '{new_n}'" for old_n, new_n in sorted(self.fields.items())]
        )
        return f"{self.__class__.__name__}(fields=[{_renames}])"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""
        return {
            "fields": self.fields,
        }

    @classmethod
    def _from_dict(cls, raw_dict: dict[str, Any]) -> AlterTypeRenameFields:
        """
        Create an instance of AlterTypeRenameFields from a dictionary
        such as one suitable as (partial) command payload.
        """

        _warn_residual_keys(cls, raw_dict, {"fields"})
        return AlterTypeRenameFields(
            fields={old_n: new_n for old_n, new_n in raw_dict["fields"].items()},
        )

    @classmethod
    def coerce(
        cls, raw_input: AlterTypeRenameFields | dict[str, Any]
    ) -> AlterTypeRenameFields:
        """
        Normalize the input, whether an object already or a plain dictionary
        of the right structure, into an AlterTypeRenameFields.
        """

        if isinstance(raw_input, AlterTypeRenameFields):
            return raw_input
        else:
            return cls._from_dict(raw_input)

    @classmethod
    def stack(cls, operations: list[AlterTypeRenameFields]) -> AlterTypeRenameFields:
        fields_dict: dict[str, str] = {}
        for ataf in operations:
            fields_dict = {**fields_dict, **ataf.fields}
        return AlterTypeRenameFields(fields=fields_dict)
