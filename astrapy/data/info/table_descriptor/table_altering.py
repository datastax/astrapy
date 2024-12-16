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
from typing import Any, Dict, cast

from astrapy.data.info.table_descriptor.table_columns import (
    TableColumnTypeDescriptor,
)
from astrapy.data.info.vectorize import VectorServiceOptions
from astrapy.utils.parsing import _warn_residual_keys


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
                Dict[str, VectorServiceOptions],
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
