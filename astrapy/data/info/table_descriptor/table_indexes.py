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

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from astrapy.data.info.table_descriptor.table_columns import TableColumnTypeDescriptor
from astrapy.data.utils.table_types import (
    TableKeyValuedColumnType,
    TableValuedColumnType,
)
from astrapy.utils.parsing import _warn_residual_keys
from astrapy.utils.str_enum import StrEnum
from astrapy.utils.unset import _UNSET, UnsetType

logger = logging.getLogger(__name__)


class TableIndexType(StrEnum):
    """
    Enum to describe the index types for Table columns.
    """

    REGULAR = "regular"
    TEXT = "text"
    UNKNOWN = "UNKNOWN"
    VECTOR = "vector"


def _serialize_index_column_spec(
    column_spec: str | dict[str, str],
) -> str | dict[str, str]:
    """Ensure to re-cast str / dict index 'column' specification in a payload form.

    This amounts to leaving anything unchanged, except: {"col": "$entries"} ==> "col".
    """

    if isinstance(column_spec, str):
        return column_spec
    if len(column_spec) != 1:
        msg = f"Unrecognized column_spec for table index: {column_spec}"
        raise ValueError(msg)
    key, value = next(iter(column_spec.items()))
    if value == "$entries":
        return key
    else:
        return column_spec


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
        column: the name of the indexed column. For an index on a map column,
            it can be an object in a format such as {"column": "$values"} and similar.
    """

    column: str | dict[str, str]
    _index_type: TableIndexType

    @abstractmethod
    def as_dict(self) -> dict[str, Any]: ...

    @classmethod
    def _from_dict(
        cls,
        raw_input: dict[str, Any],
        *,
        columns: dict[str, TableColumnTypeDescriptor],
    ) -> TableBaseIndexDefinition:
        """
        Create an instance of TableBaseIndexDefinition from a dictionary
        such as one from the Data API. This method inspects the input dictionary
        to select the right class to use so as to represent the index definition.
        """

        if "options" not in raw_input:
            if raw_input["column"] == "UNKNOWN" and "apiSupport" in raw_input:
                return TableUnsupportedIndexDefinition.coerce(
                    raw_input, columns=columns
                )
            else:
                return TableIndexDefinition.coerce(raw_input, columns=columns)
        else:
            if "metric" in raw_input["options"]:
                return TableVectorIndexDefinition.coerce(raw_input, columns=columns)
            else:
                return TableIndexDefinition.coerce(raw_input, columns=columns)


@dataclass
class TableIndexDefinition(TableBaseIndexDefinition):
    """
    An object describing a regular (non-vector) index definition,
    including the name of the indexed column and the index options.

    Attributes:
        column: the name of the indexed column. For an index on a map column,
            it can be an object in a format such as {"column": "$values"} and similar.
        options: a `TableIndexOptions` detailing the index configuration.
    """

    options: TableIndexOptions

    def __init__(
        self,
        column: str | dict[str, str],
        options: TableIndexOptions | UnsetType = _UNSET,
    ) -> None:
        self._index_type = TableIndexType.REGULAR
        self.column = column
        self.options = (
            TableIndexOptions() if isinstance(options, UnsetType) else options
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.column}, options={self.options})"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            k: v
            for k, v in {
                "column": _serialize_index_column_spec(self.column),
                "options": self.options.as_dict(),
            }.items()
            if v
        }

    @classmethod
    def _from_dict(
        cls,
        raw_dict: dict[str, Any],
        *,
        columns: dict[str, TableColumnTypeDescriptor],
    ) -> TableIndexDefinition:
        """
        Create an instance of TableIndexDefinition from a dictionary
        such as one from the Data API.
        """

        # Handling 'col_name' becoming either 'col_name' / {'col_name': '$entries'}:

        col_spec = raw_dict["column"]
        recast_column: str | dict[str, str]
        if isinstance(col_spec, str) and col_spec in columns:
            column_type = columns[col_spec].column_type
            if isinstance(column_type, TableKeyValuedColumnType):
                recast_column = {col_spec: "$entries"}
            elif isinstance(column_type, TableValuedColumnType):
                recast_column = {col_spec: "$values"}
            else:
                recast_column = col_spec
        else:
            recast_column = col_spec

        _warn_residual_keys(cls, raw_dict, {"column", "options"})
        return TableIndexDefinition(
            column=recast_column,
            options=TableIndexOptions.coerce(raw_dict.get("options") or {}),
        )

    @classmethod
    def coerce(
        cls,
        raw_input: TableIndexDefinition | dict[str, Any],
        *,
        columns: dict[str, TableColumnTypeDescriptor],
    ) -> TableIndexDefinition:
        """
        Normalize the input, whether an object already or a plain dictionary
        of the right structure, into a TableIndexDefinition.
        """

        if isinstance(raw_input, TableIndexDefinition):
            return raw_input
        else:
            _filled_raw_input = {**{"options": {}}, **raw_input}
            return cls._from_dict(_filled_raw_input, columns=columns)


@dataclass
class TableVectorIndexDefinition(TableBaseIndexDefinition):
    """
    An object describing a vector index definition,
    including the name of the indexed column and the index options.

    Attributes:
        column: the name of the indexed column.
        options: a `TableVectorIndexOptions` detailing the index configuration.
    """

    column: str
    options: TableVectorIndexOptions

    def __init__(
        self,
        column: str,
        options: TableVectorIndexOptions,
    ) -> None:
        self._index_type = TableIndexType.VECTOR
        self.column = column
        self.options = options

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.column}, options={self.options})"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            k: v
            for k, v in {
                "column": self.column,
                "options": self.options.as_dict(),
            }.items()
            if v
        }

    @classmethod
    def _from_dict(
        cls,
        raw_dict: dict[str, Any],
        *,
        columns: dict[str, TableColumnTypeDescriptor],
    ) -> TableVectorIndexDefinition:
        """
        Create an instance of TableIndexDefinition from a dictionary
        such as one from the Data API.
        """

        _warn_residual_keys(cls, raw_dict, {"column", "options"})
        return TableVectorIndexDefinition(
            column=raw_dict["column"],
            options=TableVectorIndexOptions.coerce(raw_dict.get("options") or {}),
        )

    @classmethod
    def coerce(
        cls,
        raw_input: TableVectorIndexDefinition | dict[str, Any],
        *,
        columns: dict[str, TableColumnTypeDescriptor],
    ) -> TableVectorIndexDefinition:
        """
        Normalize the input, whether an object already or a plain dictionary
        of the right structure, into a TableVectorIndexDefinition.
        """

        if isinstance(raw_input, TableVectorIndexDefinition):
            return raw_input
        else:
            _filled_raw_input = {**{"options": {}}, **raw_input}
            return cls._from_dict(_filled_raw_input, columns=columns)


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

    column: str
    api_support: TableAPIIndexSupportDescriptor

    def __init__(
        self,
        column: str,
        api_support: TableAPIIndexSupportDescriptor,
    ) -> None:
        self._index_type = TableIndexType.UNKNOWN
        self.column = column
        self.api_support = api_support

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.api_support.cql_definition})"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "column": self.column,
            "apiSupport": self.api_support.as_dict(),
        }

    @classmethod
    def _from_dict(
        cls,
        raw_dict: dict[str, Any],
        *,
        columns: dict[str, TableColumnTypeDescriptor],
    ) -> TableUnsupportedIndexDefinition:
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
        cls,
        raw_input: TableUnsupportedIndexDefinition | dict[str, Any],
        *,
        columns: dict[str, TableColumnTypeDescriptor],
    ) -> TableUnsupportedIndexDefinition:
        """
        Normalize the input, whether an object already or a plain dictionary
        of the right structure, into a TableUnsupportedIndexDefinition.
        """

        if isinstance(raw_input, TableUnsupportedIndexDefinition):
            return raw_input
        else:
            return cls._from_dict(raw_input, columns=columns)


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
        index_type: a value in the TableIndexType enum, describing the index type.
    """

    name: str
    definition: TableBaseIndexDefinition
    index_type: TableIndexType

    def __init__(
        self,
        name: str,
        definition: TableBaseIndexDefinition,
        index_type: TableIndexType | str | UnsetType = _UNSET,
    ) -> None:
        self.name = name
        self.definition = definition
        if isinstance(index_type, UnsetType):
            self.index_type = self.definition._index_type
        else:
            self.index_type = TableIndexType.coerce(index_type)

    def __repr__(self) -> str:
        not_null_pieces = [
            pc
            for pc in (
                self.name,
                f"definition={self.definition}",
                f"index_type={self.index_type.value}",
            )
            if pc is not None
        ]
        inner_desc = ", ".join(not_null_pieces)
        return f"{self.__class__.__name__}({inner_desc})"

    def as_dict(self) -> dict[str, Any]:
        """Recast this object into a dictionary."""

        return {
            "name": self.name,
            "definition": self.definition.as_dict(),
            "indexType": self.index_type.value,
        }

    @classmethod
    def _from_dict(
        cls, raw_dict: dict[str, Any], *, columns: dict[str, TableColumnTypeDescriptor]
    ) -> TableIndexDescriptor:
        """
        Create an instance of TableIndexDescriptor from a dictionary
        such as one from the Data API.
        """

        _warn_residual_keys(cls, raw_dict, {"name", "definition", "indexType"})

        index_definition: TableBaseIndexDefinition
        # index type determination:
        if "indexType" in raw_dict:
            idx_type = raw_dict["indexType"]
            idx_def = raw_dict["definition"]
            if idx_type == TableIndexType.REGULAR.value:
                index_definition = TableIndexDefinition._from_dict(
                    idx_def,
                    columns=columns,
                )
            elif idx_type == TableIndexType.VECTOR.value:
                index_definition = TableVectorIndexDefinition._from_dict(
                    idx_def, columns=columns
                )
            elif idx_type == TableIndexType.UNKNOWN.value:
                index_definition = TableUnsupportedIndexDefinition._from_dict(
                    idx_def, columns=columns
                )
            else:
                # not throwing here. Log a warning and try the inspection path
                logger.warning(
                    f"Found an unexpected indexType when parsing a {cls.__name__} "
                    f"dictionary: {idx_type}. Falling back to inspecting the "
                    f"index definition."
                )
                index_definition = TableBaseIndexDefinition._from_dict(
                    raw_dict["definition"],
                    columns=columns,
                )
        else:
            # fall back to the 'inspection' path
            logger.info(
                f"Field 'indexType' missing when parsing a {cls.__name__} "
                f"dictionary. Falling back to inspecting the "
                f"index definition."
            )
            index_definition = TableBaseIndexDefinition._from_dict(
                raw_dict["definition"],
                columns=columns,
            )

        return TableIndexDescriptor(
            name=raw_dict["name"],
            definition=index_definition,
        )

    def coerce(
        raw_input: TableIndexDescriptor | dict[str, Any],
        *,
        columns: dict[str, TableColumnTypeDescriptor],
    ) -> TableIndexDescriptor:
        """
        Normalize the input, whether an object already or a plain dictionary
        of the right structure, into a TableIndexDescriptor.
        """

        if isinstance(raw_input, TableIndexDescriptor):
            return raw_input
        else:
            return TableIndexDescriptor._from_dict(raw_input, columns=columns)
