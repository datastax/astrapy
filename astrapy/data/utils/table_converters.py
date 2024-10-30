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

import decimal
import ipaddress
from typing import Any, Callable

from astrapy.data.info.table_descriptor import (
    TableColumnTypeDescriptor,
    TableKeyValuedColumnTypeDescriptor,
    TableScalarColumnTypeDescriptor,
    TableUnsupportedColumnTypeDescriptor,
    TableValuedColumnTypeDescriptor,
    TableVectorColumnTypeDescriptor,
)
from astrapy.data.utils.extended_json_converters import (
    convert_ejson_binary_object_to_bytes,
)
from astrapy.data.utils.table_types import (
    TableKeyValuedColumnType,
    TableScalarColumnType,
    TableUnsupportedColumnType,
    TableValuedColumnType,
    TableVectorColumnType,
)
from astrapy.data_types import (
    DataAPITimestamp,
    TableDate,
    TableDuration,
    TableMap,
    TableSet,
    TableTime,
)
from astrapy.ids import UUID


def _create_scalar_tpostprocessor(
    column_type: TableScalarColumnType,
) -> Callable[[Any], Any]:
    if column_type in {
        TableScalarColumnType.TEXT,
        TableScalarColumnType.ASCII,
    }:

        def _tpostprocessor_text(raw_value: Any) -> str | None:
            return raw_value  # type: ignore[no-any-return]

        return _tpostprocessor_text

    elif column_type == TableScalarColumnType.BOOLEAN:

        def _tpostprocessor_bool(raw_value: Any) -> bool | None:
            return raw_value  # type: ignore[no-any-return]

        return _tpostprocessor_bool

    elif column_type in {
        TableScalarColumnType.INT,
        TableScalarColumnType.VARINT,
        TableScalarColumnType.BIGINT,
        TableScalarColumnType.SMALLINT,
        TableScalarColumnType.TINYINT,
    }:

        def _tpostprocessor_int(raw_value: Any) -> int | None:
            return raw_value  # type: ignore[no-any-return]

        return _tpostprocessor_int

    elif column_type in {
        TableScalarColumnType.FLOAT,
        TableScalarColumnType.DOUBLE,
    }:

        def _tpostprocessor_float(raw_value: Any) -> float | None:
            if raw_value is None:
                return None
            return float(raw_value)

        return _tpostprocessor_float

    elif column_type == TableScalarColumnType.BLOB:

        def _tpostprocessor_bytes(raw_value: Any) -> bytes | None:
            if raw_value is None:
                return None
            if isinstance(raw_value, dict):
                # {"$binary": ...}
                return convert_ejson_binary_object_to_bytes(raw_value)
            return raw_value  # type: ignore[no-any-return]

        return _tpostprocessor_bytes

    elif column_type == TableScalarColumnType.UUID:

        def _tpostprocessor_uuid(raw_value: Any) -> UUID | None:
            if raw_value is None:
                return None
            return UUID(raw_value)

        return _tpostprocessor_uuid

    elif column_type == TableScalarColumnType.DATE:

        def _tpostprocessor_date(raw_value: Any) -> TableDate | None:
            if raw_value is None:
                return None
            return TableDate.from_string(raw_value)

        return _tpostprocessor_date

    elif column_type == TableScalarColumnType.TIME:

        def _tpostprocessor_time(raw_value: Any) -> TableTime | None:
            if raw_value is None:
                return None
            return TableTime.from_string(raw_value)

        return _tpostprocessor_time

    elif column_type == TableScalarColumnType.DURATION:

        def _tpostprocessor_duration(raw_value: Any) -> TableDuration | None:
            if raw_value is None:
                return None
            return TableDuration.from_string(raw_value)

        return _tpostprocessor_duration

    elif column_type == TableScalarColumnType.INET:

        def _tpostprocessor_inet(
            raw_value: Any,
        ) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
            if raw_value is None:
                return None
            return ipaddress.ip_address(raw_value)

        return _tpostprocessor_inet

    elif column_type == TableScalarColumnType.DECIMAL:

        def _tpostprocessor_decimal(raw_value: Any) -> decimal.Decimal | None:
            if raw_value is None:
                return None
            return decimal.Decimal(f"{raw_value}")

        return _tpostprocessor_decimal

    elif column_type == TableScalarColumnType.TIMESTAMP:

        def _tpostprocessor_timestamp(raw_value: Any) -> DataAPITimestamp | None:
            if raw_value is None:
                return None
            return DataAPITimestamp.from_string(raw_value)

        return _tpostprocessor_timestamp

    else:
        raise ValueError(f"Unrecognized scalar type for reads: {column_type}")


def _create_unsupported_tpostprocessor(cql_definition: str) -> Callable[[Any], Any]:
    if cql_definition == "counter":
        return _create_scalar_tpostprocessor(column_type=TableScalarColumnType.INT)
    elif cql_definition == "varchar":
        return _create_scalar_tpostprocessor(column_type=TableScalarColumnType.TEXT)
    elif cql_definition == "timeuuid":
        return _create_scalar_tpostprocessor(column_type=TableScalarColumnType.UUID)
    else:
        raise ValueError(
            f"Unrecognized table unsupported-column cqlDefinition for reads: {cql_definition}"
        )


def _create_column_tpostprocessor(
    col_def: TableColumnTypeDescriptor,
) -> Callable[[Any], Any]:
    if isinstance(col_def, TableScalarColumnTypeDescriptor):
        return _create_scalar_tpostprocessor(col_def.column_type)
    elif isinstance(col_def, TableVectorColumnTypeDescriptor):
        if col_def.column_type == TableVectorColumnType.VECTOR:
            value_tpostprocessor = _create_scalar_tpostprocessor(
                TableScalarColumnType.FLOAT
            )

            def _tpostprocessor_vector(
                raw_items: list[float] | None,
            ) -> list[float] | None:
                if raw_items is None:
                    return None
                return [value_tpostprocessor(item) for item in raw_items]

            return _tpostprocessor_vector
        else:
            raise ValueError(
                f"Unrecognized table vector-column descriptor for reads: {col_def.as_dict()}"
            )
    elif isinstance(col_def, TableValuedColumnTypeDescriptor):
        if col_def.column_type == TableValuedColumnType.LIST:
            value_tpostprocessor = _create_scalar_tpostprocessor(col_def.value_type)

            def _tpostprocessor_list(raw_items: list[Any] | None) -> list[Any] | None:
                if raw_items is None:
                    return None
                return [value_tpostprocessor(item) for item in raw_items]

            return _tpostprocessor_list

        elif TableValuedColumnType.SET:
            value_tpostprocessor = _create_scalar_tpostprocessor(col_def.value_type)

            def _tpostprocessor_set(raw_items: set[Any] | None) -> TableSet[Any] | None:
                if raw_items is None:
                    return None
                return TableSet(value_tpostprocessor(item) for item in raw_items)

            return _tpostprocessor_set

        else:
            raise ValueError(
                f"Unrecognized table valued-column descriptor for reads: {col_def.as_dict()}"
            )
    elif isinstance(col_def, TableKeyValuedColumnTypeDescriptor):
        if col_def.column_type == TableKeyValuedColumnType.MAP:
            key_tpostprocessor = _create_scalar_tpostprocessor(col_def.key_type)
            value_tpostprocessor = _create_scalar_tpostprocessor(col_def.value_type)

            def _tpostprocessor_map(
                raw_items: dict[Any, Any] | None,
            ) -> TableMap[Any, Any] | None:
                if raw_items is None:
                    return None
                return TableMap(
                    (key_tpostprocessor(k), value_tpostprocessor(v))
                    for k, v in raw_items.items()
                )

            return _tpostprocessor_map

        else:
            raise ValueError(
                f"Unrecognized table key-valued-column descriptor for reads: {col_def.as_dict()}"
            )
    elif isinstance(col_def, TableUnsupportedColumnTypeDescriptor):
        if col_def.column_type == TableUnsupportedColumnType.UNSUPPORTED:
            # if UNSUPPORTED columns encountered: find the 'type' in the right place:
            return _create_unsupported_tpostprocessor(
                cql_definition=col_def.api_support.cql_definition
            )
        else:
            raise ValueError(
                f"Unrecognized table unsupported-column descriptor for reads: {col_def.as_dict()}"
            )
    else:
        raise ValueError(
            f"Unrecognized table column descriptor for reads: {col_def.as_dict()}"
        )


def create_row_tpostprocessor(
    columns: dict[str, TableColumnTypeDescriptor],
) -> Callable[[dict[str, Any]], dict[str, Any]]:
    tpostprocessor_map = {
        col_name: _create_column_tpostprocessor(col_definition)
        for col_name, col_definition in columns.items()
    }

    def _tpostprocessor(raw_dict: dict[str, Any]) -> dict[str, Any]:
        # TODO warn if extraneous fields
        return {
            col_name: (
                None if col_name not in raw_dict else tpostprocessor(raw_dict[col_name])
            )
            for col_name, tpostprocessor in tpostprocessor_map.items()
        }

    return _tpostprocessor
