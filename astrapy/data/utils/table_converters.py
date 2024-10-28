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

import datetime
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
from astrapy.data.utils.table_types import (
    TableKeyValuedColumnType,
    TableScalarColumnType,
    TableUnsupportedColumnType,
    TableValuedColumnType,
    TableVectorColumnType,
)
from astrapy.data_types import TableDate, TableDuration, TableMap, TableSet, TableTime
from astrapy.ids import UUID

# TODO this will be replaced by a specific parser with its own class
TIMESTAMP_DATETIME_FORMATS = [
    "%Y-%m-%dT%H:%M:%S.%fZ",
    # TODO additional formats (and the parsing troubles)
    # "%Y-%m-%dT%H:%M:%S.%f%:z",
]


def _create_scalar_converter(
    column_type: TableScalarColumnType,
) -> Callable[[Any], Any]:
    if column_type in {
        TableScalarColumnType.TEXT,
        TableScalarColumnType.ASCII,
    }:

        def _converter_text(raw_value: Any) -> str | None:
            return raw_value  # type: ignore[no-any-return]

        return _converter_text

    elif column_type == TableScalarColumnType.BOOLEAN:

        def _converter_bool(raw_value: Any) -> bool | None:
            return raw_value  # type: ignore[no-any-return]

        return _converter_bool

    elif column_type in {
        TableScalarColumnType.INT,
        TableScalarColumnType.VARINT,
        TableScalarColumnType.BIGINT,
        TableScalarColumnType.SMALLINT,
        TableScalarColumnType.TINYINT,
    }:

        def _converter_int(raw_value: Any) -> int | None:
            return raw_value  # type: ignore[no-any-return]

        return _converter_int

    elif column_type in {
        TableScalarColumnType.FLOAT,
        TableScalarColumnType.DOUBLE,
    }:

        def _converter_float(raw_value: Any) -> float | None:
            if raw_value is None:
                return None
            return float(raw_value)

        return _converter_float

    elif column_type == TableScalarColumnType.BLOB:

        def _converter_bytes(raw_value: Any) -> bytes | None:
            if raw_value is None:
                return None
            return raw_value  # type: ignore[no-any-return]

        return _converter_bytes

    elif column_type == TableScalarColumnType.UUID:

        def _converter_uuid(raw_value: Any) -> UUID | None:
            if raw_value is None:
                return None
            return UUID(raw_value)

        return _converter_uuid

    elif column_type == TableScalarColumnType.DATE:

        def _converter_date(raw_value: Any) -> TableDate | None:
            if raw_value is None:
                return None
            return TableDate.from_string(raw_value)

        return _converter_date

    elif column_type == TableScalarColumnType.TIME:

        def _converter_time(raw_value: Any) -> TableTime | None:
            if raw_value is None:
                return None
            return TableTime.from_string(raw_value)

        return _converter_time

    elif column_type == TableScalarColumnType.DURATION:

        def _converter_duration(raw_value: Any) -> TableDuration | None:
            if raw_value is None:
                return None
            return TableDuration.from_string(raw_value)

        return _converter_duration

    elif column_type == TableScalarColumnType.INET:

        def _converter_inet(
            raw_value: Any,
        ) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
            if raw_value is None:
                return None
            return ipaddress.ip_address(raw_value)

        return _converter_inet

    elif column_type == TableScalarColumnType.DECIMAL:

        def _converter_decimal(raw_value: Any) -> decimal.Decimal | None:
            if raw_value is None:
                return None
            return decimal.Decimal(f"{raw_value}")

        return _converter_decimal

    elif column_type == TableScalarColumnType.TIMESTAMP:

        def _converter_timestamp(raw_value: Any) -> datetime.datetime | None:
            if raw_value is None:
                return None
            for dt_format in TIMESTAMP_DATETIME_FORMATS:
                try:
                    return datetime.datetime.strptime(raw_value, dt_format)
                except ValueError:
                    pass
            raise ValueError(
                "Unparsable date string according to available formats for reads: "
                + ", ".join(
                    f'"{dt_format}"' for dt_format in TIMESTAMP_DATETIME_FORMATS
                )
            )

        return _converter_timestamp

    else:
        raise ValueError(f"Unrecognized scalar type for reads: {column_type}")


def _create_unsupported_converter(cql_definition: str) -> Callable[[Any], Any]:
    if cql_definition == "counter":
        return _create_scalar_converter(column_type=TableScalarColumnType.INT)
    elif cql_definition == "varchar":
        return _create_scalar_converter(column_type=TableScalarColumnType.TEXT)
    elif cql_definition == "timeuuid":
        return _create_scalar_converter(column_type=TableScalarColumnType.UUID)
    else:
        raise ValueError(
            f"Unrecognized table unsupported-column cqlDefinition for reads: {cql_definition}"
        )


def _create_column_converter(
    col_def: TableColumnTypeDescriptor,
) -> Callable[[Any], Any]:
    if isinstance(col_def, TableScalarColumnTypeDescriptor):
        return _create_scalar_converter(col_def.column_type)
    elif isinstance(col_def, TableVectorColumnTypeDescriptor):
        if col_def.column_type == TableVectorColumnType.VECTOR:
            value_converter = _create_scalar_converter(TableScalarColumnType.FLOAT)

            def _converter_vector(raw_items: list[float] | None) -> list[float] | None:
                if raw_items is None:
                    return None
                return [value_converter(item) for item in raw_items]

            return _converter_vector
        else:
            raise ValueError(
                f"Unrecognized table vector-column descriptor for reads: {col_def.as_dict()}"
            )
    elif isinstance(col_def, TableValuedColumnTypeDescriptor):
        if col_def.column_type == TableValuedColumnType.LIST:
            value_converter = _create_scalar_converter(col_def.value_type)

            def _converter_list(raw_items: list[Any] | None) -> list[Any] | None:
                if raw_items is None:
                    return None
                return [value_converter(item) for item in raw_items]

            return _converter_list

        elif TableValuedColumnType.SET:
            value_converter = _create_scalar_converter(col_def.value_type)

            def _converter_set(raw_items: set[Any] | None) -> TableSet[Any] | None:
                if raw_items is None:
                    return None
                return TableSet(value_converter(item) for item in raw_items)

            return _converter_set

        else:
            raise ValueError(
                f"Unrecognized table valued-column descriptor for reads: {col_def.as_dict()}"
            )
    elif isinstance(col_def, TableKeyValuedColumnTypeDescriptor):
        if col_def.column_type == TableKeyValuedColumnType.MAP:
            key_converter = _create_scalar_converter(col_def.key_type)
            value_converter = _create_scalar_converter(col_def.value_type)

            def _converter_map(
                raw_items: dict[Any, Any] | None,
            ) -> TableMap[Any, Any] | None:
                if raw_items is None:
                    return None
                return TableMap(
                    (key_converter(k), value_converter(v)) for k, v in raw_items.items()
                )

            return _converter_map

        else:
            raise ValueError(
                f"Unrecognized table key-valued-column descriptor for reads: {col_def.as_dict()}"
            )
    elif isinstance(col_def, TableUnsupportedColumnTypeDescriptor):
        if col_def.column_type == TableUnsupportedColumnType.UNSUPPORTED:
            # if UNSUPPORTED columns encountered: find the 'type' in the right place:
            return _create_unsupported_converter(
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


def create_row_converter(
    columns: dict[str, TableColumnTypeDescriptor],
) -> Callable[[dict[str, Any]], dict[str, Any]]:
    converter_map = {
        col_name: _create_column_converter(col_definition)
        for col_name, col_definition in columns.items()
    }

    def _converter(raw_dict: dict[str, Any]) -> dict[str, Any]:
        # TODO warn if extraneous fields
        return {
            col_name: (
                None if col_name not in raw_dict else converter(raw_dict[col_name])
            )
            for col_name, converter in converter_map.items()
        }

    return _converter
