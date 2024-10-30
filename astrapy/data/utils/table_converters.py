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
import math
from typing import Any, Callable, cast

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
    convert_to_ejson_bytes,
)
from astrapy.data.utils.table_types import (
    TableKeyValuedColumnType,
    TableScalarColumnType,
    TableUnsupportedColumnType,
    TableValuedColumnType,
    TableVectorColumnType,
)
from astrapy.data.utils.vector_coercion import ensure_unrolled_if_iterable
from astrapy.data_types import (
    DataAPITimestamp,
    DataAPIVector,
    TableDate,
    TableDuration,
    TableMap,
    TableSet,
    TableTime,
)
from astrapy.ids import UUID, ObjectId
from astrapy.utils.api_options import FullWireFormatOptions
from astrapy.utils.date_utils import _get_datetime_offset

NAN_FLOAT_STRING_REPRESENTATION = "NaN"
PLUS_INFINITY_FLOAT_STRING_REPRESENTATION = "Infinity"
MINUS_INFINITY_FLOAT_STRING_REPRESENTATION = "-Infinity"
DATETIME_TIME_FORMAT = "%H:%M:%S.%f"
DATETIME_DATE_FORMAT = "%Y-%m-%d"
DATETIME_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


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
            elif isinstance(raw_value, str):
                return float(raw_value)
            # just a float already
            return cast(float, raw_value)

        return _tpostprocessor_float

    elif column_type == TableScalarColumnType.BLOB:

        def _tpostprocessor_bytes(raw_value: Any) -> bytes | None:
            if raw_value is None:
                return None
            if isinstance(raw_value, dict):
                # {"$binary": ...}
                return convert_ejson_binary_object_to_bytes(raw_value)
            elif isinstance(raw_value, str):
                # within PKSchema, a bare string (e.g. "q83vASNFZ4k=") is encountered
                return convert_ejson_binary_object_to_bytes({"$binary": raw_value})
            else:
                raise ValueError(
                    f"Unexpected value type encountered for a blob column: {column_type}"
                )

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

    elif column_type == TableScalarColumnType.TIMESTAMP:

        def _tpostprocessor_timestamp(raw_value: Any) -> DataAPITimestamp | None:
            if raw_value is None:
                return None
            return DataAPITimestamp.from_string(raw_value)

        return _tpostprocessor_timestamp

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
    column_name_set = set(tpostprocessor_map.keys())

    def _tpostprocessor(raw_dict: dict[str, Any]) -> dict[str, Any]:
        extra_fields = set(raw_dict.keys()) - column_name_set
        if extra_fields:
            xf_desc = ", ".join(f'"{f}"' for f in sorted(extra_fields))
            raise ValueError(f"Returned row has unexpected fields: {xf_desc}")
        return {
            col_name: (
                None if col_name not in raw_dict else tpostprocessor(raw_dict[col_name])
            )
            for col_name, tpostprocessor in tpostprocessor_map.items()
        }

    return _tpostprocessor


def create_key_ktpostprocessor(
    primary_key_schema: dict[str, TableColumnTypeDescriptor],
) -> Callable[[list[Any]], dict[str, Any]]:
    ktpostprocessor_list: list[tuple[str, Callable[[Any], Any]]] = [
        (col_name, _create_column_tpostprocessor(col_definition))
        for col_name, col_definition in primary_key_schema.items()
    ]

    def _ktpostprocessor(primary_key_list: list[Any]) -> dict[str, Any]:
        if len(primary_key_list) != len(ktpostprocessor_list):
            raise ValueError(
                "Primary key list length / schema mismatch "
                f"(expected {len(ktpostprocessor_list)}, "
                f"received {len(primary_key_list)} fields)"
            )
        return {
            pk_col_name: ktpostprocessor(pk_col_value)
            for pk_col_value, (pk_col_name, ktpostprocessor) in zip(
                primary_key_list,
                ktpostprocessor_list,
            )
        }

    return _ktpostprocessor


def preprocess_table_payload_value(
    path: list[str], value: Any, options: FullWireFormatOptions
) -> Any:
    """
    Walk a payload for Tables and apply the necessary and required conversions
    to make it into a ready-to-jsondumps object.
    """

    # is this a nesting structure?
    if isinstance(value, (dict, TableMap)):
        return {
            preprocess_table_payload_value(
                path, k, options=options
            ): preprocess_table_payload_value(path + [k], v, options=options)
            for k, v in value.items()
        }
    elif isinstance(value, (list, set, TableSet)):
        return [
            preprocess_table_payload_value(path + [""], v, options=options)
            for v in value
        ]

    # it's a scalar of some kind (which includes DataAPIVector)
    if isinstance(value, float):
        # Non-numbers must be manually made into a string
        if math.isnan(value):
            return NAN_FLOAT_STRING_REPRESENTATION
        elif math.isinf(value):
            if value > 0:
                return PLUS_INFINITY_FLOAT_STRING_REPRESENTATION
            else:
                return MINUS_INFINITY_FLOAT_STRING_REPRESENTATION
        return value
    elif isinstance(value, bytes):
        return convert_to_ejson_bytes(value)
    elif isinstance(value, DataAPIVector):
        if options.binary_encode_vectors:
            return convert_to_ejson_bytes(value.to_bytes())
        else:
            # regular list of floats - which can contain non-numbers:
            return [
                preprocess_table_payload_value(path + [""], fval, options=options)
                for fval in value.data
            ]
    elif isinstance(value, DataAPITimestamp):
        # TODO encode
        return "NOT_YET"
    elif isinstance(value, TableDate):
        return value.to_string()
    elif isinstance(value, TableTime):
        return value.to_string()
    elif isinstance(value, datetime.datetime):
        # encoding in two steps (that's because the '%:z' strftime directive
        # is not in all supported Python versions).
        offset_tuple = _get_datetime_offset(value)
        if offset_tuple is None:
            raise ValueError(
                "Cannot encode a datetime without timezone information ('tzinfo')."
            )
        date_part_str = value.strftime(DATETIME_DATETIME_FORMAT)
        offset_h, offset_m = offset_tuple
        offset_part_str = f"{offset_h:+03}:{offset_m:02}"
        return f"{date_part_str}{offset_part_str}"
    elif isinstance(value, datetime.date):
        # there's no format to specify - and this is compliant anyway:
        return value.strftime(DATETIME_DATE_FORMAT)
    elif isinstance(value, datetime.time):
        return value.strftime(DATETIME_TIME_FORMAT)
    elif isinstance(value, decimal.Decimal):
        return str(value)  # TODO finalize handling (check latest encoding choices)
    elif isinstance(value, TableDuration):
        return value.to_string()
    elif isinstance(value, UUID):
        return str(value)
    elif isinstance(value, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
        return str(value)
    elif isinstance(value, datetime.timedelta):
        raise ValueError(
            "Instances of `datetime.timedelta` not allowed. Please refer to the "
            "`TableDuration` class for the idiomatic way of expressing durations."
        )
    elif isinstance(value, ObjectId):
        raise ValueError(
            "Values of type ObjectId are not supported. Consider switching to "
            "using UUID-based identifiers instead."
        )

    # Now it is either a generator-like or a "safe" scalar
    # value can be something that must be unrolled:
    if options.unroll_iterables_to_lists:
        _value = ensure_unrolled_if_iterable(value)
        # process it as
        if isinstance(_value, list):
            return [
                preprocess_table_payload_value(path + [""], v, options=options)
                for v in _value
            ]
        return _value

    # all options are exhausted save for str, int, bool, None:
    return value


def preprocess_table_payload(
    payload: dict[str, Any] | None, options: FullWireFormatOptions
) -> dict[str, Any] | None:
    """
    Normalize a payload for API calls.
    This includes e.g. ensuring values for "$vector" key
    are made into plain lists of floats.

    Args:
        payload (dict[str, Any]): A dict expressing a payload for an API call

    Returns:
        dict[str, Any]: a payload dict, pre-processed, ready for HTTP requests.
    """

    if payload:
        return cast(
            dict[str, Any],
            preprocess_table_payload_value([], payload, options=options),
        )
    else:
        return payload
