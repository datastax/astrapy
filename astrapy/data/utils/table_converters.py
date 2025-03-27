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

import copy
import datetime
import decimal
import hashlib
import ipaddress
import json
import math
from typing import Any, Callable, Dict, Generic, cast

from astrapy.constants import ROW, MapEncodingMode
from astrapy.data.info.table_descriptor.table_columns import (
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
    ColumnType,
    TableKeyValuedColumnType,
    TableUnsupportedColumnType,
    TableValuedColumnType,
    TableVectorColumnType,
)
from astrapy.data.utils.vector_coercion import ensure_unrolled_if_iterable
from astrapy.data_types import (
    DataAPIDate,
    DataAPIDuration,
    DataAPIMap,
    DataAPISet,
    DataAPITime,
    DataAPITimestamp,
    DataAPIVector,
)
from astrapy.ids import UUID, ObjectId
from astrapy.settings.error_messages import CANNOT_ENCODE_NAIVE_DATETIME_ERROR_MESSAGE
from astrapy.utils.api_options import FullSerdesOptions
from astrapy.utils.date_utils import _get_datetime_offset

NAN_FLOAT_STRING_REPRESENTATION = "NaN"
PLUS_INFINITY_FLOAT_STRING_REPRESENTATION = "Infinity"
MINUS_INFINITY_FLOAT_STRING_REPRESENTATION = "-Infinity"
DATETIME_TIME_FORMAT = "%H:%M:%S.%f"
DATETIME_DATE_FORMAT = "%Y-%m-%d"
DATETIME_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


def _create_scalar_tpostprocessor(
    column_type: ColumnType,
    options: FullSerdesOptions,
) -> Callable[[Any], Any]:
    if column_type in {
        ColumnType.TEXT,
        ColumnType.ASCII,
    }:

        def _tpostprocessor_text(raw_value: Any) -> str | None:
            return raw_value  # type: ignore[no-any-return]

        return _tpostprocessor_text

    elif column_type == ColumnType.BOOLEAN:

        def _tpostprocessor_bool(raw_value: Any) -> bool | None:
            return raw_value  # type: ignore[no-any-return]

        return _tpostprocessor_bool

    elif column_type in {
        ColumnType.INT,
        ColumnType.VARINT,
        ColumnType.BIGINT,
        ColumnType.SMALLINT,
        ColumnType.TINYINT,
        ColumnType.COUNTER,
    }:

        def _tpostprocessor_int(raw_value: Any) -> int | None:
            if raw_value is None:
                return None
            # the 'int(...)' handles Decimal's
            return int(raw_value)

        return _tpostprocessor_int

    elif column_type in {
        ColumnType.FLOAT,
        ColumnType.DOUBLE,
    }:

        def _tpostprocessor_float(raw_value: Any) -> float | None:
            if raw_value is None:
                return None
            elif isinstance(raw_value, (str, decimal.Decimal)):
                return float(raw_value)
            # just a float already
            return cast(float, raw_value)

        return _tpostprocessor_float

    elif column_type == ColumnType.BLOB:

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

    elif column_type in {ColumnType.UUID, ColumnType.TIMEUUID}:

        def _tpostprocessor_uuid(raw_value: Any) -> UUID | None:
            if raw_value is None:
                return None
            return UUID(raw_value)

        return _tpostprocessor_uuid

    elif column_type == ColumnType.DATE:
        if options.custom_datatypes_in_reading:

            def _tpostprocessor_date(raw_value: Any) -> DataAPIDate | None:
                if raw_value is None:
                    return None
                return DataAPIDate.from_string(raw_value)

            return _tpostprocessor_date

        else:

            def _tpostprocessor_date_stdlib(raw_value: Any) -> datetime.date | None:
                if raw_value is None:
                    return None
                return DataAPIDate.from_string(raw_value).to_date()

            return _tpostprocessor_date_stdlib

    elif column_type == ColumnType.TIME:
        if options.custom_datatypes_in_reading:

            def _tpostprocessor_time(raw_value: Any) -> DataAPITime | None:
                if raw_value is None:
                    return None
                return DataAPITime.from_string(raw_value)

            return _tpostprocessor_time

        else:

            def _tpostprocessor_time_stdlib(raw_value: Any) -> datetime.time | None:
                if raw_value is None:
                    return None
                return DataAPITime.from_string(raw_value).to_time()

            return _tpostprocessor_time_stdlib

    elif column_type == ColumnType.TIMESTAMP:
        if options.custom_datatypes_in_reading:

            def _tpostprocessor_timestamp(raw_value: Any) -> DataAPITimestamp | None:
                if raw_value is None:
                    return None
                return DataAPITimestamp.from_string(raw_value)

            return _tpostprocessor_timestamp

        else:

            def _tpostprocessor_timestamp_stdlib(
                raw_value: Any,
            ) -> datetime.datetime | None:
                if raw_value is None:
                    return None
                da_timestamp = DataAPITimestamp.from_string(raw_value)
                return da_timestamp.to_datetime(tz=options.datetime_tzinfo)

            return _tpostprocessor_timestamp_stdlib

    elif column_type == ColumnType.DURATION:
        if options.custom_datatypes_in_reading:

            def _tpostprocessor_duration(raw_value: Any) -> DataAPIDuration | None:
                if raw_value is None:
                    return None
                return DataAPIDuration.from_string(raw_value)

            return _tpostprocessor_duration

        else:

            def _tpostprocessor_duration_stdlib(
                raw_value: Any,
            ) -> datetime.timedelta | None:
                if raw_value is None:
                    return None
                return DataAPIDuration.from_string(raw_value).to_timedelta()

            return _tpostprocessor_duration_stdlib

    elif column_type == ColumnType.INET:

        def _tpostprocessor_inet(
            raw_value: Any,
        ) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
            if raw_value is None:
                return None
            return ipaddress.ip_address(raw_value)

        return _tpostprocessor_inet

    elif column_type == ColumnType.DECIMAL:

        def _tpostprocessor_decimal(raw_value: Any) -> decimal.Decimal | None:
            if raw_value is None:
                return None
            elif isinstance(raw_value, decimal.Decimal):
                return raw_value
            # else: it is "NaN", "-Infinity" or "Infinity"
            return decimal.Decimal(f"{raw_value}")

        return _tpostprocessor_decimal
    else:
        raise ValueError(f"Unrecognized scalar type for reads: {column_type}")


def _create_unsupported_tpostprocessor(
    cql_definition: str,
    options: FullSerdesOptions,
) -> Callable[[Any], Any]:
    if cql_definition == "counter":
        return _create_scalar_tpostprocessor(
            column_type=ColumnType.INT, options=options
        )
    elif cql_definition == "varchar":
        return _create_scalar_tpostprocessor(
            column_type=ColumnType.TEXT, options=options
        )
    elif cql_definition == "timeuuid":
        return _create_scalar_tpostprocessor(
            column_type=ColumnType.UUID, options=options
        )
    else:
        raise ValueError(
            f"Unrecognized table unsupported-column cqlDefinition for reads: {cql_definition}"
        )


def _column_filler_value(
    col_def: TableColumnTypeDescriptor,
    options: FullSerdesOptions,
) -> Any:
    if isinstance(col_def, TableScalarColumnTypeDescriptor):
        return None
    elif isinstance(col_def, TableVectorColumnTypeDescriptor):
        if col_def.column_type == TableVectorColumnType.VECTOR:
            return None
        else:
            raise ValueError(
                f"Unrecognized table vector-column descriptor for reads: {col_def.as_dict()}"
            )
    elif isinstance(col_def, TableValuedColumnTypeDescriptor):
        if col_def.column_type == TableValuedColumnType.LIST:
            return []
        elif TableValuedColumnType.SET:
            if options.custom_datatypes_in_reading:
                return DataAPISet()
            else:
                return set()
        else:
            raise ValueError(
                f"Unrecognized table valued-column descriptor for reads: {col_def.as_dict()}"
            )
    elif isinstance(col_def, TableKeyValuedColumnTypeDescriptor):
        if col_def.column_type == TableKeyValuedColumnType.MAP:
            if options.custom_datatypes_in_reading:
                return DataAPIMap()
            else:
                return {}
        else:
            raise ValueError(
                f"Unrecognized table key-valued-column descriptor for reads: {col_def.as_dict()}"
            )
    elif isinstance(col_def, TableUnsupportedColumnTypeDescriptor):
        # For lack of better information,
        # the filler for unreported unsupported columns is a None:
        return None
    else:
        raise ValueError(
            f"Unrecognized table column descriptor for reads: {col_def.as_dict()}"
        )


def _create_column_tpostprocessor(
    col_def: TableColumnTypeDescriptor,
    options: FullSerdesOptions,
) -> Callable[[Any], Any]:
    if isinstance(col_def, TableScalarColumnTypeDescriptor):
        return _create_scalar_tpostprocessor(col_def.column_type, options=options)
    elif isinstance(col_def, TableVectorColumnTypeDescriptor):
        if col_def.column_type == TableVectorColumnType.VECTOR:
            value_tpostprocessor = _create_scalar_tpostprocessor(
                ColumnType.FLOAT,
                options=options,
            )

            if options.custom_datatypes_in_reading:

                def _tpostprocessor_vector(
                    raw_items: list[float] | dict[str, str] | None,
                ) -> DataAPIVector | None:
                    if raw_items is None:
                        return None
                    elif isinstance(raw_items, dict):
                        # {"$binary": ...}
                        return DataAPIVector.from_bytes(
                            convert_ejson_binary_object_to_bytes(raw_items)
                        )
                    return DataAPIVector(
                        [value_tpostprocessor(item) for item in raw_items]
                    )

                return _tpostprocessor_vector

            else:

                def _tpostprocessor_vector_as_list(
                    raw_items: list[float] | dict[str, str] | None,
                ) -> list[float] | None:
                    if raw_items is None:
                        return None
                    elif isinstance(raw_items, dict):
                        # {"$binary": ...}
                        return DataAPIVector.from_bytes(
                            convert_ejson_binary_object_to_bytes(raw_items)
                        ).data
                    return [value_tpostprocessor(item) for item in raw_items]

                return _tpostprocessor_vector_as_list

        else:
            raise ValueError(
                f"Unrecognized table vector-column descriptor for reads: {col_def.as_dict()}"
            )
    elif isinstance(col_def, TableValuedColumnTypeDescriptor):
        if col_def.column_type == TableValuedColumnType.LIST:
            value_tpostprocessor = _create_scalar_tpostprocessor(
                col_def.value_type, options=options
            )

            def _tpostprocessor_list(raw_items: list[Any] | None) -> list[Any] | None:
                if raw_items is None:
                    return None
                return [value_tpostprocessor(item) for item in raw_items]

            return _tpostprocessor_list

        elif TableValuedColumnType.SET:
            value_tpostprocessor = _create_scalar_tpostprocessor(
                col_def.value_type, options=options
            )

            if options.custom_datatypes_in_reading:

                def _tpostprocessor_dataapiset(
                    raw_items: set[Any] | None,
                ) -> DataAPISet[Any] | None:
                    if raw_items is None:
                        return None
                    return DataAPISet(value_tpostprocessor(item) for item in raw_items)

                return _tpostprocessor_dataapiset

            else:

                def _tpostprocessor_dataapiset_as_set(
                    raw_items: set[Any] | None,
                ) -> set[Any] | None:
                    if raw_items is None:
                        return None
                    return {value_tpostprocessor(item) for item in raw_items}

                return _tpostprocessor_dataapiset_as_set

        else:
            raise ValueError(
                f"Unrecognized table valued-column descriptor for reads: {col_def.as_dict()}"
            )
    elif isinstance(col_def, TableKeyValuedColumnTypeDescriptor):
        if col_def.column_type == TableKeyValuedColumnType.MAP:
            key_tpostprocessor = _create_scalar_tpostprocessor(
                col_def.key_type, options=options
            )
            value_tpostprocessor = _create_scalar_tpostprocessor(
                col_def.value_type, options=options
            )

            if options.custom_datatypes_in_reading:

                def _tpostprocessor_dataapimap(
                    raw_items: dict[Any, Any] | list[list[Any]] | None,
                ) -> DataAPIMap[Any, Any] | None:
                    if raw_items is None:
                        return None
                    if isinstance(raw_items, dict):
                        return DataAPIMap(
                            (key_tpostprocessor(k), value_tpostprocessor(v))
                            for k, v in raw_items.items()
                        )
                    # it's a list-of-2tuples
                    return DataAPIMap(
                        (key_tpostprocessor(k), value_tpostprocessor(v))
                        for k, v in raw_items
                    )

                return _tpostprocessor_dataapimap

            else:

                def _tpostprocessor_dataapimap_as_dict(
                    raw_items: dict[Any, Any] | list[list[Any]] | None,
                ) -> dict[Any, Any] | None:
                    if raw_items is None:
                        return None
                    if isinstance(raw_items, dict):
                        return {
                            key_tpostprocessor(k): value_tpostprocessor(v)
                            for k, v in raw_items.items()
                        }
                    # it's a list-of-2tuples
                    return {
                        key_tpostprocessor(k): value_tpostprocessor(v)
                        for k, v in raw_items
                    }

                return _tpostprocessor_dataapimap_as_dict
        else:
            raise ValueError(
                f"Unrecognized table key-valued-column descriptor for reads: {col_def.as_dict()}"
            )
    elif isinstance(col_def, TableUnsupportedColumnTypeDescriptor):
        if col_def.column_type == TableUnsupportedColumnType.UNSUPPORTED:
            # if UNSUPPORTED columns encountered: find the 'type' in the right place:
            return _create_unsupported_tpostprocessor(
                cql_definition=col_def.api_support.cql_definition,
                options=options,
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
    options: FullSerdesOptions,
    similarity_pseudocolumn: str | None,
) -> Callable[[dict[str, Any]], dict[str, Any]]:
    tpostprocessor_map = {
        col_name: _create_column_tpostprocessor(col_definition, options=options)
        for col_name, col_definition in columns.items()
    }
    tfiller_map = {
        col_name: _column_filler_value(col_definition, options=options)
        for col_name, col_definition in columns.items()
    }
    if similarity_pseudocolumn is not None:
        # whatever in the passed schema, requiring similarity overrides that 'column':
        tpostprocessor_map[similarity_pseudocolumn] = _create_scalar_tpostprocessor(
            column_type=ColumnType.FLOAT, options=options
        )
        tfiller_map[similarity_pseudocolumn] = None
    column_name_set = set(tpostprocessor_map.keys())

    def _tpostprocessor(raw_dict: dict[str, Any]) -> dict[str, Any]:
        extra_fields = set(raw_dict.keys()) - column_name_set
        if extra_fields:
            xf_desc = ", ".join(f'"{f}"' for f in sorted(extra_fields))
            raise ValueError(f"Returned row has unexpected fields: {xf_desc}")
        return {
            col_name: (
                # making a copy here, since the user may mutate e.g. a map:
                copy.copy(tfiller_map[col_name])
                if col_name not in raw_dict
                else tpostprocessor(raw_dict[col_name])
            )
            for col_name, tpostprocessor in tpostprocessor_map.items()
        }

    return _tpostprocessor


def create_key_ktpostprocessor(
    primary_key_schema: dict[str, TableColumnTypeDescriptor],
    options: FullSerdesOptions,
) -> Callable[[list[Any]], tuple[tuple[Any, ...], dict[str, Any]]]:
    ktpostprocessor_list: list[tuple[str, Callable[[Any], Any]]] = [
        (col_name, _create_column_tpostprocessor(col_definition, options=options))
        for col_name, col_definition in primary_key_schema.items()
    ]

    def _ktpostprocessor(
        primary_key_list: list[Any],
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        if len(primary_key_list) != len(ktpostprocessor_list):
            raise ValueError(
                "Primary key list length / schema mismatch "
                f"(expected {len(ktpostprocessor_list)}, "
                f"received {len(primary_key_list)} fields)"
            )
        k_tuple = tuple(
            [
                ktpostprocessor(pk_col_value)
                for pk_col_value, (_, ktpostprocessor) in zip(
                    primary_key_list,
                    ktpostprocessor_list,
                )
            ]
        )
        k_dict = {
            pk_col_name: pk_processed_value
            for pk_processed_value, (pk_col_name, _) in zip(
                k_tuple,
                ktpostprocessor_list,
            )
        }
        return k_tuple, k_dict

    return _ktpostprocessor


def preprocess_table_payload_value(
    path: list[str],
    value: Any,
    options: FullSerdesOptions,
    map2tuple_checker: Callable[[list[str]], bool] | None,
) -> Any:
    """
    Walk a payload for Tables and apply the necessary and required conversions
    to make it into a ready-to-jsondumps object.
    """

    # is this a nesting structure?
    if isinstance(value, (dict, DataAPIMap)):
        maps_can_become_tuples: bool
        if options.encode_maps_as_lists_in_tables == MapEncodingMode.NEVER:
            maps_can_become_tuples = False
        elif options.encode_maps_as_lists_in_tables == MapEncodingMode.DATAAPIMAPS:
            maps_can_become_tuples = isinstance(value, DataAPIMap)
        else:
            # 'ALWAYS' setting
            maps_can_become_tuples = True

        maps_become_tuples: bool
        if maps_can_become_tuples:
            if map2tuple_checker is None:
                maps_become_tuples = False
            else:
                maps_become_tuples = map2tuple_checker(path)
        else:
            maps_become_tuples = False

        if maps_become_tuples:
            return [
                [
                    preprocess_table_payload_value(
                        path,
                        k,
                        options=options,
                        map2tuple_checker=map2tuple_checker,
                    ),
                    preprocess_table_payload_value(
                        path + [k],
                        v,
                        options=options,
                        map2tuple_checker=map2tuple_checker,
                    ),
                ]
                for k, v in value.items()
            ]

        return {
            preprocess_table_payload_value(
                path, k, options=options, map2tuple_checker=map2tuple_checker
            ): preprocess_table_payload_value(
                path + [k], v, options=options, map2tuple_checker=map2tuple_checker
            )
            for k, v in value.items()
        }
    elif isinstance(value, (list, set, DataAPISet)):
        return [
            preprocess_table_payload_value(
                path + [""], v, options=options, map2tuple_checker=map2tuple_checker
            )
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
                preprocess_table_payload_value(
                    path + [""],
                    fval,
                    options=options,
                    map2tuple_checker=map2tuple_checker,
                )
                for fval in value.data
            ]
    elif isinstance(value, DataAPITimestamp):
        return value.to_string()
    elif isinstance(value, DataAPIDate):
        return value.to_string()
    elif isinstance(value, DataAPITime):
        return value.to_string()
    elif isinstance(value, datetime.datetime):
        # encoding in two steps (that's because the '%:z' strftime directive
        # is not in all supported Python versions).
        offset_tuple = _get_datetime_offset(value)
        if offset_tuple is None:
            if options.accept_naive_datetimes:
                return DataAPITimestamp(int(value.timestamp() * 1000)).to_string()
            raise ValueError(CANNOT_ENCODE_NAIVE_DATETIME_ERROR_MESSAGE)
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
        # Non-numbers must be manually made into a string, just like floats
        if math.isnan(value):
            return NAN_FLOAT_STRING_REPRESENTATION
        elif math.isinf(value):
            if value > 0:
                return PLUS_INFINITY_FLOAT_STRING_REPRESENTATION
            else:
                return MINUS_INFINITY_FLOAT_STRING_REPRESENTATION
        # actually-numeric decimals: leave them as they are for the encoding step,
        # which will apply the nasty trick to ensure all digits get there.
        return value
    elif isinstance(value, DataAPIDuration):
        # using to_c_string over to_string until the ISO-format parsing can
        # cope with subsecond fractions:
        return value.to_c_string()
    elif isinstance(value, UUID):
        return str(value)
    elif isinstance(value, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
        return str(value)
    elif isinstance(value, datetime.timedelta):
        return DataAPIDuration.from_timedelta(value).to_c_string()
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
                preprocess_table_payload_value(
                    path + [""], v, options=options, map2tuple_checker=map2tuple_checker
                )
                for v in _value
            ]
        return _value

    # all options are exhausted save for str, int, bool, None:
    return value


def preprocess_table_payload(
    payload: dict[str, Any] | None,
    options: FullSerdesOptions,
    map2tuple_checker: Callable[[list[str]], bool] | None,
) -> dict[str, Any] | None:
    """
    Normalize a payload for API calls.
    This includes e.g. ensuring values for "$vector" key
    are made into plain lists of floats.

    Args:
        payload (dict[str, Any]): A dict expressing a payload for an API call
        options: a FullSerdesOptions setting the preprocessing configuration
        map2tuple_checker: a boolean function of a path in the doc, that returns
            True for "doc-like" portions of a payload, i.e. whose maps/DataAPIMaps
            can be converted into association lists, if such autoconversion is
            turned on. If this parameter is None, no paths are autoconverted.

    Returns:
        dict[str, Any]: a payload dict, pre-processed, ready for HTTP requests.
    """

    if payload:
        return cast(
            Dict[str, Any],
            preprocess_table_payload_value(
                [],
                payload,
                options=options,
                map2tuple_checker=map2tuple_checker,
            ),
        )
    else:
        return payload


class _DecimalCleaner(json.JSONEncoder):
    """
    This class cleans decimal (coming from decimal-oriented parsing of responses)
    so that the schema can be made into a string, hashed, and used as key to the
    converters cache safely.
    """

    def default(self, obj: object) -> Any:
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return super().default(obj)


class _TableConverterAgent(Generic[ROW]):
    options: FullSerdesOptions
    row_postprocessors: dict[
        tuple[str, str | None], Callable[[dict[str, Any]], dict[str, Any]]
    ]
    key_postprocessors: dict[
        str, Callable[[list[Any]], tuple[tuple[Any, ...], dict[str, Any]]]
    ]

    def __init__(self, *, options: FullSerdesOptions) -> None:
        self.options = options
        self.row_postprocessors = {}
        self.key_postprocessors = {}

    @staticmethod
    def _hash_dict(input_dict: dict[str, Any]) -> str:
        return hashlib.md5(
            json.dumps(
                input_dict,
                sort_keys=True,
                separators=(",", ":"),
                cls=_DecimalCleaner,
            ).encode()
        ).hexdigest()

    def _get_key_postprocessor(
        self, primary_key_schema_dict: dict[str, Any]
    ) -> Callable[[list[Any]], tuple[tuple[Any, ...], dict[str, Any]]]:
        schema_hash = self._hash_dict(primary_key_schema_dict)
        if schema_hash not in self.key_postprocessors:
            primary_key_schema: dict[str, TableColumnTypeDescriptor] = {
                col_name: TableColumnTypeDescriptor.coerce(col_dict)
                for col_name, col_dict in primary_key_schema_dict.items()
            }
            self.key_postprocessors[schema_hash] = create_key_ktpostprocessor(
                primary_key_schema=primary_key_schema,
                options=self.options,
            )
        return self.key_postprocessors[schema_hash]

    def _get_row_postprocessor(
        self,
        columns_dict: dict[str, Any],
        similarity_pseudocolumn: str | None,
    ) -> Callable[[dict[str, Any]], dict[str, Any]]:
        schema_cache_key = (self._hash_dict(columns_dict), similarity_pseudocolumn)
        if schema_cache_key not in self.row_postprocessors:
            columns: dict[str, TableColumnTypeDescriptor] = {
                col_name: TableColumnTypeDescriptor.coerce(col_dict)
                for col_name, col_dict in columns_dict.items()
            }
            self.row_postprocessors[schema_cache_key] = create_row_tpostprocessor(
                columns=columns,
                options=self.options,
                similarity_pseudocolumn=similarity_pseudocolumn,
            )
        return self.row_postprocessors[schema_cache_key]

    def preprocess_payload(
        self,
        payload: dict[str, Any] | None,
        map2tuple_checker: Callable[[list[str]], bool] | None,
    ) -> dict[str, Any] | None:
        return preprocess_table_payload(
            payload,
            options=self.options,
            map2tuple_checker=map2tuple_checker,
        )

    def postprocess_key(
        self, primary_key_list: list[Any], *, primary_key_schema_dict: dict[str, Any]
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        """
        The primary key schema is not coerced here, just parsed from its json
        """
        return self._get_key_postprocessor(
            primary_key_schema_dict=primary_key_schema_dict
        )(primary_key_list)

    def postprocess_keys(
        self,
        primary_key_lists: list[list[Any]],
        *,
        primary_key_schema_dict: dict[str, Any],
    ) -> list[tuple[tuple[Any, ...], dict[str, Any]]]:
        """
        The primary key schema is not coerced here, just parsed from its json
        """
        if primary_key_lists:
            _k_postprocessor = self._get_key_postprocessor(
                primary_key_schema_dict=primary_key_schema_dict
            )
            return [
                _k_postprocessor(primary_key_list)
                for primary_key_list in primary_key_lists
            ]
        else:
            return []

    def postprocess_row(
        self,
        raw_dict: dict[str, Any],
        *,
        columns_dict: dict[str, Any],
        similarity_pseudocolumn: str | None,
    ) -> ROW:
        """
        The columns schema is not coerced here, just parsed from its json
        """
        return self._get_row_postprocessor(
            columns_dict=columns_dict, similarity_pseudocolumn=similarity_pseudocolumn
        )(raw_dict)  # type: ignore[return-value]

    def postprocess_rows(
        self,
        raw_dicts: list[dict[str, Any]],
        *,
        columns_dict: dict[str, Any],
        similarity_pseudocolumn: str | None,
    ) -> list[ROW]:
        """
        The columns schema is not coerced here, just parsed from its json
        """
        if raw_dicts:
            _r_postprocessor = self._get_row_postprocessor(
                columns_dict=columns_dict,
                similarity_pseudocolumn=similarity_pseudocolumn,
            )
            return [cast(ROW, _r_postprocessor(raw_dict)) for raw_dict in raw_dicts]
        else:
            return []
