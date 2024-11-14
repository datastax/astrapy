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
from typing import Any

import pytest

from astrapy.data.utils.extended_json_converters import (
    convert_ejson_binary_object_to_bytes,
)
from astrapy.data.utils.table_converters import (
    create_key_ktpostprocessor,
    create_row_tpostprocessor,
    preprocess_table_payload,
)
from astrapy.data.utils.table_types import TableScalarColumnType
from astrapy.data_types import (
    DataAPIDate,
    DataAPIDuration,
    DataAPIMap,
    DataAPISet,
    DataAPITime,
    DataAPITimestamp,
    DataAPIVector,
)
from astrapy.data_types.data_api_vector import bytes_to_floats
from astrapy.ids import UUID, ObjectId
from astrapy.info import TableDescriptor, TableScalarColumnTypeDescriptor
from astrapy.utils.api_options import FullSerdesOptions

from ..conftest import _repaint_NaNs

TABLE_DESCRIPTION = {
    "name": "table_simple",
    "definition": {
        "columns": {
            "p_text": {"type": "text"},
            "p_boolean": {"type": "boolean"},
            "p_int": {"type": "int"},
            "p_float": {"type": "float"},
            "p_float_nan": {"type": "float"},
            "p_float_pinf": {"type": "float"},
            "p_float_minf": {"type": "float"},
            "p_blob": {"type": "blob"},
            "p_uuid": {"type": "uuid"},
            "p_decimal": {"type": "decimal"},
            "p_date": {"type": "date"},
            "p_duration": {"type": "duration"},
            "p_inet": {"type": "inet"},
            "p_time": {"type": "time"},
            "p_timestamp": {"type": "timestamp"},
            "p_timestamp_out_offset": {"type": "timestamp"},
            "p_list_int": {"type": "list", "valueType": "int"},
            "p_list_double": {"type": "list", "valueType": "double"},
            "p_set_ascii": {"type": "set", "valueType": "ascii"},
            "p_set_float": {"type": "set", "valueType": "float"},
            "p_map_text_float": {
                "type": "map",
                "valueType": "float",
                "keyType": "text",
            },
            "p_map_float_text": {
                "type": "map",
                "valueType": "text",
                "keyType": "float",
            },
            "somevector": {"type": "vector", "dimension": 3},
            "embeddings": {
                "type": "vector",
                "dimension": 2,
                "service": {
                    "provider": "openai",
                    "modelName": "text-embedding-3-small",
                },
            },
            "p_counter": {
                "type": "UNSUPPORTED",
                "apiSupport": {
                    "createTable": False,
                    "insert": False,
                    "read": False,
                    "cqlDefinition": "counter",
                },
            },
            "p_varchar": {
                "type": "UNSUPPORTED",
                "apiSupport": {
                    "createTable": False,
                    "insert": False,
                    "read": False,
                    "cqlDefinition": "varchar",
                },
            },
            "p_timeuuid": {
                "type": "UNSUPPORTED",
                "apiSupport": {
                    "createTable": False,
                    "insert": False,
                    "read": False,
                    "cqlDefinition": "timeuuid",
                },
            },
        },
        "primaryKey": {"partitionBy": [], "partitionSort": {}},
    },
}

OUTPUT_ROW_TO_POSTPROCESS = {
    "p_text": "italy",
    "p_boolean": True,
    "p_int": 123,
    "p_float": 1.2,
    "p_float_nan": "NaN",
    "p_float_pinf": "Infinity",
    "p_float_minf": "-Infinity",
    "p_blob": {"$binary": "YWJjMTIz"},  # b"abc123",
    "p_uuid": "9c5b94b1-35ad-49bb-b118-8e8fc24abf80",
    "p_decimal": decimal.Decimal("123.456"),
    "p_date": "+11111-09-30",
    "p_duration": "P1M1DT1M0.000000001S",
    "p_inet": "10.1.1.2",
    "p_time": "12:34:56.78912",
    "p_timestamp": "2015-05-03T13:30:54.234Z",
    "p_timestamp_out_offset": "-0123-04-03T13:13:04.123+1:00",
    "p_list_int": [99, 100, 101],
    "p_list_double": [1.1, 2.2, "NaN", "Infinity", "-Infinity", 9.9],
    "p_set_ascii": ["a", "b", "c"],
    "p_set_float": [1.1, "NaN", "-Infinity", 9.9],
    "p_map_text_float": {"a": 0.1, "b": 0.2},
    "p_map_float_text": {1.1: "1-1", "NaN": "NANNN!"},
    "somevector": {"$binary": "PczMzb5MzM0+mZma"},  # [0.1, -0.2, 0.3] but not bit-wise
    "embeddings": [0.1, -0.2, 0.3],
    "p_counter": 100,
    "p_varchar": "the_varchar",
    "p_timeuuid": "0de779c0-92e3-11ef-96a4-a745ae2c0a0b",
}

EXPECTED_POSTPROCESSED_ROW = {
    "p_text": "italy",
    "p_boolean": True,
    "p_int": 123,
    "p_float": 1.2,
    "p_float_nan": float("NaN"),
    "p_float_pinf": float("Infinity"),
    "p_float_minf": float("-Infinity"),
    "p_blob": b"abc123",
    "p_uuid": UUID("9c5b94b1-35ad-49bb-b118-8e8fc24abf80"),
    "p_decimal": decimal.Decimal("123.456"),
    "p_date": DataAPIDate.from_string("+11111-09-30"),
    "p_duration": DataAPIDuration(signum=+1, months=1, days=1, nanoseconds=60000000001),
    "p_inet": ipaddress.ip_address("10.1.1.2"),
    "p_time": DataAPITime(12, 34, 56, 789120000),
    "p_timestamp": DataAPITimestamp.from_string("2015-05-03T13:30:54.234Z"),
    "p_timestamp_out_offset": DataAPITimestamp.from_string(
        "-0123-04-03T13:13:04.123+1:00"
    ),
    "p_list_int": [99, 100, 101],
    "p_list_double": [
        1.1,
        2.2,
        float("NaN"),
        float("Infinity"),
        float("-Infinity"),
        9.9,
    ],
    "p_set_ascii": DataAPISet(["a", "b", "c"]),
    "p_set_float": DataAPISet([1.1, float("NaN"), float("-Infinity"), 9.9]),
    "p_map_text_float": DataAPIMap({"a": 0.1, "b": 0.2}),
    "p_map_float_text": DataAPIMap({1.1: "1-1", float("NaN"): "NANNN!"}),
    "somevector": DataAPIVector.from_bytes(
        convert_ejson_binary_object_to_bytes({"$binary": "PczMzb5MzM0+mZma"})
    ),
    "embeddings": DataAPIVector([0.1, -0.2, 0.3]),
    "p_counter": 100,
    "p_varchar": "the_varchar",
    "p_timeuuid": UUID("0de779c0-92e3-11ef-96a4-a745ae2c0a0b"),
}

OUTPUT_NONCUSTOMTYPES_ROW_TO_POSTPROCESS = {
    "p_timestamp": "2015-05-03T13:30:54.234Z",
    "somevector": {"$binary": "PczMzb5MzM0+mZma"},  # [0.1, -0.2, 0.3] but not bit-wise
    "embeddings": [0.1, -0.2, 0.3],
    "p_date": "1956-09-30",
    "p_duration": "P1DT2M3S",
    "p_time": "12:34:56.78912",
    "p_set_ascii": ["a", "b", "c"],
    "p_set_float": [1.1, "NaN", "-Infinity", 9.9],
    "p_map_text_float": {"a": 0.1, "b": 0.2},
    "p_map_float_text": {1.1: "1-1", "NaN": "NANNN!"},
}

EXPECTED_NONCUSTOMTYPES_POSTPROCESSED_ROW = {
    "p_timestamp": datetime.datetime(
        2015, 5, 3, 13, 30, 54, 234000, tzinfo=datetime.timezone.utc
    ),
    "somevector": bytes_to_floats(
        convert_ejson_binary_object_to_bytes({"$binary": "PczMzb5MzM0+mZma"})
    ),
    "embeddings": [0.1, -0.2, 0.3],
    "p_date": datetime.date(1956, 9, 30),
    "p_duration": datetime.timedelta(days=1, minutes=2, seconds=3),
    "p_time": datetime.time(12, 34, 56, 789120),
    "p_set_ascii": {"a", "b", "c"},
    "p_set_float": {1.1, float("NaN"), float("-Infinity"), 9.9},
    "p_map_text_float": {"a": 0.1, "b": 0.2},
    "p_map_float_text": {1.1: "1-1", float("NaN"): "NANNN!"},
}

OUTPUT_FILLERS_ROW_TO_POSTPROCESS: dict[str, Any] = {}
EXPECTED_FILLERS_POSTPROCESSED_ROW: dict[str, Any] = {
    "p_text": None,
    "p_boolean": None,
    "p_int": None,
    "p_float": None,
    "p_float_nan": None,
    "p_float_pinf": None,
    "p_float_minf": None,
    "p_blob": None,
    "p_uuid": None,
    "p_decimal": None,
    "p_date": None,
    "p_duration": None,
    "p_inet": None,
    "p_time": None,
    "p_timestamp": None,
    "p_timestamp_out_offset": None,
    "p_list_int": [],
    "p_list_double": [],
    "p_set_ascii": DataAPISet(),
    "p_set_float": DataAPISet(),
    "p_map_text_float": DataAPIMap({}),
    "p_map_float_text": DataAPIMap({}),
    "somevector": None,
    "embeddings": None,
    "p_counter": None,
    "p_varchar": None,
    "p_timeuuid": None,
}
EXPECTED_FILLERS_NONCUSTOMTYPES_POSTPROCESSED_ROW: dict[str, Any] = {
    "p_text": None,
    "p_boolean": None,
    "p_int": None,
    "p_float": None,
    "p_float_nan": None,
    "p_float_pinf": None,
    "p_float_minf": None,
    "p_blob": None,
    "p_uuid": None,
    "p_decimal": None,
    "p_date": None,
    "p_duration": None,
    "p_inet": None,
    "p_time": None,
    "p_timestamp": None,
    "p_timestamp_out_offset": None,
    "p_list_int": [],
    "p_list_double": [],
    "p_set_ascii": set(),
    "p_set_float": set(),
    "p_map_text_float": {},
    "p_map_float_text": {},
    "somevector": None,
    "embeddings": None,
    "p_counter": None,
    "p_varchar": None,
    "p_timeuuid": None,
}


INPUT_ROW_TO_PREPROCESS = {
    "none": None,
    "str": "the str",
    "int": 987,
    "bool": True,
    "float": 123.4,
    "float_nan": float("NaN"),
    "float_pinf": float("Infinity"),
    "float_minf": float("-Infinity"),
    "bytes": b"\xcd\xcc\xcc={\x00\x00\x00",  # {'$binary': 'zczMPXsAAAA='}
    "DataAPITimestamp": DataAPITimestamp(-65403045296110),  # "-103-06-17T07:25:03.890Z"
    "DataAPIDate": DataAPIDate(1724, 4, 22),  # Kant's date of birth
    "DataAPITime": DataAPITime(4, 5, 6, 789000000),
    "d.datetime": datetime.datetime(
        1724, 4, 22, 4, 5, 6, 789000, tzinfo=datetime.timezone.utc
    ),
    "d.date": datetime.date(1724, 4, 22),
    "d.time": datetime.time(4, 5, 6, 789000),
    "Decimal": decimal.Decimal("123.456"),
    "Decimal_nan": decimal.Decimal("NaN"),
    "Decimal_pinf": decimal.Decimal("Infinity"),
    "Decimal_ninf": decimal.Decimal("-Infinity"),
    #
    "DataAPIVector": DataAPIVector([0.1, -0.2, 0.3]),
    "DataAPIMap_f": DataAPIMap(
        [
            (float("NaN"), 1.0),
            ("k2", float("NaN")),
            ("k3", "v3"),
        ],
    ),
    "dict_f": {
        float("NaN"): 1.0,
        "k2": float("NaN"),
        "k3": "v3",
    },
    "list_f": [0.1, float("Infinity"), 0.3],
    "DataAPISet_f": DataAPISet([0.1, float("Infinity"), 0.3]),
    "set_f": {float("Infinity")},  # 1-item, because of regular sets being unordered
}

EXPECTED_PREPROCESSED_ROW = {
    "none": None,
    "str": "the str",
    "int": 987,
    "bool": True,
    "float": 123.4,
    "float_nan": "NaN",
    "float_pinf": "Infinity",
    "float_minf": "-Infinity",
    "bytes": {"$binary": "zczMPXsAAAA="},
    "DataAPITimestamp": "-0103-06-17T07:25:03.890Z",
    "DataAPIDate": "1724-04-22",
    "DataAPITime": "04:05:06.789",
    "d.datetime": "1724-04-22T04:05:06.789000+00:00",
    "d.date": "1724-04-22",
    "d.time": "04:05:06.789000",
    "Decimal": decimal.Decimal("123.456"),
    "Decimal_nan": "NaN",
    "Decimal_pinf": "Infinity",
    "Decimal_ninf": "-Infinity",
    #
    "DataAPIVector": {"$binary": "PczMzb5MzM0+mZma"},  # [0.1, -0.2, 0.3],
    "DataAPIMap_f": {
        "NaN": 1.0,
        "k2": "NaN",
        "k3": "v3",
    },
    "dict_f": {
        "NaN": 1.0,
        "k2": "NaN",
        "k3": "v3",
    },
    "list_f": [0.1, "Infinity", 0.3],
    "DataAPISet_f": [0.1, "Infinity", 0.3],
    "set_f": ["Infinity"],
}


class TestTableConverters:
    @pytest.mark.describe("test of row postprocessors from schema")
    def test_row_postprocessors_from_schema(self) -> None:
        col_desc = TableDescriptor.coerce(TABLE_DESCRIPTION)
        tpostprocessor = create_row_tpostprocessor(
            columns=col_desc.definition.columns,
            options=FullSerdesOptions(
                binary_encode_vectors=True,
                custom_datatypes_in_reading=True,
                unroll_iterables_to_lists=False,
                use_decimals_in_collections=False,
            ),
            similarity_pseudocolumn=None,
        )

        converted_column = tpostprocessor(OUTPUT_ROW_TO_POSTPROCESS)
        assert _repaint_NaNs(converted_column) == _repaint_NaNs(
            EXPECTED_POSTPROCESSED_ROW
        )

        with pytest.raises(ValueError):
            tpostprocessor({"bippy": 123})

    @pytest.mark.describe("test of row postprocessors from schema, no custom types")
    def test_row_postprocessors_from_schema_nocustom(self) -> None:
        col_desc = TableDescriptor.coerce(TABLE_DESCRIPTION)
        tpostprocessor = create_row_tpostprocessor(
            columns={
                c_n: c_d
                for c_n, c_d in col_desc.definition.columns.items()
                if c_n in EXPECTED_NONCUSTOMTYPES_POSTPROCESSED_ROW
            },
            options=FullSerdesOptions(
                binary_encode_vectors=True,
                custom_datatypes_in_reading=False,
                unroll_iterables_to_lists=False,
                use_decimals_in_collections=False,
            ),
            similarity_pseudocolumn=None,
        )

        converted_column = tpostprocessor(OUTPUT_NONCUSTOMTYPES_ROW_TO_POSTPROCESS)
        assert _repaint_NaNs(converted_column) == _repaint_NaNs(
            EXPECTED_NONCUSTOMTYPES_POSTPROCESSED_ROW
        )

    @pytest.mark.describe("test of primary-key postprocessors based on pk-schema")
    def test_pk_postprocessors_from_schema(self) -> None:
        # constructing a fake "primary key list" and associated test assets
        # from the full 'row' for the row-postprocess test.
        # Careful in the different input format for blobs
        primary_key_schema = {
            col_name: col_dict
            for col_name, col_dict in TableDescriptor.coerce(
                TABLE_DESCRIPTION
            ).definition.columns.items()
        }
        primary_key_list = [
            (
                OUTPUT_ROW_TO_POSTPROCESS[col_name]["$binary"]  # type: ignore[index]
                if isinstance(col_desc, TableScalarColumnTypeDescriptor)
                and col_desc.column_type == TableScalarColumnType.BLOB
                else OUTPUT_ROW_TO_POSTPROCESS[col_name]
            )
            for col_name, col_desc in primary_key_schema.items()
        ]
        expected_primary_key = EXPECTED_POSTPROCESSED_ROW

        ktpostprocessor = create_key_ktpostprocessor(
            primary_key_schema=primary_key_schema,
            options=FullSerdesOptions(
                binary_encode_vectors=True,
                custom_datatypes_in_reading=True,
                unroll_iterables_to_lists=False,
                use_decimals_in_collections=False,
            ),
        )

        assert _repaint_NaNs(ktpostprocessor(primary_key_list)) == _repaint_NaNs(
            expected_primary_key
        )

        with pytest.raises(ValueError):
            ktpostprocessor(["bippy", 123])

    @pytest.mark.describe(
        "test of primary-key postprocessors based on pk-schema, no custom types"
    )
    def test_pk_postprocessors_from_schema_nocustom(self) -> None:
        primary_key_schema = {
            col_name: col_dict
            for col_name, col_dict in TableDescriptor.coerce(
                TABLE_DESCRIPTION
            ).definition.columns.items()
            if col_name in EXPECTED_NONCUSTOMTYPES_POSTPROCESSED_ROW
        }
        primary_key_list = [
            (
                OUTPUT_NONCUSTOMTYPES_ROW_TO_POSTPROCESS[col_name]["$binary"]  # type: ignore[index]
                if isinstance(col_desc, TableScalarColumnTypeDescriptor)
                and col_desc.column_type == TableScalarColumnType.BLOB
                else OUTPUT_NONCUSTOMTYPES_ROW_TO_POSTPROCESS[col_name]
            )
            for col_name, col_desc in primary_key_schema.items()
        ]
        expected_primary_key = EXPECTED_NONCUSTOMTYPES_POSTPROCESSED_ROW

        ktpostprocessor = create_key_ktpostprocessor(
            primary_key_schema=primary_key_schema,
            options=FullSerdesOptions(
                binary_encode_vectors=True,
                custom_datatypes_in_reading=False,
                unroll_iterables_to_lists=False,
                use_decimals_in_collections=False,
            ),
        )

        assert _repaint_NaNs(ktpostprocessor(primary_key_list)) == _repaint_NaNs(
            expected_primary_key
        )

    @pytest.mark.descripte("test of type-based row preprocessor")
    def test_row_preprocessors_from_types(self) -> None:
        ptp_opts = FullSerdesOptions(
            binary_encode_vectors=True,
            custom_datatypes_in_reading=True,
            unroll_iterables_to_lists=True,
            use_decimals_in_collections=False,
        )
        preprocessed_row = preprocess_table_payload(
            INPUT_ROW_TO_PREPROCESS,
            options=ptp_opts,
        )
        assert preprocessed_row == EXPECTED_PREPROCESSED_ROW

        # unroll-iterables option
        gen_row_0 = {"gen_col": (i for i in range(5))}
        preprocessed_gen_0 = preprocess_table_payload(
            gen_row_0,
            options=ptp_opts,
        )
        assert preprocessed_gen_0 == {"gen_col": [0, 1, 2, 3, 4]}
        gen_row_1 = {"gen_col": (i for i in range(5))}
        preprocessed_gen_1 = preprocess_table_payload(
            gen_row_1,
            options=FullSerdesOptions(
                binary_encode_vectors=True,
                custom_datatypes_in_reading=True,
                unroll_iterables_to_lists=False,
                use_decimals_in_collections=False,
            ),
        )
        assert preprocessed_gen_1 == gen_row_1

        # vector-encoding
        vec_data = [0.1, -0.2, 0.3]
        dvec_row = {"dvec": DataAPIVector(vec_data)}
        preprocessed_dvec_0 = preprocess_table_payload(
            dvec_row,
            options=ptp_opts,
        )
        assert preprocessed_dvec_0 == {"dvec": {"$binary": "PczMzb5MzM0+mZma"}}
        preprocessed_dvec_1 = preprocess_table_payload(
            dvec_row,
            options=FullSerdesOptions(
                binary_encode_vectors=False,
                custom_datatypes_in_reading=True,
                unroll_iterables_to_lists=True,
                use_decimals_in_collections=False,
            ),
        )
        assert preprocessed_dvec_1 == {"dvec": vec_data}

        with pytest.raises(ValueError):
            preprocess_table_payload(
                {"err_field": ObjectId()},
                options=ptp_opts,
            )

    @pytest.mark.describe("test of row postprocessors from schema, fillers")
    def test_row_postprocessors_from_schema_fillers(self) -> None:
        col_desc = TableDescriptor.coerce(TABLE_DESCRIPTION)
        tpostprocessor = create_row_tpostprocessor(
            columns=col_desc.definition.columns,
            options=FullSerdesOptions(
                binary_encode_vectors=True,
                custom_datatypes_in_reading=True,
                unroll_iterables_to_lists=False,
                use_decimals_in_collections=False,
            ),
            similarity_pseudocolumn=None,
        )

        converted_column = tpostprocessor(OUTPUT_FILLERS_ROW_TO_POSTPROCESS)
        assert _repaint_NaNs(converted_column) == _repaint_NaNs(
            EXPECTED_FILLERS_POSTPROCESSED_ROW
        )

    @pytest.mark.describe(
        "test of row postprocessors from schema, fillers, no custom types"
    )
    def test_row_postprocessors_from_schema_fillers_nocustom(self) -> None:
        col_desc = TableDescriptor.coerce(TABLE_DESCRIPTION)
        tpostprocessor = create_row_tpostprocessor(
            columns=col_desc.definition.columns,
            options=FullSerdesOptions(
                binary_encode_vectors=True,
                custom_datatypes_in_reading=False,
                unroll_iterables_to_lists=False,
                use_decimals_in_collections=False,
            ),
            similarity_pseudocolumn=None,
        )

        converted_column = tpostprocessor(OUTPUT_FILLERS_ROW_TO_POSTPROCESS)
        assert _repaint_NaNs(converted_column) == _repaint_NaNs(
            EXPECTED_FILLERS_NONCUSTOMTYPES_POSTPROCESSED_ROW
        )
