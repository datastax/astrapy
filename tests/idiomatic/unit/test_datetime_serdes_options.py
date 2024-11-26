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

import pytest

from astrapy.data.utils.collection_converters import (
    postprocess_collection_response,
    preprocess_collection_payload,
)
from astrapy.data_types import DataAPITimestamp
from astrapy.utils.api_options import FullSerdesOptions


class TestDatetimeSerdesOptions:
    @pytest.mark.describe("test reading $date from collection, custom dt")
    def test_ddate_collection_reading_customdt(self) -> None:
        tz = datetime.timezone(datetime.timedelta(hours=2, minutes=45))
        sd_options = FullSerdesOptions(
            binary_encode_vectors=False,
            custom_datatypes_in_reading=True,
            unroll_iterables_to_lists=False,
            use_decimals_in_collections=False,
            accept_naive_datetimes=False,
            datetime_tzinfo=tz,
        )
        ts = DataAPITimestamp.from_string("1991-07-23T12:34:56+01:30")
        raw_response = {"mu": {"$date": ts.timestamp_ms}}
        postprocessed = postprocess_collection_response(
            raw_response,
            options=sd_options,
        )
        assert postprocessed["mu"] == ts

    @pytest.mark.describe("test reading $date from collection, stdlib and tzaware")
    def test_ddate_collection_reading_stdlibtz(self) -> None:
        tz = datetime.timezone(datetime.timedelta(hours=2, minutes=45))
        sd_options = FullSerdesOptions(
            binary_encode_vectors=False,
            custom_datatypes_in_reading=False,
            unroll_iterables_to_lists=False,
            use_decimals_in_collections=False,
            accept_naive_datetimes=False,
            datetime_tzinfo=tz,
        )
        dt = datetime.datetime(1991, 7, 23, 12, 34, 56, tzinfo=tz)
        raw_response = {"mu": {"$date": dt.timestamp() * 1000}}
        postprocessed = postprocess_collection_response(
            raw_response,
            options=sd_options,
        )
        assert postprocessed["mu"] == dt

    @pytest.mark.describe("test reading $date from collection, stdlib and naive")
    def test_ddate_collection_reading_stdlibnaive(self) -> None:
        sd_options = FullSerdesOptions(
            binary_encode_vectors=False,
            custom_datatypes_in_reading=False,
            unroll_iterables_to_lists=False,
            use_decimals_in_collections=False,
            accept_naive_datetimes=False,
            datetime_tzinfo=None,
        )
        dt = datetime.datetime(1991, 7, 23, 12, 34, 56)
        raw_response = {"mu": {"$date": dt.timestamp() * 1000}}
        postprocessed = postprocess_collection_response(
            raw_response,
            options=sd_options,
        )
        assert postprocessed["mu"] == dt

    @pytest.mark.describe(
        "test writing tzaware datetime to collection, no naive allowed"
    )
    def test_ddate_collection_writing_tzaware_strict(self) -> None:
        tz = datetime.timezone(datetime.timedelta(hours=2, minutes=45))
        sd_options = FullSerdesOptions(
            binary_encode_vectors=False,
            custom_datatypes_in_reading=False,
            unroll_iterables_to_lists=False,
            use_decimals_in_collections=False,
            accept_naive_datetimes=False,
            datetime_tzinfo=None,
        )
        dt = datetime.datetime(1991, 7, 23, 12, 34, 56, tzinfo=tz)
        preprocessed = preprocess_collection_payload({"mu": dt}, options=sd_options)
        assert preprocessed is not None
        assert preprocessed["mu"]["$date"] == dt.timestamp() * 1000

    @pytest.mark.describe("test writing naive datetime to collection, no naive allowed")
    def test_ddate_collection_writing_naive_strict(self) -> None:
        sd_options = FullSerdesOptions(
            binary_encode_vectors=False,
            custom_datatypes_in_reading=False,
            unroll_iterables_to_lists=False,
            use_decimals_in_collections=False,
            accept_naive_datetimes=False,
            datetime_tzinfo=None,
        )
        dt = datetime.datetime(1991, 7, 23, 12, 34, 56)
        with pytest.raises(ValueError, match="tz"):
            preprocess_collection_payload({"mu": dt}, options=sd_options)

    @pytest.mark.describe(
        "test writing tzaware datetime to collection, naive permitted"
    )
    def test_ddate_collection_writing_tzaware_relaxed(self) -> None:
        tz = datetime.timezone(datetime.timedelta(hours=2, minutes=45))
        sd_options = FullSerdesOptions(
            binary_encode_vectors=False,
            custom_datatypes_in_reading=False,
            unroll_iterables_to_lists=False,
            use_decimals_in_collections=False,
            accept_naive_datetimes=True,
            datetime_tzinfo=None,
        )
        dt = datetime.datetime(1991, 7, 23, 12, 34, 56, tzinfo=tz)
        preprocessed = preprocess_collection_payload({"mu": dt}, options=sd_options)
        assert preprocessed is not None
        assert preprocessed["mu"]["$date"] == dt.timestamp() * 1000

    @pytest.mark.describe("test writing naive datetime to collection, naive permitted")
    def test_ddate_collection_writing_naive_relaxed(self) -> None:
        sd_options = FullSerdesOptions(
            binary_encode_vectors=False,
            custom_datatypes_in_reading=False,
            unroll_iterables_to_lists=False,
            use_decimals_in_collections=False,
            accept_naive_datetimes=True,
            datetime_tzinfo=None,
        )
        dt = datetime.datetime(1991, 7, 23, 12, 34, 56)
        preprocessed = preprocess_collection_payload({"mu": dt}, options=sd_options)
        assert preprocessed is not None
        assert preprocessed["mu"]["$date"] == dt.timestamp() * 1000
