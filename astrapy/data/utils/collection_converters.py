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
from typing import Any, Dict, cast

from astrapy.constants import DefaultDocumentType
from astrapy.data.utils.extended_json_converters import (
    convert_ejson_binary_object_to_bytes,
    convert_ejson_date_object_to_apitimestamp,
    convert_ejson_date_object_to_datetime,
    convert_ejson_objectid_object_to_objectid,
    convert_ejson_uuid_object_to_uuid,
    convert_to_ejson_apitimestamp_object,
    convert_to_ejson_bytes,
    convert_to_ejson_date_object,
    convert_to_ejson_objectid_object,
    convert_to_ejson_uuid_object,
)
from astrapy.data.utils.vector_coercion import (
    convert_vector_to_floats,
    ensure_unrolled_if_iterable,
    is_list_of_floats,
)
from astrapy.data_types import DataAPIDate, DataAPIMap, DataAPITimestamp, DataAPIVector
from astrapy.ids import UUID, ObjectId
from astrapy.settings.error_messages import CANNOT_ENCODE_NAIVE_DATETIME_ERROR_MESSAGE
from astrapy.utils.api_options import FullSerdesOptions

FIND_AND_RERANK_VECTOR_FLOAT_PATH = [
    "status",
    "documentResponses",
    "",
    "scores",
    "$vector",
]


def preprocess_collection_payload_value(
    path: list[str], value: Any, options: FullSerdesOptions
) -> Any:
    """
    The path helps determining special treatments
    """

    # vector-related pre-processing and coercion
    _value = value
    # is this value in the place for vectors?
    if path[-1:] == ["$vector"] and path[-2:] != ["projection", "$vector"]:
        # must coerce list-likes broadly, and is it the case to do it?
        if options.unroll_iterables_to_lists and not (
            is_list_of_floats(_value) or isinstance(_value, DataAPIVector)
        ):
            _value = convert_vector_to_floats(_value)
        # now _value is either a list or a DataAPIVector.
        # can/should it be binary-encoded?
        can_bin_encode = path[0] in {"insertOne", "insertMany"}
        # will it be bin-encoded?
        if isinstance(_value, DataAPIVector):
            # if I can, I will
            if can_bin_encode and options.binary_encode_vectors:
                return convert_to_ejson_bytes(_value.to_bytes())
            else:
                # back to a regular list
                return _value.data
        else:
            # this is a list. Encode if serdes options allow it
            if can_bin_encode and options.binary_encode_vectors:
                return convert_to_ejson_bytes(DataAPIVector(_value).to_bytes())
            else:
                return _value

    if options.unroll_iterables_to_lists:
        _value = ensure_unrolled_if_iterable(_value)
    if isinstance(_value, (dict, DataAPIMap)):
        return {
            k: preprocess_collection_payload_value(path + [k], v, options=options)
            for k, v in _value.items()
        }
    elif isinstance(_value, list):
        return [
            preprocess_collection_payload_value(path + [""], list_item, options=options)
            for list_item in _value
        ]
    elif isinstance(_value, datetime.datetime):
        if _value.utcoffset() is None and not options.accept_naive_datetimes:
            raise ValueError(CANNOT_ENCODE_NAIVE_DATETIME_ERROR_MESSAGE)
        return convert_to_ejson_date_object(_value)
    elif isinstance(_value, datetime.date):
        # Note: since 'datetime' subclasses 'date', this must come after the previous.
        # Timezone-related subtleties may make supporting this data type a "risk"
        return convert_to_ejson_date_object(_value)
    elif isinstance(_value, bytes):
        return convert_to_ejson_bytes(_value)
    elif isinstance(_value, UUID):
        return convert_to_ejson_uuid_object(_value)
    elif isinstance(_value, ObjectId):
        return convert_to_ejson_objectid_object(_value)
    elif isinstance(_value, DataAPITimestamp):
        return convert_to_ejson_apitimestamp_object(_value)
    elif isinstance(_value, DataAPIDate):
        # Despite similar timezone-concerns as for `date`, this is supported as well
        return convert_to_ejson_date_object(_value.to_date())
    else:
        return _value


def preprocess_collection_payload(
    payload: dict[str, Any] | None, options: FullSerdesOptions
) -> dict[str, Any] | None:
    """
    Normalize a payload for API calls.
    This includes e.g. ensuring values for "$vector" key are properly coerced.

    Args:
        payload (dict[str, Any]): A dict expressing a payload for an API call

    Returns:
        dict[str, Any]: a payload dict, pre-processed, ready for HTTP requests.
    """

    if payload:
        return cast(
            Dict[str, Any],
            preprocess_collection_payload_value([], payload, options=options),
        )
    else:
        return payload


def postprocess_collection_response_value(
    path: list[str], value: Any, options: FullSerdesOptions
) -> Any:
    """
    The path helps determining special treatments
    """

    # for reads, (almost) everywhere there's a $vector it can be treated as such and reconverted
    if path[-1:] == ["$vector"] and path != FIND_AND_RERANK_VECTOR_FLOAT_PATH:
        # custom faster handling for the $vector path:
        if isinstance(value, list):
            if options.custom_datatypes_in_reading:
                return DataAPIVector(value)
            else:
                return value
        elif isinstance(value, dict):
            _bytes = convert_ejson_binary_object_to_bytes(value)
            if options.custom_datatypes_in_reading:
                return DataAPIVector.from_bytes(_bytes)
            else:
                return DataAPIVector.from_bytes(_bytes).data
        else:
            return value

    if isinstance(value, dict):
        value_keys = set(value.keys())
        if value_keys == {"$date"}:
            # this is `{"$date": 123456}`.
            # Restore to the appropriate APIOptions-required object
            if options.custom_datatypes_in_reading:
                return convert_ejson_date_object_to_apitimestamp(value)
            else:
                return convert_ejson_date_object_to_datetime(
                    value, tz=options.datetime_tzinfo
                )
        elif value_keys == {"$uuid"}:
            # this is `{"$uuid": "abc123..."}`, restore to UUID
            return convert_ejson_uuid_object_to_uuid(value)
        elif value_keys == {"$objectId"}:
            # this is `{"$objectId": "123abc..."}`, restore to ObjectId
            return convert_ejson_objectid_object_to_objectid(value)
        elif value_keys == {"$binary"}:
            # this is `{"$binary": "xyz=="}`, restore to `bytes`
            return convert_ejson_binary_object_to_bytes(value)
        else:
            return {
                k: postprocess_collection_response_value(path + [k], v, options=options)
                for k, v in value.items()
            }
    elif isinstance(value, list):
        return [
            postprocess_collection_response_value(
                path + [""], list_item, options=options
            )
            for list_item in value
        ]
    else:
        return value


def postprocess_collection_response(
    response: DefaultDocumentType, options: FullSerdesOptions
) -> DefaultDocumentType:
    """
    Process a dictionary just returned from the API.
    This is the place where e.g. `{"$date": 123}` is
    converted back into a datetime object.
    """
    return cast(
        DefaultDocumentType,
        postprocess_collection_response_value([], response, options=options),
    )
