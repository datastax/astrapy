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
from typing import Any, cast

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
    is_list_of_floats,
)
from astrapy.data_types import DataAPITimestamp, DataAPIVector
from astrapy.ids import UUID, ObjectId
from astrapy.utils.api_options import FullWireFormatOptions


def preprocess_collection_payload_value(
    path: list[str], value: Any, options: FullWireFormatOptions
) -> Any:
    """
    The path helps determining special treatments
    """
    # TODO improve this flow, rewriting the path-dependent choices

    # vector-related pre-processing and coercion
    _value = value
    # is this value in the place for vectors?
    if path[-1:] == ["$vector"] and path[-2:] != ["projection", "$vector"]:
        # must coerce list-likes broadly, and is it the case to do it?
        if options.coerce_iterables_to_vectors and not (
            is_list_of_floats(_value) or isinstance(_value, DataAPIVector)
        ):
            _value = convert_vector_to_floats(_value)
        # now _value is either a list or a DataAPIVector.
        # can/should it be binary-encoded?
        can_bin_encode = path[0] in {"insertOne", "insertMany"}
        # will it be bin-encoded?
        if isinstance(_value, DataAPIVector):
            # if I can, I will
            if can_bin_encode:
                return convert_to_ejson_bytes(_value.to_bytes())
            else:
                # back to a regular list
                return _value.data
        else:
            # this is a list. Encode if set as default wire mode
            if can_bin_encode and options.binary_encode_vectors:
                return convert_to_ejson_bytes(DataAPIVector(_value).to_bytes())
            else:
                return _value
    if isinstance(_value, dict):
        return {
            k: preprocess_collection_payload_value(path + [k], v, options=options)
            for k, v in _value.items()
        }
    elif isinstance(_value, list):
        return [
            preprocess_collection_payload_value(path + [""], list_item, options=options)
            for list_item in _value
        ]
    elif isinstance(_value, datetime.datetime) or isinstance(_value, datetime.date):
        return convert_to_ejson_date_object(_value)
    elif isinstance(_value, bytes):
        return convert_to_ejson_bytes(_value)
    elif isinstance(_value, UUID):
        return convert_to_ejson_uuid_object(_value)
    elif isinstance(_value, ObjectId):
        return convert_to_ejson_objectid_object(_value)
    elif isinstance(_value, DataAPITimestamp):
        return convert_to_ejson_apitimestamp_object(_value)
    else:
        return _value


def preprocess_collection_payload(
    payload: dict[str, Any] | None, options: FullWireFormatOptions
) -> dict[str, Any] | None:
    """
    Normalize a payload for API calls.
    This includes e.g. ensuring values for "$vector" key
    are made into plain lists of floats.

    Args:
        payload (dict[str, Any]): A dict expressing a payload for an API call

    Returns:
        dict[str, Any]: a "normalized" payload dict
    """

    if payload:
        return cast(
            dict[str, Any],
            preprocess_collection_payload_value([], payload, options=options),
        )
    else:
        return payload


def postprocess_collection_response_value(
    path: list[str], value: Any, options: FullWireFormatOptions
) -> Any:
    """
    The path helps determining special treatments
    """
    # TODO improve the response postprocessing once rewriting this logic (path mgmt)

    # for reads, everywhere there's a $vector it can be treated as such and reconverted
    if path[-1:] == ["$vector"]:
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
            raise ValueError(
                f"Response parsing failed: unexpected data type found under $vector: {type(value)}"
            )
    if isinstance(value, dict):
        value_keys = set(value.keys())
        if value_keys == {"$date"}:
            # this is `{"$date": 123456}`.
            # Restore to the appropriate APIOptions-required object
            if options.custom_datatypes_in_reading:
                return convert_ejson_date_object_to_apitimestamp(value)
            else:
                return convert_ejson_date_object_to_datetime(value)
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
    response: DefaultDocumentType, options: FullWireFormatOptions
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
