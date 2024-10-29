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

import base64
import datetime
import time
from typing import Any, Dict, Iterable, cast

from astrapy.constants import DefaultDocumentType
from astrapy.data_types import DataAPITimestamp, DataAPIVector
from astrapy.ids import UUID, ObjectId
from astrapy.utils.api_options import FullWireFormatOptions


def convert_vector_to_floats(vector: Iterable[Any]) -> list[float]:
    """
    Convert a vector of strings to a vector of floats.

    Args:
        vector (list): A vector of objects.

    Returns:
        list: A vector of floats.
    """
    return [float(value) for value in vector]


def is_list_of_floats(vector: Iterable[Any]) -> bool:
    """
    Safely determine if it's a list of floats.
    Assumption: if list, and first item is float, then all items are.
    """
    return isinstance(vector, list) and (
        len(vector) == 0 or isinstance(vector[0], float) or isinstance(vector[0], int)
    )


def convert_to_ejson_date_object(
    date_value: datetime.date | datetime.datetime,
) -> dict[str, int]:
    if isinstance(date_value, datetime.datetime):
        return {"$date": int(date_value.timestamp() * 1000)}
    return {"$date": int(time.mktime(date_value.timetuple()) * 1000)}


def convert_to_ejson_apitimestamp_object(
    date_value: DataAPITimestamp,
) -> dict[str, int]:
    return {"$date": date_value.timestamp_ms}


def convert_to_ejson_bytes(bytes_value: bytes) -> dict[str, str]:
    return {"$binary": base64.b64encode(bytes_value).decode()}


def convert_to_ejson_uuid_object(uuid_value: UUID) -> dict[str, str]:
    return {"$uuid": str(uuid_value)}


def convert_to_ejson_objectid_object(objectid_value: ObjectId) -> dict[str, str]:
    return {"$objectId": str(objectid_value)}


def convert_ejson_date_object_to_datetime(
    date_object: dict[str, int],
) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(date_object["$date"] / 1000.0)


def convert_ejson_date_object_to_apitimestamp(
    date_object: dict[str, int],
) -> DataAPITimestamp:
    return DataAPITimestamp(date_object["$date"])


def convert_ejson_binary_object_to_bytes(
    binary_object: dict[str, str],
) -> bytes:
    return base64.b64decode(binary_object["$binary"])


def convert_ejson_uuid_object_to_uuid(uuid_object: dict[str, str]) -> UUID:
    return UUID(uuid_object["$uuid"])


def convert_ejson_objectid_object_to_objectid(
    objectid_object: dict[str, str],
) -> ObjectId:
    return ObjectId(objectid_object["$objectId"])


def normalize_payload_value(
    path: list[str], value: Any, options: FullWireFormatOptions
) -> Any:
    """
    The path helps determining special treatments
    """
    _l2 = ".".join(path[-2:])
    _l1 = ".".join(path[-1:])
    ACCEPT_BROAD_LISTLIKE_FOR_VECTORS = options.coerce_iterables_to_vectors

    # vector-related pre-processing and coercion
    _value = value
    # is this value in the place for vectors?
    if _l1 == "$vector" and _l2 != "projection.$vector":
        # must coerce list-likes broadly, and is it the case to do it?
        if ACCEPT_BROAD_LISTLIKE_FOR_VECTORS and not (
            is_list_of_floats(_value) or isinstance(_value, DataAPIVector)
        ):
            _value = convert_vector_to_floats(_value)
        # now _value is either a list or a DataAPIVector.
        if isinstance(_value, DataAPIVector):
            return convert_to_ejson_bytes(_value.to_bytes())
        else:
            if options.binary_encode_vectors:
                return convert_to_ejson_bytes(DataAPIVector(_value).to_bytes())
            else:
                return _value
    if isinstance(_value, dict):
        return {
            k: normalize_payload_value(path + [k], v, options=options)
            for k, v in _value.items()
        }
    elif isinstance(_value, list):
        return [
            normalize_payload_value(path + [""], list_item, options=options)
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


def normalize_for_api(
    payload: dict[str, Any] | None, options: FullWireFormatOptions
) -> dict[str, Any] | None:
    """
    Normalize a payload for API calls.
    This includes e.g. ensuring values for "$vector" key
    are made into plain lists of floats.

    Args:
        payload (Dict[str, Any]): A dict expressing a payload for an API call

    Returns:
        Dict[str, Any]: a "normalized" payload dict
    """

    if payload:
        return cast(
            Dict[str, Any],
            normalize_payload_value([], payload, options=options),
        )
    else:
        return payload


def restore_response_value(
    path: list[str], value: Any, options: FullWireFormatOptions
) -> Any:
    """
    The path helps determining special treatments
    """
    _l2 = ".".join(path[-2:])
    _l1 = ".".join(path[-1:])

    if _l1 == "$vector" and _l2 != "projection.$vector":
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
                k: restore_response_value(path + [k], v, options=options)
                for k, v in value.items()
            }
    elif isinstance(value, list):
        return [
            restore_response_value(path + [""], list_item, options=options)
            for list_item in value
        ]
    else:
        return value


def restore_from_api(
    response: DefaultDocumentType, options: FullWireFormatOptions
) -> DefaultDocumentType:
    """
    Process a dictionary just returned from the API.
    This is the place where e.g. `{"$date": 123}` is
    converted back into a datetime object.
    """
    return cast(
        DefaultDocumentType,
        restore_response_value([], response, options=options),
    )
