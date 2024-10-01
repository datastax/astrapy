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
import time
from typing import Any, Dict, Iterable, cast

from astrapy.constants import DocumentType
from astrapy.ids import UUID, ObjectId


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
    return {"$date": int(time.mktime(date_value.timetuple()) * 1000)}


def convert_to_ejson_uuid_object(uuid_value: UUID) -> dict[str, str]:
    return {"$uuid": str(uuid_value)}


def convert_to_ejson_objectid_object(objectid_value: ObjectId) -> dict[str, str]:
    return {"$objectId": str(objectid_value)}


def convert_ejson_date_object_to_datetime(
    date_object: dict[str, int],
) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(date_object["$date"] / 1000.0)


def convert_ejson_uuid_object_to_uuid(uuid_object: dict[str, str]) -> UUID:
    return UUID(uuid_object["$uuid"])


def convert_ejson_objectid_object_to_objectid(
    objectid_object: dict[str, str],
) -> ObjectId:
    return ObjectId(objectid_object["$objectId"])


def normalize_payload_value(path: list[str], value: Any) -> Any:
    """
    The path helps determining special treatments
    """
    _l2 = ".".join(path[-2:])
    _l1 = ".".join(path[-1:])
    if _l1 == "$vector" and _l2 != "projection.$vector":
        if not is_list_of_floats(value):
            return convert_vector_to_floats(value)
        else:
            return value
    else:
        if isinstance(value, dict):
            return {k: normalize_payload_value(path + [k], v) for k, v in value.items()}
        elif isinstance(value, list):
            return [
                normalize_payload_value(path + [""], list_item) for list_item in value
            ]
        else:
            if isinstance(value, datetime.datetime) or isinstance(value, datetime.date):
                return convert_to_ejson_date_object(value)
            elif isinstance(value, UUID):
                return convert_to_ejson_uuid_object(value)
            elif isinstance(value, ObjectId):
                return convert_to_ejson_objectid_object(value)
            else:
                return value


def normalize_for_api(payload: dict[str, Any] | None) -> dict[str, Any] | None:
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
        return cast(Dict[str, Any], normalize_payload_value([], payload))
    else:
        return payload


def restore_response_value(path: list[str], value: Any) -> Any:
    """
    The path helps determining special treatments
    """
    if isinstance(value, dict):
        if len(value) == 1 and "$date" in value:
            # this is `{"$date": 123456}`, restore to datetime.datetime
            return convert_ejson_date_object_to_datetime(value)
        elif len(value) == 1 and "$uuid" in value:
            # this is `{"$uuid": "abc123..."}`, restore to UUID
            return convert_ejson_uuid_object_to_uuid(value)
        elif len(value) == 1 and "$objectId" in value:
            # this is `{"$objectId": "123abc..."}`, restore to ObjectId
            return convert_ejson_objectid_object_to_objectid(value)
        else:
            return {k: restore_response_value(path + [k], v) for k, v in value.items()}
    elif isinstance(value, list):
        return [restore_response_value(path + [""], list_item) for list_item in value]
    else:
        return value


def restore_from_api(response: DocumentType) -> DocumentType:
    """
    Process a dictionary just returned from the API.
    This is the place where e.g. `{"$date": 123}` is
    converted back into a datetime object.
    """
    return cast(DocumentType, restore_response_value([], response))
