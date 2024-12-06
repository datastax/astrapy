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

from astrapy.data_types import DataAPITimestamp
from astrapy.ids import UUID, ObjectId


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
    date_object: dict[str, int], tz: datetime.timezone | None
) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(date_object["$date"] / 1000.0, tz=tz)


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
