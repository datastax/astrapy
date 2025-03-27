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

from typing import Any, Dict, Iterable, List, Optional, Tuple, TypeVar, Union

from astrapy.data_types import DataAPIVector
from astrapy.settings.defaults import (
    DATA_API_ENVIRONMENT_CASSANDRA,
    DATA_API_ENVIRONMENT_DEV,
    DATA_API_ENVIRONMENT_DSE,
    DATA_API_ENVIRONMENT_HCD,
    DATA_API_ENVIRONMENT_OTHER,
    DATA_API_ENVIRONMENT_PROD,
    DATA_API_ENVIRONMENT_TEST,
)
from astrapy.utils.str_enum import StrEnum

DefaultDocumentType = Dict[str, Any]
DefaultRowType = Dict[str, Any]
ProjectionType = Union[
    Iterable[str], Dict[str, Union[bool, Dict[str, Union[int, Iterable[int]]]]]
]
SortType = Dict[str, Any]
HybridSortType = Dict[
    str, Union[str, Dict[str, Union[str, List[float], DataAPIVector]]]
]
FilterType = Dict[str, Any]
CallerType = Tuple[Optional[str], Optional[str]]


ROW = TypeVar("ROW")
ROW2 = TypeVar("ROW2")
DOC = TypeVar("DOC")
DOC2 = TypeVar("DOC2")


def normalize_optional_projection(
    projection: ProjectionType | None,
) -> dict[str, bool | dict[str, int | Iterable[int]]] | None:
    if projection:
        if isinstance(projection, dict):
            # already a dictionary
            return projection
        else:
            # an iterable over strings: coerce to allow-list projection
            return {field: True for field in projection}
    else:
        return None


class ReturnDocument:
    """
    Admitted values for the `return_document` parameter in
    `find_one_and_replace` and `find_one_and_update` collection
    methods.
    """

    def __init__(self) -> None:
        raise NotImplementedError

    BEFORE = "before"
    AFTER = "after"


class SortMode:
    """
    Admitted values for the `sort` parameter in the find collection methods,
    e.g. `sort={"field": SortMode.ASCENDING}`.
    """

    def __init__(self) -> None:
        raise NotImplementedError

    ASCENDING = 1
    DESCENDING = -1


class VectorMetric:
    """
    Admitted values for the "metric" parameter to use in CollectionVectorOptions
    object, needed when creating vector collections through the database
    `create_collection` method.
    """

    def __init__(self) -> None:
        raise NotImplementedError

    DOT_PRODUCT = "dot_product"
    EUCLIDEAN = "euclidean"
    COSINE = "cosine"


class DefaultIdType:
    """
    Admitted values for the "default_id_type" parameter to use in
    CollectionDefaultIDOptions object, needed when creating collections
    through the database `create_collection` method.
    """

    def __init__(self) -> None:
        raise NotImplementedError

    UUID = "uuid"
    OBJECTID = "objectId"
    UUIDV6 = "uuidv6"
    UUIDV7 = "uuidv7"
    DEFAULT = "uuid"


class Environment:
    """
    Admitted values for `environment` property,
    denoting the targeted API deployment type.
    """

    def __init__(self) -> None:
        raise NotImplementedError

    PROD = DATA_API_ENVIRONMENT_PROD
    DEV = DATA_API_ENVIRONMENT_DEV
    TEST = DATA_API_ENVIRONMENT_TEST
    DSE = DATA_API_ENVIRONMENT_DSE
    HCD = DATA_API_ENVIRONMENT_HCD
    CASSANDRA = DATA_API_ENVIRONMENT_CASSANDRA
    OTHER = DATA_API_ENVIRONMENT_OTHER

    values = {PROD, DEV, TEST, DSE, HCD, CASSANDRA, OTHER}
    astra_db_values = {PROD, DEV, TEST}


class MapEncodingMode(StrEnum):
    """
    Enum for the possible values of the setting controlling whether to encode
    dicts/DataAPIMaps as lists of pairs ("association lists") in table payloads.
    """

    NEVER = "NEVER"
    DATAAPIMAPS = "DATAAPIMAPS"
    ALWAYS = "ALWAYS"


__all__ = [
    "DefaultIdType",
    "Environment",
    "MapEncodingMode",
    "ReturnDocument",
    "SortMode",
    "VectorMetric",
]

__pdoc__ = {
    "normalize_optional_projection": False,
}
