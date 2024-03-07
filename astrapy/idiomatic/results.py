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

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union


@dataclass
class DeleteResult:
    """
    Class that represents the result of delete operations.

    Attributes:
        deleted_count: number of deleted documents
        raw_result: response/responses from the Data API call.
            Depending on the exact delete method being used, this
            can be a list of raw responses or a single raw response.
        acknowledged: whether the server acknowledged the write operation
    """
    deleted_count: Optional[int]
    raw_result: Union[Dict[str, Any], List[Dict[str, Any]]]
    acknowledged: bool = True


@dataclass
class InsertOneResult:
    """
    Class that represents the result of insert_one operations.

    Attributes:
        raw_result: response from the Data API call
        inserted_id: the ID of the inserted document
        acknowledged: whether the server acknowledged the write operation
    """
    raw_result: Dict[str, Any]
    inserted_id: Any
    acknowledged: bool = True


@dataclass
class InsertManyResult:
    """
    Class that represents the result of insert_many operations.

    Attributes:
        raw_result: response from the Data API call
        inserted_ids: list of the IDs of the inserted documents
        acknowledged: whether the server acknowledged the write operation
    """
    raw_result: List[Dict[str, Any]]
    inserted_ids: List[Any]
    acknowledged: bool = True


@dataclass
class UpdateResult:
    """
    Class that represents the result of any update operation.

    Attributes:
        raw_result: response from the Data API call
        update_info: a dictionary reporting about the update
        acknowledged: whether the server acknowledged the write operation

    Note:
        the "update_info" field has the following fields: "n" (int),
        "updatedExisting" (bool), "ok" (float), "nModified" (int)
        and optionally "upserted" containing the ID of an upserted document.

    """
    raw_result: Dict[str, Any]
    update_info: Dict[str, Any]
    acknowledged: bool = True


@dataclass
class BulkWriteResult:
    """
    Class that represents the result of a bulk write operations.

    Indices in the maps below refer to the position of each write operation
    in the list of operations passed to the bulk_write command.

    The numeric counts refer to the whole of the bulk write.

    Attributes:
        bulk_api_results: a map from indices to the corresponding raw responses
        deleted_count: number of deleted documents
        inserted_count: number of inserted documents
        matched_count: number of matched documents
        modified_count: number of modified documents
        upserted_count: number of upserted documents
        upserted_ids: a (sparse) map from indices to ID of the upserted document
        acknowledged: whether the server acknowledged the write operation
    """
    bulk_api_results: Dict[int, Union[Dict[str, Any], List[Dict[str, Any]]]]
    deleted_count: Optional[int]
    inserted_count: int
    matched_count: int
    modified_count: int
    upserted_count: int
    upserted_ids: Dict[int, Any]
    acknowledged: bool = True
