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

from abc import ABC
from dataclasses import dataclass
from typing import Any


@dataclass
class OperationResult(ABC):
    """
    Class that represents the generic result of a single mutation operation.

    Attributes:
        raw_results: response/responses from the Data API call.
            Depending on the exact delete method being used, this
            list of raw responses can contain exactly one or a number of items.
    """

    raw_results: list[dict[str, Any]]

    def _piecewise_repr(self, pieces: list[str | None]) -> str:
        return f"{self.__class__.__name__}({', '.join(pc for pc in pieces if pc)})"


@dataclass
class CollectionDeleteResult(OperationResult):
    """
    Class that represents the result of delete operations on a collection.

    Attributes:
        deleted_count: number of deleted documents
        raw_results: response/responses from the Data API call.
            Depending on the exact delete method being used, this
            list of raw responses can contain exactly one or a number of items.
    """

    deleted_count: int

    def __repr__(self) -> str:
        return self._piecewise_repr(
            [
                f"deleted_count={self.deleted_count}",
                "raw_results=..." if self.raw_results is not None else None,
            ]
        )


@dataclass
class CollectionInsertOneResult(OperationResult):
    """
    Class that represents the result of insert_one operations on a collection.

    Attributes:
        raw_results: one-item list with the response from the Data API call
        inserted_id: the ID of the inserted document
    """

    inserted_id: Any

    def __repr__(self) -> str:
        return self._piecewise_repr(
            [
                f"inserted_id={self.inserted_id}",
                "raw_results=..." if self.raw_results is not None else None,
            ]
        )


@dataclass
class CollectionInsertManyResult(OperationResult):
    """
    Class that represents the result of insert_many operations on a collection.

    Attributes:
        raw_results: responses from the Data API calls
        inserted_ids: list of the IDs of the inserted documents
    """

    inserted_ids: list[Any]

    def __repr__(self) -> str:
        _ins_ids_str: str
        if len(self.inserted_ids) > 5:
            _ins_ids_str = (
                f"[{', '.join(str(_iid) for _iid in self.inserted_ids[:5])} "
                f"... ({len(self.inserted_ids)} total)]"
            )
        else:
            _ins_ids_str = str(self.inserted_ids)
        return self._piecewise_repr(
            [
                f"inserted_ids={_ins_ids_str}",
                "raw_results=..." if self.raw_results is not None else None,
            ]
        )


@dataclass
class CollectionUpdateResult(OperationResult):
    """
    Class that represents the result of any update operation on a collection.

    Attributes:
        raw_results: responses from the Data API calls
        update_info: a dictionary reporting about the update

    Note:
        the "update_info" field has the following fields: "n" (int),
        "updatedExisting" (bool), "ok" (float), "nModified" (int)
        and optionally "upserted" containing the ID of an upserted document.
    """

    update_info: dict[str, Any]

    def __repr__(self) -> str:
        return self._piecewise_repr(
            [
                f"update_info={self.update_info}",
                "raw_results=..." if self.raw_results is not None else None,
            ]
        )


@dataclass
class TableInsertOneResult(OperationResult):
    """
    Class that represents the result of insert_one operations on a table.

    Attributes:
        raw_results: one-item list with the response from the Data API call
        inserted_id: the ID of the inserted document in the form of a dict
        inserted_id_tuple: an ordered-tuple version of the same ID
    """

    inserted_id: Any
    inserted_id_tuple: tuple[Any, ...]

    def __repr__(self) -> str:
        return self._piecewise_repr(
            [
                f"inserted_id={self.inserted_id}",
                f"inserted_id_tuple={self.inserted_id_tuple}",
                "raw_results=..." if self.raw_results is not None else None,
            ]
        )


@dataclass
class TableInsertManyResult(OperationResult):
    """
    Class that represents the result of insert_many operations on a collection.

    Attributes:
        raw_results: responses from the Data API calls
        inserted_ids: list of the IDs of the inserted documents in the form of dictionaries
        inserted_id_tuple: an ordered-tuple version of the same inserted IDs
    """

    inserted_ids: list[Any]
    inserted_id_tuples: list[tuple[Any, ...]]

    def __repr__(self) -> str:
        _ins_ids_str: str
        if len(self.inserted_ids) > 5:
            _ins_ids_str = (
                f"[{', '.join(str(_iid) for _iid in self.inserted_ids[:5])} "
                f"... ({len(self.inserted_ids)} total)]"
            )
        else:
            _ins_ids_str = str(self.inserted_ids)
        _ins_idts_str: str
        if len(self.inserted_id_tuples) > 5:
            _ins_idts_str = (
                f"[{', '.join(str(_iidt) for _iidt in self.inserted_id_tuples[:5])} "
                f"... ({len(self.inserted_id_tuples)} total)]"
            )
        else:
            _ins_idts_str = str(self.inserted_id_tuples)
        return self._piecewise_repr(
            [
                f"inserted_ids={_ins_ids_str}",
                f"inserted_id_tuples={_ins_idts_str}",
                "raw_results=..." if self.raw_results is not None else None,
            ]
        )
