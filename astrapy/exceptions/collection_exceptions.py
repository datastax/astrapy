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
from typing import TYPE_CHECKING, Any, Sequence

from astrapy.exceptions.data_api_exceptions import DataAPIException

if TYPE_CHECKING:
    from astrapy.results import (
        CollectionDeleteResult,
        CollectionUpdateResult,
    )


@dataclass
class TooManyDocumentsToCountException(DataAPIException):
    """
    A `count_documents()` operation on a collection failed because the resulting
    number of documents exceeded either the upper bound set by the caller or the
    hard limit imposed by the Data API.

    Attributes:
        text: a text message about the exception.
        server_max_count_exceeded: True if the count limit imposed by the API
            is reached. In that case, increasing the upper bound in the method
            invocation is of no help.
    """

    text: str
    server_max_count_exceeded: bool

    def __init__(
        self,
        text: str,
        *,
        server_max_count_exceeded: bool,
    ) -> None:
        super().__init__(text)
        self.text = text
        self.server_max_count_exceeded = server_max_count_exceeded


@dataclass
class CollectionInsertManyException(DataAPIException):
    """
    An exception occurring within an insert_many (an operation that can span
    several requests). As such, it represents both the root error(s) that happened
    and information on the portion of the documents that were successfully inserted.

    The behaviour of insert_many (concurrency and the `ordered` setting) make it
    possible that more than one "root errors" are collected.

    Attributes:
        inserted_ids: a list of the document IDs that have been successfully inserted.
        exceptions: a list of the root exceptions leading to this error. The list,
            under normal circumstances, is not empty.
    """

    inserted_ids: list[Any]
    exceptions: Sequence[Exception]

    def __str__(self) -> str:
        num_ids = len(self.inserted_ids)
        if self.exceptions:
            exc_desc: str
            excs_strs = [exc.__str__() for exc in self.exceptions[:8]]
            if len(self.exceptions) > 8:
                exc_desc = ", ".join(excs_strs) + " ... (more exceptions)"
            else:
                exc_desc = ", ".join(excs_strs)
            return (
                f"{self.__class__.__name__}({exc_desc} "
                f"[with {num_ids} inserted ids])"
            )
        else:
            return f"{self.__class__.__name__}()"


@dataclass
class CollectionDeleteManyException(DataAPIException):
    """
    An exception occurring during a delete_many (an operation that can span
    several requests). As such, besides information on the root-cause error,
    there may be a partial result about the part that succeeded.

    Attributes:
        partial_result: a CollectionDeleteResult object, just like the one that would
            be the return value of the operation, had it succeeded completely.
        cause: a root exception that happened during the delete_many, causing
            the method call to stop and raise this error.
    """

    partial_result: CollectionDeleteResult
    cause: Exception

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.cause.__str__()})"


@dataclass
class CollectionUpdateManyException(DataAPIException):
    """
    An exception occurring during an update_many (an operation that can span
    several requests). As such, besides information on the root-cause error,
    there may be a partial result about the part that succeeded.

    Attributes:
        partial_result: a CollectionUpdateResult object, just like the one that would
            be the return value of the operation, had it succeeded completely.
        cause: a root exception that happened during the update_many, causing
            the method call to stop and raise this error.
    """

    partial_result: CollectionUpdateResult
    cause: Exception

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.cause.__str__()})"
