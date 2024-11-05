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
from typing import TYPE_CHECKING, Any

from astrapy.exceptions import (
    DataAPIException,
    DataAPIResponseException,
)

if TYPE_CHECKING:
    from astrapy.results import (
        OperationResult,
        TableDeleteResult,
        TableInsertManyResult,
        TableUpdateResult,
    )


@dataclass
class TableNotFoundException(DataAPIException):
    """
    A table is found non-existing and the requested operation
    cannot be performed.

    Attributes:
        text: a text message about the exception.
        keyspace: the keyspace where the table was supposed to be.
        table_name: the name of the expected table.
    """

    text: str
    keyspace: str
    table_name: str

    def __init__(
        self,
        text: str,
        *,
        keyspace: str,
        table_name: str,
    ) -> None:
        super().__init__(text)
        self.text = text
        self.keyspace = keyspace
        self.table_name = table_name


@dataclass
class TooManyRowsToCountException(DataAPIException):
    """
    A `count_documents()` operation on a table failed because of the excessive amount
    of rows to count.

    Attributes:
        text: a text message about the exception.
    """

    text: str

    def __init__(
        self,
        text: str,
    ) -> None:
        super().__init__(text)
        self.text = text


class TableCumulativeOperationException(DataAPIResponseException):
    """
    An exception of type DataAPIResponseException (see) occurred
    during an operation that in general spans several requests.
    As such, besides information on the error, it may have accumulated
    a partial result from past successful Data API requests.

    Attributes:
        text: a text message about the exception.
        error_descriptors: a list of all DataAPIErrorDescriptor objects
            found across all requests involved in this exception, which are
            possibly more than one.
        detailed_error_descriptors: a list of DataAPIDetailedErrorDescriptor
            objects, one for each of the requests performed during this operation.
            For single-request methods, such as insert_one, this list always
            has a single element.
        partial_result: an OperationResult object, just like the one that would
            be the return value of the operation, had it succeeded completely.
    """

    partial_result: OperationResult


@dataclass
class TableInsertManyException(TableCumulativeOperationException):
    """
    An exception of type DataAPIResponseException (see) occurred
    during an insert_many (that in general spans several requests).
    As such, besides information on the error, it may have accumulated
    a partial result from past successful Data API requests.

    Attributes:
        text: a text message about the exception.
        error_descriptors: a list of all DataAPIErrorDescriptor objects
            found across all requests involved in this exception, which are
            possibly more than one.
        detailed_error_descriptors: a list of DataAPIDetailedErrorDescriptor
            objects, one for each of the requests performed during this operation.
            For single-request methods, such as insert_one, this list always
            has a single element.
        partial_result: a TableInsertManyResult object, just like the one
            that would be the return value of the operation, had it succeeded
            completely.
    """

    partial_result: TableInsertManyResult

    def __init__(
        self,
        text: str,
        partial_result: TableInsertManyResult,
        *pargs: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(text, *pargs, **kwargs)
        self.partial_result = partial_result


@dataclass
class TableDeleteManyException(TableCumulativeOperationException):
    """
    An exception of type DataAPIResponseException (see) occurred
    during a delete_many (that in general spans several requests).
    As such, besides information on the error, it may have accumulated
    a partial result from past successful Data API requests.

    Attributes:
        text: a text message about the exception.
        error_descriptors: a list of all DataAPIErrorDescriptor objects
            found across all requests involved in this exception, which are
            possibly more than one.
        detailed_error_descriptors: a list of DataAPIDetailedErrorDescriptor
            objects, one for each of the requests performed during this operation.
            For single-request methods, such as insert_one, this list always
            has a single element.
        partial_result: a TableDeleteResult object, just like the one that would
            be the return value of the operation, had it succeeded completely.
    """

    partial_result: TableDeleteResult

    def __init__(
        self,
        text: str,
        partial_result: TableDeleteResult,
        *pargs: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(text, *pargs, **kwargs)
        self.partial_result = partial_result


@dataclass
class TableUpdateManyException(TableCumulativeOperationException):
    """
    An exception of type DataAPIResponseException (see) occurred
    during an update_many (that in general spans several requests).
    As such, besides information on the error, it may have accumulated
    a partial result from past successful Data API requests.

    Attributes:
        text: a text message about the exception.
        error_descriptors: a list of all DataAPIErrorDescriptor objects
            found across all requests involved in this exception, which are
            possibly more than one.
        detailed_error_descriptors: a list of DataAPIDetailedErrorDescriptor
            objects, one for each of the requests performed during this operation.
            For single-request methods, such as insert_one, this list always
            has a single element.
        partial_result: a TableUpdateResult object, just like the one that
        would be the return value of the operation, had it succeeded completely.
    """

    partial_result: TableUpdateResult

    def __init__(
        self,
        text: str,
        partial_result: TableUpdateResult,
        *pargs: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(text, *pargs, **kwargs)
        self.partial_result = partial_result
