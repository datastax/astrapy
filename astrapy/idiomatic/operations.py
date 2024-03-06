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

from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import reduce
from typing import (
    Any,
    Dict,
    Iterable,
    List,
)

from astrapy.idiomatic.types import DocumentType
from astrapy.idiomatic.results import BulkWriteResult
from astrapy.idiomatic.collection import AsyncCollection, Collection


def reduce_bulk_write_results(results: List[BulkWriteResult]) -> BulkWriteResult:
    zero = BulkWriteResult(
        bulk_api_results={},
        deleted_count=0,
        inserted_count=0,
        matched_count=0,
        modified_count=0,
        upserted_count=0,
        upserted_ids={},
    )

    def _sum_results(r1: BulkWriteResult, r2: BulkWriteResult) -> BulkWriteResult:
        bulk_api_results = {**r1.bulk_api_results, **r2.bulk_api_results}
        if r1.deleted_count is None or r2.deleted_count is None:
            deleted_count = None
        else:
            deleted_count = r1.deleted_count + r2.deleted_count
        inserted_count = r1.inserted_count + r2.inserted_count
        matched_count = r1.matched_count + r2.matched_count
        modified_count = r1.modified_count + r2.modified_count
        upserted_count = r1.upserted_count + r2.upserted_count
        upserted_ids = {**r1.upserted_ids, **r2.upserted_ids}
        return BulkWriteResult(
            bulk_api_results=bulk_api_results,
            deleted_count=deleted_count,
            inserted_count=inserted_count,
            matched_count=matched_count,
            modified_count=modified_count,
            upserted_count=upserted_count,
            upserted_ids=upserted_ids,
        )

    return reduce(_sum_results, results, zero)


class BaseOperation(ABC):
    @abstractmethod
    def execute(
        self, collection: Collection, index_in_bulk_write: int
    ) -> BulkWriteResult: ...


@dataclass
class InsertOne(BaseOperation):
    document: DocumentType

    def __init__(
        self,
        document: DocumentType,
    ) -> None:
        self.document = document

    def execute(
        self, collection: Collection, index_in_bulk_write: int
    ) -> BulkWriteResult:
        op_result = collection.insert_one(document=self.document)
        return BulkWriteResult(
            bulk_api_results={index_in_bulk_write: op_result.raw_result},
            deleted_count=0,
            inserted_count=1,
            matched_count=0,
            modified_count=0,
            upserted_count=0,
            upserted_ids={},
        )


@dataclass
class InsertMany(BaseOperation):
    documents: Iterable[DocumentType]
    ordered: bool

    def __init__(
        self,
        documents: Iterable[DocumentType],
        ordered: bool = True,
    ) -> None:
        self.documents = documents
        self.ordered = ordered

    def execute(
        self, collection: Collection, index_in_bulk_write: int
    ) -> BulkWriteResult:
        op_result = collection.insert_many(
            documents=self.documents,
            ordered=self.ordered,
        )
        return BulkWriteResult(
            bulk_api_results={index_in_bulk_write: op_result.raw_result},
            deleted_count=0,
            inserted_count=len(op_result.inserted_ids),
            matched_count=0,
            modified_count=0,
            upserted_count=0,
            upserted_ids={},
        )


@dataclass
class UpdateOne(BaseOperation):
    filter: Dict[str, Any]
    update: Dict[str, Any]
    upsert: bool

    def __init__(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        *,
        upsert: bool = False,
    ) -> None:
        self.filter = filter
        self.update = update
        self.upsert = upsert

    def execute(
        self, collection: Collection, index_in_bulk_write: int
    ) -> BulkWriteResult:
        op_result = collection.update_one(
            filter=self.filter,
            update=self.update,
            upsert=self.upsert,
        )
        inserted_count = 1 if "upserted" in op_result.update_info else 0
        matched_count = (op_result.update_info.get("n") or 0) - inserted_count
        if "upserted" in op_result.update_info:
            upserted_ids = {index_in_bulk_write: op_result.update_info["upserted"]}
        else:
            upserted_ids = {}
        return BulkWriteResult(
            bulk_api_results={index_in_bulk_write: op_result.raw_result},
            deleted_count=0,
            inserted_count=inserted_count,
            matched_count=matched_count,
            modified_count=op_result.update_info.get("nModified") or 0,
            upserted_count=1 if "upserted" in op_result.update_info else 0,
            upserted_ids=upserted_ids,
        )


@dataclass
class UpdateMany(BaseOperation):
    filter: Dict[str, Any]
    update: Dict[str, Any]
    upsert: bool

    def __init__(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        *,
        upsert: bool = False,
    ) -> None:
        self.filter = filter
        self.update = update
        self.upsert = upsert

    def execute(
        self, collection: Collection, index_in_bulk_write: int
    ) -> BulkWriteResult:
        op_result = collection.update_many(
            filter=self.filter,
            update=self.update,
            upsert=self.upsert,
        )
        inserted_count = 1 if "upserted" in op_result.update_info else 0
        matched_count = (op_result.update_info.get("n") or 0) - inserted_count
        if "upserted" in op_result.update_info:
            upserted_ids = {index_in_bulk_write: op_result.update_info["upserted"]}
        else:
            upserted_ids = {}
        return BulkWriteResult(
            bulk_api_results={index_in_bulk_write: op_result.raw_result},
            deleted_count=0,
            inserted_count=inserted_count,
            matched_count=matched_count,
            modified_count=op_result.update_info.get("nModified") or 0,
            upserted_count=1 if "upserted" in op_result.update_info else 0,
            upserted_ids=upserted_ids,
        )


@dataclass
class ReplaceOne(BaseOperation):
    filter: Dict[str, Any]
    replacement: DocumentType
    upsert: bool

    def __init__(
        self,
        filter: Dict[str, Any],
        replacement: DocumentType,
        *,
        upsert: bool = False,
    ) -> None:
        self.filter = filter
        self.replacement = replacement
        self.upsert = upsert

    def execute(
        self, collection: Collection, index_in_bulk_write: int
    ) -> BulkWriteResult:
        op_result = collection.replace_one(
            filter=self.filter,
            replacement=self.replacement,
            upsert=self.upsert,
        )
        inserted_count = 1 if "upserted" in op_result.update_info else 0
        matched_count = (op_result.update_info.get("n") or 0) - inserted_count
        if "upserted" in op_result.update_info:
            upserted_ids = {index_in_bulk_write: op_result.update_info["upserted"]}
        else:
            upserted_ids = {}
        return BulkWriteResult(
            bulk_api_results={index_in_bulk_write: op_result.raw_result},
            deleted_count=0,
            inserted_count=inserted_count,
            matched_count=matched_count,
            modified_count=op_result.update_info.get("nModified") or 0,
            upserted_count=1 if "upserted" in op_result.update_info else 0,
            upserted_ids=upserted_ids,
        )


@dataclass
class DeleteOne(BaseOperation):
    filter: Dict[str, Any]

    def __init__(
        self,
        filter: Dict[str, Any],
    ) -> None:
        self.filter = filter

    def execute(
        self, collection: Collection, index_in_bulk_write: int
    ) -> BulkWriteResult:
        op_result = collection.delete_one(filter=self.filter)
        return BulkWriteResult(
            bulk_api_results={index_in_bulk_write: op_result.raw_result},
            deleted_count=op_result.deleted_count,
            inserted_count=0,
            matched_count=0,
            modified_count=0,
            upserted_count=0,
            upserted_ids={},
        )


@dataclass
class DeleteMany(BaseOperation):
    filter: Dict[str, Any]

    def __init__(
        self,
        filter: Dict[str, Any],
    ) -> None:
        self.filter = filter

    def execute(
        self, collection: Collection, index_in_bulk_write: int
    ) -> BulkWriteResult:
        op_result = collection.delete_many(filter=self.filter)
        return BulkWriteResult(
            bulk_api_results={index_in_bulk_write: op_result.raw_result},
            deleted_count=op_result.deleted_count,
            inserted_count=0,
            matched_count=0,
            modified_count=0,
            upserted_count=0,
            upserted_ids={},
        )


class AsyncBaseOperation(ABC):
    @abstractmethod
    async def execute(
        self, collection: AsyncCollection, index_in_bulk_write: int
    ) -> BulkWriteResult: ...


@dataclass
class AsyncInsertOne(AsyncBaseOperation):
    document: DocumentType

    def __init__(
        self,
        document: DocumentType,
    ) -> None:
        self.document = document

    async def execute(
        self, collection: AsyncCollection, index_in_bulk_write: int
    ) -> BulkWriteResult:
        op_result = await collection.insert_one(document=self.document)
        return BulkWriteResult(
            bulk_api_results={index_in_bulk_write: op_result.raw_result},
            deleted_count=0,
            inserted_count=1,
            matched_count=0,
            modified_count=0,
            upserted_count=0,
            upserted_ids={},
        )


@dataclass
class AsyncInsertMany(AsyncBaseOperation):
    documents: Iterable[DocumentType]
    ordered: bool

    def __init__(
        self,
        documents: Iterable[DocumentType],
        ordered: bool = True,
    ) -> None:
        self.documents = documents
        self.ordered = ordered

    async def execute(
        self, collection: AsyncCollection, index_in_bulk_write: int
    ) -> BulkWriteResult:
        op_result = await collection.insert_many(
            documents=self.documents,
            ordered=self.ordered,
        )
        return BulkWriteResult(
            bulk_api_results={index_in_bulk_write: op_result.raw_result},
            deleted_count=0,
            inserted_count=len(op_result.inserted_ids),
            matched_count=0,
            modified_count=0,
            upserted_count=0,
            upserted_ids={},
        )


@dataclass
class AsyncUpdateOne(AsyncBaseOperation):
    filter: Dict[str, Any]
    update: Dict[str, Any]
    upsert: bool

    def __init__(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        *,
        upsert: bool = False,
    ) -> None:
        self.filter = filter
        self.update = update
        self.upsert = upsert

    async def execute(
        self, collection: AsyncCollection, index_in_bulk_write: int
    ) -> BulkWriteResult:
        op_result = await collection.update_one(
            filter=self.filter,
            update=self.update,
            upsert=self.upsert,
        )
        inserted_count = 1 if "upserted" in op_result.update_info else 0
        matched_count = (op_result.update_info.get("n") or 0) - inserted_count
        if "upserted" in op_result.update_info:
            upserted_ids = {index_in_bulk_write: op_result.update_info["upserted"]}
        else:
            upserted_ids = {}
        return BulkWriteResult(
            bulk_api_results={index_in_bulk_write: op_result.raw_result},
            deleted_count=0,
            inserted_count=inserted_count,
            matched_count=matched_count,
            modified_count=op_result.update_info.get("nModified") or 0,
            upserted_count=1 if "upserted" in op_result.update_info else 0,
            upserted_ids=upserted_ids,
        )


@dataclass
class AsyncUpdateMany(AsyncBaseOperation):
    filter: Dict[str, Any]
    update: Dict[str, Any]
    upsert: bool

    def __init__(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        *,
        upsert: bool = False,
    ) -> None:
        self.filter = filter
        self.update = update
        self.upsert = upsert

    async def execute(
        self, collection: AsyncCollection, index_in_bulk_write: int
    ) -> BulkWriteResult:
        op_result = await collection.update_many(
            filter=self.filter,
            update=self.update,
            upsert=self.upsert,
        )
        inserted_count = 1 if "upserted" in op_result.update_info else 0
        matched_count = (op_result.update_info.get("n") or 0) - inserted_count
        if "upserted" in op_result.update_info:
            upserted_ids = {index_in_bulk_write: op_result.update_info["upserted"]}
        else:
            upserted_ids = {}
        return BulkWriteResult(
            bulk_api_results={index_in_bulk_write: op_result.raw_result},
            deleted_count=0,
            inserted_count=inserted_count,
            matched_count=matched_count,
            modified_count=op_result.update_info.get("nModified") or 0,
            upserted_count=1 if "upserted" in op_result.update_info else 0,
            upserted_ids=upserted_ids,
        )


@dataclass
class AsyncReplaceOne(AsyncBaseOperation):
    filter: Dict[str, Any]
    replacement: DocumentType
    upsert: bool

    def __init__(
        self,
        filter: Dict[str, Any],
        replacement: DocumentType,
        *,
        upsert: bool = False,
    ) -> None:
        self.filter = filter
        self.replacement = replacement
        self.upsert = upsert

    async def execute(
        self, collection: AsyncCollection, index_in_bulk_write: int
    ) -> BulkWriteResult:
        op_result = await collection.replace_one(
            filter=self.filter,
            replacement=self.replacement,
            upsert=self.upsert,
        )
        inserted_count = 1 if "upserted" in op_result.update_info else 0
        matched_count = (op_result.update_info.get("n") or 0) - inserted_count
        if "upserted" in op_result.update_info:
            upserted_ids = {index_in_bulk_write: op_result.update_info["upserted"]}
        else:
            upserted_ids = {}
        return BulkWriteResult(
            bulk_api_results={index_in_bulk_write: op_result.raw_result},
            deleted_count=0,
            inserted_count=inserted_count,
            matched_count=matched_count,
            modified_count=op_result.update_info.get("nModified") or 0,
            upserted_count=1 if "upserted" in op_result.update_info else 0,
            upserted_ids=upserted_ids,
        )


@dataclass
class AsyncDeleteOne(AsyncBaseOperation):
    filter: Dict[str, Any]

    def __init__(
        self,
        filter: Dict[str, Any],
    ) -> None:
        self.filter = filter

    async def execute(
        self, collection: AsyncCollection, index_in_bulk_write: int
    ) -> BulkWriteResult:
        op_result = await collection.delete_one(filter=self.filter)
        return BulkWriteResult(
            bulk_api_results={index_in_bulk_write: op_result.raw_result},
            deleted_count=op_result.deleted_count,
            inserted_count=0,
            matched_count=0,
            modified_count=0,
            upserted_count=0,
            upserted_ids={},
        )


@dataclass
class AsyncDeleteMany(AsyncBaseOperation):
    filter: Dict[str, Any]

    def __init__(
        self,
        filter: Dict[str, Any],
    ) -> None:
        self.filter = filter

    async def execute(
        self, collection: AsyncCollection, index_in_bulk_write: int
    ) -> BulkWriteResult:
        op_result = await collection.delete_many(filter=self.filter)
        return BulkWriteResult(
            bulk_api_results={index_in_bulk_write: op_result.raw_result},
            deleted_count=op_result.deleted_count,
            inserted_count=0,
            matched_count=0,
            modified_count=0,
            upserted_count=0,
            upserted_ids={},
        )
