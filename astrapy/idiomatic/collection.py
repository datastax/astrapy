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

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Iterable, List, Optional, Union, TYPE_CHECKING

from astrapy.db import AstraDBCollection, AsyncAstraDBCollection
from astrapy.idiomatic.types import (
    DocumentType,
    ProjectionType,
    ReturnDocument,
    normalize_optional_projection,
)
from astrapy.idiomatic.database import AsyncDatabase, Database
from astrapy.idiomatic.results import (
    DeleteResult,
    InsertManyResult,
    InsertOneResult,
    UpdateResult,
    BulkWriteResult,
)
from astrapy.idiomatic.cursors import AsyncCursor, Cursor


if TYPE_CHECKING:
    from astrapy.idiomatic.operations import AsyncBaseOperation, BaseOperation


INSERT_MANY_CONCURRENCY = 20
BULK_WRITE_CONCURRENCY = 10


def _prepare_update_info(status: Dict[str, Any]) -> Dict[str, Any]:
    return {
        **{
            "n": status.get("matchedCount") + (1 if "upsertedId" in status else 0),  # type: ignore[operator]
            "updatedExisting": (status.get("modifiedCount") or 0) > 0,
            "ok": 1.0,
            "nModified": status.get("modifiedCount"),
        },
        **({"upserted": status["upsertedId"]} if "upsertedId" in status else {}),
    }


class Collection:
    def __init__(
        self,
        database: Database,
        name: str,
        *,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> None:
        self._astra_db_collection = AstraDBCollection(
            collection_name=name,
            astra_db=database._astra_db,
            namespace=namespace,
            caller_name=caller_name,
            caller_version=caller_version,
        )
        # this comes after the above, lets AstraDBCollection resolve namespace
        self._database = database.copy(namespace=self.namespace)

    @property
    def database(self) -> Database:
        return self._database

    @property
    def namespace(self) -> str:
        return self._astra_db_collection.astra_db.namespace

    @property
    def name(self) -> str:
        return self._astra_db_collection.collection_name

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}[_astra_db_collection="{self._astra_db_collection}"]'

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Collection):
            return self._astra_db_collection == other._astra_db_collection
        else:
            return False

    def __call__(self, *pargs: Any, **kwargs: Any) -> None:
        raise TypeError(
            f"'{self.__class__.__name__}' object is not callable. If you "
            f"meant to call the '{self.name}' method on a "
            f"'{self.database.__class__.__name__}' object "
            "it is failing because no such method exists."
        )

    def copy(
        self,
        *,
        database: Optional[Database] = None,
        name: Optional[str] = None,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> Collection:
        return Collection(
            database=database or self.database,
            name=name or self._astra_db_collection.collection_name,
            namespace=namespace or self.namespace,
            caller_name=caller_name or self._astra_db_collection.caller_name,
            caller_version=caller_version or self._astra_db_collection.caller_version,
        )

    def with_options(
        self,
        *,
        name: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> Collection:
        return self.copy(
            name=name,
            caller_name=caller_name,
            caller_version=caller_version,
        )

    def to_async(
        self,
        *,
        database: Optional[AsyncDatabase] = None,
        name: Optional[str] = None,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> AsyncCollection:
        return AsyncCollection(
            database=database or self.database.to_async(),
            name=name or self._astra_db_collection.collection_name,
            namespace=namespace or self.namespace,
            caller_name=caller_name or self._astra_db_collection.caller_name,
            caller_version=caller_version or self._astra_db_collection.caller_version,
        )

    def set_caller(
        self,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> None:
        self._astra_db_collection.set_caller(
            caller_name=caller_name,
            caller_version=caller_version,
        )

    def insert_one(
        self,
        document: DocumentType,
    ) -> InsertOneResult:
        io_response = self._astra_db_collection.insert_one(document)
        if "insertedIds" in io_response.get("status", {}):
            if io_response["status"]["insertedIds"]:
                inserted_id = io_response["status"]["insertedIds"][0]
                return InsertOneResult(
                    raw_result=io_response,
                    inserted_id=inserted_id,
                )
            else:
                raise ValueError(
                    "Could not complete a insert_one operation. "
                    f"(gotten '${json.dumps(io_response)}')"
                )
        else:
            raise ValueError(
                "Could not complete a insert_one operation. "
                f"(gotten '${json.dumps(io_response)}')"
            )

    def insert_many(
        self,
        documents: Iterable[DocumentType],
        *,
        ordered: bool = True,
    ) -> InsertManyResult:
        if ordered:
            cim_responses = self._astra_db_collection.chunked_insert_many(
                documents=list(documents),
                options={"ordered": True},
                partial_failures_allowed=False,
                concurrency=1,
            )
        else:
            # unordered insertion: can do chunks concurrently
            cim_responses = self._astra_db_collection.chunked_insert_many(
                documents=list(documents),
                options={"ordered": False},
                partial_failures_allowed=True,
                concurrency=INSERT_MANY_CONCURRENCY,
            )
        _exceptions = [cim_r for cim_r in cim_responses if isinstance(cim_r, Exception)]
        _errors_in_response = [
            err
            for response in cim_responses
            if isinstance(response, dict)
            for err in (response.get("errors") or [])
        ]
        if _exceptions:
            raise _exceptions[0]
        elif _errors_in_response:
            raise ValueError(str(_errors_in_response[0]))
        else:
            inserted_ids = [
                ins_id
                for response in cim_responses
                if isinstance(response, dict)
                for ins_id in (response.get("status") or {}).get("insertedIds", [])
            ]
            return InsertManyResult(
                # if we are here, cim_responses are all dicts (no exceptions)
                raw_result=cim_responses,  # type: ignore[arg-type]
                inserted_ids=inserted_ids,
            )

    def find(
        self,
        filter: Optional[Dict[str, Any]] = None,
        *,
        projection: Optional[ProjectionType] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Optional[Dict[str, Any]] = None,
    ) -> Cursor:
        return (
            Cursor(
                collection=self,
                filter=filter,
                projection=projection,
            )
            .skip(skip)
            .limit(limit)
            .sort(sort)
        )

    def find_one(
        self,
        filter: Optional[Dict[str, Any]] = None,
        *,
        projection: Optional[ProjectionType] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Optional[Dict[str, Any]] = None,
    ) -> Union[DocumentType, None]:
        fo_cursor = self.find(
            filter=filter,
            projection=projection,
            skip=skip,
            limit=limit,
            sort=sort,
        )
        try:
            document = fo_cursor.__next__()
            return document
        except StopIteration:
            return None

    def distinct(
        self,
        key: str,
        *,
        filter: Optional[Dict[str, Any]] = None,
    ) -> List[Any]:
        return self.find(
            filter=filter,
            projection={key: True},
        ).distinct(key)

    def count_documents(
        self,
        filter: Dict[str, Any],
    ) -> int:
        cd_response = self._astra_db_collection.count_documents(filter=filter)
        if "count" in cd_response.get("status", {}):
            return cd_response["status"]["count"]  # type: ignore[no-any-return]
        else:
            raise ValueError(
                "Could not complete a count_documents operation. "
                f"(gotten '${json.dumps(cd_response)}')"
            )

    def find_one_and_replace(
        self,
        filter: Dict[str, Any],
        replacement: DocumentType,
        *,
        projection: Optional[ProjectionType] = None,
        sort: Optional[Dict[str, Any]] = None,
        upsert: bool = False,
        return_document: ReturnDocument = ReturnDocument.BEFORE,
    ) -> Union[DocumentType, None]:
        options = {
            "returnDocument": return_document.value,
            "upsert": upsert,
        }
        fo_response = self._astra_db_collection.find_one_and_replace(
            replacement=replacement,
            filter=filter,
            projection=normalize_optional_projection(projection),
            sort=sort,
            options=options,
        )
        if "document" in fo_response.get("data", {}):
            ret_document = fo_response.get("data", {}).get("document")
            if ret_document is None:
                return None
            else:
                return ret_document  # type: ignore[no-any-return]
        else:
            raise ValueError(
                "Could not complete a find_one_and_replace operation. "
                f"(gotten '${json.dumps(fo_response)}')"
            )

    def replace_one(
        self,
        filter: Dict[str, Any],
        replacement: DocumentType,
        *,
        upsert: bool = False,
    ) -> UpdateResult:
        options = {
            "upsert": upsert,
        }
        fo_response = self._astra_db_collection.find_one_and_replace(
            replacement=replacement,
            filter=filter,
            options=options,
        )
        if "document" in fo_response.get("data", {}):
            fo_status = fo_response.get("status") or {}
            _update_info = _prepare_update_info(fo_status)
            return UpdateResult(
                raw_result=fo_status,
                update_info=_update_info,
            )
        else:
            raise ValueError(
                "Could not complete a find_one_and_replace operation. "
                f"(gotten '${json.dumps(fo_response)}')"
            )

    def find_one_and_update(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        *,
        projection: Optional[ProjectionType] = None,
        sort: Optional[Dict[str, Any]] = None,
        upsert: bool = False,
        return_document: ReturnDocument = ReturnDocument.BEFORE,
    ) -> Union[DocumentType, None]:
        options = {
            "returnDocument": return_document.value,
            "upsert": upsert,
        }
        fo_response = self._astra_db_collection.find_one_and_update(
            update=update,
            filter=filter,
            projection=normalize_optional_projection(projection),
            sort=sort,
            options=options,
        )
        if "document" in fo_response.get("data", {}):
            ret_document = fo_response.get("data", {}).get("document")
            if ret_document is None:
                return None
            else:
                return ret_document  # type: ignore[no-any-return]
        else:
            raise ValueError(
                "Could not complete a find_one_and_update operation. "
                f"(gotten '${json.dumps(fo_response)}')"
            )

    def update_one(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        *,
        upsert: bool = False,
    ) -> UpdateResult:
        options = {
            "upsert": upsert,
        }
        fo_response = self._astra_db_collection.find_one_and_update(
            update=update,
            filter=filter,
            options=options,
        )
        if "document" in fo_response.get("data", {}):
            fo_status = fo_response.get("status") or {}
            _update_info = _prepare_update_info(fo_status)
            return UpdateResult(
                raw_result=fo_status,
                update_info=_update_info,
            )
        else:
            raise ValueError(
                "Could not complete a find_one_and_update operation. "
                f"(gotten '${json.dumps(fo_response)}')"
            )

    def update_many(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        *,
        upsert: bool = False,
    ) -> UpdateResult:
        options = {
            "upsert": upsert,
        }
        um_response = self._astra_db_collection.update_many(
            update=update,
            filter=filter,
            options=options,
        )
        um_status = um_response.get("status") or {}
        _update_info = _prepare_update_info(um_status)
        return UpdateResult(
            raw_result=um_status,
            update_info=_update_info,
        )

    def find_one_and_delete(
        self,
        filter: Dict[str, Any],
        *,
        projection: Optional[ProjectionType] = None,
        sort: Optional[Dict[str, Any]] = None,
    ) -> Union[DocumentType, None]:
        _projection = normalize_optional_projection(projection, ensure_fields={"_id"})
        target_document = self.find_one(
            filter=filter, projection=_projection, sort=sort
        )
        if target_document is not None:
            target_id = target_document["_id"]
            self.delete_one({"_id": target_id})
            # this is not an API atomic operation.
            # If someone deletes the document between the find and the delete,
            # this delete would silently be a no-op and we'd be returning the
            # document. By a 'infinitesimal' shift-backward of the time of this
            # operation, we recover a non-surprising behaviour. So:
            return target_document
        else:
            return target_document

    def delete_one(
        self,
        filter: Dict[str, Any],
    ) -> DeleteResult:
        do_response = self._astra_db_collection.delete_one_by_predicate(filter=filter)
        if "deletedCount" in do_response.get("status", {}):
            deleted_count = do_response["status"]["deletedCount"]
            if deleted_count == -1:
                return DeleteResult(
                    deleted_count=None,
                    raw_result=do_response,
                )
            else:
                # expected a non-negative integer:
                return DeleteResult(
                    deleted_count=deleted_count,
                    raw_result=do_response,
                )
        else:
            raise ValueError(
                "Could not complete a delete_one operation. "
                f"(gotten '${json.dumps(do_response)}')"
            )

    def delete_many(
        self,
        filter: Dict[str, Any],
    ) -> DeleteResult:
        dm_responses = self._astra_db_collection.chunked_delete_many(filter=filter)
        deleted_counts = [
            resp["status"]["deletedCount"]
            for resp in dm_responses
            if "deletedCount" in resp.get("status", {})
        ]
        if deleted_counts:
            # the "-1" occurs when len(deleted_counts) == 1 only
            deleted_count = sum(deleted_counts)
            if deleted_count == -1:
                return DeleteResult(
                    deleted_count=None,
                    raw_result=dm_responses,
                )
            else:
                # per API specs, deleted_count has to be a non-negative integer.
                return DeleteResult(
                    deleted_count=deleted_count,
                    raw_result=dm_responses,
                )
        else:
            raise ValueError(
                "Could not complete a chunked_delete_many operation. "
                f"(gotten '${json.dumps(dm_responses)}')"
            )

    def bulk_write(
        self,
        requests: Iterable[BaseOperation],
        *,
        ordered: bool = True,
    ) -> BulkWriteResult:
        # lazy importing here against circular-import error
        from astrapy.idiomatic.operations import reduce_bulk_write_results

        if ordered:
            bulk_write_results = [
                operation.execute(self, operation_i)
                for operation_i, operation in enumerate(requests)
            ]
            return reduce_bulk_write_results(bulk_write_results)
        else:
            with ThreadPoolExecutor(max_workers=BULK_WRITE_CONCURRENCY) as executor:
                bulk_write_futures = [
                    executor.submit(
                        operation.execute,
                        self,
                        operation_i,
                    )
                    for operation_i, operation in enumerate(requests)
                ]
                bulk_write_results = [
                    bulk_write_future.result()
                    for bulk_write_future in bulk_write_futures
                ]
                return reduce_bulk_write_results(bulk_write_results)

    def drop(self) -> Dict[str, Any]:
        return self.database.drop_collection(self)


class AsyncCollection:
    def __init__(
        self,
        database: AsyncDatabase,
        name: str,
        *,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> None:
        self._astra_db_collection = AsyncAstraDBCollection(
            collection_name=name,
            astra_db=database._astra_db,
            namespace=namespace,
            caller_name=caller_name,
            caller_version=caller_version,
        )
        # this comes after the above, lets AstraDBCollection resolve namespace
        self._database = database.copy(namespace=self.namespace)

    @property
    def database(self) -> AsyncDatabase:
        return self._database

    @property
    def namespace(self) -> str:
        return self._astra_db_collection.astra_db.namespace

    @property
    def name(self) -> str:
        return self._astra_db_collection.collection_name

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}[_astra_db_collection="{self._astra_db_collection}"]'

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, AsyncCollection):
            return self._astra_db_collection == other._astra_db_collection
        else:
            return False

    def __call__(self, *pargs: Any, **kwargs: Any) -> None:
        raise TypeError(
            f"'{self.__class__.__name__}' object is not callable. If you "
            f"meant to call the '{self.name}' method on a "
            f"'{self.database.__class__.__name__}' object "
            "it is failing because no such method exists."
        )

    def copy(
        self,
        *,
        database: Optional[AsyncDatabase] = None,
        name: Optional[str] = None,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> AsyncCollection:
        return AsyncCollection(
            database=database or self.database,
            name=name or self._astra_db_collection.collection_name,
            namespace=namespace or self.namespace,
            caller_name=caller_name or self._astra_db_collection.caller_name,
            caller_version=caller_version or self._astra_db_collection.caller_version,
        )

    def with_options(
        self,
        *,
        name: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> AsyncCollection:
        return self.copy(
            name=name,
            caller_name=caller_name,
            caller_version=caller_version,
        )

    def to_sync(
        self,
        *,
        database: Optional[Database] = None,
        name: Optional[str] = None,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> Collection:
        return Collection(
            database=database or self.database.to_sync(),
            name=name or self._astra_db_collection.collection_name,
            namespace=namespace or self.namespace,
            caller_name=caller_name or self._astra_db_collection.caller_name,
            caller_version=caller_version or self._astra_db_collection.caller_version,
        )

    def set_caller(
        self,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> None:
        self._astra_db_collection.set_caller(
            caller_name=caller_name,
            caller_version=caller_version,
        )

    async def insert_one(
        self,
        document: DocumentType,
    ) -> InsertOneResult:
        io_response = await self._astra_db_collection.insert_one(document)
        if "insertedIds" in io_response.get("status", {}):
            if io_response["status"]["insertedIds"]:
                inserted_id = io_response["status"]["insertedIds"][0]
                return InsertOneResult(
                    raw_result=io_response,
                    inserted_id=inserted_id,
                )
            else:
                raise ValueError(
                    "Could not complete a insert_one operation. "
                    f"(gotten '${json.dumps(io_response)}')"
                )
        else:
            raise ValueError(
                "Could not complete a insert_one operation. "
                f"(gotten '${json.dumps(io_response)}')"
            )

    async def insert_many(
        self,
        documents: Iterable[DocumentType],
        *,
        ordered: bool = True,
    ) -> InsertManyResult:
        if ordered:
            cim_responses = await self._astra_db_collection.chunked_insert_many(
                documents=list(documents),
                options={"ordered": True},
                partial_failures_allowed=False,
                concurrency=1,
            )
        else:
            # unordered insertion: can do chunks concurrently
            cim_responses = await self._astra_db_collection.chunked_insert_many(
                documents=list(documents),
                options={"ordered": False},
                partial_failures_allowed=True,
                concurrency=INSERT_MANY_CONCURRENCY,
            )
        _exceptions = [cim_r for cim_r in cim_responses if isinstance(cim_r, Exception)]
        _errors_in_response = [
            err
            for response in cim_responses
            if isinstance(response, dict)
            for err in (response.get("errors") or [])
        ]
        if _exceptions:
            raise _exceptions[0]
        elif _errors_in_response:
            raise ValueError(str(_errors_in_response[0]))
        else:
            inserted_ids = [
                ins_id
                for response in cim_responses
                if isinstance(response, dict)
                for ins_id in (response.get("status") or {}).get("insertedIds", [])
            ]
            return InsertManyResult(
                # if we are here, cim_responses are all dicts (no exceptions)
                raw_result=cim_responses,  # type: ignore[arg-type]
                inserted_ids=inserted_ids,
            )

    def find(
        self,
        filter: Optional[Dict[str, Any]] = None,
        *,
        projection: Optional[ProjectionType] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Optional[Dict[str, Any]] = None,
    ) -> AsyncCursor:
        return (
            AsyncCursor(
                collection=self,
                filter=filter,
                projection=projection,
            )
            .skip(skip)
            .limit(limit)
            .sort(sort)
        )

    async def find_one(
        self,
        filter: Optional[Dict[str, Any]] = None,
        *,
        projection: Optional[ProjectionType] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Optional[Dict[str, Any]] = None,
    ) -> Union[DocumentType, None]:
        fo_cursor = self.find(
            filter=filter,
            projection=projection,
            skip=skip,
            limit=limit,
            sort=sort,
        )
        try:
            document = await fo_cursor.__anext__()
            return document
        except StopAsyncIteration:
            return None

    async def distinct(
        self,
        key: str,
        *,
        filter: Optional[Dict[str, Any]] = None,
    ) -> List[Any]:
        cursor = self.find(
            filter=filter,
            projection={key: True},
        )
        return await cursor.distinct(key)

    async def count_documents(
        self,
        filter: Dict[str, Any],
    ) -> int:
        cd_response = await self._astra_db_collection.count_documents(filter=filter)
        if "count" in cd_response.get("status", {}):
            return cd_response["status"]["count"]  # type: ignore[no-any-return]
        else:
            raise ValueError(
                "Could not complete a count_documents operation. "
                f"(gotten '${json.dumps(cd_response)}')"
            )

    async def find_one_and_replace(
        self,
        filter: Dict[str, Any],
        replacement: DocumentType,
        *,
        projection: Optional[ProjectionType] = None,
        sort: Optional[Dict[str, Any]] = None,
        upsert: bool = False,
        return_document: ReturnDocument = ReturnDocument.BEFORE,
    ) -> Union[DocumentType, None]:
        options = {
            "returnDocument": return_document.value,
            "upsert": upsert,
        }
        fo_response = await self._astra_db_collection.find_one_and_replace(
            replacement=replacement,
            filter=filter,
            projection=normalize_optional_projection(projection),
            sort=sort,
            options=options,
        )
        if "document" in fo_response.get("data", {}):
            ret_document = fo_response.get("data", {}).get("document")
            if ret_document is None:
                return None
            else:
                return ret_document  # type: ignore[no-any-return]
        else:
            raise ValueError(
                "Could not complete a find_one_and_replace operation. "
                f"(gotten '${json.dumps(fo_response)}')"
            )

    async def replace_one(
        self,
        filter: Dict[str, Any],
        replacement: DocumentType,
        *,
        upsert: bool = False,
    ) -> UpdateResult:
        options = {
            "upsert": upsert,
        }
        fo_response = await self._astra_db_collection.find_one_and_replace(
            replacement=replacement,
            filter=filter,
            options=options,
        )
        if "document" in fo_response.get("data", {}):
            fo_status = fo_response.get("status") or {}
            _update_info = _prepare_update_info(fo_status)
            return UpdateResult(
                raw_result=fo_status,
                update_info=_update_info,
            )
        else:
            raise ValueError(
                "Could not complete a find_one_and_replace operation. "
                f"(gotten '${json.dumps(fo_response)}')"
            )

    async def find_one_and_update(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        *,
        projection: Optional[ProjectionType] = None,
        sort: Optional[Dict[str, Any]] = None,
        upsert: bool = False,
        return_document: ReturnDocument = ReturnDocument.BEFORE,
    ) -> Union[DocumentType, None]:
        options = {
            "returnDocument": return_document.value,
            "upsert": upsert,
        }
        fo_response = await self._astra_db_collection.find_one_and_update(
            update=update,
            filter=filter,
            projection=normalize_optional_projection(projection),
            sort=sort,
            options=options,
        )
        if "document" in fo_response.get("data", {}):
            ret_document = fo_response.get("data", {}).get("document")
            if ret_document is None:
                return None
            else:
                return ret_document  # type: ignore[no-any-return]
        else:
            raise ValueError(
                "Could not complete a find_one_and_update operation. "
                f"(gotten '${json.dumps(fo_response)}')"
            )

    async def update_one(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        *,
        upsert: bool = False,
    ) -> UpdateResult:
        options = {
            "upsert": upsert,
        }
        fo_response = await self._astra_db_collection.find_one_and_update(
            update=update,
            filter=filter,
            options=options,
        )
        if "document" in fo_response.get("data", {}):
            fo_status = fo_response.get("status") or {}
            _update_info = _prepare_update_info(fo_status)
            return UpdateResult(
                raw_result=fo_status,
                update_info=_update_info,
            )
        else:
            raise ValueError(
                "Could not complete a find_one_and_update operation. "
                f"(gotten '${json.dumps(fo_response)}')"
            )

    async def update_many(
        self,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        *,
        upsert: bool = False,
    ) -> UpdateResult:
        options = {
            "upsert": upsert,
        }
        um_response = await self._astra_db_collection.update_many(
            update=update,
            filter=filter,
            options=options,
        )
        um_status = um_response.get("status") or {}
        _update_info = _prepare_update_info(um_status)
        return UpdateResult(
            raw_result=um_status,
            update_info=_update_info,
        )

    async def find_one_and_delete(
        self,
        filter: Dict[str, Any],
        *,
        projection: Optional[ProjectionType] = None,
        sort: Optional[Dict[str, Any]] = None,
    ) -> Union[DocumentType, None]:
        _projection = normalize_optional_projection(projection, ensure_fields={"_id"})
        target_document = await self.find_one(
            filter=filter, projection=_projection, sort=sort
        )
        if target_document is not None:
            target_id = target_document["_id"]
            await self.delete_one({"_id": target_id})
            # this is not an API atomic operation.
            # If someone deletes the document between the find and the delete,
            # this delete would silently be a no-op and we'd be returning the
            # document. By a 'infinitesimal' shift-backward of the time of this
            # operation, we recover a non-surprising behaviour. So:
            return target_document
        else:
            return target_document

    async def delete_one(
        self,
        filter: Dict[str, Any],
    ) -> DeleteResult:
        do_response = await self._astra_db_collection.delete_one_by_predicate(
            filter=filter
        )
        if "deletedCount" in do_response.get("status", {}):
            deleted_count = do_response["status"]["deletedCount"]
            if deleted_count == -1:
                return DeleteResult(
                    deleted_count=None,
                    raw_result=do_response,
                )
            else:
                # expected a non-negative integer:
                return DeleteResult(
                    deleted_count=deleted_count,
                    raw_result=do_response,
                )
        else:
            raise ValueError(
                "Could not complete a delete_one operation. "
                f"(gotten '${json.dumps(do_response)}')"
            )

    async def delete_many(
        self,
        filter: Dict[str, Any],
        *,
        let: Optional[int] = None,
    ) -> DeleteResult:
        dm_responses = await self._astra_db_collection.chunked_delete_many(
            filter=filter
        )
        deleted_counts = [
            resp["status"]["deletedCount"]
            for resp in dm_responses
            if "deletedCount" in resp.get("status", {})
        ]
        if deleted_counts:
            # the "-1" occurs when len(deleted_counts) == 1 only
            deleted_count = sum(deleted_counts)
            if deleted_count == -1:
                return DeleteResult(
                    deleted_count=None,
                    raw_result=dm_responses,
                )
            else:
                # per API specs, deleted_count has to be a non-negative integer.
                return DeleteResult(
                    deleted_count=deleted_count,
                    raw_result=dm_responses,
                )
        else:
            raise ValueError(
                "Could not complete a chunked_delete_many operation. "
                f"(gotten '${json.dumps(dm_responses)}')"
            )

    async def bulk_write(
        self,
        requests: Iterable[AsyncBaseOperation],
        *,
        ordered: bool = True,
    ) -> BulkWriteResult:
        # lazy importing here against circular-import error
        from astrapy.idiomatic.operations import reduce_bulk_write_results

        if ordered:
            bulk_write_results = [
                await operation.execute(self, operation_i)
                for operation_i, operation in enumerate(requests)
            ]
            return reduce_bulk_write_results(bulk_write_results)
        else:
            sem = asyncio.Semaphore(BULK_WRITE_CONCURRENCY)

            async def concurrent_execute_operation(
                operation: AsyncBaseOperation,
                collection: AsyncCollection,
                index_in_bulk_write: int,
            ) -> BulkWriteResult:
                async with sem:
                    return await operation.execute(
                        collection=collection, index_in_bulk_write=index_in_bulk_write
                    )

            tasks = [
                asyncio.create_task(
                    concurrent_execute_operation(operation, self, operation_i)
                )
                for operation_i, operation in enumerate(requests)
            ]
            bulk_write_results = await asyncio.gather(*tasks)
            return reduce_bulk_write_results(bulk_write_results)

    async def drop(self) -> Dict[str, Any]:
        return await self.database.drop_collection(self)
