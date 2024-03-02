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

import json
from typing import Any, Dict, Optional

from astrapy.db import AstraDBCollection, AsyncAstraDBCollection
from astrapy.idiomatic.utils import raise_unsupported_parameter, unsupported
from astrapy.idiomatic.database import AsyncDatabase, Database
from astrapy.idiomatic.results import DeleteResult, InsertOneResult


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
        document: Dict[str, Any],
        *,
        bypass_document_validation: Optional[bool] = None,
    ) -> InsertOneResult:
        if bypass_document_validation:
            raise_unsupported_parameter(
                class_name=self.__class__.__name__,
                method_name="insert_one",
                parameter_name="bypass_document_validation",
            )
        io_response = self._astra_db_collection.insert_one(document)
        if "insertedIds" in io_response.get("status", {}):
            if io_response["status"]["insertedIds"]:
                inserted_id = io_response["status"]["insertedIds"][0]
                return InsertOneResult(
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

    def count_documents(
        self,
        filter: Dict[str, Any],
        *,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> int:
        if skip:
            raise_unsupported_parameter(
                class_name=self.__class__.__name__,
                method_name="count_documents",
                parameter_name="skip",
            )
        if limit:
            raise_unsupported_parameter(
                class_name=self.__class__.__name__,
                method_name="count_documents",
                parameter_name="limit",
            )
        cd_response = self._astra_db_collection.count_documents(filter=filter)
        if "count" in cd_response.get("status", {}):
            return cd_response["status"]["count"]  # type: ignore[no-any-return]
        else:
            raise ValueError(
                "Could not complete a count_documents operation. "
                f"(gotten '${json.dumps(cd_response)}')"
            )

    def delete_one(
        self,
        filter: Dict[str, Any],
        *,
        let: Optional[int] = None,
    ) -> DeleteResult:
        if let:
            raise_unsupported_parameter(
                class_name=self.__class__.__name__,
                method_name="delete_one",
                parameter_name="let",
            )
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
        *,
        let: Optional[int] = None,
    ) -> DeleteResult:
        if let:
            raise_unsupported_parameter(
                class_name=self.__class__.__name__,
                method_name="delete_many",
                parameter_name="let",
            )
        dm_response = self._astra_db_collection.delete_many(filter=filter)
        if "deletedCount" in dm_response.get("status", {}):
            deleted_count = dm_response["status"]["deletedCount"]
            if deleted_count == -1:
                return DeleteResult(
                    deleted_count=None,
                    raw_result=dm_response,
                )
            else:
                # expected a non-negative integer:
                return DeleteResult(
                    deleted_count=deleted_count,
                    raw_result=dm_response,
                )
        else:
            raise ValueError(
                "Could not complete a delete_many operation. "
                f"(gotten '${json.dumps(dm_response)}')"
            )

    @unsupported
    def find_raw_batches(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    def aggregate(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    def aggregate_raw_batches(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    def watch(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    def rename(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    def create_index(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    def create_indexes(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    def drop_index(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    def drop_indexes(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    def list_indexes(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    def index_information(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    def create_search_index(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    def create_search_indexes(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    def drop_search_index(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    def list_search_indexes(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    def update_search_index(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    def distinct(*pargs: Any, **kwargs: Any) -> Any: ...


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
        document: Dict[str, Any],
        *,
        bypass_document_validation: Optional[bool] = None,
    ) -> InsertOneResult:
        if bypass_document_validation:
            raise_unsupported_parameter(
                class_name=self.__class__.__name__,
                method_name="insert_one",
                parameter_name="bypass_document_validation",
            )
        io_response = await self._astra_db_collection.insert_one(document)
        if "insertedIds" in io_response.get("status", {}):
            if io_response["status"]["insertedIds"]:
                inserted_id = io_response["status"]["insertedIds"][0]
                return InsertOneResult(
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

    async def count_documents(
        self,
        filter: Dict[str, Any],
        *,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> int:
        if skip:
            raise_unsupported_parameter(
                class_name=self.__class__.__name__,
                method_name="count_documents",
                parameter_name="skip",
            )
        if limit:
            raise_unsupported_parameter(
                class_name=self.__class__.__name__,
                method_name="count_documents",
                parameter_name="limit",
            )
        cd_response = await self._astra_db_collection.count_documents(filter=filter)
        if "count" in cd_response.get("status", {}):
            return cd_response["status"]["count"]  # type: ignore[no-any-return]
        else:
            raise ValueError(
                "Could not complete a count_documents operation. "
                f"(gotten '${json.dumps(cd_response)}')"
            )

    async def delete_one(
        self,
        filter: Dict[str, Any],
        *,
        let: Optional[int] = None,
    ) -> DeleteResult:
        if let:
            raise_unsupported_parameter(
                class_name=self.__class__.__name__,
                method_name="delete_one",
                parameter_name="let",
            )
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
        if let:
            raise_unsupported_parameter(
                class_name=self.__class__.__name__,
                method_name="delete_many",
                parameter_name="let",
            )
        dm_response = await self._astra_db_collection.delete_many(filter=filter)
        if "deletedCount" in dm_response.get("status", {}):
            deleted_count = dm_response["status"]["deletedCount"]
            if deleted_count == -1:
                return DeleteResult(
                    deleted_count=None,
                    raw_result=dm_response,
                )
            else:
                # expected a non-negative integer:
                return DeleteResult(
                    deleted_count=deleted_count,
                    raw_result=dm_response,
                )
        else:
            raise ValueError(
                "Could not complete a delete_many operation. "
                f"(gotten '${json.dumps(dm_response)}')"
            )

    @unsupported
    async def find_raw_batches(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    async def aggregate(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    async def aggregate_raw_batches(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    async def watch(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    async def rename(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    async def create_index(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    async def create_indexes(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    async def drop_index(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    async def drop_indexes(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    async def list_indexes(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    async def index_information(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    async def create_search_index(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    async def create_search_indexes(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    async def drop_search_index(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    async def list_search_indexes(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    async def update_search_index(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    async def distinct(*pargs: Any, **kwargs: Any) -> Any: ...
