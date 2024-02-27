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

from typing import Any, Optional, TypedDict
from astrapy.db import AstraDBCollection, AsyncAstraDBCollection
from astrapy.idiomatic.utils import unsupported
from astrapy.idiomatic.database import AsyncDatabase, Database


class CollectionConstructorParams(TypedDict):
    database: Database
    name: str
    namespace: Optional[str]
    caller_name: Optional[str]
    caller_version: Optional[str]


class AsyncCollectionConstructorParams(TypedDict):
    database: AsyncDatabase
    name: str
    namespace: Optional[str]
    caller_name: Optional[str]
    caller_version: Optional[str]


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
        self._constructor_params: CollectionConstructorParams = {
            "database": database,
            "name": name,
            "namespace": namespace,
            "caller_name": caller_name,
            "caller_version": caller_version,
        }
        self._astra_db_collection = AstraDBCollection(
            collection_name=name,
            astra_db=database._astra_db,
            namespace=namespace,
            caller_name=caller_name,
            caller_version=caller_version,
        )

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}[_astra_db_collection="{self._astra_db_collection}"]'

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Collection):
            return self._astra_db_collection == other._astra_db_collection
        else:
            return False

    def copy(self) -> Collection:
        return Collection(**self._constructor_params)

    def to_async(self) -> AsyncCollection:
        return AsyncCollection(
            **{  # type: ignore[arg-type]
                **self._constructor_params,
                **{"database": self._constructor_params["database"].to_async()},
            }
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
        self._constructor_params: AsyncCollectionConstructorParams = {
            "database": database,
            "name": name,
            "namespace": namespace,
            "caller_name": caller_name,
            "caller_version": caller_version,
        }
        self._astra_db_collection = AsyncAstraDBCollection(
            collection_name=name,
            astra_db=database._astra_db,
            namespace=namespace,
            caller_name=caller_name,
            caller_version=caller_version,
        )

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}[_astra_db_collection="{self._astra_db_collection}"]'

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, AsyncCollection):
            return self._astra_db_collection == other._astra_db_collection
        else:
            return False

    def copy(self) -> AsyncCollection:
        return AsyncCollection(**self._constructor_params)

    def to_sync(self) -> Collection:
        return Collection(
            **{  # type: ignore[arg-type]
                **self._constructor_params,
                **{"database": self._constructor_params["database"].to_sync()},
            }
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
