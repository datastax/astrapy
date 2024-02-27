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

from typing import Any, Optional
from astrapy.db import AstraDB, AstraDBCollection, AsyncAstraDB, AsyncAstraDBCollection
from astrapy.idiomatic.utils import unsupported


class Collection:
    def __init__(
        self,
        collection_name: str,
        astra_db: Optional[AstraDB] = None,  # FIXME
        token: Optional[str] = None,
        api_endpoint: Optional[str] = None,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> None:
        self._astra_db_collection = AstraDBCollection(
            collection_name=collection_name,
            astra_db=astra_db,
            token=token,
            api_endpoint=api_endpoint,
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

    def copy(self) -> AstraDBCollection:
        raise NotImplementedError  # FIXME

    def to_async(self) -> AsyncAstraDBCollection:
        raise NotImplementedError  # FIXME

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
    def find_raw_batches(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    def aggregate(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    def aggregate_raw_batches(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    def watch(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    def rename(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    def create_index(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    def create_indexes(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    def drop_index(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    def drop_indexes(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    def list_indexes(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    def index_information(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    def create_search_index(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    def create_search_indexes(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    def drop_search_index(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    def list_search_indexes(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    def update_search_index(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    def distinct(*pargs: Any, **kwargs: Any) -> Any:
        ...


class AsyncCollection:
    def __init__(
        self,
        collection_name: str,
        astra_db: Optional[AsyncAstraDB] = None,
        token: Optional[str] = None,
        api_endpoint: Optional[str] = None,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> None:
        self._astra_db_collection = AsyncAstraDBCollection(
            collection_name=collection_name,
            astra_db=astra_db,
            token=token,
            api_endpoint=api_endpoint,
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

    def copy(self) -> AsyncAstraDBCollection:
        raise NotImplementedError  # FIXME

    def to_sync(self) -> AstraDBCollection:
        raise NotImplementedError  # FIXME

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
    async def find_raw_batches(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    async def aggregate(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    async def aggregate_raw_batches(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    async def watch(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    async def rename(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    async def create_index(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    async def create_indexes(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    async def drop_index(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    async def drop_indexes(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    async def list_indexes(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    async def index_information(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    async def create_search_index(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    async def create_search_indexes(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    async def drop_search_index(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    async def list_search_indexes(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    async def update_search_index(*pargs: Any, **kwargs: Any) -> Any:
        ...

    @unsupported
    async def distinct(*pargs: Any, **kwargs: Any) -> Any:
        ...
