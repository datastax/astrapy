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
from types import TracebackType
from typing import Any, Dict, List, Optional, Type, TypedDict, Union, TYPE_CHECKING

from astrapy.db import AstraDB, AsyncAstraDB
from astrapy.idiomatic.utils import raise_unsupported_parameter, unsupported

if TYPE_CHECKING:
    from astrapy.idiomatic.collection import AsyncCollection, Collection


def _validate_create_collection_options(
    dimension: Optional[int] = None,
    metric: Optional[str] = None,
    indexing: Optional[Dict[str, Any]] = None,
    additional_options: Optional[Dict[str, Any]] = None,
) -> None:
    if additional_options:
        if "vector" in additional_options:
            raise ValueError(
                "`additional_options` dict parameter to create_collection "
                "cannot have a `vector` key. Please use the specific "
                "method parameter."
            )
        if "indexing" in additional_options:
            raise ValueError(
                "`additional_options` dict parameter to create_collection "
                "cannot have a `indexing` key. Please use the specific "
                "method parameter."
            )
    if dimension is None and metric is not None:
        raise ValueError(
            "Cannot specify `metric` and not `vector_dimension` in the "
            "create_collection method."
        )


class DatabaseConstructorParams(TypedDict):
    api_endpoint: str
    token: str
    namespace: Optional[str]
    caller_name: Optional[str]
    caller_version: Optional[str]
    api_path: Optional[str]
    api_version: Optional[str]


class Database:
    def __init__(
        self,
        api_endpoint: str,
        token: str,
        *,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
        api_path: Optional[str] = None,
        api_version: Optional[str] = None,
    ) -> None:
        self._constructor_params: DatabaseConstructorParams = {
            "api_endpoint": api_endpoint,
            "token": token,
            "namespace": namespace,
            "caller_name": caller_name,
            "caller_version": caller_version,
            "api_path": api_path,
            "api_version": api_version,
        }
        self._astra_db = AstraDB(
            token=token,
            api_endpoint=api_endpoint,
            api_path=api_path,
            api_version=api_version,
            namespace=namespace,
            caller_name=caller_name,
            caller_version=caller_version,
        )

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}[_astra_db={self._astra_db}"]'

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Database):
            return self._astra_db == other._astra_db
        else:
            return False

    def copy(self) -> Database:
        return Database(**self._constructor_params)

    def to_async(self) -> AsyncDatabase:
        return AsyncDatabase(**self._constructor_params)

    def set_caller(
        self,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> None:
        self._astra_db.caller_name = caller_name
        self._astra_db.caller_version = caller_version

    def get_collection(
        self, name: str, *, namespace: Optional[str] = None
    ) -> Collection:
        # lazy importing here against circular-import error
        from astrapy.idiomatic.collection import Collection

        _namespace = namespace or self._constructor_params["namespace"]
        return Collection(self, name, namespace=_namespace)

    def create_collection(
        self,
        name: str,
        *,
        namespace: Optional[str] = None,
        dimension: Optional[int] = None,
        metric: Optional[str] = None,
        indexing: Optional[Dict[str, Any]] = None,
        additional_options: Optional[Dict[str, Any]] = None,
    ) -> Collection:
        _validate_create_collection_options(
            dimension=dimension,
            metric=metric,
            indexing=indexing,
            additional_options=additional_options,
        )
        _options = {
            **(additional_options or {}),
            **({"indexing": indexing} if indexing else {}),
        }
        if namespace is not None:
            self._astra_db.copy(namespace=namespace).create_collection(
                name,
                options=_options,
                dimension=dimension,
                metric=metric,
            )
        else:
            self._astra_db.create_collection(
                name,
                options=_options,
                dimension=dimension,
                metric=metric,
            )
        return self.get_collection(name, namespace=namespace)

    # TODO, the return type should be a Dict[str, Any] (investigate what)
    def drop_collection(self, name_or_collection: Union[str, Collection]) -> None:
        # lazy importing here against circular-import error
        from astrapy.idiomatic.collection import Collection

        _name: str
        if isinstance(name_or_collection, Collection):
            _name = name_or_collection._astra_db_collection.collection_name
        else:
            _name = name_or_collection
        self._astra_db.delete_collection(_name)

    def list_collection_names(
        self,
        *,
        namespace: Optional[str] = None,
        filter: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        if filter:
            raise_unsupported_parameter(
                class_name=self.__class__.__name__,
                method_name="list_collection_names",
                parameter_name="filter",
            )
        if namespace:
            _client = self._astra_db.copy(namespace=namespace)
        else:
            _client = self._astra_db
        gc_response = _client.get_collections()
        if "collections" not in gc_response.get("status", {}):
            raise ValueError(
                "Could not complete a get_collections operation. "
                f"(gotten '${json.dumps(gc_response)}')"
            )
        else:
            # we know this is a list of strings
            return gc_response["status"]["collections"]  # type: ignore[no-any-return]

    @unsupported
    def aggregate(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    def cursor_command(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    def dereference(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    def watch(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    def validate_collection(*pargs: Any, **kwargs: Any) -> Any: ...


class AsyncDatabase:
    def __init__(
        self,
        api_endpoint: str,
        token: str,
        *,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
        api_path: Optional[str] = None,
        api_version: Optional[str] = None,
    ) -> None:
        self._constructor_params: DatabaseConstructorParams = {
            "api_endpoint": api_endpoint,
            "token": token,
            "namespace": namespace,
            "caller_name": caller_name,
            "caller_version": caller_version,
            "api_path": api_path,
            "api_version": api_version,
        }
        self._astra_db = AsyncAstraDB(
            token=token,
            api_endpoint=api_endpoint,
            api_path=api_path,
            api_version=api_version,
            namespace=namespace,
            caller_name=caller_name,
            caller_version=caller_version,
        )

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}[_astra_db={self._astra_db}"]'

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, AsyncDatabase):
            return self._astra_db == other._astra_db
        else:
            return False

    async def __aenter__(self) -> AsyncDatabase:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> None:
        await self._astra_db.__aexit__(
            exc_type=exc_type,
            exc_value=exc_value,
            traceback=traceback,
        )

    def copy(self) -> AsyncDatabase:
        return AsyncDatabase(**self._constructor_params)

    def to_sync(self) -> Database:
        return Database(**self._constructor_params)

    def set_caller(
        self,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> None:
        self._astra_db.caller_name = caller_name
        self._astra_db.caller_version = caller_version

    async def get_collection(
        self, name: str, *, namespace: Optional[str] = None
    ) -> AsyncCollection:
        # lazy importing here against circular-import error
        from astrapy.idiomatic.collection import AsyncCollection

        _namespace = namespace or self._constructor_params["namespace"]
        return AsyncCollection(self, name, namespace=_namespace)

    async def create_collection(
        self,
        name: str,
        *,
        namespace: Optional[str] = None,
        dimension: Optional[int] = None,
        metric: Optional[str] = None,
        indexing: Optional[Dict[str, Any]] = None,
        additional_options: Optional[Dict[str, Any]] = None,
    ) -> AsyncCollection:
        _validate_create_collection_options(
            dimension=dimension,
            metric=metric,
            indexing=indexing,
            additional_options=additional_options,
        )
        _options = {
            **(additional_options or {}),
            **({"indexing": indexing} if indexing else {}),
        }
        if namespace is not None:
            await self._astra_db.copy(namespace=namespace).create_collection(
                name,
                options=_options,
                dimension=dimension,
                metric=metric,
            )
        else:
            await self._astra_db.create_collection(
                name,
                options=_options,
                dimension=dimension,
                metric=metric,
            )
        return await self.get_collection(name, namespace=namespace)

    # TODO, the return type should be a Dict[str, Any] (investigate what)
    async def drop_collection(
        self, name_or_collection: Union[str, AsyncCollection]
    ) -> None:
        # lazy importing here against circular-import error
        from astrapy.idiomatic.collection import AsyncCollection

        _name: str
        if isinstance(name_or_collection, AsyncCollection):
            _name = name_or_collection._astra_db_collection.collection_name
        else:
            _name = name_or_collection
        await self._astra_db.delete_collection(_name)

    async def list_collection_names(
        self,
        *,
        namespace: Optional[str] = None,
        filter: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        if filter:
            raise_unsupported_parameter(
                class_name=self.__class__.__name__,
                method_name="list_collection_names",
                parameter_name="filter",
            )
        gc_response = await self._astra_db.copy(namespace=namespace).get_collections()
        if "collections" not in gc_response.get("status", {}):
            raise ValueError(
                "Could not complete a get_collections operation. "
                f"(gotten '${json.dumps(gc_response)}')"
            )
        else:
            # we know this is a list of strings
            return gc_response["status"]["collections"]  # type: ignore[no-any-return]

    @unsupported
    async def aggregate(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    async def cursor_command(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    async def dereference(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    async def watch(*pargs: Any, **kwargs: Any) -> Any: ...

    @unsupported
    async def validate_collection(*pargs: Any, **kwargs: Any) -> Any: ...
