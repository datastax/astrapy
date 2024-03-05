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
from typing import Any, Dict, List, Optional, Type, Union, TYPE_CHECKING

from astrapy.db import AstraDB, AsyncAstraDB
from astrapy.ops import AstraDBOps

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


def _recast_api_collection_dict(api_coll_dict: Dict[str, Any]) -> Dict[str, Any]:
    _name = api_coll_dict["name"]
    _options = api_coll_dict.get("options") or {}
    _v_options0 = _options.get("vector") or {}
    _indexing = _options.get("indexing") or {}
    _v_dimension = _v_options0.get("dimension")
    _v_metric = _v_options0.get("metric")
    _additional_options = {
        k: v for k, v in _options.items() if k not in {"vector", "indexing"}
    }
    recast_dict0 = {
        "name": _name,
        "dimension": _v_dimension,
        "metric": _v_metric,
        "indexing": _indexing,
        "additional_options": _additional_options,
    }
    recast_dict = {k: v for k, v in recast_dict0.items() if v}
    return recast_dict


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
        self._astra_db = AstraDB(
            token=token,
            api_endpoint=api_endpoint,
            api_path=api_path,
            api_version=api_version,
            namespace=namespace,
            caller_name=caller_name,
            caller_version=caller_version,
        )

        self.client_options = {
            "token": "",
            "api_endpoint": "",
            "api_path": "",
            "api_version": "",
            "namespace": "",
            "caller_name": "",
            "caller_version": "",
        }

        astraDBOps = AstraDBOps(token=token)

        # Get the database object and name
        if "-" in api_endpoint:
            self.dbid = api_endpoint.split("/")[2].split(".")[0][:36]

            details = astraDBOps.get_database(database=self.dbid)
            self.info: Optional[Dict[str, Any]] = details["info"]
            self.name: Optional[str] = details["info"]["name"]
            self.region: Optional[str] = details["info"]["region"]
            self.database: Optional[Dict[str, Any]] = {
                "id": self.dbid,
                "name": self.name,
                "region": self.region,
            }

    def __getattr__(self, collection_name: str) -> Collection:
        return self.get_collection(name=collection_name)

    def __getitem__(self, collection_name: str) -> Collection:
        return self.get_collection(name=collection_name)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}[_astra_db={self._astra_db}"]'

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Database):
            return self._astra_db == other._astra_db
        else:
            return False

    @property
    def namespace(self) -> str:
        return self._astra_db.namespace

    def copy(
        self,
        *,
        api_endpoint: Optional[str] = None,
        token: Optional[str] = None,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
        api_path: Optional[str] = None,
        api_version: Optional[str] = None,
    ) -> Database:
        return Database(
            api_endpoint=api_endpoint or self._astra_db.api_endpoint,
            token=token or self._astra_db.token,
            namespace=namespace or self._astra_db.namespace,
            caller_name=caller_name or self._astra_db.caller_name,
            caller_version=caller_version or self._astra_db.caller_version,
            api_path=api_path or self._astra_db.api_path,
            api_version=api_version or self._astra_db.api_version,
        )

    def to_async(
        self,
        *,
        api_endpoint: Optional[str] = None,
        token: Optional[str] = None,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
        api_path: Optional[str] = None,
        api_version: Optional[str] = None,
    ) -> AsyncDatabase:
        return AsyncDatabase(
            api_endpoint=api_endpoint or self._astra_db.api_endpoint,
            token=token or self._astra_db.token,
            namespace=namespace or self._astra_db.namespace,
            caller_name=caller_name or self._astra_db.caller_name,
            caller_version=caller_version or self._astra_db.caller_version,
            api_path=api_path or self._astra_db.api_path,
            api_version=api_version or self._astra_db.api_version,
        )

    def set_caller(
        self,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> None:
        self._astra_db.set_caller(
            caller_name=caller_name,
            caller_version=caller_version,
        )

    def get_collection(
        self, name: str, *, namespace: Optional[str] = None
    ) -> Collection:
        # lazy importing here against circular-import error
        from astrapy.idiomatic.collection import Collection

        _namespace = namespace or self._astra_db.namespace
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
        check_exists: Optional[bool] = None,
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

        if check_exists is None:
            _check_exists = True
        else:
            _check_exists = check_exists
        existing_names: List[str]
        if _check_exists:
            existing_names = self.list_collection_names(namespace=namespace)
        else:
            existing_names = []
        if name in existing_names:
            raise ValueError(f"CollectionInvalid: collection {name} already exists")

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

    def drop_collection(
        self, name_or_collection: Union[str, Collection]
    ) -> Dict[str, Any]:
        
        # lazy importing here against circular-import error
        from astrapy.idiomatic.collection import Collection

        if isinstance(name_or_collection, Collection):
            _namespace = name_or_collection.namespace
            _name: str = name_or_collection._astra_db_collection.collection_name
            dc_response = self._astra_db.copy(namespace=_namespace).delete_collection(
                _name
            )
            return dc_response.get("status", {})  # type: ignore[no-any-return]
        else:
            dc_response = self._astra_db.delete_collection(name_or_collection)
            return dc_response.get("status", {})  # type: ignore[no-any-return]

    def list_collections(
        self,
        *,
        namespace: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        if namespace:
            _client = self._astra_db.copy(namespace=namespace)
        else:
            _client = self._astra_db
        gc_response = _client.get_collections(options={"explain": True})
        if "collections" not in gc_response.get("status", {}):
            raise ValueError(
                "Could not complete a get_collections operation. "
                f"(gotten '${json.dumps(gc_response)}')"
            )
        else:
            # we know this is a list of dicts which need a little adjusting
            return [
                _recast_api_collection_dict(col_dict)
                for col_dict in gc_response["status"]["collections"]
            ]

    def list_collection_names(
        self,
        *,
        namespace: Optional[str] = None,
    ) -> List[str]:
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
        self._astra_db = AsyncAstraDB(
            token=token,
            api_endpoint=api_endpoint,
            api_path=api_path,
            api_version=api_version,
            namespace=namespace,
            caller_name=caller_name,
            caller_version=caller_version,
        )

        astraDBOps = AstraDBOps(token=token)

        if "-" in api_endpoint:
            self.dbid = api_endpoint.split("/")[2].split(".")[0][:36]

            details = astraDBOps.get_database(database=self.dbid)
            self.info: Optional[Dict[str, Any]] = details["info"]
            self.name: Optional[str] = details["info"]["name"]
            self.region: Optional[str] = details["info"]["region"]
            self.database: Optional[Dict[str, Any]] = {
                "id": self.dbid,
                "name": self.name,
                "region": self.region,
            }

        astraDBOps = AstraDBOps(token=token)

    async def __getattr__(self, collection_name: str) -> AsyncCollection:
        return await self.get_collection(name=collection_name)

    async def __getitem__(self, collection_name: str) -> AsyncCollection:
        return await self.get_collection(name=collection_name)

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

    @property
    def namespace(self) -> str:
        return self._astra_db.namespace

    def copy(
        self,
        *,
        api_endpoint: Optional[str] = None,
        token: Optional[str] = None,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
        api_path: Optional[str] = None,
        api_version: Optional[str] = None,
    ) -> AsyncDatabase:
        return AsyncDatabase(
            api_endpoint=api_endpoint or self._astra_db.api_endpoint,
            token=token or self._astra_db.token,
            namespace=namespace or self._astra_db.namespace,
            caller_name=caller_name or self._astra_db.caller_name,
            caller_version=caller_version or self._astra_db.caller_version,
            api_path=api_path or self._astra_db.api_path,
            api_version=api_version or self._astra_db.api_version,
        )

    def to_sync(
        self,
        *,
        api_endpoint: Optional[str] = None,
        token: Optional[str] = None,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
        api_path: Optional[str] = None,
        api_version: Optional[str] = None,
    ) -> Database:
        return Database(
            api_endpoint=api_endpoint or self._astra_db.api_endpoint,
            token=token or self._astra_db.token,
            namespace=namespace or self._astra_db.namespace,
            caller_name=caller_name or self._astra_db.caller_name,
            caller_version=caller_version or self._astra_db.caller_version,
            api_path=api_path or self._astra_db.api_path,
            api_version=api_version or self._astra_db.api_version,
        )

    def set_caller(
        self,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> None:
        self._astra_db.set_caller(
            caller_name=caller_name,
            caller_version=caller_version,
        )

    async def get_collection(
        self, name: str, *, namespace: Optional[str] = None
    ) -> AsyncCollection:
        # lazy importing here against circular-import error
        from astrapy.idiomatic.collection import AsyncCollection

        _namespace = namespace or self._astra_db.namespace
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
        check_exists: Optional[bool] = None,
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

        if check_exists is None:
            _check_exists = True
        else:
            _check_exists = check_exists
        existing_names: List[str]
        if _check_exists:
            existing_names = await self.list_collection_names(namespace=namespace)
        else:
            existing_names = []
        if name in existing_names:
            raise ValueError(f"CollectionInvalid: collection {name} already exists")

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

    async def drop_collection(
        self, name_or_collection: Union[str, AsyncCollection]
    ) -> Dict[str, Any]:
        # lazy importing here against circular-import error
        from astrapy.idiomatic.collection import AsyncCollection

        if isinstance(name_or_collection, AsyncCollection):
            _namespace = name_or_collection.namespace
            _name = name_or_collection._astra_db_collection.collection_name
            dc_response = await self._astra_db.copy(
                namespace=_namespace
            ).delete_collection(_name)
            return dc_response.get("status", {})  # type: ignore[no-any-return]
        else:
            dc_response = await self._astra_db.delete_collection(name_or_collection)
            return dc_response.get("status", {})  # type: ignore[no-any-return]

    async def list_collections(
        self,
        *,
        namespace: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        _client: AsyncAstraDB
        if namespace:
            _client = self._astra_db.copy(namespace=namespace)
        else:
            _client = self._astra_db
        gc_response = await _client.get_collections(options={"explain": True})
        if "collections" not in gc_response.get("status", {}):
            raise ValueError(
                "Could not complete a get_collections operation. "
                f"(gotten '${json.dumps(gc_response)}')"
            )
        else:
            # we know this is a list of dicts which need a little adjusting
            return [
                _recast_api_collection_dict(col_dict)
                for col_dict in gc_response["status"]["collections"]
            ]

    async def list_collection_names(
        self,
        *,
        namespace: Optional[str] = None,
    ) -> List[str]:
        gc_response = await self._astra_db.copy(namespace=namespace).get_collections()
        if "collections" not in gc_response.get("status", {}):
            raise ValueError(
                "Could not complete a get_collections operation. "
                f"(gotten '${json.dumps(gc_response)}')"
            )
        else:
            # we know this is a list of strings
            return gc_response["status"]["collections"]  # type: ignore[no-any-return]
