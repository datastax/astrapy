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

from types import TracebackType
from typing import Any, Optional, Type, TypedDict
from astrapy.db import AstraDB, AsyncAstraDB
from astrapy.idiomatic.utils import unsupported


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
