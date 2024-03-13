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

from astrapy.core.db import AstraDB, AsyncAstraDB
from astrapy.exceptions import (
    CollectionAlreadyExistsException,
    recast_method_sync,
    recast_method_async,
)
from astrapy.cursors import AsyncCommandCursor, CommandCursor
from astrapy.info import DatabaseInfo, get_database_info

if TYPE_CHECKING:
    from astrapy.collection import AsyncCollection, Collection


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
    """
    A Data API database. This is the entry-point object for doing database-level
    DML, such as creating/deleting collections, and for obtaining Collection
    objects themselves. This class has a synchronous interface.

    A Database comes with an "API Endpoint", which implies a Database object
    instance reaches a specific region (relevant point in case of multi-region
    databases).

    Args:
        api_endpoint: the full "API Endpoint" string used to reach the Data API.
            Example: "https://<database_id>-<region>.apps.astra.datastax.com"
        token: an Access Token to the database. Example: "AstraCS:xyz..."
        namespace: this is the namespace all method calls will target, unless
            one is explicitly specified in the call. If no namespace is supplied
            when creating a Database, the name "default_namespace" is set.
        caller_name: name of the application, or framework, on behalf of which
            the Data API calls are performed. This ends up in the request user-agent.
        caller_version: version of the caller.
        api_path: path to append to the API Endpoint. In typical usage, this
            should be left to its default of "/api/json".
        api_version: version specifier to append to the API path. In typical
            usage, this should be left to its default of "v1".

    Note:
        creating an instance of Database does not trigger actual creation
        of the database itself, which should exist beforehand.
    """

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
        self._database_info: Optional[DatabaseInfo] = None

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

    def _copy(
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

    def with_options(
        self,
        *,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> Database:
        """
        Create a clone of this database with some changed attributes.

        Args:
            namespace: this is the namespace all method calls will target, unless
                one is explicitly specified in the call. If no namespace is supplied
                when creating a Database, the name "default_namespace" is set.
            caller_name: name of the application, or framework, on behalf of which
                the Data API calls are performed. This ends up in the request user-agent.
            caller_version: version of the caller.

        Returns:
            a new Database instance.
        """

        return self._copy(
            namespace=namespace,
            caller_name=caller_name,
            caller_version=caller_version,
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
        """
        Create an AsyncDatabase from this one. Save for the arguments
        explicitly provided as overrides, everything else is kept identical
        to this database in the copy.

        Args:
            api_endpoint: the full "API Endpoint" string used to reach the Data API.
                Example: "https://<database_id>-<region>.apps.astra.datastax.com"
            token: an Access Token to the database. Example: "AstraCS:xyz..."
            namespace: this is the namespace all method calls will target, unless
                one is explicitly specified in the call. If no namespace is supplied
                when creating a Database, the name "default_namespace" is set.
            caller_name: name of the application, or framework, on behalf of which
                the Data API calls are performed. This ends up in the request user-agent.
            caller_version: version of the caller.
            api_path: path to append to the API Endpoint. In typical usage, this
                should be left to its default of "/api/json".
            api_version: version specifier to append to the API path. In typical
                usage, this should be left to its default of "v1".

        Returns:
            the new copy, an AsyncDatabase instance.
        """

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
        """
        Set a new identity for the application/framework on behalf of which
        the Data API calls are performed (the "caller").

        Args:
            caller_name: name of the application, or framework, on behalf of which
                the Data API calls are performed. This ends up in the request user-agent.
            caller_version: version of the caller.
        """
        self._astra_db.set_caller(
            caller_name=caller_name,
            caller_version=caller_version,
        )

    @property
    def info(self) -> DatabaseInfo:
        """
        Additional information on the database as a DatabaseInfo instance.

        On accessing this property the first time, a call to the DevOps API
        is made; it is then cached for subsequent access.
        """
        if self._database_info is None:
            self._database_info = get_database_info(
                self._astra_db.api_endpoint,
                token=self._astra_db.token,
                namespace=self.namespace,
            )
        return self._database_info

    @property
    def id(self) -> Optional[str]:
        """
        The ID of this database.
        """

        return self.info.id

    @property
    def name(self) -> Optional[str]:
        """
        The name of this database. Note that this bears no unicity guarantees.
        """

        return self.info.name

    @property
    def namespace(self) -> str:
        """
        The namespace this database uses as target for all commands when
        no method-call-specific namespace is specified.
        """

        return self._astra_db.namespace

    def get_collection(
        self, name: str, *, namespace: Optional[str] = None
    ) -> Collection:
        """
        Spawn a Collection object instance representing a collection
        on this database.

        Creating a Collection instance does not have any effect on the
        actual state of the database: in other words, for the created
        Collection instance to be used meaningfully, the collection
        must exist already (for instance, it should have been created
        previously by calling the `create_collection` method).

        Args:
            name: the name of the collection.
            namespace: the namespace containing the collection. If no namespace
                is specified, the general setting for this database is used.

        Returns:
            a Collection instance, representing the desired collection
                (but without any form of validation).
        """

        # lazy importing here against circular-import error
        from astrapy.collection import Collection

        _namespace = namespace or self._astra_db.namespace
        return Collection(self, name, namespace=_namespace)

    @recast_method_sync
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
        """
        Creates a collection on the database and return the Collection
        instance that represents it.

        This is a blocking operation: the method returns when the collection
        is ready to be used. As opposed to the `get_collection` instance,
        this method triggers causes the collection to be actually created on DB.

        Args:
            name: the name of the collection.
            namespace: the namespace where the collection is to be created.
                If not specified, the general setting for this database is used.
            dimension: for vector collections, the dimension of the vectors
                (i.e. the number of their components). If not specified, a
                a non-vector collection is created.
            metric: the metric used for similarity searches.
                Allowed values are "dot_product", "euclidean" and "cosine"
                (see the VectorMetric object).
            indexing: optional specification of the indexing options for
                the collection, in the form of a dictionary such as
                    {"deny": [...]}
                or
                    {"allow": [...]}
            additional_options: any further set of key-value pairs that will
                be added to the "options" part of the payload when sending
                the Data API command to create a collection.
            check_exists: whether to run an existence check for the collection
                name before attempting to create the collection:
                If check_exists is True, an error is raised when creating
                an existing collection.
                If it is False, the creation is attempted. In this case, for
                preexisting collections, the command will succeed or fail
                depending on whether the options match or not.

        Returns:
            a (synchronous) Collection instance, representing the
            newly-created collection.
        """

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
        driver_db = self._astra_db.copy(namespace=namespace)
        if name in existing_names:
            raise CollectionAlreadyExistsException(
                text=f"CollectionInvalid: collection {name} already exists",
                namespace=driver_db.namespace,
                collection_name=name,
            )

        driver_db.create_collection(
            name,
            options=_options,
            dimension=dimension,
            metric=metric,
        )
        return self.get_collection(name, namespace=namespace)

    def drop_collection(
        self, name_or_collection: Union[str, Collection]
    ) -> Dict[str, Any]:
        """
        Drop a collection from the database, along with all documents therein.

        Args:
            name_or_collection: either the name of a collection or
            a Collection instance.

        Returns:
            a dictionary in the form {"ok": 1} if the command succeeds.

        Note:
            when providing a collection name, it is assumed that the collection
            is to be found in the namespace set at database instance level.
        """

        # lazy importing here against circular-import error
        from astrapy.collection import Collection

        if isinstance(name_or_collection, Collection):
            _namespace = name_or_collection.namespace
            _name: str = name_or_collection.name
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
    ) -> CommandCursor[Dict[str, Any]]:
        """
        List all collections in a given namespace for this database.

        Args:
            namespace: the namespace to be inspected. If not specified,
            the general setting for this database is assumed.

        Returns:
            a `CommandCursor` to iterate over dictionaries, each
            expressing a collection as a set of key-value pairs
            matching the arguments of a `create_collection` call.
        """

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
            return CommandCursor(
                address=self._astra_db.base_url,
                items=[
                    _recast_api_collection_dict(col_dict)
                    for col_dict in gc_response["status"]["collections"]
                ],
            )

    def list_collection_names(
        self,
        *,
        namespace: Optional[str] = None,
    ) -> List[str]:
        """
        List the names of all collections in a given namespace of this database.

        Args:
            namespace: the namespace to be inspected. If not specified,
            the general setting for this database is assumed.

        Returns:
            a list of the collection names as strings, in no particular order.
        """

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

    def command(
        self,
        body: Dict[str, Any],
        *,
        namespace: Optional[str] = None,
        collection_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Send a POST request to the Data API for this database with
        an arbitrary, caller-provided payload.

        Args:
            body: a JSON-serializable dictionary, the payload of the request.
        Args:
            namespace: the namespace to use. Requests always target a namespace:
            if not specified, the general setting for this database is assumed.
        collection_name: if provided, the collection name is appended at the end
            of the endpoint. In this way, this method allows collection-level
            arbitrary POST requests as well.

        Returns:
            a dictionary with the response of the HTTP request.
        """

        if namespace:
            _client = self._astra_db.copy(namespace=namespace)
        else:
            _client = self._astra_db
        if collection_name:
            _collection = _client.collection(collection_name)
            return _collection.post_raw_request(body=body)
        else:
            return _client.post_raw_request(body=body)


class AsyncDatabase:
    """
    A Data API database. This is the entry-point object for doing database-level
    DML, such as creating/deleting collections, and for obtaining Collection
    objects themselves. This class has an asynchronous interface.

    A Database comes with an "API Endpoint", which implies a Database object
    instance reaches a specific region (relevant point in case of multi-region
    databases).

    Args:
        api_endpoint: the full "API Endpoint" string used to reach the Data API.
            Example: "https://<database_id>-<region>.apps.astra.datastax.com"
        token: an Access Token to the database. Example: "AstraCS:xyz..."
        namespace: this is the namespace all method calls will target, unless
            one is explicitly specified in the call. If no namespace is supplied
            when creating a Database, the name "default_namespace" is set.
        caller_name: name of the application, or framework, on behalf of which
            the Data API calls are performed. This ends up in the request user-agent.
        caller_version: version of the caller.
        api_path: path to append to the API Endpoint. In typical usage, this
            should be left to its default of "/api/json".
        api_version: version specifier to append to the API path. In typical
            usage, this should be left to its default of "v1".

    Note:
        creating an instance of Database does not trigger actual creation
        of the database itself, which should exist beforehand.
    """

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
        self._database_info: Optional[DatabaseInfo] = None

    def __getattr__(self, collection_name: str) -> AsyncCollection:
        return self.to_sync().get_collection(name=collection_name).to_async()

    def __getitem__(self, collection_name: str) -> AsyncCollection:
        return self.to_sync().get_collection(name=collection_name).to_async()

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

    def _copy(
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

    def with_options(
        self,
        *,
        namespace: Optional[str] = None,
        caller_name: Optional[str] = None,
        caller_version: Optional[str] = None,
    ) -> AsyncDatabase:
        """
        Create a clone of this database with some changed attributes.

        Args:
            namespace: this is the namespace all method calls will target, unless
                one is explicitly specified in the call. If no namespace is supplied
                when creating a Database, the name "default_namespace" is set.
            caller_name: name of the application, or framework, on behalf of which
                the Data API calls are performed. This ends up in the request user-agent.
            caller_version: version of the caller.

        Returns:
            a new AsyncDatabase instance.
        """

        return self._copy(
            namespace=namespace,
            caller_name=caller_name,
            caller_version=caller_version,
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
        """
        Create a (synchronous) Database from this one. Save for the arguments
        explicitly provided as overrides, everything else is kept identical
        to this database in the copy.

        Args:
            api_endpoint: the full "API Endpoint" string used to reach the Data API.
                Example: "https://<database_id>-<region>.apps.astra.datastax.com"
            token: an Access Token to the database. Example: "AstraCS:xyz..."
            namespace: this is the namespace all method calls will target, unless
                one is explicitly specified in the call. If no namespace is supplied
                when creating a Database, the name "default_namespace" is set.
            caller_name: name of the application, or framework, on behalf of which
                the Data API calls are performed. This ends up in the request user-agent.
            caller_version: version of the caller.
            api_path: path to append to the API Endpoint. In typical usage, this
                should be left to its default of "/api/json".
            api_version: version specifier to append to the API path. In typical
                usage, this should be left to its default of "v1".

        Returns:
            the new copy, a Database instance.
        """

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
        """
        Set a new identity for the application/framework on behalf of which
        the Data API calls are performed (the "caller").

        Args:
            caller_name: name of the application, or framework, on behalf of which
                the Data API calls are performed. This ends up in the request user-agent.
            caller_version: version of the caller.
        """

        self._astra_db.set_caller(
            caller_name=caller_name,
            caller_version=caller_version,
        )

    @property
    def info(self) -> DatabaseInfo:
        """
        Additional information on the database as a DatabaseInfo instance.

        On accessing this property the first time, a call to the DevOps API
        is made; it is then cached for subsequent access.
        """

        if self._database_info is None:
            self._database_info = get_database_info(
                self._astra_db.api_endpoint,
                token=self._astra_db.token,
                namespace=self.namespace,
            )
        return self._database_info

    @property
    def id(self) -> Optional[str]:
        """
        The ID of this database.
        """

        return self.info.id

    @property
    def name(self) -> Optional[str]:
        """
        The name of this database. Note that this bears no unicity guarantees.
        """

        return self.info.name

    @property
    def namespace(self) -> str:
        """
        The namespace this database uses as target for all commands when
        no method-call-specific namespace is specified.
        """

        return self._astra_db.namespace

    async def get_collection(
        self, name: str, *, namespace: Optional[str] = None
    ) -> AsyncCollection:
        """
        Spawn an AsyncCollection object instance representing a collection
        on this database.

        Creating an AsyncCollection instance does not have any effect on the
        actual state of the database: in other words, for the created
        AsyncCollection instance to be used meaningfully, the collection
        must exist already (for instance, it should have been created
        previously by calling the `create_collection` method).

        Args:
            name: the name of the collection.
            namespace: the namespace containing the collection. If no namespace
                is specified, the setting for this database is used.

        Returns:
            an AsyncCollection instance, representing the desired collection
                (but without any form of validation).
        """

        # lazy importing here against circular-import error
        from astrapy.collection import AsyncCollection

        _namespace = namespace or self._astra_db.namespace
        return AsyncCollection(self, name, namespace=_namespace)

    @recast_method_async
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
        """
        Creates a collection on the database and return the AsyncCollection
        instance that represents it.

        This is a blocking operation: the method returns when the collection
        is ready to be used. As opposed to the `get_collection` instance,
        this method triggers causes the collection to be actually created on DB.

        Args:
            name: the name of the collection.
            namespace: the namespace where the collection is to be created.
                If not specified, the general setting for this database is used.
            dimension: for vector collections, the dimension of the vectors
                (i.e. the number of their components). If not specified, a
                a non-vector collection is created.
            metric: the metric used for similarity searches.
                Allowed values are "dot_product", "euclidean" and "cosine"
                (see the VectorMetric object).
            indexing: optional specification of the indexing options for
                the collection, in the form of a dictionary such as
                    {"deny": [...]}
                or
                    {"allow": [...]}
            additional_options: any further set of key-value pairs that will
                be added to the "options" part of the payload when sending
                the Data API command to create a collection.
            check_exists: whether to run an existence check for the collection
                name before attempting to create the collection:
                If check_exists is True, an error is raised when creating
                an existing collection.
                If it is False, the creation is attempted. In this case, for
                preexisting collections, the command will succeed or fail
                depending on whether the options match or not.

        Returns:
            an AsyncCollection instance, representing the newly-created collection.
        """

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
        driver_db = self._astra_db.copy(namespace=namespace)
        if name in existing_names:
            raise CollectionAlreadyExistsException(
                text=f"CollectionInvalid: collection {name} already exists",
                namespace=driver_db.namespace,
                collection_name=name,
            )

        await driver_db.create_collection(
            name,
            options=_options,
            dimension=dimension,
            metric=metric,
        )
        return await self.get_collection(name, namespace=namespace)

    async def drop_collection(
        self, name_or_collection: Union[str, AsyncCollection]
    ) -> Dict[str, Any]:
        """
        Drop a collection from the database, along with all documents therein.

        Args:
            name_or_collection: either the name of a collection or
            an AsyncCollection instance.

        Returns:
            a dictionary in the form {"ok": 1} if the command succeeds.

        Note:
            when providing a collection name, it is assumed that the collection
            is to be found in the namespace set at database instance level.
        """

        # lazy importing here against circular-import error
        from astrapy.collection import AsyncCollection

        if isinstance(name_or_collection, AsyncCollection):
            _namespace = name_or_collection.namespace
            _name = name_or_collection.name
            dc_response = await self._astra_db.copy(
                namespace=_namespace
            ).delete_collection(_name)
            return dc_response.get("status", {})  # type: ignore[no-any-return]
        else:
            dc_response = await self._astra_db.delete_collection(name_or_collection)
            return dc_response.get("status", {})  # type: ignore[no-any-return]

    def list_collections(
        self,
        *,
        namespace: Optional[str] = None,
    ) -> AsyncCommandCursor[Dict[str, Any]]:
        """
        List all collections in a given namespace for this database.

        Args:
            namespace: the namespace to be inspected. If not specified,
            the general setting for this database is assumed.

        Returns:
            an `AsyncCommandCursor` to iterate over dictionaries, each
            expressing a collection as a set of key-value pairs
            matching the arguments of a `create_collection` call.
        """

        _client: AsyncAstraDB
        if namespace:
            _client = self._astra_db.copy(namespace=namespace)
        else:
            _client = self._astra_db
        gc_response = _client.to_sync().get_collections(options={"explain": True})
        if "collections" not in gc_response.get("status", {}):
            raise ValueError(
                "Could not complete a get_collections operation. "
                f"(gotten '${json.dumps(gc_response)}')"
            )
        else:
            # we know this is a list of dicts which need a little adjusting
            return AsyncCommandCursor(
                address=self._astra_db.base_url,
                items=[
                    _recast_api_collection_dict(col_dict)
                    for col_dict in gc_response["status"]["collections"]
                ],
            )

    async def list_collection_names(
        self,
        *,
        namespace: Optional[str] = None,
    ) -> List[str]:
        """
        List the names of all collections in a given namespace of this database.

        Args:
            namespace: the namespace to be inspected. If not specified,
            the general setting for this database is assumed.

        Returns:
            a list of the collection names as strings, in no particular order.
        """

        gc_response = await self._astra_db.copy(namespace=namespace).get_collections()
        if "collections" not in gc_response.get("status", {}):
            raise ValueError(
                "Could not complete a get_collections operation. "
                f"(gotten '${json.dumps(gc_response)}')"
            )
        else:
            # we know this is a list of strings
            return gc_response["status"]["collections"]  # type: ignore[no-any-return]

    async def command(
        self,
        body: Dict[str, Any],
        *,
        namespace: Optional[str] = None,
        collection_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Send a POST request to the Data API for this database with
        an arbitrary, caller-provided payload.

        Args:
            body: a JSON-serializable dictionary, the payload of the request.
        Args:
            namespace: the namespace to use. Requests always target a namespace:
            if not specified, the general setting for this database is assumed.
        collection_name: if provided, the collection name is appended at the end
            of the endpoint. In this way, this method allows collection-level
            arbitrary POST requests as well.

        Returns:
            a dictionary with the response of the HTTP request.
        """

        if namespace:
            _client = self._astra_db.copy(namespace=namespace)
        else:
            _client = self._astra_db
        if collection_name:
            _collection = await _client.collection(collection_name)
            return await _collection.post_raw_request(body=body)
        else:
            return await _client.post_raw_request(body=body)
