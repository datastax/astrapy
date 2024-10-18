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

import logging
from typing import Any

from astrapy.database import AsyncDatabase, Database
from astrapy.exceptions import (
    DataAPIFaultyResponseException,
)
from astrapy.info import TableIndexDefinition, TableVectorIndexDefinition
from astrapy.settings.defaults import DEFAULT_DATA_API_AUTH_HEADER
from astrapy.utils.api_commander import APICommander
from astrapy.utils.api_options import FullAPIOptions

logger = logging.getLogger(__name__)


class Table:
    """
    TODO
    A Data API collection, the main object to interact with the Data API,
    especially for DDL operations.
    This class has a synchronous interface.

    This class is not meant for direct instantiation by the user, rather
    it is obtained by invoking methods such as `get_collection` of Database,
    wherefrom the Collection inherits its API options such as authentication
    token and API endpoint.

    Args:
        database: a Database object, instantiated earlier. This represents
            the database the collection belongs to.
        name: the collection name. This parameter should match an existing
            collection on the database.
        keyspace: this is the keyspace to which the collection belongs.
            If nothing is specified, the database's working keyspace is used.
        api_options: a complete specification of the API Options for this instance.

    Examples:
        >>> from astrapy import DataAPIClient, Collection
        >>> my_client = astrapy.DataAPIClient("AstraCS:...")
        >>> my_db = my_client.get_database(
        ...    "https://01234567-....apps.astra.datastax.com"
        ... )
        >>> my_coll_1 = Collection(database=my_db, name="my_collection")
        >>> my_coll_2 = my_db.create_collection(
        ...     "my_v_collection",
        ...     dimension=3,
        ...     metric="cosine",
        ... )
        >>> my_coll_3a = my_db.get_collection("my_already_existing_collection")
        >>> my_coll_3b = my_db.my_already_existing_collection
        >>> my_coll_3c = my_db["my_already_existing_collection"]

    Note:
        creating an instance of Collection does not trigger actual creation
        of the collection on the database. The latter should have been created
        beforehand, e.g. through the `create_collection` method of a Database.
    """

    def __init__(
        self,
        *,
        database: Database,
        name: str,
        keyspace: str | None,
        api_options: FullAPIOptions,
    ) -> None:
        self.api_options = api_options
        self._name = name
        _keyspace = keyspace if keyspace is not None else database.keyspace

        if _keyspace is None:
            raise ValueError("Attempted to create Table with 'keyspace' unset.")

        self._database = database._copy(keyspace=_keyspace)
        self._commander_headers = {
            **{DEFAULT_DATA_API_AUTH_HEADER: self.api_options.token.get_token()},
            **self.api_options.embedding_api_key.get_headers(),
            **self.api_options.database_additional_headers,
        }
        self._api_commander = self._get_api_commander()

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}(name="{self.name}", '
            f'keyspace="{self.keyspace}", database={self.database}, '
            f"api_options={self.api_options})"
        )

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Table):
            return all(
                [
                    self._name == other._name,
                    self._database == other._database,
                    self.api_options == other.api_options,
                ]
            )
        else:
            return False

    def _get_api_commander(self) -> APICommander:
        """Instantiate a new APICommander based on the properties of this class."""

        if self._database.keyspace is None:
            raise ValueError(
                "No keyspace specified. Table requires a keyspace to "
                "be set, e.g. through the `keyspace` constructor parameter."
            )

        base_path_components = [
            comp
            for comp in (
                ncomp.strip("/")
                for ncomp in (
                    self._database.api_options.data_api_url_options.api_path,
                    self._database.api_options.data_api_url_options.api_version,
                    self._database.keyspace,
                    self._name,
                )
                if ncomp is not None
            )
            if comp != ""
        ]
        base_path = f"/{'/'.join(base_path_components)}"
        api_commander = APICommander(
            api_endpoint=self._database.api_endpoint,
            path=base_path,
            headers=self._commander_headers,
            callers=self.api_options.callers,
            redacted_header_names=self.api_options.redacted_header_names,
        )
        return api_commander

    @property
    def database(self) -> Database:
        """
        a Database object, the database this table belongs to.

        Example:
            >>> my_table.database.name
            'the_db'
        """

        return self._database

    @property
    def keyspace(self) -> str:
        """
        The keyspace this table is in.

        Example:
            >>> my_table.keyspace
            'default_keyspace'
        """

        _keyspace = self.database.keyspace
        if _keyspace is None:
            raise ValueError("The table's DB is set with keyspace=None")
        return _keyspace

    @property
    def name(self) -> str:
        """
        The name of this table.

        Example:
            >>> my_table.name
            'my_table'
        """

        return self._name

    def _create_generic_index(
        self,
        i_name: str,
        ci_definition: dict[str, Any],
        ci_command: str,
        if_not_exists: bool | None,
        schema_operation_timeout_ms: int | None,
        max_time_ms: int | None,
    ) -> None:
        _if_not_exists = False if if_not_exists is None else if_not_exists
        ci_options = {"ifNotExists": _if_not_exists}
        _schema_operation_timeout_ms = (
            schema_operation_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.schema_operation_timeout_ms
        )
        ci_payload = {
            ci_command: {
                "name": i_name,
                "definition": ci_definition,
                "options": ci_options,
            }
        }
        logger.info(f"{ci_command}('{i_name}')")
        ci_response = self._api_commander.request(
            payload=ci_payload,
            timeout_info=_schema_operation_timeout_ms,
        )
        if ci_response.get("status") != {"ok": 1}:
            raise DataAPIFaultyResponseException(
                text=f"Faulty response from {ci_command} API command.",
                raw_response=ci_response,
            )
        logger.info(f"finished {ci_command}('{i_name}')")

    def create_index(
        self,
        name: str,
        *,
        definition: TableIndexDefinition | dict[str, Any],
        if_not_exists: bool | None = None,
        schema_operation_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> None:
        """
        Creates an index on a non-vector column of the table.

        This is a blocking operation: the method returns once the index
        is created and ready to use.

        For creation of a vector index, see method `create_vector_index` instead.

        Args:
            name: the name of the index.
            definition: a complete definition for the index. This can be an instance
                of `TableIndexDefinition` or an equivalent (nested) dictionary,
                in which case it will be parsed into a `TableIndexDefinition`.
                See the `astrapy.info.TableIndexDefinition` class for more details.
            if_not_exists: if set to True, the command will succeed even if an index
                with the specified name already exists (in which case no actual
                index creation takes place on the database). Defaults to False,
                i.e. an error is raised by the API in case of index-name collision.
            schema_operation_timeout_ms: a timeout, in milliseconds, for the
                createIndex HTTP request.
            max_time_ms: an alias for `schema_operation_timeout_ms`.

        Example:
            TODO
            >>> table_def = (
            ...     TableDefinition.zero()
            ...     .add_column("id", "text")
            ...     .add_column("name", "text")
            ...     .add_partition_by(["id"])
            ... )
            ...
            >>> my_table = my_db.create_table("my_table", definition=table_def)
        """

        ci_definition: dict[str, Any] = TableIndexDefinition.coerce(
            definition
        ).as_dict()
        ci_command = "createIndex"
        return self._create_generic_index(
            i_name=name,
            ci_definition=ci_definition,
            ci_command=ci_command,
            if_not_exists=if_not_exists,
            schema_operation_timeout_ms=schema_operation_timeout_ms,
            max_time_ms=max_time_ms,
        )

    def create_vector_index(
        self,
        name: str,
        *,
        definition: TableVectorIndexDefinition | dict[str, Any],
        if_not_exists: bool | None = None,
        schema_operation_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> None:
        """
        Creates a vector index on a vector column of the table, enabling vector
        similarity search operations on it.

        This is a blocking operation: the method returns once the index
        is created and ready to use.

        For creation of a non-vector index, see method `create_index` instead.

        Args:
            name: the name of the index.
            definition: a complete definition for the index. This can be an instance
                of `TableVectorIndexDefinition` or an equivalent (nested) dictionary,
                in which case it will be parsed into a `TableVectorIndexDefinition`.
                See the `astrapy.info.TableVectorIndexDefinition` class for more details.
            if_not_exists: if set to True, the command will succeed even if an index
                with the specified name already exists (in which case no actual
                index creation takes place on the database). Defaults to False,
                i.e. an error is raised by the API in case of index-name collision.
            schema_operation_timeout_ms: a timeout, in milliseconds, for the
                createVectorIndex HTTP request.
            max_time_ms: an alias for `schema_operation_timeout_ms`.

        Example:
            TODO
            >>> table_def = (
            ...     TableDefinition.zero()
            ...     .add_column("id", "text")
            ...     .add_column("name", "text")
            ...     .add_partition_by(["id"])
            ... )
            ...
            >>> my_table = my_db.create_table("my_table", definition=table_def)
        """

        ci_definition: dict[str, Any] = TableVectorIndexDefinition.coerce(
            definition
        ).as_dict()
        ci_command = "createVectorIndex"
        return self._create_generic_index(
            i_name=name,
            ci_definition=ci_definition,
            ci_command=ci_command,
            if_not_exists=if_not_exists,
            schema_operation_timeout_ms=schema_operation_timeout_ms,
            max_time_ms=max_time_ms,
        )

    def drop_index(
        self,
        name: str,
        *,
        schema_operation_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> None:
        """
        Drops (deletes) an index (of any kind) from the table.

        This is a blocking operation: the method returns once the index
        is created and ready to use. If the index does not exist already,
        nothing changes on the database and this method succeeds.

        Args:
            name: the name of the index.
            schema_operation_timeout_ms: a timeout, in milliseconds, for the
                dropIndex HTTP request.
            max_time_ms: an alias for `schema_operation_timeout_ms`.

        Example:
            TODO
            >>> table_def = (
            ...     TableDefinition.zero()
            ...     .add_column("id", "text")
            ...     .add_column("name", "text")
            ...     .add_partition_by(["id"])
            ... )
            ...
            >>> my_table = my_db.create_table("my_table", definition=table_def)
        """

        _schema_operation_timeout_ms = (
            schema_operation_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.schema_operation_timeout_ms
        )
        di_payload = {"dropIndex": {"indexName": name}}
        logger.info(f"dropIndex('{name}')")
        di_response = self._api_commander.request(
            payload=di_payload,
            timeout_info=_schema_operation_timeout_ms,
        )
        if di_response.get("status") != {"ok": 1}:
            raise DataAPIFaultyResponseException(
                text="Faulty response from dropIndex API command.",
                raw_response=di_response,
            )
        logger.info(f"finished dropIndex('{name}')")


class AsyncTable:
    """
    TODO
    A Data API collection, the main object to interact with the Data API,
    especially for DDL operations.
    This class has a synchronous interface.

    This class is not meant for direct instantiation by the user, rather
    it is obtained by invoking methods such as `get_collection` of Database,
    wherefrom the Collection inherits its API options such as authentication
    token and API endpoint.

    Args:
        database: a Database object, instantiated earlier. This represents
            the database the collection belongs to.
        name: the collection name. This parameter should match an existing
            collection on the database.
        keyspace: this is the keyspace to which the collection belongs.
            If nothing is specified, the database's working keyspace is used.
        api_options: a complete specification of the API Options for this instance.

    Examples:
        >>> from astrapy import DataAPIClient, Collection
        >>> my_client = astrapy.DataAPIClient("AstraCS:...")
        >>> my_db = my_client.get_database(
        ...    "https://01234567-....apps.astra.datastax.com"
        ... )
        >>> my_coll_1 = Collection(database=my_db, name="my_collection")
        >>> my_coll_2 = my_db.create_collection(
        ...     "my_v_collection",
        ...     dimension=3,
        ...     metric="cosine",
        ... )
        >>> my_coll_3a = my_db.get_collection("my_already_existing_collection")
        >>> my_coll_3b = my_db.my_already_existing_collection
        >>> my_coll_3c = my_db["my_already_existing_collection"]

    Note:
        creating an instance of Collection does not trigger actual creation
        of the collection on the database. The latter should have been created
        beforehand, e.g. through the `create_collection` method of a Database.
    """

    def __init__(
        self,
        *,
        database: AsyncDatabase,
        name: str,
        keyspace: str | None,
        api_options: FullAPIOptions,
    ) -> None:
        self.api_options = api_options
        self._name = name
        _keyspace = keyspace if keyspace is not None else database.keyspace

        if _keyspace is None:
            raise ValueError("Attempted to create Table with 'keyspace' unset.")

        self._database = database._copy(keyspace=_keyspace)
        self._commander_headers = {
            **{DEFAULT_DATA_API_AUTH_HEADER: self.api_options.token.get_token()},
            **self.api_options.embedding_api_key.get_headers(),
            **self.api_options.database_additional_headers,
        }
        self._api_commander = self._get_api_commander()

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}(name="{self.name}", '
            f'keyspace="{self.keyspace}", database={self.database}, '
            f"api_options={self.api_options})"
        )

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, AsyncTable):
            return all(
                [
                    self._name == other._name,
                    self._database == other._database,
                    self.api_options == other.api_options,
                ]
            )
        else:
            return False

    def _get_api_commander(self) -> APICommander:
        """Instantiate a new APICommander based on the properties of this class."""

        if self._database.keyspace is None:
            raise ValueError(
                "No keyspace specified. AsyncTable requires a keyspace to "
                "be set, e.g. through the `keyspace` constructor parameter."
            )

        base_path_components = [
            comp
            for comp in (
                ncomp.strip("/")
                for ncomp in (
                    self._database.api_options.data_api_url_options.api_path,
                    self._database.api_options.data_api_url_options.api_version,
                    self._database.keyspace,
                    self._name,
                )
                if ncomp is not None
            )
            if comp != ""
        ]
        base_path = f"/{'/'.join(base_path_components)}"
        api_commander = APICommander(
            api_endpoint=self._database.api_endpoint,
            path=base_path,
            headers=self._commander_headers,
            callers=self.api_options.callers,
            redacted_header_names=self.api_options.redacted_header_names,
        )
        return api_commander

    @property
    def database(self) -> AsyncDatabase:
        """
        a Database object, the database this table belongs to.

        Example:
            >>> my_async_table.database.name
            'the_db'
        """

        return self._database

    @property
    def keyspace(self) -> str:
        """
        The keyspace this table is in.

        Example:
            >>> my_async_table.keyspace
            'default_keyspace'
        """

        _keyspace = self.database.keyspace
        if _keyspace is None:
            raise ValueError("The table's DB is set with keyspace=None")
        return _keyspace

    @property
    def name(self) -> str:
        """
        The name of this table.

        Example:
            >>> my_async_table.name
            'my_table'
        """

        return self._name

    async def _create_generic_index(
        self,
        i_name: str,
        ci_definition: dict[str, Any],
        ci_command: str,
        if_not_exists: bool | None,
        schema_operation_timeout_ms: int | None,
        max_time_ms: int | None,
    ) -> None:
        _if_not_exists = False if if_not_exists is None else if_not_exists
        ci_options = {"ifNotExists": _if_not_exists}
        _schema_operation_timeout_ms = (
            schema_operation_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.schema_operation_timeout_ms
        )
        ci_payload = {
            ci_command: {
                "name": i_name,
                "definition": ci_definition,
                "options": ci_options,
            }
        }
        logger.info(f"{ci_command}('{i_name}')")
        ci_response = await self._api_commander.async_request(
            payload=ci_payload,
            timeout_info=_schema_operation_timeout_ms,
        )
        if ci_response.get("status") != {"ok": 1}:
            raise DataAPIFaultyResponseException(
                text=f"Faulty response from {ci_command} API command.",
                raw_response=ci_response,
            )
        logger.info(f"finished {ci_command}('{i_name}')")

    async def create_index(
        self,
        name: str,
        *,
        definition: TableIndexDefinition | dict[str, Any],
        if_not_exists: bool | None = None,
        schema_operation_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> None:
        """
        Creates an index on a non-vector column of the table.

        This is a blocking operation: the method returns once the index
        is created and ready to use.

        For creation of a vector index, see method `create_vector_index` instead.

        Args:
            name: the name of the index.
            definition: a complete definition for the index. This can be an instance
                of `TableIndexDefinition` or an equivalent (nested) dictionary,
                in which case it will be parsed into a `TableIndexDefinition`.
                See the `astrapy.info.TableIndexDefinition` class for more details.
            if_not_exists: if set to True, the command will succeed even if an index
                with the specified name already exists (in which case no actual
                index creation takes place on the database). Defaults to False,
                i.e. an error is raised by the API in case of index-name collision.
            schema_operation_timeout_ms: a timeout, in milliseconds, for the
                createIndex HTTP request.
            max_time_ms: an alias for `schema_operation_timeout_ms`.

        Example:
            TODO
            >>> table_def = (
            ...     TableDefinition.zero()
            ...     .add_column("id", "text")
            ...     .add_column("name", "text")
            ...     .add_partition_by(["id"])
            ... )
            ...
            >>> my_table = my_db.create_table("my_table", definition=table_def)
        """

        ci_definition: dict[str, Any] = TableIndexDefinition.coerce(
            definition
        ).as_dict()
        ci_command = "createIndex"
        return await self._create_generic_index(
            i_name=name,
            ci_definition=ci_definition,
            ci_command=ci_command,
            if_not_exists=if_not_exists,
            schema_operation_timeout_ms=schema_operation_timeout_ms,
            max_time_ms=max_time_ms,
        )

    async def create_vector_index(
        self,
        name: str,
        *,
        definition: TableVectorIndexDefinition | dict[str, Any],
        if_not_exists: bool | None = None,
        schema_operation_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> None:
        """
        Creates a vector index on a vector column of the table, enabling vector
        similarity search operations on it.

        This is a blocking operation: the method returns once the index
        is created and ready to use.

        For creation of a non-vector index, see method `create_index` instead.

        Args:
            name: the name of the index.
            definition: a complete definition for the index. This can be an instance
                of `TableVectorIndexDefinition` or an equivalent (nested) dictionary,
                in which case it will be parsed into a `TableVectorIndexDefinition`.
                See the `astrapy.info.TableVectorIndexDefinition` class for more details.
            if_not_exists: if set to True, the command will succeed even if an index
                with the specified name already exists (in which case no actual
                index creation takes place on the database). Defaults to False,
                i.e. an error is raised by the API in case of index-name collision.
            schema_operation_timeout_ms: a timeout, in milliseconds, for the
                createVectorIndex HTTP request.
            max_time_ms: an alias for `schema_operation_timeout_ms`.

        Example:
            TODO
            >>> table_def = (
            ...     TableDefinition.zero()
            ...     .add_column("id", "text")
            ...     .add_column("name", "text")
            ...     .add_partition_by(["id"])
            ... )
            ...
            >>> my_table = my_db.create_table("my_table", definition=table_def)
        """

        ci_definition: dict[str, Any] = TableVectorIndexDefinition.coerce(
            definition
        ).as_dict()
        ci_command = "createVectorIndex"
        return await self._create_generic_index(
            i_name=name,
            ci_definition=ci_definition,
            ci_command=ci_command,
            if_not_exists=if_not_exists,
            schema_operation_timeout_ms=schema_operation_timeout_ms,
            max_time_ms=max_time_ms,
        )

    async def drop_index(
        self,
        name: str,
        *,
        schema_operation_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> None:
        """
        Drops (deletes) an index (of any kind) from the table.

        This is a blocking operation: the method returns once the index
        is created and ready to use. If the index does not exist already,
        nothing changes on the database and this method succeeds.

        Args:
            name: the name of the index.
            schema_operation_timeout_ms: a timeout, in milliseconds, for the
                dropIndex HTTP request.
            max_time_ms: an alias for `schema_operation_timeout_ms`.

        Example:
            TODO
            >>> table_def = (
            ...     TableDefinition.zero()
            ...     .add_column("id", "text")
            ...     .add_column("name", "text")
            ...     .add_partition_by(["id"])
            ... )
            ...
            >>> my_table = my_db.create_table("my_table", definition=table_def)
        """

        _schema_operation_timeout_ms = (
            schema_operation_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.schema_operation_timeout_ms
        )
        di_payload = {"dropIndex": {"indexName": name}}
        logger.info(f"dropIndex('{name}')")
        di_response = await self._api_commander.async_request(
            payload=di_payload,
            timeout_info=_schema_operation_timeout_ms,
        )
        if di_response.get("status") != {"ok": 1}:
            raise DataAPIFaultyResponseException(
                text="Faulty response from dropIndex API command.",
                raw_response=di_response,
            )
        logger.info(f"finished dropIndex('{name}')")
