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
from typing import TYPE_CHECKING, Any, Generic, Sequence

from astrapy.authentication import coerce_possible_embedding_headers_provider
from astrapy.constants import (
    ROW,
    CallerType,
    FilterType,
)
from astrapy.database import AsyncDatabase, Database
from astrapy.exceptions import (
    DataAPIFaultyResponseException,
    TableNotFoundException,
)
from astrapy.info import TableIndexDefinition, TableInfo, TableVectorIndexDefinition
from astrapy.results import DeleteResult, InsertOneResult
from astrapy.settings.defaults import DEFAULT_DATA_API_AUTH_HEADER
from astrapy.utils.api_commander import APICommander
from astrapy.utils.api_options import APIOptions, FullAPIOptions, TimeoutOptions
from astrapy.utils.unset import _UNSET, UnsetType

if TYPE_CHECKING:
    from astrapy.authentication import EmbeddingHeadersProvider
    from astrapy.info import TableDefinition


logger = logging.getLogger(__name__)


class Table(Generic[ROW]):
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

    def _copy(
        self: Table[ROW],
        *,
        database: Database | None = None,
        name: str | None = None,
        keyspace: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        callers: Sequence[CallerType] | UnsetType = _UNSET,
        request_timeout_ms: int | UnsetType = _UNSET,
        collection_max_time_ms: int | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> Table[ROW]:
        # a double override for the timeout aliasing
        resulting_api_options = (
            self.api_options.with_override(
                api_options,
            )
            .with_override(
                APIOptions(
                    callers=callers,
                    embedding_api_key=coerce_possible_embedding_headers_provider(
                        embedding_api_key
                    ),
                    timeout_options=TimeoutOptions(
                        request_timeout_ms=collection_max_time_ms,
                    ),
                )
            )
            .with_override(
                APIOptions(
                    timeout_options=TimeoutOptions(
                        request_timeout_ms=request_timeout_ms,
                    ),
                )
            )
        )
        return Table(
            database=database or self.database,
            name=name or self.name,
            keyspace=keyspace or self.keyspace,
            api_options=resulting_api_options,
        )

    def with_options(
        self: Table[ROW],
        *,
        name: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        callers: Sequence[CallerType] | UnsetType = _UNSET,
        request_timeout_ms: int | UnsetType = _UNSET,
        collection_max_time_ms: int | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> Table[ROW]:
        """
        TODO
        """
        return self._copy(
            name=name,
            embedding_api_key=embedding_api_key,
            callers=callers,
            request_timeout_ms=request_timeout_ms,
            collection_max_time_ms=collection_max_time_ms,
            api_options=api_options,
        )

    def to_async(
        self: Table[ROW],
        *,
        database: AsyncDatabase | None = None,
        name: str | None = None,
        keyspace: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        callers: Sequence[CallerType] | UnsetType = _UNSET,
        request_timeout_ms: int | UnsetType = _UNSET,
        collection_max_time_ms: int | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncTable[ROW]:
        """
        TODO
        """

        # a double override for the timeout aliasing
        resulting_api_options = (
            self.api_options.with_override(
                api_options,
            )
            .with_override(
                APIOptions(
                    callers=callers,
                    embedding_api_key=coerce_possible_embedding_headers_provider(
                        embedding_api_key
                    ),
                    timeout_options=TimeoutOptions(
                        request_timeout_ms=collection_max_time_ms,
                    ),
                )
            )
            .with_override(
                APIOptions(
                    timeout_options=TimeoutOptions(
                        request_timeout_ms=request_timeout_ms,
                    ),
                )
            )
        )
        return AsyncTable(
            database=database or self.database.to_async(),
            name=name or self.name,
            keyspace=keyspace or self.keyspace,
            api_options=resulting_api_options,
        )

    def definition(
        self,
        *,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> TableDefinition:
        """
        TODO
        """

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )
        logger.info(f"getting tables in search of '{self.name}'")
        self_descriptors = [
            table_desc
            for table_desc in self.database.list_tables(max_time_ms=_request_timeout_ms)
            if table_desc.name == self.name
        ]
        logger.info(f"finished getting tables in search of '{self.name}'")
        if self_descriptors:
            return self_descriptors[0].definition
        else:
            raise TableNotFoundException(
                text=f"Table {self.keyspace}.{self.name} not found.",
                keyspace=self.keyspace,
                table_name=self.name,
            )

    def info(
        self,
        *,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> TableInfo:
        """
        TODO
        """

        return TableInfo(
            database_info=self.database.info(
                request_timeout_ms=request_timeout_ms,
                max_time_ms=max_time_ms,
            ),
            keyspace=self.keyspace,
            name=self.name,
            full_name=self.full_name,
        )

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

    @property
    def full_name(self) -> str:
        """
        TODO
        """

        return f"{self.keyspace}.{self.name}"

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
            timeout_ms=_schema_operation_timeout_ms,
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
            timeout_ms=_schema_operation_timeout_ms,
        )
        if di_response.get("status") != {"ok": 1}:
            raise DataAPIFaultyResponseException(
                text="Faulty response from dropIndex API command.",
                raw_response=di_response,
            )
        logger.info(f"finished dropIndex('{name}')")

    def insert_one(
        self,
        row: ROW,
        *,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> InsertOneResult:
        """
        TODO
        """

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )
        io_payload = {"insertOne": {"document": row}}
        logger.info(f"insertOne on '{self.name}'")
        io_response = self._api_commander.request(
            payload=io_payload,
            timeout_ms=_request_timeout_ms,
        )
        logger.info(f"finished insertOne on '{self.name}'")
        if "insertedIds" in io_response.get("status", {}):
            if io_response["status"]["insertedIds"]:
                inserted_id = io_response["status"]["insertedIds"][0]
                return InsertOneResult(
                    raw_results=[io_response],
                    inserted_id=inserted_id,
                )
            else:
                raise DataAPIFaultyResponseException(
                    text="Faulty response from insert_one API command.",
                    raw_response=io_response,
                )
        else:
            raise DataAPIFaultyResponseException(
                text="Faulty response from insert_one API command.",
                raw_response=io_response,
            )

    def find_one(
        self,
        filter: FilterType | None = None,
        *,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> ROW | None:
        """
        TODO
        """

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )
        fo_payload = {"findOne": {"filter": filter}}
        fo_response = self._api_commander.request(
            payload=fo_payload,
            timeout_ms=_request_timeout_ms,
        )
        # TODO reinstate this once proper response for no-matches
        # if "document" not in (fo_response.get("data") or {}):
        #     raise DataAPIFaultyResponseException(
        #         text="Faulty response from findOne API command.",
        #         raw_response=fo_response,
        #     )
        # TODO replace next line with the one after that:
        doc_response = fo_response.get("data", {}).get("document")
        # doc_response = fo_response["data"]["document"]
        if doc_response is None:
            return None
        return fo_response["data"]["document"]  # type: ignore[no-any-return]

    def delete_one(
        self,
        filter: FilterType,
        *,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> DeleteResult:
        """
        TODO
        """

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )
        do_payload = {
            "deleteOne": {
                k: v
                for k, v in {
                    "filter": filter,
                }.items()
                if v is not None
            }
        }
        logger.info(f"deleteOne on '{self.name}'")
        do_response = self._api_commander.request(
            payload=do_payload,
            timeout_ms=_request_timeout_ms,
        )
        logger.info(f"finished deleteOne on '{self.name}'")
        if do_response.get("status", {}).get("deletedCount") == -1:
            return DeleteResult(
                deleted_count=-1,  # TODO adjust and erase
                raw_results=[do_response],
            )
        else:
            raise DataAPIFaultyResponseException(
                text="Faulty response from delete_one API command.",
                raw_response=do_response,
            )

    def drop(
        self,
        *,
        schema_operation_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> None:
        """
        TODO
        """

        _schema_operation_timeout_ms = (
            schema_operation_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.schema_operation_timeout_ms
        )
        logger.info(f"dropping table '{self.name}' (self)")
        self.database.drop_table(
            self.name, schema_operation_timeout_ms=_schema_operation_timeout_ms
        )
        logger.info(f"finished dropping table '{self.name}' (self)")

    def command(
        self,
        body: dict[str, Any],
        *,
        raise_api_errors: bool = True,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        TODO
        """

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )
        _cmd_desc = ",".join(sorted(body.keys()))
        logger.info(f"command={_cmd_desc} on '{self.name}'")
        command_result = self._api_commander.request(
            payload=body,
            raise_api_errors=raise_api_errors,
            timeout_ms=_request_timeout_ms,
        )
        logger.info(f"finished command={_cmd_desc} on '{self.name}'")
        return command_result


class AsyncTable(Generic[ROW]):
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

    def _copy(
        self: AsyncTable[ROW],
        *,
        database: AsyncDatabase | None = None,
        name: str | None = None,
        keyspace: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        callers: Sequence[CallerType] | UnsetType = _UNSET,
        request_timeout_ms: int | UnsetType = _UNSET,
        collection_max_time_ms: int | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncTable[ROW]:
        # a double override for the timeout aliasing
        resulting_api_options = (
            self.api_options.with_override(
                api_options,
            )
            .with_override(
                APIOptions(
                    callers=callers,
                    embedding_api_key=coerce_possible_embedding_headers_provider(
                        embedding_api_key
                    ),
                    timeout_options=TimeoutOptions(
                        request_timeout_ms=collection_max_time_ms,
                    ),
                )
            )
            .with_override(
                APIOptions(
                    timeout_options=TimeoutOptions(
                        request_timeout_ms=request_timeout_ms,
                    ),
                )
            )
        )
        return AsyncTable(
            database=database or self.database,
            name=name or self.name,
            keyspace=keyspace or self.keyspace,
            api_options=resulting_api_options,
        )

    def with_options(
        self: AsyncTable[ROW],
        *,
        name: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        callers: Sequence[CallerType] | UnsetType = _UNSET,
        request_timeout_ms: int | UnsetType = _UNSET,
        collection_max_time_ms: int | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncTable[ROW]:
        """
        TODO
        """
        return self._copy(
            name=name,
            embedding_api_key=embedding_api_key,
            callers=callers,
            request_timeout_ms=request_timeout_ms,
            collection_max_time_ms=collection_max_time_ms,
            api_options=api_options,
        )

    def to_sync(
        self: AsyncTable[ROW],
        *,
        database: Database | None = None,
        name: str | None = None,
        keyspace: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        callers: Sequence[CallerType] | UnsetType = _UNSET,
        request_timeout_ms: int | UnsetType = _UNSET,
        collection_max_time_ms: int | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> Table[ROW]:
        """
        TODO
        """

        # a double override for the timeout aliasing
        resulting_api_options = (
            self.api_options.with_override(
                api_options,
            )
            .with_override(
                APIOptions(
                    callers=callers,
                    embedding_api_key=coerce_possible_embedding_headers_provider(
                        embedding_api_key
                    ),
                    timeout_options=TimeoutOptions(
                        request_timeout_ms=collection_max_time_ms,
                    ),
                )
            )
            .with_override(
                APIOptions(
                    timeout_options=TimeoutOptions(
                        request_timeout_ms=request_timeout_ms,
                    ),
                )
            )
        )
        return Table(
            database=database or self.database.to_sync(),
            name=name or self.name,
            keyspace=keyspace or self.keyspace,
            api_options=resulting_api_options,
        )

    async def definition(
        self,
        *,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> TableDefinition:
        """
        TODO
        """

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )
        logger.info(f"getting tables in search of '{self.name}'")
        self_descriptors = [
            table_desc
            for table_desc in await self.database.list_tables(
                max_time_ms=_request_timeout_ms
            )
            if table_desc.name == self.name
        ]
        logger.info(f"finished getting tables in search of '{self.name}'")
        if self_descriptors:
            return self_descriptors[0].definition
        else:
            raise TableNotFoundException(
                text=f"Table {self.keyspace}.{self.name} not found.",
                keyspace=self.keyspace,
                table_name=self.name,
            )

    def info(
        self,
        *,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> TableInfo:
        """
        TODO
        """

        return TableInfo(
            database_info=self.database.info(
                request_timeout_ms=request_timeout_ms,
                max_time_ms=max_time_ms,
            ),
            keyspace=self.keyspace,
            name=self.name,
            full_name=self.full_name,
        )

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

    @property
    def full_name(self) -> str:
        """
        TODO
        """

        return f"{self.keyspace}.{self.name}"

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
            timeout_ms=_schema_operation_timeout_ms,
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
            timeout_ms=_schema_operation_timeout_ms,
        )
        if di_response.get("status") != {"ok": 1}:
            raise DataAPIFaultyResponseException(
                text="Faulty response from dropIndex API command.",
                raw_response=di_response,
            )
        logger.info(f"finished dropIndex('{name}')")

    async def insert_one(
        self,
        row: ROW,
        *,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> InsertOneResult:
        """
        TODO
        """

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )
        io_payload = {"insertOne": {"document": row}}
        logger.info(f"insertOne on '{self.name}'")
        io_response = await self._api_commander.async_request(
            payload=io_payload,
            timeout_ms=_request_timeout_ms,
        )
        logger.info(f"finished insertOne on '{self.name}'")
        if "insertedIds" in io_response.get("status", {}):
            if io_response["status"]["insertedIds"]:
                inserted_id = io_response["status"]["insertedIds"][0]
                return InsertOneResult(
                    raw_results=[io_response],
                    inserted_id=inserted_id,
                )
            else:
                raise DataAPIFaultyResponseException(
                    text="Faulty response from insert_one API command.",
                    raw_response=io_response,
                )
        else:
            raise DataAPIFaultyResponseException(
                text="Faulty response from insert_one API command.",
                raw_response=io_response,
            )

    async def find_one(
        self,
        filter: FilterType | None = None,
        *,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> ROW | None:
        """
        TODO
        """

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )
        fo_payload = {"findOne": {"filter": filter}}
        fo_response = await self._api_commander.async_request(
            payload=fo_payload,
            timeout_ms=_request_timeout_ms,
        )
        # TODO reinstate this once proper response for no-matches
        # if "document" not in (fo_response.get("data") or {}):
        #     raise DataAPIFaultyResponseException(
        #         text="Faulty response from findOne API command.",
        #         raw_response=fo_response,
        #     )
        doc_response = fo_response["data"]["document"]
        if doc_response is None:
            return None
        return fo_response["data"]["document"]  # type: ignore[no-any-return]

    async def delete_one(
        self,
        filter: FilterType,
        *,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> DeleteResult:
        """
        TODO
        """

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )
        do_payload = {
            "deleteOne": {
                k: v
                for k, v in {
                    "filter": filter,
                }.items()
                if v is not None
            }
        }
        logger.info(f"deleteOne on '{self.name}'")
        do_response = await self._api_commander.async_request(
            payload=do_payload,
            timeout_ms=_request_timeout_ms,
        )
        logger.info(f"finished deleteOne on '{self.name}'")
        if do_response.get("status", {}).get("deletedCount") == -1:
            return DeleteResult(
                deleted_count=-1,  # TODO adjust and erase
                raw_results=[do_response],
            )
        else:
            raise DataAPIFaultyResponseException(
                text="Faulty response from delete_one API command.",
                raw_response=do_response,
            )

    async def drop(
        self,
        *,
        schema_operation_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        TODO
        """

        _schema_operation_timeout_ms = (
            schema_operation_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.schema_operation_timeout_ms
        )
        logger.info(f"dropping table '{self.name}' (self)")
        drop_result = await self.database.drop_table(
            self.name, schema_operation_timeout_ms=_schema_operation_timeout_ms
        )
        logger.info(f"finished dropping table '{self.name}' (self)")
        return drop_result

    async def command(
        self,
        body: dict[str, Any],
        *,
        raise_api_errors: bool = True,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        TODO
        """

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )
        _cmd_desc = ",".join(sorted(body.keys()))
        logger.info(f"command={_cmd_desc} on '{self.name}'")
        command_result = await self._api_commander.async_request(
            payload=body,
            raise_api_errors=raise_api_errors,
            timeout_ms=_request_timeout_ms,
        )
        logger.info(f"finished command={_cmd_desc} on '{self.name}'")
        return command_result
