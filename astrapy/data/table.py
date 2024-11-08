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
from types import TracebackType
from typing import TYPE_CHECKING, Any, Generic, Sequence

from astrapy.authentication import coerce_possible_embedding_headers_provider
from astrapy.constants import (
    ROW,
    CallerType,
    FilterType,
    ProjectionType,
    SortType,
    normalize_optional_projection,
)
from astrapy.data.utils.distinct_extractors import (
    _create_document_key_extractor,
    _hash_document,
    _reduce_distinct_key_to_shallow_safe,
)
from astrapy.data.utils.table_converters import _TableConverterAgent
from astrapy.database import AsyncDatabase, Database
from astrapy.exceptions import (
    TooManyRowsToCountException,
    UnexpectedDataAPIResponseException,
    _TimeoutContext,
)
from astrapy.info import TableIndexDefinition, TableInfo, TableVectorIndexDefinition
from astrapy.results import TableInsertOneResult
from astrapy.settings.defaults import DEFAULT_DATA_API_AUTH_HEADER
from astrapy.utils.api_commander import APICommander
from astrapy.utils.api_options import APIOptions, FullAPIOptions, TimeoutOptions
from astrapy.utils.unset import _UNSET, UnsetType

if TYPE_CHECKING:
    from astrapy.authentication import EmbeddingHeadersProvider
    from astrapy.cursors import AsyncTableCursor, TableCursor
    from astrapy.data.info.table_descriptor import AlterTableOperation
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
        self._converter_agent: _TableConverterAgent[ROW] = _TableConverterAgent(
            options=self.api_options.serdes_options,
        )

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
            handle_decimals_writes=True,
            handle_decimals_reads=True,
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
        table_max_time_ms: int | UnsetType = _UNSET,
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
                        request_timeout_ms=table_max_time_ms,
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
        table_max_time_ms: int | UnsetType = _UNSET,
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
            table_max_time_ms=table_max_time_ms,
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
        table_max_time_ms: int | UnsetType = _UNSET,
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
                        request_timeout_ms=table_max_time_ms,
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
            raise ValueError(
                f"Table {self.keyspace}.{self.name} not found.",
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
        ci_options: dict[str, bool]
        if if_not_exists is not None:
            ci_options = {"ifNotExists": if_not_exists}
        else:
            ci_options = {}
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
            timeout_context=_TimeoutContext(request_ms=_schema_operation_timeout_ms),
        )
        if ci_response.get("status") != {"ok": 1}:
            raise UnexpectedDataAPIResponseException(
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

    def alter(
        self,
        operation: AlterTableOperation,
        *,
        schema_operation_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> None:
        """
        Executes one of the available alter-table operations on this table,
        such as adding/dropping columns.

        This is a blocking operation: the method returns once the index
        is created and ready to use.

        Args:
            operation: an instance of one of the `astrapy.info.AlterTable*` classes.
            schema_operation_timeout_ms: a timeout, in milliseconds, for the
                schema-altering HTTP request.
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
        at_operation_name = operation._name
        at_payload = {
            "alterTable": {
                "operation": {
                    at_operation_name: operation.as_dict(),
                },
            },
        }
        logger.info(f"alterTable({at_operation_name})")
        at_response = self._api_commander.request(
            payload=at_payload,
            timeout_context=_TimeoutContext(request_ms=_schema_operation_timeout_ms),
        )
        if at_response.get("status") != {"ok": 1}:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from alterTable API command.",
                raw_response=at_response,
            )
        logger.info(f"finished alterTable({at_operation_name})")

    def insert_one(
        self,
        row: ROW,
        *,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> TableInsertOneResult:
        """
        TODO
        """

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )
        io_payload = self._converter_agent.preprocess_payload(
            {"insertOne": {"document": row}}
        )
        logger.info(f"insertOne on '{self.name}'")
        io_response = self._api_commander.request(
            payload=io_payload,
            timeout_context=_TimeoutContext(request_ms=_request_timeout_ms),
        )
        logger.info(f"finished insertOne on '{self.name}'")
        if "insertedIds" in io_response.get("status", {}):
            if not io_response["status"]["insertedIds"]:
                raise UnexpectedDataAPIResponseException(
                    text="Response from insertOne API command has empty 'insertedIds'.",
                    raw_response=io_response,
                )
            if not io_response["status"]["primaryKeySchema"]:
                raise UnexpectedDataAPIResponseException(
                    text="Response from insertOne API command has empty 'primaryKeySchema'.",
                    raw_response=io_response,
                )
            inserted_id_list = io_response["status"]["insertedIds"][0]
            inserted_id = self._converter_agent.postprocess_key(
                inserted_id_list,
                primary_key_schema_dict=io_response["status"]["primaryKeySchema"],
            )
            inserted_id_tuple = tuple(inserted_id_list)
            return TableInsertOneResult(
                raw_results=[io_response],
                inserted_id=inserted_id,
                inserted_id_tuple=inserted_id_tuple,
            )
        else:
            raise UnexpectedDataAPIResponseException(
                text="Response from insertOne API command missing 'insertedIds'.",
                raw_response=io_response,
            )

    def find(
        self,
        filter: FilterType | None = None,
        *,
        projection: ProjectionType | None = None,
        skip: int | None = None,
        limit: int | None = None,
        include_similarity: bool | None = None,
        include_sort_vector: bool | None = None,
        sort: SortType | None = None,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> TableCursor[ROW, ROW]:
        """
        TODO
        """

        # lazy-import here to avoid circular import issues
        from astrapy.cursors import TableCursor

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )
        # TODO reinstate vectors
        # if include_similarity is not None and not _is_vector_sort(sort):
        #     raise ValueError(
        #         "Cannot use `include_similarity` unless for vector search."
        #     )
        return (
            TableCursor(
                table=self,
                request_timeout_ms=_request_timeout_ms,
                overall_timeout_ms=None,
            )
            .filter(filter)
            .project(projection)
            .skip(skip)
            .limit(limit)
            .sort(sort)
            .include_similarity(include_similarity)
            .include_sort_vector(include_sort_vector)
        )

    def find_one(
        self,
        filter: FilterType | None = None,
        *,
        projection: ProjectionType | None = None,
        include_similarity: bool | None = None,
        sort: SortType | None = None,
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
        fo_options = (
            None
            if include_similarity is None
            else {"includeSimilarity": include_similarity}
        )
        fo_payload = self._converter_agent.preprocess_payload(
            {
                "findOne": {
                    k: v
                    for k, v in {
                        "filter": filter,
                        "projection": normalize_optional_projection(projection),
                        "options": fo_options,
                        "sort": sort,
                    }.items()
                    if v is not None
                }
            }
        )
        fo_response = self._api_commander.request(
            payload=fo_payload,
            timeout_context=_TimeoutContext(request_ms=_request_timeout_ms),
        )
        if "document" not in (fo_response.get("data") or {}):
            raise UnexpectedDataAPIResponseException(
                text="Response from findOne API command missing 'document'.",
                raw_response=fo_response,
            )
        if "projectionSchema" not in (fo_response.get("status") or {}):
            raise UnexpectedDataAPIResponseException(
                text="Response from findOne API command missing 'projectionSchema'.",
                raw_response=fo_response,
            )
        doc_response = fo_response["data"]["document"]
        if doc_response is None:
            return None
        return self._converter_agent.postprocess_row(
            fo_response["data"]["document"],
            columns_dict=fo_response["status"]["projectionSchema"],
        )

    def distinct(
        self,
        key: str,
        *,
        filter: FilterType | None = None,
        request_timeout_ms: int | None = None,
        data_operation_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> list[Any]:
        """
        TODO
        """

        # lazy-import here to avoid circular import issues
        from astrapy.cursors import TableCursor

        _request_timeout_ms = (
            request_timeout_ms or self.api_options.timeout_options.request_timeout_ms
        )
        _data_operation_timeout_ms = (
            data_operation_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.data_operation_timeout_ms
        )
        # preparing cursor:
        _extractor = _create_document_key_extractor(key)
        _key = _reduce_distinct_key_to_shallow_safe(key)
        if _key == "":
            raise ValueError(
                "The 'key' parameter for distinct cannot be empty "
                "or start with a list index."
            )
        # relaxing the type hint (limited to within this method body)
        f_cursor: TableCursor[dict[str, Any], dict[str, Any]] = (
            TableCursor(
                table=self,
                request_timeout_ms=_request_timeout_ms,
                overall_timeout_ms=_data_operation_timeout_ms,
            )  # type: ignore[assignment]
            .filter(filter)
            .project({_key: True})
        )
        # consuming it:
        _item_hashes = set()
        distinct_items: list[Any] = []
        logger.info(f"running distinct() on '{self.name}'")
        for document in f_cursor:
            for item in _extractor(document):
                _item_hash = _hash_document(
                    item, options=self.api_options.serdes_options
                )
                if _item_hash not in _item_hashes:
                    _item_hashes.add(_item_hash)
                    distinct_items.append(item)
        logger.info(f"finished running distinct() on '{self.name}'")
        return distinct_items

    def count_documents(
        self,
        filter: FilterType,
        *,
        upper_bound: int,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> int:
        """
        Count the row in the table matching the specified filter.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"name": "John", "age": 59}
                    {"$and": [{"name": {"$eq": "John"}}, {"age": {"$gt": 58}}]}
                See the Data API documentation for the full set of operators.
            upper_bound: a required ceiling on the result of the count operation.
                If the actual number of rows exceeds this value,
                an exception will be raised.
                Furthermore, if the actual number of rows exceeds the maximum
                count that the Data API can reach (regardless of upper_bound),
                an exception will be raised.
            request_timeout_ms: a timeout, in milliseconds, for the API HTTP request.
                If not passed, the table-level setting is used instead.
            max_time_ms: an alias for `request_timeout_ms`.

        Returns:
            the exact count of matching rows.

        Example:
            >>> my_table.insert_many([{"seq": i} for i in range(20)])
            TableInsertManyResult(...)
            >>> my_table.count_documents({}, upper_bound=100)
            20
            >>> my_table.count_documents({"seq":{"$gt": 15}}, upper_bound=100)
            4
            >>> my_table.count_documents({}, upper_bound=10)
            Traceback (most recent call last):
                ... ...
            astrapy.exceptions.TooManyRowsToCountException

        Note:
            Count operations are expensive: for this reason, the best practice
            is to provide a reasonable `upper_bound` according to the caller
            expectations. Moreover, indiscriminate usage of count operations
            for sizeable amounts of rows (i.e. in the thousands and more)
            is discouraged in favor of alternative application-specific solutions.
            Keep in mind that the Data API has a hard upper limit on the amount
            of rows it will count, and that an exception will be thrown
            by this method if this limit is encountered.
        """

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )
        cd_payload = {"countDocuments": {"filter": filter}}
        logger.info(f"countDocuments on '{self.name}'")
        cd_response = self._api_commander.request(
            payload=cd_payload,
            timeout_context=_TimeoutContext(request_ms=_request_timeout_ms),
        )
        logger.info(f"finished countDocuments on '{self.name}'")
        if "count" in cd_response.get("status", {}):
            count: int = cd_response["status"]["count"]
            if cd_response["status"].get("moreData", False):
                raise TooManyRowsToCountException(
                    text=f"Document count exceeds {count}, the maximum allowed by the server",
                    server_max_count_exceeded=True,
                )
            else:
                if count > upper_bound:
                    raise TooManyRowsToCountException(
                        text="Document count exceeds required upper bound",
                        server_max_count_exceeded=False,
                    )
                else:
                    return count
        else:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from countDocuments API command.",
                raw_response=cd_response,
            )

    def count_rows(
        self,
        filter: FilterType,
        *,
        upper_bound: int,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> int:
        """
        Count the row in the table matching the specified filter.

        This method is an alias for `count_documents`.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"name": "John", "age": 59}
                    {"$and": [{"name": {"$eq": "John"}}, {"age": {"$gt": 58}}]}
                See the Data API documentation for the full set of operators.
            upper_bound: a required ceiling on the result of the count operation.
                If the actual number of rows exceeds this value,
                an exception will be raised.
                Furthermore, if the actual number of rows exceeds the maximum
                count that the Data API can reach (regardless of upper_bound),
                an exception will be raised.
            request_timeout_ms: a timeout, in milliseconds, for the API HTTP request.
                If not passed, the table-level setting is used instead.
            max_time_ms: an alias for `request_timeout_ms`.

        Returns:
            the exact count of matching rows.

        Example:
            >>> my_table.insert_many([{"seq": i} for i in range(20)])
            TableInsertManyResult(...)
            >>> my_table.count_documents({}, upper_bound=100)
            20
            >>> my_table.count_documents({"seq":{"$gt": 15}}, upper_bound=100)
            4
            >>> my_table.count_documents({}, upper_bound=10)
            Traceback (most recent call last):
                ... ...
            astrapy.exceptions.TooManyRowsToCountException

        Note:
            Count operations are expensive: for this reason, the best practice
            is to provide a reasonable `upper_bound` according to the caller
            expectations. Moreover, indiscriminate usage of count operations
            for sizeable amounts of rows (i.e. in the thousands and more)
            is discouraged in favor of alternative application-specific solutions.
            Keep in mind that the Data API has a hard upper limit on the amount
            of rows it will count, and that an exception will be thrown
            by this method if this limit is encountered.
        """

        return self.count_documents(
            filter=filter,
            upper_bound=upper_bound,
            request_timeout_ms=request_timeout_ms,
            max_time_ms=max_time_ms,
        )

    def estimated_document_count(
        self,
        *,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> int:
        """
        Query the API server for an estimate of the document count in the table.

        Contrary to `count_documents`, this method has no filtering parameters.

        Args:
            request_timeout_ms: a timeout, in milliseconds, for the API HTTP request.
                If not passed, the table-level setting is used instead.
            max_time_ms: an alias for `request_timeout_ms`.

        Returns:
            a server-provided estimate count of the documents in the table.

        Example:
            >>> my_table.estimated_document_count()
            5820
        """
        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )
        ed_payload: dict[str, Any] = {"estimatedDocumentCount": {}}
        logger.info(f"estimatedDocumentCount on '{self.name}'")
        ed_response = self._api_commander.request(
            payload=ed_payload,
            timeout_context=_TimeoutContext(request_ms=_request_timeout_ms),
        )
        logger.info(f"finished estimatedDocumentCount on '{self.name}'")
        if "count" in ed_response.get("status", {}):
            count: int = ed_response["status"]["count"]
            return count
        else:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from estimatedDocumentCount API command.",
                raw_response=ed_response,
            )

    def estimated_row_count(
        self,
        *,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> int:
        """
        Query the API server for an estimate of the document count in the table.

        Contrary to `count_documents`, this method has no filtering parameters.
        This method is an alias for `estimated_document_count`.

        Args:
            request_timeout_ms: a timeout, in milliseconds, for the API HTTP request.
                If not passed, the table-level setting is used instead.
            max_time_ms: an alias for `request_timeout_ms`.

        Returns:
            a server-provided estimate count of the documents in the table.

        Example:
            >>> my_table.estimated_row_count()
            5820
        """

        return self.estimated_document_count(
            request_timeout_ms=request_timeout_ms,
            max_time_ms=max_time_ms,
        )

    def delete_one(
        self,
        filter: FilterType,
        *,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> None:
        """
        TODO
        """

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )
        do_payload = self._converter_agent.preprocess_payload(
            {
                "deleteOne": {
                    k: v
                    for k, v in {
                        "filter": filter,
                    }.items()
                    if v is not None
                }
            }
        )
        logger.info(f"deleteOne on '{self.name}'")
        do_response = self._api_commander.request(
            payload=do_payload,
            timeout_context=_TimeoutContext(request_ms=_request_timeout_ms),
        )
        logger.info(f"finished deleteOne on '{self.name}'")
        if do_response.get("status", {}).get("deletedCount") == -1:
            return
        else:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from deleteOne API command.",
                raw_response=do_response,
            )

    def delete_many(
        self,
        filter: FilterType,
        *,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> None:
        """
        TODO
        """

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )
        dm_payload = self._converter_agent.preprocess_payload(
            {
                "deleteMany": {
                    k: v
                    for k, v in {
                        "filter": filter,
                    }.items()
                    if v is not None
                }
            }
        )
        logger.info(f"deleteMany on '{self.name}'")
        dm_response = self._api_commander.request(
            payload=dm_payload,
            timeout_context=_TimeoutContext(request_ms=_request_timeout_ms),
        )
        logger.info(f"finished deleteMany on '{self.name}'")
        if dm_response.get("status", {}).get("deletedCount") == -1:
            return
        else:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from deleteMany API command.",
                raw_response=dm_response,
            )

    def drop(
        self,
        *,
        if_exists: bool | None = None,
        schema_operation_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> None:
        """
        TODO

            if_exists: if passed as True, trying to drop a non-existing table
                will not error, just silently do nothing instead. If not provided,
                the API default behaviour will hold.
        """

        _schema_operation_timeout_ms = (
            schema_operation_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.schema_operation_timeout_ms
        )
        logger.info(f"dropping table '{self.name}' (self)")
        self.database.drop_table(
            self,
            if_exists=if_exists,
            schema_operation_timeout_ms=_schema_operation_timeout_ms,
        )
        logger.info(f"finished dropping table '{self.name}' (self)")

    def command(
        self,
        body: dict[str, Any] | None,
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
        _cmd_desc: str
        if body:
            _cmd_desc = ",".join(sorted(body.keys()))
        else:
            _cmd_desc = "(none)"
        logger.info(f"command={_cmd_desc} on '{self.name}'")
        command_result = self._api_commander.request(
            payload=body,
            raise_api_errors=raise_api_errors,
            timeout_context=_TimeoutContext(request_ms=_request_timeout_ms),
        )
        logger.info(f"finished command={_cmd_desc} on '{self.name}'")
        return command_result


class AsyncTable(Generic[ROW]):
    """
    TODO
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
        self._converter_agent: _TableConverterAgent[ROW] = _TableConverterAgent(
            options=self.api_options.serdes_options,
        )

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
            handle_decimals_writes=True,
            handle_decimals_reads=True,
        )
        return api_commander

    async def __aenter__(self: AsyncTable[ROW]) -> AsyncTable[ROW]:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        if self._api_commander is not None:
            await self._api_commander.__aexit__(
                exc_type=exc_type,
                exc_value=exc_value,
                traceback=traceback,
            )

    def _copy(
        self: AsyncTable[ROW],
        *,
        database: AsyncDatabase | None = None,
        name: str | None = None,
        keyspace: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        callers: Sequence[CallerType] | UnsetType = _UNSET,
        request_timeout_ms: int | UnsetType = _UNSET,
        table_max_time_ms: int | UnsetType = _UNSET,
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
                        request_timeout_ms=table_max_time_ms,
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
        table_max_time_ms: int | UnsetType = _UNSET,
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
            table_max_time_ms=table_max_time_ms,
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
        table_max_time_ms: int | UnsetType = _UNSET,
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
                        request_timeout_ms=table_max_time_ms,
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
            raise ValueError(
                f"Table {self.keyspace}.{self.name} not found.",
            )

    async def info(
        self,
        *,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> TableInfo:
        """
        TODO
        """

        db_info = await self.database.info(
            request_timeout_ms=request_timeout_ms,
            max_time_ms=max_time_ms,
        )
        return TableInfo(
            database_info=db_info,
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
        ci_options: dict[str, bool]
        if if_not_exists is not None:
            ci_options = {"ifNotExists": if_not_exists}
        else:
            ci_options = {}
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
            timeout_context=_TimeoutContext(request_ms=_schema_operation_timeout_ms),
        )
        if ci_response.get("status") != {"ok": 1}:
            raise UnexpectedDataAPIResponseException(
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

    async def alter(
        self,
        operation: AlterTableOperation,
        *,
        schema_operation_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> None:
        """
        Executes one of the available alter-table operations on this table,
        such as adding/dropping columns.

        This is a blocking operation: the method returns once the index
        is created and ready to use.

        Args:
            operation: an instance of one of the `astrapy.info.AlterTable*` classes.
            schema_operation_timeout_ms: a timeout, in milliseconds, for the
                schema-altering HTTP request.
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
        at_operation_name = operation._name
        at_payload = {
            "alterTable": {
                "operation": {
                    at_operation_name: operation.as_dict(),
                },
            },
        }
        logger.info(f"alterTable({at_operation_name})")
        at_response = await self._api_commander.async_request(
            payload=at_payload,
            timeout_context=_TimeoutContext(request_ms=_schema_operation_timeout_ms),
        )
        if at_response.get("status") != {"ok": 1}:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from alterTable API command.",
                raw_response=at_response,
            )
        logger.info(f"finished alterTable({at_operation_name})")

    async def insert_one(
        self,
        row: ROW,
        *,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> TableInsertOneResult:
        """
        TODO
        """

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )
        io_payload = self._converter_agent.preprocess_payload(
            {"insertOne": {"document": row}}
        )
        logger.info(f"insertOne on '{self.name}'")
        io_response = await self._api_commander.async_request(
            payload=io_payload,
            timeout_context=_TimeoutContext(request_ms=_request_timeout_ms),
        )
        logger.info(f"finished insertOne on '{self.name}'")
        if "insertedIds" in io_response.get("status", {}):
            if not io_response["status"]["insertedIds"]:
                raise UnexpectedDataAPIResponseException(
                    text="Response from insertOne API command has empty 'insertedIds'.",
                    raw_response=io_response,
                )
            if not io_response["status"]["primaryKeySchema"]:
                raise UnexpectedDataAPIResponseException(
                    text="Response from insertOne API command has empty 'primaryKeySchema'.",
                    raw_response=io_response,
                )
            inserted_id_list = io_response["status"]["insertedIds"][0]
            inserted_id = self._converter_agent.postprocess_key(
                inserted_id_list,
                primary_key_schema_dict=io_response["status"]["primaryKeySchema"],
            )
            inserted_id_tuple = tuple(inserted_id_list)
            return TableInsertOneResult(
                raw_results=[io_response],
                inserted_id=inserted_id,
                inserted_id_tuple=inserted_id_tuple,
            )
        else:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from insertOne API command.",
                raw_response=io_response,
            )

    def find(
        self,
        filter: FilterType | None = None,
        *,
        projection: ProjectionType | None = None,
        skip: int | None = None,
        limit: int | None = None,
        include_similarity: bool | None = None,
        include_sort_vector: bool | None = None,
        sort: SortType | None = None,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> AsyncTableCursor[ROW, ROW]:
        """
        TODO
        """

        # lazy-import here to avoid circular import issues
        from astrapy.cursors import AsyncTableCursor

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )
        # TODO reinstate vectors
        # if include_similarity is not None and not _is_vector_sort(sort):
        #     raise ValueError(
        #         "Cannot use `include_similarity` unless for vector search."
        #     )
        return (
            AsyncTableCursor(
                table=self,
                request_timeout_ms=_request_timeout_ms,
                overall_timeout_ms=None,
            )
            .filter(filter)
            .project(projection)
            .skip(skip)
            .limit(limit)
            .sort(sort)
            .include_similarity(include_similarity)
            .include_sort_vector(include_sort_vector)
        )

    async def find_one(
        self,
        filter: FilterType | None = None,
        *,
        projection: ProjectionType | None = None,
        include_similarity: bool | None = None,
        sort: SortType | None = None,
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
        fo_options = (
            None
            if include_similarity is None
            else {"includeSimilarity": include_similarity}
        )
        fo_payload = self._converter_agent.preprocess_payload(
            {
                "findOne": {
                    k: v
                    for k, v in {
                        "filter": filter,
                        "projection": normalize_optional_projection(projection),
                        "options": fo_options,
                        "sort": sort,
                    }.items()
                    if v is not None
                }
            }
        )
        fo_response = await self._api_commander.async_request(
            payload=fo_payload,
            timeout_context=_TimeoutContext(request_ms=_request_timeout_ms),
        )
        if "document" not in (fo_response.get("data") or {}):
            raise UnexpectedDataAPIResponseException(
                text="Response from findOne API command missing 'document'.",
                raw_response=fo_response,
            )
        if "projectionSchema" not in (fo_response.get("status") or {}):
            raise UnexpectedDataAPIResponseException(
                text="Response from findOne API command missing 'projectionSchema'.",
                raw_response=fo_response,
            )
        doc_response = fo_response["data"]["document"]
        if doc_response is None:
            return None
        return self._converter_agent.postprocess_row(
            fo_response["data"]["document"],
            columns_dict=fo_response["status"]["projectionSchema"],
        )

    async def distinct(
        self,
        key: str,
        *,
        filter: FilterType | None = None,
        request_timeout_ms: int | None = None,
        data_operation_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> list[Any]:
        """
        TODO
        """

        # lazy-import here to avoid circular import issues
        from astrapy.cursors import AsyncTableCursor

        _request_timeout_ms = (
            request_timeout_ms or self.api_options.timeout_options.request_timeout_ms
        )
        _data_operation_timeout_ms = (
            data_operation_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.data_operation_timeout_ms
        )
        # preparing cursor:
        _extractor = _create_document_key_extractor(key)
        _key = _reduce_distinct_key_to_shallow_safe(key)
        if _key == "":
            raise ValueError(
                "The 'key' parameter for distinct cannot be empty "
                "or start with a list index."
            )
        # relaxing the type hint (limited to within this method body)
        f_cursor: AsyncTableCursor[dict[str, Any], dict[str, Any]] = (
            AsyncTableCursor(
                table=self,
                request_timeout_ms=_request_timeout_ms,
                overall_timeout_ms=_data_operation_timeout_ms,
            )  # type: ignore[assignment]
            .filter(filter)
            .project({_key: True})
        )
        # consuming it:
        _item_hashes = set()
        distinct_items: list[Any] = []
        logger.info(f"running distinct() on '{self.name}'")
        async for document in f_cursor:
            for item in _extractor(document):
                _item_hash = _hash_document(
                    item, options=self.api_options.serdes_options
                )
                if _item_hash not in _item_hashes:
                    _item_hashes.add(_item_hash)
                    distinct_items.append(item)
        logger.info(f"finished running distinct() on '{self.name}'")
        return distinct_items

    async def count_documents(
        self,
        filter: FilterType,
        *,
        upper_bound: int,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> int:
        """
        Count the row in the table matching the specified filter.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"name": "John", "age": 59}
                    {"$and": [{"name": {"$eq": "John"}}, {"age": {"$gt": 58}}]}
                See the Data API documentation for the full set of operators.
            upper_bound: a required ceiling on the result of the count operation.
                If the actual number of rows exceeds this value,
                an exception will be raised.
                Furthermore, if the actual number of rows exceeds the maximum
                count that the Data API can reach (regardless of upper_bound),
                an exception will be raised.
            request_timeout_ms: a timeout, in milliseconds, for the API HTTP request.
                If not passed, the table-level setting is used instead.
            max_time_ms: an alias for `request_timeout_ms`.

        Returns:
            the exact count of matching rows.

        Example:
            >>> my_table.insert_many([{"seq": i} for i in range(20)])
            TableInsertManyResult(...)
            >>> my_table.count_documents({}, upper_bound=100)
            20
            >>> my_table.count_documents({"seq":{"$gt": 15}}, upper_bound=100)
            4
            >>> my_table.count_documents({}, upper_bound=10)
            Traceback (most recent call last):
                ... ...
            astrapy.exceptions.TooManyRowsToCountException

        Note:
            Count operations are expensive: for this reason, the best practice
            is to provide a reasonable `upper_bound` according to the caller
            expectations. Moreover, indiscriminate usage of count operations
            for sizeable amounts of rows (i.e. in the thousands and more)
            is discouraged in favor of alternative application-specific solutions.
            Keep in mind that the Data API has a hard upper limit on the amount
            of rows it will count, and that an exception will be thrown
            by this method if this limit is encountered.
        """

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )
        cd_payload = {"countDocuments": {"filter": filter}}
        logger.info(f"countDocuments on '{self.name}'")
        cd_response = await self._api_commander.async_request(
            payload=cd_payload,
            timeout_context=_TimeoutContext(request_ms=_request_timeout_ms),
        )
        logger.info(f"finished countDocuments on '{self.name}'")
        if "count" in cd_response.get("status", {}):
            count: int = cd_response["status"]["count"]
            if cd_response["status"].get("moreData", False):
                raise TooManyRowsToCountException(
                    text=f"Document count exceeds {count}, the maximum allowed by the server",
                    server_max_count_exceeded=True,
                )
            else:
                if count > upper_bound:
                    raise TooManyRowsToCountException(
                        text="Document count exceeds required upper bound",
                        server_max_count_exceeded=False,
                    )
                else:
                    return count
        else:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from countDocuments API command.",
                raw_response=cd_response,
            )

    async def count_rows(
        self,
        filter: FilterType,
        *,
        upper_bound: int,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> int:
        """
        Count the row in the table matching the specified filter.

        This method is an alias for `count_documents`.

        Args:
            filter: a predicate expressed as a dictionary according to the
                Data API filter syntax. Examples are:
                    {}
                    {"name": "John"}
                    {"name": "John", "age": 59}
                    {"$and": [{"name": {"$eq": "John"}}, {"age": {"$gt": 58}}]}
                See the Data API documentation for the full set of operators.
            upper_bound: a required ceiling on the result of the count operation.
                If the actual number of rows exceeds this value,
                an exception will be raised.
                Furthermore, if the actual number of rows exceeds the maximum
                count that the Data API can reach (regardless of upper_bound),
                an exception will be raised.
            request_timeout_ms: a timeout, in milliseconds, for the API HTTP request.
                If not passed, the table-level setting is used instead.
            max_time_ms: an alias for `request_timeout_ms`.

        Returns:
            the exact count of matching rows.

        Example:
            >>> my_table.insert_many([{"seq": i} for i in range(20)])
            TableInsertManyResult(...)
            >>> my_table.count_documents({}, upper_bound=100)
            20
            >>> my_table.count_documents({"seq":{"$gt": 15}}, upper_bound=100)
            4
            >>> my_table.count_documents({}, upper_bound=10)
            Traceback (most recent call last):
                ... ...
            astrapy.exceptions.TooManyRowsToCountException

        Note:
            Count operations are expensive: for this reason, the best practice
            is to provide a reasonable `upper_bound` according to the caller
            expectations. Moreover, indiscriminate usage of count operations
            for sizeable amounts of rows (i.e. in the thousands and more)
            is discouraged in favor of alternative application-specific solutions.
            Keep in mind that the Data API has a hard upper limit on the amount
            of rows it will count, and that an exception will be thrown
            by this method if this limit is encountered.
        """

        return await self.count_documents(
            filter=filter,
            upper_bound=upper_bound,
            request_timeout_ms=request_timeout_ms,
            max_time_ms=max_time_ms,
        )

    async def estimated_document_count(
        self,
        *,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> int:
        """
        Query the API server for an estimate of the document count in the table.

        Contrary to `count_documents`, this method has no filtering parameters.

        Args:
            request_timeout_ms: a timeout, in milliseconds, for the API HTTP request.
                If not passed, the table-level setting is used instead.
            max_time_ms: an alias for `request_timeout_ms`.

        Returns:
            a server-provided estimate count of the documents in the table.

        Example:
            >>> my_table.estimated_document_count()
            5820
        """
        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )
        ed_payload: dict[str, Any] = {"estimatedDocumentCount": {}}
        logger.info(f"estimatedDocumentCount on '{self.name}'")
        ed_response = await self._api_commander.async_request(
            payload=ed_payload,
            timeout_context=_TimeoutContext(request_ms=_request_timeout_ms),
        )
        logger.info(f"finished estimatedDocumentCount on '{self.name}'")
        if "count" in ed_response.get("status", {}):
            count: int = ed_response["status"]["count"]
            return count
        else:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from estimatedDocumentCount API command.",
                raw_response=ed_response,
            )

    async def estimated_row_count(
        self,
        *,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> int:
        """
        Query the API server for an estimate of the document count in the table.

        Contrary to `count_documents`, this method has no filtering parameters.
        This method is an alias for `estimated_document_count`.

        Args:
            request_timeout_ms: a timeout, in milliseconds, for the API HTTP request.
                If not passed, the table-level setting is used instead.
            max_time_ms: an alias for `request_timeout_ms`.

        Returns:
            a server-provided estimate count of the documents in the table.

        Example:
            >>> my_table.estimated_row_count()
            5820
        """

        return await self.estimated_document_count(
            request_timeout_ms=request_timeout_ms,
            max_time_ms=max_time_ms,
        )

    async def delete_one(
        self,
        filter: FilterType,
        *,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> None:
        """
        TODO
        """

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )
        do_payload = self._converter_agent.preprocess_payload(
            {
                "deleteOne": {
                    k: v
                    for k, v in {
                        "filter": filter,
                    }.items()
                    if v is not None
                }
            }
        )
        logger.info(f"deleteOne on '{self.name}'")
        do_response = await self._api_commander.async_request(
            payload=do_payload,
            timeout_context=_TimeoutContext(request_ms=_request_timeout_ms),
        )
        logger.info(f"finished deleteOne on '{self.name}'")
        if do_response.get("status", {}).get("deletedCount") == -1:
            return
        else:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from deleteOne API command.",
                raw_response=do_response,
            )

    async def delete_many(
        self,
        filter: FilterType,
        *,
        request_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> None:
        """
        TODO
        """

        _request_timeout_ms = (
            request_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.request_timeout_ms
        )
        dm_payload = self._converter_agent.preprocess_payload(
            {
                "deleteMany": {
                    k: v
                    for k, v in {
                        "filter": filter,
                    }.items()
                    if v is not None
                }
            }
        )
        logger.info(f"deleteMany on '{self.name}'")
        dm_response = await self._api_commander.async_request(
            payload=dm_payload,
            timeout_context=_TimeoutContext(request_ms=_request_timeout_ms),
        )
        logger.info(f"finished deleteMany on '{self.name}'")
        if dm_response.get("status", {}).get("deletedCount") == -1:
            return
        else:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from deleteMany API command.",
                raw_response=dm_response,
            )

    async def drop(
        self,
        *,
        if_exists: bool | None = None,
        schema_operation_timeout_ms: int | None = None,
        max_time_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        TODO

            if_exists: if passed as True, trying to drop a non-existing table
                will not error, just silently do nothing instead. If not provided,
                the API default behaviour will hold.
        """

        _schema_operation_timeout_ms = (
            schema_operation_timeout_ms
            or max_time_ms
            or self.api_options.timeout_options.schema_operation_timeout_ms
        )
        logger.info(f"dropping table '{self.name}' (self)")
        drop_result = await self.database.drop_table(
            self,
            if_exists=if_exists,
            schema_operation_timeout_ms=_schema_operation_timeout_ms,
        )
        logger.info(f"finished dropping table '{self.name}' (self)")
        return drop_result

    async def command(
        self,
        body: dict[str, Any] | None,
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
        _cmd_desc: str
        if body:
            _cmd_desc = ",".join(sorted(body.keys()))
        else:
            _cmd_desc = "(none)"
        logger.info(f"command={_cmd_desc} on '{self.name}'")
        command_result = await self._api_commander.async_request(
            payload=body,
            raise_api_errors=raise_api_errors,
            timeout_context=_TimeoutContext(request_ms=_request_timeout_ms),
        )
        logger.info(f"finished command={_cmd_desc} on '{self.name}'")
        return command_result
