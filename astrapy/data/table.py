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

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from types import TracebackType
from typing import TYPE_CHECKING, Any, Generic, Iterable, TypeVar, overload

from astrapy.constants import (
    ROW,
    ROW2,
    DefaultRowType,
    FilterType,
    ProjectionType,
    SortType,
    normalize_optional_projection,
)
from astrapy.data.info.table_descriptor.table_altering import AlterTableOperation
from astrapy.data.utils.distinct_extractors import (
    _create_document_key_extractor,
    _hash_table_document,
    _reduce_distinct_key_to_shallow_safe,
)
from astrapy.data.utils.table_converters import _TableConverterAgent
from astrapy.database import AsyncDatabase, Database
from astrapy.exceptions import (
    DataAPIResponseException,
    MultiCallTimeoutManager,
    TableInsertManyException,
    TooManyRowsToCountException,
    UnexpectedDataAPIResponseException,
    _first_valid_timeout,
    _select_singlereq_timeout_gm,
    _select_singlereq_timeout_ta,
    _TimeoutContext,
)
from astrapy.info import (
    TableIndexDefinition,
    TableIndexDescriptor,
    TableIndexOptions,
    TableInfo,
    TableVectorIndexDefinition,
    TableVectorIndexOptions,
)
from astrapy.results import TableInsertManyResult, TableInsertOneResult
from astrapy.settings.defaults import (
    DEFAULT_DATA_API_AUTH_HEADER,
    DEFAULT_INSERT_MANY_CHUNK_SIZE,
    DEFAULT_INSERT_MANY_CONCURRENCY,
)
from astrapy.utils.api_commander import APICommander
from astrapy.utils.api_options import APIOptions, FullAPIOptions
from astrapy.utils.unset import _UNSET, UnsetType

if TYPE_CHECKING:
    from astrapy.authentication import (
        EmbeddingHeadersProvider,
        RerankingHeadersProvider,
    )
    from astrapy.cursors import AsyncTableFindCursor, TableFindCursor
    from astrapy.info import ListTableDefinition


logger = logging.getLogger(__name__)

NEW_ROW = TypeVar("NEW_ROW")


# path checkers for map-to-tuple automatic conversion of payloads
def map2tuple_checker_insert_many(path: list[str]) -> bool:
    _lp = len(path)
    if _lp >= 4:
        return path[:3] == ["insertMany", "documents", ""]
    else:
        return False


def map2tuple_checker_insert_one(path: list[str]) -> bool:
    _lp = len(path)
    if _lp >= 3:
        return path[:2] == ["insertOne", "document"]
    else:
        return False


def map2tuple_checker_update_one(path: list[str]) -> bool:
    _lp = len(path)
    if _lp >= 4:
        return path[:3] == ["updateOne", "update", "$set"]
    else:
        return False


class Table(Generic[ROW]):
    """
    A Data API table, the object to interact with the Data API for structured data,
    especially for DDL operations. This class has a synchronous interface.

    This class is not meant for direct instantiation by the user, rather
    it is obtained by invoking methods such as `get_table` of Database,
    wherefrom the Table inherits its API options such as authentication
    token and API endpoint.
    In order to create a table, instead, one should call the `create_table`
    method of Database, providing a table definition parameter that can be built
    in different ways (see the `CreateTableDefinition` object and examples below).

    Args:
        database: a Database object, instantiated earlier. This represents
            the database the table belongs to.
        name: the table name. This parameter should match an existing
            table on the database.
        keyspace: this is the keyspace to which the table belongs.
            If nothing is specified, the database's working keyspace is used.
        api_options: a complete specification of the API Options for this instance.

    Examples:
        >>> from astrapy import DataAPIClient
        >>> client = DataAPIClient()
        >>> database = client.get_database(
        ...     "https://01234567-....apps.astra.datastax.com",
        ...     token="AstraCS:..."
        ... )
        >>>

        >>> # Create a table using the fluent syntax for definition
        >>> from astrapy.constants import SortMode
        >>> from astrapy.info import (
        ...     CreateTableDefinition,
        ...     ColumnType,
        ... )
        >>> table_definition = (
        ...     CreateTableDefinition.builder()
        ...     .add_column("match_id", ColumnType.TEXT)
        ...     .add_column("round", ColumnType.INT)
        ...     .add_vector_column("m_vector", dimension=3)
        ...     .add_column("score", ColumnType.INT)
        ...     .add_column("when", ColumnType.TIMESTAMP)
        ...     .add_column("winner", ColumnType.TEXT)
        ...     .add_set_column("fighters", ColumnType.UUID)
        ...     .add_partition_by(["match_id"])
        ...     .add_partition_sort({"round": SortMode.ASCENDING})
        ...     .build()
        ... )
        >>> my_table = database.create_table(
        ...     "games",
        ...     definition=table_definition,
        ... )

        >>> # Create a table with the definition as object
        >>> # (and do not raise an error if the table exists already)
        >>> from astrapy.info import (
        ...     CreateTableDefinition,
        ...     TablePrimaryKeyDescriptor,
        ...     TableScalarColumnTypeDescriptor,
        ...     TableValuedColumnType,
        ...     TableValuedColumnTypeDescriptor,
        ...     TableVectorColumnTypeDescriptor,
        ... )
        >>> table_definition_1 = CreateTableDefinition(
        ...     columns={
        ...         "match_id": TableScalarColumnTypeDescriptor(
        ...             ColumnType.TEXT,
        ...         ),
        ...         "round": TableScalarColumnTypeDescriptor(
        ...             ColumnType.INT,
        ...         ),
        ...         "m_vector": TableVectorColumnTypeDescriptor(
        ...             column_type="vector", dimension=3
        ...         ),
        ...         "score": TableScalarColumnTypeDescriptor(
        ...             ColumnType.INT,
        ...         ),
        ...         "when": TableScalarColumnTypeDescriptor(
        ...             ColumnType.TIMESTAMP,
        ...         ),
        ...         "winner": TableScalarColumnTypeDescriptor(
        ...             ColumnType.TEXT,
        ...         ),
        ...         "fighters": TableValuedColumnTypeDescriptor(
        ...             column_type=TableValuedColumnType.SET,
        ...             value_type=ColumnType.UUID,
        ...         ),
        ...     },
        ...     primary_key=TablePrimaryKeyDescriptor(
        ...         partition_by=["match_id"],
        ...         partition_sort={"round": SortMode.ASCENDING},
        ...     ),
        ... )
        >>> my_table_1 = database.create_table(
        ...     "games",
        ...     definition=table_definition_1,
        ...     if_not_exists=True,
        ... )

        >>> # Create a table with the definition as plain dictionary
        >>> # (and do not raise an error if the table exists already)
        >>> table_definition_2 = {
        ...     "columns": {
        ...         "match_id": {"type": "text"},
        ...         "round": {"type": "int"},
        ...         "m_vector": {"type": "vector", "dimension": 3},
        ...         "score": {"type": "int"},
        ...         "when": {"type": "timestamp"},
        ...         "winner": {"type": "text"},
        ...         "fighters": {"type": "set", "valueType": "uuid"},
        ...     },
        ...     "primaryKey": {
        ...         "partitionBy": ["match_id"],
        ...         "partitionSort": {"round": 1},
        ...     },
        ... }
        >>> my_table_2 = database.create_table(
        ...     "games",
        ...     definition=table_definition_2,
        ...     if_not_exists=True,
        ... )

        >>> # Get a reference to an existing table
        >>> # (no checks are performed on DB)
        >>> my_table_3 = database.get_table("games")

    Note:
        creating an instance of Table does not trigger, in itself, actual
        creation of the table on the database. The latter should have been created
        beforehand, e.g. through the `create_table` method of a Database.
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

        self._database = database._copy(
            keyspace=_keyspace, api_options=self.api_options
        )
        self._commander_headers = {
            **{DEFAULT_DATA_API_AUTH_HEADER: self.api_options.token.get_token()},
            **self.api_options.embedding_api_key.get_headers(),
            **self.api_options.reranking_api_key.get_headers(),
            **self.api_options.database_additional_headers,
        }
        self._api_commander = self._get_api_commander()
        self._converter_agent: _TableConverterAgent[ROW] = _TableConverterAgent(
            options=self.api_options.serdes_options,
        )

    def __repr__(self) -> str:
        _db_desc = f'database.api_endpoint="{self.database.api_endpoint}"'
        return (
            f'{self.__class__.__name__}(name="{self.name}", '
            f'keyspace="{self.keyspace}", {_db_desc}, '
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
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> Table[ROW]:
        arg_api_options = APIOptions(
            embedding_api_key=embedding_api_key,
            reranking_api_key=reranking_api_key,
        )
        final_api_options = self.api_options.with_override(api_options).with_override(
            arg_api_options
        )
        return Table(
            database=self.database,
            name=self.name,
            keyspace=self.keyspace,
            api_options=final_api_options,
        )

    def with_options(
        self: Table[ROW],
        *,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> Table[ROW]:
        """
        Create a clone of this table with some changed attributes.

        Args:
            embedding_api_key: optional API key(s) for interacting with the table.
                If an embedding service is configured, and this parameter is not None,
                each Data API call will include the necessary embedding-related headers
                as specified by this parameter. If a string is passed, it translates
                into the one "embedding api key" header
                (i.e. `astrapy.authentication.EmbeddingAPIKeyHeaderProvider`).
                For some vectorize providers/models, if using header-based authentication,
                specialized subclasses of `astrapy.authentication.EmbeddingHeadersProvider`
                should be supplied.
            reranking_api_key: optional API key(s) for interacting with the table.
                If a reranker is configured for the table, and this parameter
                is not None, Data API calls will include the appropriate
                reranker-related headers according to this parameter. Reranker services
                may not necessarily require this setting (e.g. if the service needs no
                authentication, or one is configured as part of the table
                definition relying on a "shared secret").
                If a string is passed, it is translated into an instance of
                `astrapy.authentication.RerankingAPIKeyHeaderProvider`.
            api_options: any additional options to set for the clone, in the form of
                an APIOptions instance (where one can set just the needed attributes).
                In case the same setting is also provided as named parameter,
                the latter takes precedence.

        Returns:
            a new Table instance.

        Example:
            >>> table_with_api_key_configured = my_table.with_options(
            ...     embedding_api_key="secret-key-0123abcd...",
            ... )
        """

        return self._copy(
            embedding_api_key=embedding_api_key,
            reranking_api_key=reranking_api_key,
            api_options=api_options,
        )

    def to_async(
        self: Table[ROW],
        *,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncTable[ROW]:
        """
        Create an AsyncTable from this one. Save for the arguments
        explicitly provided as overrides, everything else is kept identical
        to this table in the copy (the database is converted into
        an async object).

        Args:
            embedding_api_key: optional API key(s) for interacting with the table.
                If an embedding service is configured, and this parameter is not None,
                each Data API call will include the necessary embedding-related headers
                as specified by this parameter. If a string is passed, it translates
                into the one "embedding api key" header
                (i.e. `astrapy.authentication.EmbeddingAPIKeyHeaderProvider`).
                For some vectorize providers/models, if using header-based authentication,
                specialized subclasses of `astrapy.authentication.EmbeddingHeadersProvider`
                should be supplied.
            reranking_api_key: optional API key(s) for interacting with the table.
                If a reranker is configured for the table, and this parameter
                is not None, Data API calls will include the appropriate
                reranker-related headers according to this parameter. Reranker services
                may not necessarily require this setting (e.g. if the service needs no
                authentication, or one is configured as part of the table
                definition relying on a "shared secret").
                If a string is passed, it is translated into an instance of
                `astrapy.authentication.RerankingAPIKeyHeaderProvider`.
            api_options: any additional options to set for the result, in the form of
                an APIOptions instance (where one can set just the needed attributes).
                In case the same setting is also provided as named parameter,
                the latter takes precedence.

        Returns:
            the new copy, an AsyncTable instance.

        Example:
            >>> asyncio.run(my_table.to_async().find_one(
            ...     {"match_id": "fight4"},
            ...     projection={"winner": True},
            ... ))
            {"pk": 1, "column": "value}
        """

        arg_api_options = APIOptions(
            embedding_api_key=embedding_api_key,
            reranking_api_key=reranking_api_key,
        )
        final_api_options = self.api_options.with_override(api_options).with_override(
            arg_api_options
        )
        return AsyncTable(
            database=self.database.to_async(),
            name=self.name,
            keyspace=self.keyspace,
            api_options=final_api_options,
        )

    def definition(
        self,
        *,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> ListTableDefinition:
        """
        Query the Data API and return a structure defining the table schema.
        If there are no unsupported colums in the table, the return value has
        the same contents as could have been provided to a `create_table` method call.

        Args:
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Returns:
            A `ListTableDefinition` object, available for inspection.

        Example:
            >>> my_table.definition()
            ListTableDefinition(columns=[match_id,round,fighters, ...  # shortened
        """

        _table_admin_timeout_ms, _ta_label = _select_singlereq_timeout_ta(
            timeout_options=self.api_options.timeout_options,
            table_admin_timeout_ms=table_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info(f"getting tables in search of '{self.name}'")
        self_descriptors = [
            table_desc
            for table_desc in self.database._list_tables_ctx(
                keyspace=None,
                timeout_context=_TimeoutContext(
                    request_ms=_table_admin_timeout_ms,
                    label=_ta_label,
                ),
            )
            if table_desc.name == self.name
        ]
        logger.info(f"finished getting tables in search of '{self.name}'")
        if self_descriptors:
            return self_descriptors[0].definition
        else:
            raise RuntimeError(
                f"Table {self.keyspace}.{self.name} not found.",
            )

    def info(
        self,
        *,
        database_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> TableInfo:
        """
        Return information on the table. This should not be confused with the table
        definition (i.e. the schema).

        Args:
            database_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying DevOps API request.
                If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `database_admin_timeout_ms`.
            timeout_ms: an alias for `database_admin_timeout_ms`.

        Returns:
            A TableInfo object for inspection.

        Example:
            >>> # Note: output reformatted for clarity.
            >>> my_table.info()
            TableInfo(
                database_info=AstraDBDatabaseInfo(id=..., name=..., ...),
                keyspace='default_keyspace',
                name='games',
                full_name='default_keyspace.games'
            )
        """

        return TableInfo(
            database_info=self.database.info(
                database_admin_timeout_ms=database_admin_timeout_ms,
                request_timeout_ms=request_timeout_ms,
                timeout_ms=timeout_ms,
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
            raise RuntimeError("The table's DB is set with keyspace=None")
        return _keyspace

    @property
    def name(self) -> str:
        """
        The name of this table.

        Example:
            >>> my_table.name
            'games'
        """

        return self._name

    @property
    def full_name(self) -> str:
        """
        The fully-qualified table name within the database,
        in the form "keyspace.table_name".

        Example:
            >>> my_table.full_name
            'default_keyspace.my_table'
        """

        return f"{self.keyspace}.{self.name}"

    def _create_generic_index(
        self,
        i_name: str,
        ci_definition: dict[str, Any],
        ci_command: str,
        if_not_exists: bool | None,
        table_admin_timeout_ms: int | None,
        request_timeout_ms: int | None,
        timeout_ms: int | None,
    ) -> None:
        ci_options: dict[str, bool]
        if if_not_exists is not None:
            ci_options = {"ifNotExists": if_not_exists}
        else:
            ci_options = {}
        _table_admin_timeout_ms, _ta_label = _select_singlereq_timeout_ta(
            timeout_options=self.api_options.timeout_options,
            table_admin_timeout_ms=table_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
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
            timeout_context=_TimeoutContext(
                request_ms=_table_admin_timeout_ms, label=_ta_label
            ),
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
        column: str | dict[str, str],
        *,
        options: TableIndexOptions | dict[str, Any] | None = None,
        if_not_exists: bool | None = None,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        """
        Create an index on a non-vector column of the table.

        This is a blocking operation: the method returns once the index
        is created and ready to use.

        For creation of a vector index, see method `create_vector_index` instead.

        Args:
            name: the name of the index. Index names must be unique across the keyspace.
            column: the table column on which the index is to be created.
                For a map column, besides a simple string, it can be an object
                in one of the two formats {"column": "$values"}, {"column": "$keys"},
            options: if passed, it must be an instance of `TableIndexOptions`,
                or an equivalent dictionary, which specifies index settings
                such as -- for a text column -- case-sensitivity and so on.
                See the `astrapy.info.TableIndexOptions` class for more details.
            if_not_exists: if set to True, the command will succeed even if an index
                with the specified name already exists (in which case no actual
                index creation takes place on the database). The API default of False
                means that an error is raised by the API in case of name collision.
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Examples:
            >>> from astrapy.info import TableIndexOptions
            >>>
            >>> # create an index on a column
            >>> my_table.create_index(
            ...     "score_index",
            ...     "score",
            ... )
            >>>
            >>> # create an index on a textual column, specifying indexing options
            >>> my_table.create_index(
            ...     "winner_index",
            ...     "winner",
            ...     options=TableIndexOptions(
            ...         ascii=False,
            ...         normalize=True,
            ...         case_sensitive=False,
            ...     ),
            ... )
        """

        ci_definition: dict[str, Any] = TableIndexDefinition(
            column=column,
            options=TableIndexOptions.coerce(options or {}),
        ).as_dict()
        ci_command = "createIndex"
        return self._create_generic_index(
            i_name=name,
            ci_definition=ci_definition,
            ci_command=ci_command,
            if_not_exists=if_not_exists,
            table_admin_timeout_ms=table_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )

    def create_vector_index(
        self,
        name: str,
        column: str,
        *,
        options: TableVectorIndexOptions | dict[str, Any] | None = None,
        if_not_exists: bool | None = None,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        """
        Create a vector index on a vector column of the table, enabling vector
        similarity search operations on it.

        This is a blocking operation: the method returns once the index
        is created and ready to use.

        For creation of a non-vector index, see method `create_index` instead.

        Args:
            name: the name of the index. Index names must be unique across the keyspace.
            column: the table column, of type "vector" on which to create the index.
            options: an instance of `TableVectorIndexOptions`, or an equivalent
                dictionary, which specifies settings for the vector index,
                such as the metric to use or, if desired, a "source model" setting.
                If omitted, the Data API defaults will apply for the index.
                See the `astrapy.info.TableVectorIndexOptions` class for more details.
            if_not_exists: if set to True, the command will succeed even if an index
                with the specified name already exists (in which case no actual
                index creation takes place on the database). The API default of False
                means that an error is raised by the API in case of name collision.
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Example:
            >>> from astrapy.constants import VectorMetric
            >>> from astrapy.info import TableVectorIndexOptions
            >>>
            >>> # create a vector index with dot-product similarity
            >>> my_table.create_vector_index(
            ...     "m_vector_index",
            ...     "m_vector",
            ...     options=TableVectorIndexOptions(
            ...         metric=VectorMetric.DOT_PRODUCT,
            ...     ),
            ... )
            >>> # specify a source_model (since the previous statement
            >>> # succeeded, this will do nothing because of `if_not_exists`):
            >>> my_table.create_vector_index(
            ...     "m_vector_index",
            ...     "m_vector",
            ...     options=TableVectorIndexOptions(
            ...         metric=VectorMetric.DOT_PRODUCT,
            ...         source_model="nv-qa-4",
            ...     ),
            ...     if_not_exists=True,
            ... )
            >>> # leave the settings to the Data API defaults of cosine
            >>> # similarity metric (since the previous statement
            >>> # succeeded, this will do nothing because of `if_not_exists`):
            >>> my_table.create_vector_index(
            ...     "m_vector_index",
            ...     "m_vector",
            ...     if_not_exists=True,
            ... )
        """

        ci_definition: dict[str, Any] = TableVectorIndexDefinition(
            column=column,
            options=TableVectorIndexOptions.coerce(options),
        ).as_dict()
        ci_command = "createVectorIndex"
        return self._create_generic_index(
            i_name=name,
            ci_definition=ci_definition,
            ci_command=ci_command,
            if_not_exists=if_not_exists,
            table_admin_timeout_ms=table_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )

    def list_index_names(
        self,
        *,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[str]:
        """
        List the names of all indexes existing on this table.

        Args:
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Returns:
            a list of the index names as strings, in no particular order.

        Example:
            >>> my_table.list_index_names()
            ['m_vector_index', 'winner_index', 'score_index']
        """

        _table_admin_timeout_ms, _ta_label = _select_singlereq_timeout_ta(
            timeout_options=self.api_options.timeout_options,
            table_admin_timeout_ms=table_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        li_payload: dict[str, Any] = {"listIndexes": {"options": {}}}
        logger.info("listIndexes")
        li_response = self._api_commander.request(
            payload=li_payload,
            timeout_context=_TimeoutContext(
                request_ms=_table_admin_timeout_ms, label=_ta_label
            ),
        )
        if "indexes" not in li_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from listIndexes API command.",
                raw_response=li_response,
            )
        else:
            logger.info("finished listIndexes")
            return li_response["status"]["indexes"]  # type: ignore[no-any-return]

    def _list_indexes(
        self,
        *,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[TableIndexDescriptor]:
        """
        List the full definitions of all indexes existing on this table.

        WARNING: method not public yet, pending completion of its API.

        Args:
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Returns:
            a list of `astrapy.info.TableIndexDescriptor` objects in no particular
            order, each providing the details of an index present on the table.

        Example:
            >>> indexes = my_table.list_indexes()
            >>> indexes
            [TableIndexDescriptor(name='m_vector_index', definition=...)...]  # Note: shortened
            >>> indexes[1].definition.column
            'winner'
            >>> indexes[1].definition.options.case_sensitive
            False
        """

        _table_admin_timeout_ms, _ta_label = _select_singlereq_timeout_ta(
            timeout_options=self.api_options.timeout_options,
            table_admin_timeout_ms=table_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        li_payload: dict[str, Any] = {"listIndexes": {"options": {"explain": True}}}
        logger.info("listIndexes")
        li_response = self._api_commander.request(
            payload=li_payload,
            timeout_context=_TimeoutContext(
                request_ms=_table_admin_timeout_ms, label=_ta_label
            ),
        )
        columns = self.definition(
            table_admin_timeout_ms=table_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        ).columns

        if "indexes" not in li_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from listIndexes API command.",
                raw_response=li_response,
            )
        else:
            logger.info("finished listIndexes")
            return [
                TableIndexDescriptor.coerce(index_object, columns=columns)
                for index_object in li_response["status"]["indexes"]
            ]

    @overload
    def alter(
        self,
        operation: AlterTableOperation | dict[str, Any],
        *,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> Table[DefaultRowType]: ...

    @overload
    def alter(
        self,
        operation: AlterTableOperation | dict[str, Any],
        *,
        row_type: type[NEW_ROW],
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> Table[NEW_ROW]: ...

    def alter(
        self,
        operation: AlterTableOperation | dict[str, Any],
        *,
        row_type: type[Any] = DefaultRowType,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> Table[NEW_ROW]:
        """
        Executes one of the available alter-table operations on this table,
        such as adding/dropping columns.

        This is a blocking operation: the method returns once the index
        is created and ready to use.

        Args:
            operation: an instance of one of the `astrapy.info.AlterTable*` classes,
                representing which alter operation to perform and the details thereof.
                A regular dictionary can also be provided, but then it must have the
                alter operation name at its top level: {"add": {"columns": ...}}.
            row_type: this parameter acts a formal specifier for the type checker.
                If omitted, the resulting Table is implicitly a `Table[dict[str, Any]]`.
                If provided, it must match the type hint specified in the assignment.
                See the examples below.
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Examples:
            >>> from astrapy.info import (
            ...     AlterTableAddColumns,
            ...     AlterTableAddVectorize,
            ...     AlterTableDropColumns,
            ...     AlterTableDropVectorize,
            ...     ColumnType,
            ...     TableScalarColumnTypeDescriptor,
            ...     VectorServiceOptions,
            ... )
            >>>
            >>> # Add a column
            >>> new_table_1 = my_table.alter(
            ...     AlterTableAddColumns(
            ...         columns={
            ...             "tie_break": TableScalarColumnTypeDescriptor(
            ...                 column_type=ColumnType.BOOLEAN,
            ...             ),
            ...         }
            ...     )
            ... )
            >>>
            >>> # Drop a column
            >>> new_table_2 = new_table_1.alter(AlterTableDropColumns(
            ...     columns=["tie_break"]
            ... ))
            >>>
            >>> # Add vectorize to a (vector) column
            >>> new_table_3 = new_table_2.alter(
            ...     AlterTableAddVectorize(
            ...         columns={
            ...             "m_vector": VectorServiceOptions(
            ...                 provider="openai",
            ...                 model_name="text-embedding-3-small",
            ...                 authentication={
            ...                     "providerKey": "ASTRA_KMS_API_KEY_NAME",
            ...                 },
            ...             ),
            ...         }
            ...     )
            ... )
            >>>
            >>> # Drop vectorize from a (vector) column
            >>> # (Also demonstrates type hint usage)
            >>> from typing import TypedDict
            >>> from astrapy import Table
            >>> from astrapy.data_types import (
            ...     DataAPISet,
            ...     DataAPITimestamp,
            ...     DataAPIVector,
            ... )
            >>> from astrapy.ids import UUID
            >>>
            >>> class MyMatch(TypedDict):
            ...     match_id: str
            ...     round: int
            ...     m_vector: DataAPIVector
            ...     score: int
            ...     when: DataAPITimestamp
            ...     winner: str
            ...     fighters: DataAPISet[UUID]
            ...
            >>> new_table_4: Table[MyMatch] = new_table_3.alter(
            ...     AlterTableDropVectorize(columns=["m_vector"]),
            ...     row_type=MyMatch,
            ... )
        """

        n_operation: AlterTableOperation
        if isinstance(operation, AlterTableOperation):
            n_operation = operation
        else:
            n_operation = AlterTableOperation.from_full_dict(operation)
        _table_admin_timeout_ms, _ta_label = _select_singlereq_timeout_ta(
            timeout_options=self.api_options.timeout_options,
            table_admin_timeout_ms=table_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        at_operation_name = n_operation._name
        at_payload = {
            "alterTable": {
                "operation": {
                    at_operation_name: n_operation.as_dict(),
                },
            },
        }
        logger.info(f"alterTable({at_operation_name})")
        at_response = self._api_commander.request(
            payload=at_payload,
            timeout_context=_TimeoutContext(
                request_ms=_table_admin_timeout_ms, label=_ta_label
            ),
        )
        if at_response.get("status") != {"ok": 1}:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from alterTable API command.",
                raw_response=at_response,
            )
        logger.info(f"finished alterTable({at_operation_name})")
        return Table(
            database=self.database,
            name=self.name,
            keyspace=self.keyspace,
            api_options=self.api_options,
        )

    def insert_one(
        self,
        row: ROW,
        *,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> TableInsertOneResult:
        """
        Insert a single row in the table,
        with implied overwrite in case of primary key collision.

        Inserting a row whose primary key correspond to an entry alredy stored
        in the table has the effect of an in-place update: the row is overwritten.
        However, if the row being inserted is partially provided, i.e. some columns
        are not specified, these are left unchanged on the database. To explicitly
        reset them, specify their value as appropriate to their data type,
        i.e. `None`, `{}` or analogous.

        Args:
            row: a dictionary expressing the row to insert. The primary key
                must be specified in full, while any other column may be omitted
                if desired (in which case it is left as is on DB).
                The values for the various columns supplied in the row must
                be of the right data type for the insertion to succeed.
                Non-primary-key columns can also be explicitly set to null.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a TableInsertOneResult object, whose attributes are the primary key
            of the inserted row both in the form of a dictionary and of a tuple.

        Examples:
            >>> # a full-row insert using astrapy's datatypes
            >>> from astrapy.data_types import (
            ...     DataAPISet,
            ...     DataAPITimestamp,
            ...     DataAPIVector,
            ... )
            >>> from astrapy.ids import UUID
            >>>
            >>> insert_result = my_table.insert_one(
            ...     {
            ...         "match_id": "mtch_0",
            ...         "round": 1,
            ...         "m_vector": DataAPIVector([0.4, -0.6, 0.2]),
            ...         "score": 18,
            ...         "when": DataAPITimestamp.from_string("2024-11-28T11:30:00Z"),
            ...         "winner": "Victor",
            ...         "fighters": DataAPISet([
            ...             UUID("0193539a-2770-8c09-a32a-111111111111"),
            ...         ]),
            ...     },
            ... )
            >>> insert_result.inserted_id
            {'match_id': 'mtch_0', 'round': 1}
            >>> insert_result.inserted_id_tuple
            ('mtch_0', 1)
            >>>
            >>> # a partial-row (which in this case overwrites some of the values)
            >>> my_table.insert_one(
            ...     {
            ...         "match_id": "mtch_0",
            ...         "round": 1,
            ...         "winner": "Victor Vector",
            ...         "fighters": DataAPISet([
            ...             UUID("0193539a-2770-8c09-a32a-111111111111"),
            ...             UUID("0193539a-2880-8875-9f07-222222222222"),
            ...         ]),
            ...     },
            ... )
            TableInsertOneResult(inserted_id={'match_id': 'mtch_0', 'round': 1} ...
            >>>
            >>> # another insertion demonstrating standard-library datatypes in values
            >>> import datetime
            >>>
            >>> my_table.insert_one(
            ...     {
            ...         "match_id": "mtch_0",
            ...         "round": 2,
            ...         "winner": "Angela",
            ...         "score": 25,
            ...         "when": datetime.datetime(
            ...             2024, 7, 13, 12, 55, 30, 889,
            ...             tzinfo=datetime.timezone.utc,
            ...         ),
            ...         "fighters": {
            ...             UUID("019353cb-8e01-8276-a190-333333333333"),
            ...         },
            ...         "m_vector": [0.4, -0.6, 0.2],
            ...     },
            ... )
            TableInsertOneResult(inserted_id={'match_id': 'mtch_0', 'round': 2}, ...
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        io_payload = self._converter_agent.preprocess_payload(
            {"insertOne": {"document": row}},
            map2tuple_checker=map2tuple_checker_insert_one,
        )
        logger.info(f"insertOne on '{self.name}'")
        io_response = self._api_commander.request(
            payload=io_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
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
                    text=(
                        "Response from insertOne API command has "
                        "empty 'primaryKeySchema'."
                    ),
                    raw_response=io_response,
                )
            inserted_id_list = io_response["status"]["insertedIds"][0]
            inserted_id_tuple, inserted_id = self._converter_agent.postprocess_key(
                inserted_id_list,
                primary_key_schema_dict=io_response["status"]["primaryKeySchema"],
            )
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

    def _prepare_keys_from_status(
        self, status: dict[str, Any] | None, raise_on_missing: bool = False
    ) -> tuple[list[dict[str, Any]], list[tuple[Any, ...]]]:
        ids: list[dict[str, Any]]
        id_tuples: list[tuple[Any, ...]]
        if status is None:
            if raise_on_missing:
                raise UnexpectedDataAPIResponseException(
                    text="'status' not found in API response",
                    raw_response=None,
                )
            else:
                ids = []
                id_tuples = []
        else:
            if "documentResponses" not in status:
                raise UnexpectedDataAPIResponseException(
                    text=(
                        "received a 'status' without 'documentResponses' "
                        f"in API response (received: {status})"
                    ),
                    raw_response=None,
                )
            raw_inserted_ids = [
                row_resp["_id"]
                for row_resp in status["documentResponses"]
                if row_resp["status"] == "OK"
            ]
            if raw_inserted_ids:
                if "primaryKeySchema" not in status:
                    raise UnexpectedDataAPIResponseException(
                        text=(
                            "received a 'status' without 'primaryKeySchema' "
                            f"in API response (received: {status})"
                        ),
                        raw_response=None,
                    )
                id_tuples_and_ids = self._converter_agent.postprocess_keys(
                    raw_inserted_ids,
                    primary_key_schema_dict=status["primaryKeySchema"],
                )
                id_tuples = [tpl for tpl, _ in id_tuples_and_ids]
                ids = [id for _, id in id_tuples_and_ids]
            else:
                ids = []
                id_tuples = []
        return ids, id_tuples

    def insert_many(
        self,
        rows: Iterable[ROW],
        *,
        ordered: bool = False,
        chunk_size: int | None = None,
        concurrency: int | None = None,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> TableInsertManyResult:
        """
        Insert a number of rows into the table,
        with implied overwrite in case of primary key collision.

        Inserting rows whose primary key correspond to entries alredy stored
        in the table has the effect of an in-place update: the rows are overwritten.
        However, if the rows being inserted are partially provided, i.e. some columns
        are not specified, these are left unchanged on the database. To explicitly
        reset them, specify their value as appropriate to their data type,
        i.e. `None`, `{}` or analogous.

        Args:
            rows: an iterable of dictionaries, each expressing a row to insert.
                Each row must at least fully specify the primary key column values,
                while any other column may be omitted if desired (in which case
                it is left as is on DB).
                The values for the various columns supplied in each row must
                be of the right data type for the insertion to succeed.
                Non-primary-key columns can also be explicitly set to null.
            ordered: if False (default), the insertions can occur in arbitrary order
                and possibly concurrently. If True, they are processed sequentially.
                If there are no specific reasons against it, unordered insertions
                re to be preferred as they complete much faster.
            chunk_size: how many rows to include in each single API request.
                Exceeding the server maximum allowed value results in an error.
                Leave it unspecified (recommended) to use the system default.
            concurrency: maximum number of concurrent requests to the API at
                a given time. It cannot be more than one for ordered insertions.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                whole operation, which may consist of several API requests.
                If not provided, this object's defaults apply.
            request_timeout_ms: a timeout, in milliseconds, to impose on each
                individual HTTP request to the Data API to accomplish the operation.
                If not provided, this object's defaults apply.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a TableInsertManyResult object, whose attributes are the primary key
            of the inserted rows both in the form of dictionaries and of tuples.

        Examples:
            >>> # Insert complete and partial rows at once (concurrently)
            >>> from astrapy.data_types import (
            ...     DataAPISet,
            ...     DataAPITimestamp,
            ...     DataAPIVector,
            ... )
            >>> from astrapy.ids import UUID
            >>>
            >>> insert_result = my_table.insert_many(
            ...     [
            ...         {
            ...             "match_id": "fight4",
            ...             "round": 1,
            ...             "winner": "Victor",
            ...             "score": 18,
            ...             "when": DataAPITimestamp.from_string(
            ...                 "2024-11-28T11:30:00Z",
            ...             ),
            ...             "fighters": DataAPISet([
            ...                 UUID("0193539a-2770-8c09-a32a-111111111111"),
            ...                 UUID('019353e3-00b4-83f9-a127-222222222222'),
            ...             ]),
            ...             "m_vector": DataAPIVector([0.4, -0.6, 0.2]),
            ...         },
            ...         {"match_id": "fight5", "round": 1, "winner": "Adam"},
            ...         {"match_id": "fight5", "round": 2, "winner": "Betta"},
            ...         {"match_id": "fight5", "round": 3, "winner": "Caio"},
            ...         {
            ...             "match_id": "challenge6",
            ...             "round": 1,
            ...             "winner": "Donna",
            ...             "m_vector": [0.9, -0.1, -0.3],
            ...         },
            ...         {"match_id": "challenge6", "round": 2, "winner": "Erick"},
            ...         {"match_id": "challenge6", "round": 3, "winner": "Fiona"},
            ...         {"match_id": "tournamentA", "round": 1, "winner": "Gael"},
            ...         {"match_id": "tournamentA", "round": 2, "winner": "Hanna"},
            ...         {
            ...             "match_id": "tournamentA",
            ...             "round": 3,
            ...             "winner": "Ian",
            ...             "fighters": DataAPISet([
            ...                 UUID("0193539a-2770-8c09-a32a-111111111111"),
            ...             ]),
            ...         },
            ...         {"match_id": "fight7", "round": 1, "winner": "Joy"},
            ...         {"match_id": "fight7", "round": 2, "winner": "Kevin"},
            ...         {"match_id": "fight7", "round": 3, "winner": "Lauretta"},
            ...     ],
            ...     concurrency=10,
            ...     chunk_size=3,
            ... )
            >>> insert_result.inserted_ids
            [{'match_id': 'fight4', 'round': 1}, {'match_id': 'fight5', ...
            >>> insert_result.inserted_id_tuples
            [('fight4', 1), ('fight5', 1), ('fight5', 2), ('fight5', 3), ...
            >>>
            >>> # Ordered insertion
            >>> # (would stop on first failure; predictable end result on DB)
            >>> my_table.insert_many(
            ...     [
            ...         {"match_id": "fight5", "round": 1, "winner": "Adam0"},
            ...         {"match_id": "fight5", "round": 2, "winner": "Betta0"},
            ...         {"match_id": "fight5", "round": 3, "winner": "Caio0"},
            ...         {"match_id": "fight5", "round": 1, "winner": "Adam Zuul"},
            ...         {"match_id": "fight5", "round": 2, "winner": "Betta Vigo"},
            ...         {"match_id": "fight5", "round": 3, "winner": "Caio Gozer"},
            ...     ],
            ...     ordered=True,
            ... )
            TableInsertManyResult(inserted_ids=[{'match_id': 'fight5', 'round': 1}, ...

        Note:
            Unordered insertions are executed with some degree of concurrency,
            so it is usually better to prefer this mode unless the order in the
            row sequence is important.

        Note:
            A failure mode for this command is related to certain faulty rows
            found among those to insert: validation may fail, for example, if the
            vector length does not match the table schema.

            For an ordered insertion, the method will raise an exception at
            the first such faulty row -- nevertheless, all rows processed
            until then will end up being written to the database.

            For unordered insertions, if the error stems from faulty rows
            the insertion proceeds until exhausting the input rows: then,
            an exception is raised -- and all insertable rows will have been
            written to the database, including those "after" the troublesome ones.

            Errors occurring during an insert_many operation, for that reason,
            may result in a `TableInsertManyException` being raised.
            This exception allows to inspect the list of row IDs that were
            successfully inserted, while accessing at the same time the underlying
            "root errors" that made the full method call to fail.
        """

        _general_method_timeout_ms, _gmt_label = _first_valid_timeout(
            (general_method_timeout_ms, "general_method_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (
                self.api_options.timeout_options.general_method_timeout_ms,
                "general_method_timeout_ms",
            ),
        )
        _request_timeout_ms, _rt_label = _first_valid_timeout(
            (request_timeout_ms, "request_timeout_ms"),
            (self.api_options.timeout_options.request_timeout_ms, "request_timeout_ms"),
        )
        if concurrency is None:
            if ordered:
                _concurrency = 1
            else:
                _concurrency = DEFAULT_INSERT_MANY_CONCURRENCY
        else:
            _concurrency = concurrency
        if _concurrency > 1 and ordered:
            raise ValueError("Cannot run ordered insert_many concurrently.")
        if chunk_size is None:
            _chunk_size = DEFAULT_INSERT_MANY_CHUNK_SIZE
        else:
            _chunk_size = chunk_size
        _rows = list(rows)
        logger.info(f"inserting {len(_rows)} rows in '{self.name}'")
        raw_results: list[dict[str, Any]] = []
        im_payloads: list[dict[str, Any] | None] = []
        timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=_general_method_timeout_ms,
            timeout_label=_gmt_label,
        )
        if ordered:
            options = {"ordered": True, "returnDocumentResponses": True}
            inserted_ids: list[Any] = []
            inserted_id_tuples: list[Any] = []
            for i in range(0, len(_rows), _chunk_size):
                im_payload = self._converter_agent.preprocess_payload(
                    {
                        "insertMany": {
                            "documents": _rows[i : i + _chunk_size],
                            "options": options,
                        },
                    },
                    map2tuple_checker=map2tuple_checker_insert_many,
                )
                logger.info(f"insertMany on '{self.name}'")
                chunk_response = self._api_commander.request(
                    payload=im_payload,
                    raise_api_errors=False,
                    timeout_context=timeout_manager.remaining_timeout(
                        cap_time_ms=_request_timeout_ms,
                        cap_timeout_label=_rt_label,
                    ),
                )
                logger.info(f"finished insertMany on '{self.name}'")
                # accumulate the results in this call
                chunk_inserted_ids, chunk_inserted_ids_tuples = (
                    self._prepare_keys_from_status(chunk_response.get("status"))
                )
                inserted_ids += chunk_inserted_ids
                inserted_id_tuples += chunk_inserted_ids_tuples
                raw_results += [chunk_response]
                im_payloads += [im_payload]
                # if errors, quit early
                if chunk_response.get("errors", []):
                    response_exception = DataAPIResponseException.from_response(
                        command=im_payload,
                        raw_response=chunk_response,
                    )
                    raise TableInsertManyException(
                        inserted_ids=inserted_ids,
                        inserted_id_tuples=inserted_id_tuples,
                        exceptions=[response_exception],
                    )

            # return
            full_result = TableInsertManyResult(
                raw_results=raw_results,
                inserted_ids=inserted_ids,
                inserted_id_tuples=inserted_id_tuples,
            )
            logger.info(f"finished inserting {len(_rows)} rows in '{self.name}'")
            return full_result

        else:
            # unordered: concurrent or not, do all of them and parse the results
            options = {"ordered": False, "returnDocumentResponses": True}
            if _concurrency > 1:
                with ThreadPoolExecutor(max_workers=_concurrency) as executor:

                    def _chunk_insertor(
                        row_chunk: list[dict[str, Any]],
                    ) -> tuple[dict[str, Any] | None, dict[str, Any]]:
                        im_payload = self._converter_agent.preprocess_payload(
                            {
                                "insertMany": {
                                    "documents": row_chunk,
                                    "options": options,
                                },
                            },
                            map2tuple_checker=map2tuple_checker_insert_many,
                        )
                        logger.info(f"insertMany(chunk) on '{self.name}'")
                        im_response = self._api_commander.request(
                            payload=im_payload,
                            raise_api_errors=False,
                            timeout_context=timeout_manager.remaining_timeout(
                                cap_time_ms=_request_timeout_ms,
                                cap_timeout_label=_rt_label,
                            ),
                        )
                        logger.info(f"finished insertMany(chunk) on '{self.name}'")
                        return im_payload, im_response

                    raw_pl_results_pairs = list(
                        executor.map(
                            _chunk_insertor,
                            (
                                _rows[i : i + _chunk_size]
                                for i in range(0, len(_rows), _chunk_size)
                            ),
                        )
                    )
                    if raw_pl_results_pairs:
                        im_payloads, raw_results = list(zip(*raw_pl_results_pairs))
                    else:
                        im_payloads, raw_results = [], []

            else:
                for i in range(0, len(_rows), _chunk_size):
                    im_payload = self._converter_agent.preprocess_payload(
                        {
                            "insertMany": {
                                "documents": _rows[i : i + _chunk_size],
                                "options": options,
                            },
                        },
                        map2tuple_checker=map2tuple_checker_insert_many,
                    )
                    logger.info(f"insertMany(chunk) on '{self.name}'")
                    im_response = self._api_commander.request(
                        payload=im_payload,
                        raise_api_errors=False,
                        timeout_context=timeout_manager.remaining_timeout(
                            cap_time_ms=_request_timeout_ms,
                            cap_timeout_label=_rt_label,
                        ),
                    )
                    logger.info(f"finished insertMany(chunk) on '{self.name}'")
                    raw_results.append(im_response)
                    im_payloads.append(im_payload)
            # recast raw_results. Each response has its schema: unfold appropriately
            ids_and_tuples_per_chunk = [
                self._prepare_keys_from_status(chunk_response.get("status"))
                for chunk_response in raw_results
            ]
            inserted_ids = [
                inserted_id
                for chunk_ids, _ in ids_and_tuples_per_chunk
                for inserted_id in chunk_ids
            ]
            inserted_id_tuples = [
                inserted_id_tuple
                for _, chunk_id_tuples in ids_and_tuples_per_chunk
                for inserted_id_tuple in chunk_id_tuples
            ]
            # check-raise
            response_exceptions = [
                DataAPIResponseException.from_response(
                    command=chunk_payload,
                    raw_response=chunk_response,
                )
                for chunk_payload, chunk_response in zip(im_payloads, raw_results)
                if chunk_response.get("errors", [])
            ]
            if response_exceptions:
                raise TableInsertManyException(
                    inserted_ids=inserted_ids,
                    inserted_id_tuples=inserted_id_tuples,
                    exceptions=response_exceptions,
                )

            # return
            full_result = TableInsertManyResult(
                raw_results=raw_results,
                inserted_ids=inserted_ids,
                inserted_id_tuples=inserted_id_tuples,
            )
            logger.info(f"finished inserting {len(_rows)} rows in '{self.name}'")
            return full_result

    @overload
    def find(
        self,
        filter: FilterType | None = None,
        *,
        projection: ProjectionType | None = None,
        row_type: None = None,
        skip: int | None = None,
        limit: int | None = None,
        include_similarity: bool | None = None,
        include_sort_vector: bool | None = None,
        sort: SortType | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> TableFindCursor[ROW, ROW]: ...

    @overload
    def find(
        self,
        filter: FilterType | None = None,
        *,
        projection: ProjectionType | None = None,
        row_type: type[ROW2],
        skip: int | None = None,
        limit: int | None = None,
        include_similarity: bool | None = None,
        include_sort_vector: bool | None = None,
        sort: SortType | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> TableFindCursor[ROW, ROW2]: ...

    def find(
        self,
        filter: FilterType | None = None,
        *,
        projection: ProjectionType | None = None,
        row_type: type[ROW2] | None = None,
        skip: int | None = None,
        limit: int | None = None,
        include_similarity: bool | None = None,
        include_sort_vector: bool | None = None,
        sort: SortType | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> TableFindCursor[ROW, ROW2]:
        """
        Find rows on the table matching the provided filters
        and according to sorting criteria including vector similarity.

        The returned TableFindCursor object, representing the stream of results,
        can be iterated over, or consumed and manipulated in several other ways
        (see the examples below and the `TableFindCursor` documentation for details).
        Since the amount of returned items can be large, TableFindCursor is a lazy
        object, that fetches new data while it is being read using the Data API
        pagination mechanism.

        Invoking `.to_list()` on a TableFindCursor will cause it to consume all
        rows and materialize the entire result set as a list. This is not recommended
        if the amount of results is very large.

        Args:
            filter: a dictionary expressing which condition the returned rows
                must satisfy. The filter can use operators, such as "$eq" for equality,
                and require columns to compare with literal values. Simple examples
                are `{}` (zero filter, not recommended for large tables),
                `{"match_no": 123}` (a shorthand for `{"match_no": {"$eq": 123}}`,
                or `{"match_no": 123, "round": "C"}` (multiple conditions are
                implicitly combined with "$and").
                Please consult the Data API documentation for a more detailed
                explanation of table search filters and tips on their usage.
            projection: a prescription on which columns to return for the matching rows.
                The projection can take the form `{"column1": True, "column2": True}`.
                `{"*": True}` (i.e. return the whole row), or the complementary
                form that excludes columns: `{"column1": False, "column2": False}`.
                To optimize bandwidth usage, it is recommended to use a projection,
                especially to avoid unnecessary columns of type vector with
                high-dimensional embeddings.
            row_type: this parameter acts a formal specifier for the type checker.
                If omitted, the resulting cursor is implicitly a
                `TableFindCursor[ROW, ROW]`, i.e. maintains the same type for
                the items it returns as that for the rows in the table. Strictly
                typed code may want to specify this parameter especially when a
                projection is given.
            skip: if provided, it is a number of rows that would be obtained first
                in the response and are instead skipped.
            limit: a maximum amount of rows to get from the table. The returned cursor
                will stop yielding rows when either this number is reached or there
                really are no more matches in the table.
            include_similarity: a boolean to request the numeric value of the
                similarity to be returned as an added "$similarity" key in each returned
                row. It can be used meaningfully only in a vector search (see `sort`).
            include_sort_vector: a boolean to request the search query vector.
                If set to True (and if the search is a vector search), calling
                the `get_sort_vector` method on the returned cursor will yield
                the vector used for the ANN search.
            sort: this dictionary parameter controls the order in which the rows
                are returned. The sort parameter can express either a vector search or
                a regular (ascending/descending, even hierarchical) sorting.
                * For a vector search the parameter takes the form
                `{"vector_column": qv}`, with the query vector `qv` of the appropriate
                type (list of floats or DataAPIVector). If the table has automatic
                embedding generation ("vectorize") enabled on that column, the form
                `{"vectorize_enabled_column": "query text"}` is also valid.
                * In the case of non-vector sorting, the parameter specifies the
                column(s) and the ascending/descending ordering required.
                If multiple columns are provided, the sorting applies them
                hierarchically to the rows. Examples are `{"score": SortMode.ASCENDING}`
                (equivalently `{"score": +1}`), `{"score": +1, "when": -1}`.
                Note that, depending on the column(s) chosen for sorting, the table
                partitioning structure, and the presence of indexes, the sorting
                may be done in-memory by the API. In that case, there may be performance
                implications and limitations on the amount of items returned.
                Consult the Data API documentation for more details on this topic.
            request_timeout_ms: a timeout, in milliseconds, to impose on each
                individual HTTP request to the Data API to accomplish the operation.
                If not provided, this object's defaults apply.
            timeout_ms: an alias for `request_timeout_ms`.

        Returns:
            a TableFindCursor object, that can be iterated over (and manipulated
            in several ways). The cursor, if needed, handles pagination under the hood
            as the rows are consumed.

        Note:
            As the rows are retrieved in chunks progressively, while the cursor
            is being iterated over, it is possible that the actual results
            obtained will reflect changes occurring to the table contents in
            real time.

        Examples:
            >>> # Iterate over results:
            >>> for row in my_table.find({"match_id": "challenge6"}):
            ...     print(f"(R:{row['round']}): winner {row['winner']}")
            ...
            (R:1): winner Donna
            (R:2): winner Erick
            (R:3): winner Fiona
            >>> # Optimize bandwidth using a projection:
            >>> proj = {"round": True, "winner": True}
            >>> for row in my_table.find({"match_id": "challenge6"}, projection=proj):
            ...     print(f"(R:{row['round']}): winner {row['winner']}")
            ...
            (R:1): winner Donna
            (R:2): winner Erick
            (R:3): winner Fiona
            >>> # Filter on the partitioning:
            >>> my_table.find({"match_id": "challenge6"}).to_list()
            [{'match_id': 'challenge6', 'round': 1, 'fighters': DataAPISet([]), ...
            >>>
            >>> # Filter on primary key:
            >>> my_table.find({"match_id": "challenge6", "round": 1}).to_list()
            [{'match_id': 'challenge6', 'round': 1, 'fighters': DataAPISet([]), ...
            >>>
            >>> # Filter on a regular indexed column:
            >>> my_table.find({"winner": "Caio Gozer"}).to_list()
            [{'match_id': 'fight5', 'round': 3, 'fighters': DataAPISet([]), ...
            >>>
            >>> # Non-equality filter on a regular indexed column:
            >>> my_table.find({"score": {"$gte": 15}}).to_list()
            [{'match_id': 'fight4', 'round': 1, 'fighters': DataAPISet([UUID('0193...
            >>>
            >>> # Filter on a regular non-indexed column:
            >>> # (not recommended performance-wise)
            >>> my_table.find(
            ...     {"when": {
            ...         "$gte": DataAPITimestamp.from_string("1999-12-31T01:23:44Z")
            ...     }}
            ... ).to_list()
            The Data API returned a warning: {'errorCode': 'MISSING_INDEX', ...
            [{'match_id': 'fight4', 'round': 1, 'fighters': DataAPISet([UUID('0193...
            >>>
            >>> # Empty filter (not recommended performance-wise):
            >>> my_table.find({}).to_list()
            The Data API returned a warning: {'errorCode': 'ZERO_FILTER_OPERATIONS', ...
            [{'match_id': 'fight4', 'round': 1, 'fighters': DataAPISet([UUID('0193...
            >>>
            >>> # Filter on the primary key and a regular non-indexed column:
            >>> # (not recommended performance-wise)
            >>> my_table.find(
            ...     {"match_id": "fight5", "round": 3, "winner": "Caio Gozer"}
            ... ).to_list()
            The Data API returned a warning: {'errorCode': 'MISSING_INDEX', ...
            [{'match_id': 'fight5', 'round': 3, 'fighters': DataAPISet([]), ...
            >>>
            >>> # Filter on a regular non-indexed column (and incomplete primary key)
            >>> # (not recommended performance-wise)
            >>> my_table.find({"round": 3, "winner": "Caio Gozer"}).to_list()
            The Data API returned a warning: {'errorCode': 'MISSING_INDEX', ...
            [{'match_id': 'fight5', 'round': 3, 'fighters': DataAPISet([]), ...
            >>>
            >>> # Vector search with "sort" (on an appropriately-indexed vector column):
            >>> my_table.find(
            ...     {},
            ...     sort={"m_vector": DataAPIVector([0.2, 0.3, 0.4])},
            ...     projection={"winner": True},
            ...     limit=3,
            ... ).to_list()
            [{'winner': 'Donna'}, {'winner': 'Victor'}]
            >>>
            >>> # Hybrid search with vector sort and non-vector filtering:
            >>> my_table.find(
            ...     {"match_id": "fight4"},
            ...     sort={"m_vector": DataAPIVector([0.2, 0.3, 0.4])},
            ...     projection={"winner": True},
            ...     limit=3,
            ... ).to_list()
            [{'winner': 'Victor'}]
            >>>
            >>> # Return the numeric value of the vector similarity
            >>> # (also demonstrating that one can pass a plain list for a vector):
            >>> my_table.find(
            ...     {},
            ...     sort={"m_vector": [0.2, 0.3, 0.4]},
            ...     projection={"winner": True},
            ...     limit=3,
            ...     include_similarity=True,
            ... ).to_list()
            [{'winner': 'Donna', '$similarity': 0.515}, {'winner': 'Victor', ...
            >>>
            >>> # Non-vector sorting on a 'partitionSort' column:
            >>> my_table.find(
            ...     {"match_id": "fight5"},
            ...     sort={"round": SortMode.DESCENDING},
            ...     projection={"winner": True},
            ... ).to_list()
            [{'winner': 'Caio Gozer'}, {'winner': 'Betta Vigo'}, ...
            >>>
            >>> # Using `skip` and `limit`:
            >>> my_table.find(
            ...     {"match_id": "fight5"},
            ...     sort={"round": SortMode.DESCENDING},
            ...     projection={"winner": True},
            ...     skip=1,
            ...     limit=2,
            ... ).to_list()
            The Data API returned a warning: {'errorCode': 'IN_MEMORY_SORTING...
            [{'winner': 'Betta Vigo'}, {'winner': 'Adam Zuul'}]
            >>>
            >>> # Non-vector sorting on a regular column:
            >>> # (not recommended performance-wise)
            >>> my_table.find(
            ...     {"match_id": "fight5"},
            ...     sort={"winner": SortMode.ASCENDING},
            ...     projection={"winner": True},
            ... ).to_list()
            The Data API returned a warning: {'errorCode': 'IN_MEMORY_SORTING...
            [{'winner': 'Adam Zuul'}, {'winner': 'Betta Vigo'}, ...
            >>>
            >>> # Using `.map()` on a cursor:
            >>> winner_cursor = my_table.find(
            ...     {"match_id": "fight5"},
            ...     sort={"round": SortMode.DESCENDING},
            ...     projection={"winner": True},
            ...     limit=5,
            ... )
            >>> print("/".join(winner_cursor.map(lambda row: row["winner"].upper())))
            CAIO GOZER/BETTA VIGO/ADAM ZUUL
            >>>
            >>> # Some other examples of cursor manipulation
            >>> matches_cursor = my_table.find(
            ...     sort={"m_vector": DataAPIVector([-0.1, 0.15, 0.3])}
            ... )
            >>> matches_cursor.has_next()
            True
            >>> next(matches_cursor)
            {'match_id': 'fight4', 'round': 1, 'fighters': DataAPISet([UUID('0193...
            >>> matches_cursor.consumed
            1
            >>> matches_cursor.rewind()
            >>> matches_cursor.consumed
            0
            >>> matches_cursor.has_next()
            True
            >>> matches_cursor.close()
            >>> try:
            ...     next(matches_cursor)
            ... except StopIteration:
            ...     print("StopIteration triggered.")
            ...
            StopIteration triggered.
        """

        # lazy-import here to avoid circular import issues
        from astrapy.cursors import TableFindCursor

        _request_timeout_ms, _rt_label = _first_valid_timeout(
            (request_timeout_ms, "request_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (self.api_options.timeout_options.request_timeout_ms, "request_timeout_ms"),
        )
        return (
            TableFindCursor(
                table=self,
                request_timeout_ms=_request_timeout_ms,
                overall_timeout_ms=None,
                request_timeout_label=_rt_label,
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
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> ROW | None:
        """
        Run a search according to the given filtering and sorting criteria
        and return the top row matching it, or nothing if there are none.

        The parameters are analogous to some of the parameters to the `find` method
        (which has a few more that do not make sense in this case, such as `limit`).

        Args:
            filter: a dictionary expressing which condition the returned row
                must satisfy. The filter can use operators, such as "$eq" for equality,
                and require columns to compare with literal values. Simple examples
                are `{}` (zero filter), `{"match_no": 123}` (a shorthand for
                `{"match_no": {"$eq": 123}}`, or `{"match_no": 123, "round": "C"}`
                (multiple conditions are implicitly combined with "$and").
                Please consult the Data API documentation for a more detailed
                explanation of table search filters and tips on their usage.
            projection: a prescription on which columns to return for the matching row.
                The projection can take the form `{"column1": True, "column2": True}`.
                `{"*": True}` (i.e. return the whole row), or the complementary
                form that excludes columns: `{"column1": False, "column2": False}`.
                To optimize bandwidth usage, it is recommended to use a projection,
                especially to avoid unnecessary columns of type vector with
                high-dimensional embeddings.
            include_similarity: a boolean to request the numeric value of the
                similarity to be returned as an added "$similarity" key in the returned
                row. It can be used meaningfully only in a vector search (see `sort`).
            sort: this dictionary parameter controls the sorting order, hence determines
                which row is being returned.
                The sort parameter can express either a vector search or
                a regular (ascending/descending, even hierarchical) sorting.
                * For a vector search the parameter takes the form
                `{"vector_column": qv}`, with the query vector `qv` of the appropriate
                type (list of floats or DataAPIVector). If the table has automatic
                embedding generation ("vectorize") enabled on that column, the form
                `{"vectorize_enabled_column": "query text"}` is also valid.
                * In the case of non-vector sorting, the parameter specifies the
                column(s) and the ascending/descending ordering required.
                If multiple columns are provided, the sorting applies them
                hierarchically to the rows. Examples are `{"score": SortMode.ASCENDING}`
                (equivalently `{"score": +1}`), `{"score": +1, "when": -1}`.
                Note that, depending on the column(s) chosen for sorting, the table
                partitioning structure, and the presence of indexes, the sorting
                may be done in-memory by the API. In that case, there may be performance
                implications.
                Consult the Data API documentation for more details on this topic.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a dictionary expressing the result if a row is found, otherwise None.

        Examples:
            >>> from astrapy.constants import SortMode
            >>> from astrapy.data_types import DataAPITimestamp, DataAPIVector
            >>>
            >>> # Filter on the partitioning:
            >>> my_table.find_one({"match_id": "challenge6"})
            {'match_id': 'challenge6', 'round': 1, 'fighters': DataAPISet([]), ...
            >>>
            >>> # A find with no matches:
            >>> str(my_table.find_one({"match_id": "not_real"}))
            'None'
            >>>
            >>> # Optimize bandwidth using a projection:
            >>> my_table.find_one(
            ...     {"match_id": "challenge6"},
            ...     projection={"round": True, "winner": True},
            ... )
            {'round': 1, 'winner': 'Donna'}
            >>>
            >>> # Filter on primary key:
            >>> my_table.find_one({"match_id": "challenge6", "round": 1})
            {'match_id': 'challenge6', 'round': 1, 'fighters': DataAPISet([]), ...
            >>>
            >>> # Filter on a regular indexed column:
            >>> my_table.find_one({"winner": "Caio Gozer"})
            {'match_id': 'fight5', 'round': 3, 'fighters': DataAPISet([]), ...
            >>>
            >>> # Non-equality filter on a regular indexed column:
            >>> my_table.find_one({"score": {"$gte": 15}})
            {'match_id': 'fight4', 'round': 1, 'fighters': DataAPISet([UUID('0193...
            >>>
            >>> # Filter on a regular non-indexed column:
            >>> # (not recommended performance-wise)
            >>> my_table.find_one(
            ...     {"when": {
            ...         "$gte": DataAPITimestamp.from_string("1999-12-31T01:23:44Z")
            ...     }}
            ... )
            The Data API returned a warning: {'errorCode': 'MISSING_INDEX', ...
            {'match_id': 'fight4', 'round': 1, 'fighters': DataAPISet([UUID('0193...
            >>>
            >>> # Empty filter:
            >>> my_table.find_one({})
            The Data API returned a warning: {'errorCode': 'ZERO_FILTER_OPERATIONS', ...
            {'match_id': 'fight4', 'round': 1, 'fighters': DataAPISet([UUID('0193...
            >>>
            >>> # Filter on the primary key and a regular non-indexed column:
            >>> # (not recommended performance-wise)
            >>> my_table.find_one(
            ...     {"match_id": "fight5", "round": 3, "winner": "Caio Gozer"}
            ... )
            The Data API returned a warning: {'errorCode': 'MISSING_INDEX', ...
            {'match_id': 'fight5', 'round': 3, 'fighters': DataAPISet([]), ...
            >>>
            >>> # Filter on a regular non-indexed column (and incomplete primary key)
            >>> # (not recommended performance-wise)
            >>> my_table.find_one({"round": 3, "winner": "Caio Gozer"})
            The Data API returned a warning: {'errorCode': 'MISSING_INDEX', ...
            {'match_id': 'fight5', 'round': 3, 'fighters': DataAPISet([]), ...
            >>>
            >>> # Vector search with "sort" (on an appropriately-indexed vector column):
            >>> my_table.find_one(
            ...     {},
            ...     sort={"m_vector": DataAPIVector([0.2, 0.3, 0.4])},
            ...     projection={"winner": True},
            ... )
            {'winner': 'Donna'}
            >>>
            >>> # Hybrid search with vector sort and non-vector filtering:
            >>> my_table.find_one(
            ...     {"match_id": "fight4"},
            ...     sort={"m_vector": DataAPIVector([0.2, 0.3, 0.4])},
            ...     projection={"winner": True},
            ... )
            {'winner': 'Victor'}
            >>>
            >>> # Return the numeric value of the vector similarity
            >>> # (also demonstrating that one can pass a plain list for a vector):
            >>> my_table.find_one(
            ...     {},
            ...     sort={"m_vector": [0.2, 0.3, 0.4]},
            ...     projection={"winner": True},
            ...     include_similarity=True,
            ... )
            {'winner': 'Donna', '$similarity': 0.515}
            >>>
            >>> # Non-vector sorting on a 'partitionSort' column:
            >>> my_table.find_one(
            ...     {"match_id": "fight5"},
            ...     sort={"round": SortMode.DESCENDING},
            ...     projection={"winner": True},
            ... )
            {'winner': 'Caio Gozer'}
            >>>
            >>> # Non-vector sorting on a regular column:
            >>> # (not recommended performance-wise)
            >>> my_table.find_one(
            ...     {"match_id": "fight5"},
            ...     sort={"winner": SortMode.ASCENDING},
            ...     projection={"winner": True},
            ... )
            The Data API returned a warning: {'errorCode': 'IN_MEMORY_SORTING...
            {'winner': 'Adam Zuul'}
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
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
            },
            map2tuple_checker=None,
        )
        fo_response = self._api_commander.request(
            payload=fo_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
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
            similarity_pseudocolumn="$similarity" if include_similarity else None,
        )

    def distinct(
        self,
        key: str | Iterable[str | int],
        *,
        filter: FilterType | None = None,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[Any]:
        """
        Return a list of the unique values of `key` across the rows
        in the table that match the provided filter.

        Args:
            key: the name of the field whose value is inspected across rows.
                Keys can be just column names (as is typically the case), but
                the dot-notation is also accepted to mean subkeys or indices
                within lists (for example, "map_column.subkey" or "list_column.2").
                If a column has literal dots or ampersands in its name, this
                parameter must be escaped to be treated properly.
                The key can also be a list of strings and numbers, in which case
                no escape is necessary: each item in the list is a field name/index,
                for example ["map_column", "subkey"] or ["list_column", 2].
                For set and list columns, individual entries are "unrolled"
                automatically.
            filter: a dictionary expressing which condition the inspected rows
                must satisfy. The filter can use operators, such as "$eq" for equality,
                and require columns to compare with literal values. Simple examples
                are `{}` (zero filter), `{"match_no": 123}` (a shorthand for
                `{"match_no": {"$eq": 123}}`, or `{"match_no": 123, "round": "C"}`
                (multiple conditions are implicitly combined with "$and").
                Please consult the Data API documentation for a more detailed
                explanation of table search filters and tips on their usage.
            general_method_timeout_ms: a timeout, in milliseconds, for the whole
                requested operation (which may involve multiple API requests).
                This method, being based on `find` (see) may entail successive HTTP API
                requests, depending on the amount of involved rows.
                If not provided, this object's defaults apply.
            request_timeout_ms: a timeout, in milliseconds, for each API request.
                If not provided, this object's defaults apply.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a list of all different values for `key` found across the rows
            that match the filter. The result list has no repeated items.

        Examples:
            >>> my_table.distinct("winner", filter={"match_id": "challenge6"})
            ['Donna', 'Erick', 'Fiona']
            >>>
            >>> # distinct values across the whole table:
            >>> # (not recommended performance-wise)
            >>> my_table.distinct("winner")
            The Data API returned a warning: {'errorCode': 'ZERO_FILTER_OPERATIONS', ...
            ['Victor', 'Adam Zuul', 'Betta Vigo', 'Caio Gozer', 'Donna', 'Erick', ...
            >>>
            >>> # Over a column containing null values
            >>> # (also with composite filter):
            >>> my_table.distinct(
            ...     "score",
            ...     filter={"match_id": {"$in": ["fight4", "tournamentA"]}},
            ... )
            [18, None]
            >>>
            >>> # distinct over a set column (automatically "unrolled"):
            >>> my_table.distinct(
            ...     "fighters",
            ...     filter={"match_id": {"$in": ["fight4", "tournamentA"]}},
            ... )
            [UUID('0193539a-2770-8c09-a32a-111111111111'), UUID('019353e3-00b4-...

        Note:
            It must be kept in mind that `distinct` is a client-side operation,
            which effectively browses all required rows using the logic
            of the `find` method and collects the unique values found for `key`.
            As such, there may be performance, latency and ultimately
            billing implications if the amount of matching rows is large.

        Note:
            For details on the behaviour of "distinct" in conjunction with
            real-time changes in the table contents, see the
            Note of the `find` command.
        """

        # lazy-import here to avoid circular import issues
        from astrapy.cursors import TableFindCursor

        _general_method_timeout_ms, _gmt_label = _first_valid_timeout(
            (general_method_timeout_ms, "general_method_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (
                self.api_options.timeout_options.general_method_timeout_ms,
                "general_method_timeout_ms",
            ),
        )
        _request_timeout_ms, _rt_label = _first_valid_timeout(
            (request_timeout_ms, "request_timeout_ms"),
            (self.api_options.timeout_options.request_timeout_ms, "request_timeout_ms"),
        )
        # preparing cursor:
        _extractor = _create_document_key_extractor(key)
        _key = _reduce_distinct_key_to_shallow_safe(key)
        # relaxing the type hint (limited to within this method body)
        f_cursor: TableFindCursor[dict[str, Any], dict[str, Any]] = (
            TableFindCursor(
                table=self,
                request_timeout_ms=_request_timeout_ms,
                overall_timeout_ms=_general_method_timeout_ms,
                request_timeout_label=_rt_label,
                overall_timeout_label=_gmt_label,
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
                _item_hash = _hash_table_document(
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
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
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
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            the exact count of matching rows.

        Examples:
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

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        cd_payload = {"countDocuments": {"filter": filter}}
        logger.info(f"countDocuments on '{self.name}'")
        cd_response = self._api_commander.request(
            payload=cd_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
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

    def estimated_document_count(
        self,
        *,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> int:
        """
        Query the API server for an estimate of the document count in the table.

        Contrary to `count_documents`, this method has no filtering parameters.

        Args:
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a server-provided estimate count of the documents in the table.

        Example:
            >>> my_table.estimated_document_count()
            5820
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        ed_payload: dict[str, Any] = {"estimatedDocumentCount": {}}
        logger.info(f"estimatedDocumentCount on '{self.name}'")
        ed_response = self._api_commander.request(
            payload=ed_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
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

    def update_one(
        self,
        filter: FilterType,
        update: dict[str, Any],
        *,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        """
        Update a single document on the table, changing some or all of the columns,
        with the implicit behaviour of inserting a new row if no match is found.

        Args:
            filter: a predicate expressing the table primary key in full,
                i.e. a dictionary defining values for all columns that form the
                primary key. An example may be `{"match_id": "fight4", "round": 1}`.
            update: the update prescription to apply to the row, expressed
                as a dictionary conforming to the Data API syntax. The update
                operators for tables are `$set` and `$unset` (in particular,
                setting a column to None has the same effect as the $unset operator).
                Examples are `{"$set": {"round": 12}}` and
                `{"$unset": {"winner": "", "score": ""}}`.
                Note that the update operation cannot alter the primary key columns.
                See the Data API documentation for more details.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Examples:
            >>> from astrapy.data_types import DataAPISet
            >>>
            >>> # Set a new value for a column
            >>> my_table.update_one(
            ...     {"match_id": "fight4", "round": 1},
            ...     update={"$set": {"winner": "Winona"}},
            ... )
            >>>
            >>> # Set a new value for a column while unsetting another colum
            >>> my_table.update_one(
            ...     {"match_id": "fight4", "round": 1},
            ...     update={"$set": {"winner": None, "score": 24}},
            ... )
            >>>
            >>> # Set a 'set' column to empty
            >>> my_table.update_one(
            ...     {"match_id": "fight4", "round": 1},
            ...     update={"$set": {"fighters": DataAPISet()}},
            ... )
            >>>
            >>> # Set a 'set' column to empty using None
            >>> my_table.update_one(
            ...     {"match_id": "fight4", "round": 1},
            ...     update={"$set": {"fighters": None}},
            ... )
            >>>
            >>> # Set a 'set' column to empty using a regular (empty) set
            >>> my_table.update_one(
            ...     {"match_id": "fight4", "round": 1},
            ...     update={"$set": {"fighters": set()}},
            ... )
            >>>
            >>> # Set a 'set' column to empty using $unset
            >>> my_table.update_one(
            ...     {"match_id": "fight4", "round": 1},
            ...     update={"$unset": {"fighters": None}},
            ... )
            >>>
            >>> # A non-existing primary key creates a new row
            >>> my_table.update_one(
            ...     {"match_id": "bar_fight", "round": 4},
            ...     update={"$set": {"score": 8, "winner": "Jack"}},
            ... )
            >>>
            >>> # Delete column values for a row (they'll read as None now)
            >>> my_table.update_one(
            ...     {"match_id": "challenge6", "round": 2},
            ...     update={"$unset": {"winner": None, "score": None}},
            ... )

        Note:
            a row created entirely with update operations (as opposed to insertions)
            may, correspondingly, be deleted by means of an $unset update on all columns.
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        uo_payload = self._converter_agent.preprocess_payload(
            {
                "updateOne": {
                    k: v
                    for k, v in {
                        "filter": filter,
                        "update": update,
                    }.items()
                    if v is not None
                }
            },
            map2tuple_checker=map2tuple_checker_update_one,
        )
        logger.info(f"updateOne on '{self.name}'")
        uo_response = self._api_commander.request(
            payload=uo_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        logger.info(f"finished updateOne on '{self.name}'")
        if "status" in uo_response:
            # the contents are disregarded and the method just returns:
            return
        else:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from updateOne API command.",
                raw_response=uo_response,
            )

    def delete_one(
        self,
        filter: FilterType,
        *,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        """
        Delete a row, matching the provided value of the primary key.
        If no row is found with that primary key, the method does nothing.

        Args:
            filter: a predicate expressing the table primary key in full,
                i.e. a dictionary defining values for all columns that form the
                primary key. A row (at most one) is deleted if it matches that primary
                key. An example filter may be `{"match_id": "fight4", "round": 1}`.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Examples:
            >>> # Count the rows matching a certain filter
            >>> len(my_table.find({"match_id": "fight7"}).to_list())
            3
            >>>
            >>> # Delete a row belonging to the group
            >>> my_table.delete_one({"match_id": "fight7", "round": 2})
            >>>
            >>> # Count again
            >>> len(my_table.find({"match_id": "fight7"}).to_list())
            2
            >>>
            >>> # Attempt the delete again (nothing to delete)
            >>> my_table.delete_one({"match_id": "fight7", "round": 2})
            >>>
            >>> # The count is unchanged
            >>> len(my_table.find({"match_id": "fight7"}).to_list())
            2
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
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
            },
            map2tuple_checker=None,
        )
        logger.info(f"deleteOne on '{self.name}'")
        do_response = self._api_commander.request(
            payload=do_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
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
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        """
        Delete all rows matching a provided filter condition.
        This operation can target from a single row to the entirety of the table.

        Args:
            filter: a filter dictionary to specify which row(s) must be deleted.
                1. If the filter is in the form `{"pk1": val1, "pk2": val2 ...}`
                and specified the primary key in full, at most one row is deleted,
                the one with that primary key.
                2. If the table has "partitionSort" columns, some or all of them
                may be left out (the least significant of them can also employ
                an inequality, or range, predicate): a range of rows, but always
                within a single partition, will be deleted.
                3. If an empty filter, `{}`, is passed, this operation empties
                the table completely. *USE WITH CARE*.
                4. Other kinds of filtering clauses are forbidden.
                In the following examples, the table is partitioned
                by columns ["pa1", "pa2"] and has partitionSort "ps1" and "ps2" in that
                order.
                Valid filter examples:
                - `{"pa1": x, "pa2": y, "ps1": z, "ps2": t}`: deletes one row
                - `{"pa1": x, "pa2": y, "ps1": z}`: deletes multiple rows
                - `{"pa1": x, "pa2": y, "ps1": z, "ps2": {"$lt": q}}`: del. multiple rows
                - `{"pa1": x, "pa2": y}`: deletes all rows in the partition
                - `{}`: empties the table (*CAUTION*)
                Invalid filter examples:
                - `{"pa1": x}`: incomplete partition key
                - `{"pa1": x, "ps1" z}`: incomplete partition key (whatever is added)
                - `{"pa1": x, "pa2": y, "ps1": {"$lt": r}, "ps2": t}`: inequality on
                  a non-least-significant partitionSort column provided.
                - `{"pa1": x, "pa2": y, "ps2": t}`: cannot skip "ps1"
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Examples:
            >>> # Delete a single row (full primary key specified):
            >>> my_table.delete_many({"match_id": "fight4", "round": 1})
            >>>
            >>> # Delete part of a partition (inequality on the
            >>> # last-mentioned 'partitionSort' column):
            >>> my_table.delete_many({"match_id": "fight5", "round": {"$gte": 5}})
            >>>
            >>> # Delete a whole partition (leave 'partitionSort' unspecified):
            >>> my_table.delete_many({"match_id": "fight7"})
            >>>
            >>> # empty the table entirely with empty filter (*CAUTION*):
            >>> my_table.delete_many({})
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
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
            },
            map2tuple_checker=None,
        )
        logger.info(f"deleteMany on '{self.name}'")
        dm_response = self._api_commander.request(
            payload=dm_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
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
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        """
        Drop the table, i.e. delete it from the database along with
        all the rows stored therein.

        Args:
            if_exists: if passed as True, trying to drop a non-existing table
                will not error, just silently do nothing instead. If not provided,
                the API default behaviour will hold.
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Example:
            >>> # List tables:
            >>> my_table.database.list_table_names()
            ['games']
            >>>
            >>> # Drop this table:
            >>> my_table.drop()
            >>>
            >>> # List tables again:
            >>> my_table.database.list_table_names()
            []
            >>>
            >>> # Try working on the table now:
            >>> from astrapy.exceptions import DataAPIResponseException
            >>> try:
            ...     my_table.find_one({})
            ... except DataAPIResponseException as err:
            ...     print(str(err))
            ...
            Collection does not exist [...] (COLLECTION_NOT_EXIST)

        Note:
            Use with caution.

        Note:
            Once the method succeeds, methods on this object can still be invoked:
            however, this hardly makes sense as the underlying actual table
            is no more.
            It is responsibility of the developer to design a correct flow
            which avoids using a deceased collection any further.
        """

        logger.info(f"dropping table '{self.name}' (self)")
        self.database.drop_table(
            self.name,
            if_exists=if_exists,
            table_admin_timeout_ms=table_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info(f"finished dropping table '{self.name}' (self)")

    def command(
        self,
        body: dict[str, Any] | None,
        *,
        raise_api_errors: bool = True,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        Send a POST request to the Data API for this table with
        an arbitrary, caller-provided payload.
        No transformations or type conversions are made on the provided payload.

        Args:
            body: a JSON-serializable dictionary, the payload of the request.
            raise_api_errors: if True, responses with a nonempty 'errors' field
                result in an astrapy exception being raised.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a dictionary with the response of the HTTP request.

        Example:
            >>> my_table.command({
            ...     "findOne": {
            ...         "filter": {"match_id": "fight4"},
            ...         "projection": {"winner": True},
            ...     }
            ... })
            {'data': {'document': {'winner': 'Victor'}}, 'status': ...  # shortened
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
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
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        logger.info(f"finished command={_cmd_desc} on '{self.name}'")
        return command_result


class AsyncTable(Generic[ROW]):
    """
    A Data API table, the object to interact with the Data API for structured data,
    especially for DDL operations.
    This class has an asynchronous interface for use with asyncio.

    This class is not meant for direct instantiation by the user, rather
    it is obtained by invoking methods such as `get_table` of AsyncDatabase,
    wherefrom the AsyncTable inherits its API options such as authentication
    token and API endpoint.
    In order to create a table, instead, one should call the `create_table`
    method of AsyncDatabase, providing a table definition parameter that can be built
    in different ways (see the `CreateTableDefinition` object and examples below).

    Args:
        database: an AsyncDatabase object, instantiated earlier. This represents
            the database the table belongs to.
        name: the table name. This parameter should match an existing
            table on the database.
        keyspace: this is the keyspace to which the table belongs.
            If nothing is specified, the database's working keyspace is used.
        api_options: a complete specification of the API Options for this instance.

    Examples:
        >>> # NOTE: may require slight adaptation to an async context.
        >>>
        >>> from astrapy import DataAPIClient, AsyncTable
        >>> client = astrapy.DataAPIClient()
        >>> async_database = client.get_async_database(
        ...     "https://01234567-....apps.astra.datastax.com",
        ...     token="AstraCS:..."
        ... )

        >>> # Create a table using the fluent syntax for definition
        >>> from astrapy.constants import SortMode
        >>> from astrapy.info import (
        ...     CreateTableDefinition,
        ...     ColumnType,
        ... )
        >>> table_definition = (
        ...     CreateTableDefinition.builder()
        ...     .add_column("match_id", ColumnType.TEXT)
        ...     .add_column("round", ColumnType.INT)
        ...     .add_vector_column("m_vector", dimension=3)
        ...     .add_column("score", ColumnType.INT)
        ...     .add_column("when", ColumnType.TIMESTAMP)
        ...     .add_column("winner", ColumnType.TEXT)
        ...     .add_set_column("fighters", ColumnType.UUID)
        ...     .add_partition_by(["match_id"])
        ...     .add_partition_sort({"round": SortMode.ASCENDING})
        ...     .build()
        ... )
        >>> my_table = await async_database.create_table(
        ...     "games",
        ...     definition=table_definition,
        ... )

        >>> # Create a table with the definition as object
        >>> # (and do not raise an error if the table exists already)
        >>> from astrapy.info import (
        ...     CreateTableDefinition,
        ...     TablePrimaryKeyDescriptor,
        ...     TableScalarColumnTypeDescriptor,
        ...     TableValuedColumnType,
        ...     TableValuedColumnTypeDescriptor,
        ...     TableVectorColumnTypeDescriptor,
        ... )
        >>> table_definition_1 = CreateTableDefinition(
        ...     columns={
        ...         "match_id": TableScalarColumnTypeDescriptor(
        ...             ColumnType.TEXT,
        ...         ),
        ...         "round": TableScalarColumnTypeDescriptor(
        ...             ColumnType.INT,
        ...         ),
        ...         "m_vector": TableVectorColumnTypeDescriptor(
        ...             column_type="vector", dimension=3
        ...         ),
        ...         "score": TableScalarColumnTypeDescriptor(
        ...             ColumnType.INT,
        ...         ),
        ...         "when": TableScalarColumnTypeDescriptor(
        ...             ColumnType.TIMESTAMP,
        ...         ),
        ...         "winner": TableScalarColumnTypeDescriptor(
        ...             ColumnType.TEXT,
        ...         ),
        ...         "fighters": TableValuedColumnTypeDescriptor(
        ...             column_type=TableValuedColumnType.SET,
        ...             value_type=ColumnType.UUID,
        ...         ),
        ...     },
        ...     primary_key=TablePrimaryKeyDescriptor(
        ...         partition_by=["match_id"],
        ...         partition_sort={"round": SortMode.ASCENDING},
        ...     ),
        ... )
        >>> my_table_1 = await async_database.create_table(
        ...     "games",
        ...     definition=table_definition_1,
        ...     if_not_exists=True,
        ... )

        >>> # Create a table with the definition as plain dictionary
        >>> # (and do not raise an error if the table exists already)
        >>> table_definition_2 = {
        ...     "columns": {
        ...         "match_id": {"type": "text"},
        ...         "round": {"type": "int"},
        ...         "m_vector": {"type": "vector", "dimension": 3},
        ...         "score": {"type": "int"},
        ...         "when": {"type": "timestamp"},
        ...         "winner": {"type": "text"},
        ...         "fighters": {"type": "set", "valueType": "uuid"},
        ...     },
        ...     "primaryKey": {
        ...         "partitionBy": ["match_id"],
        ...         "partitionSort": {"round": 1},
        ...     },
        ... }
        >>> my_table_2 = await async_database.create_table(
        ...     "games",
        ...     definition=table_definition_2,
        ...     if_not_exists=True,
        ... )

        >>> # Get a reference to an existing table
        >>> # (no checks are performed on DB)
        >>> my_table_4 = async_database.get_table("my_already_existing_table")

    Note:
        creating an instance of AsyncTable does not trigger, in itself, actual
        creation of the table on the database. The latter should have been created
        beforehand, e.g. through the `create_table` method of a Database.
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
            raise ValueError("Attempted to create AsyncTable with 'keyspace' unset.")

        self._database = database._copy(
            keyspace=_keyspace, api_options=self.api_options
        )
        self._commander_headers = {
            **{DEFAULT_DATA_API_AUTH_HEADER: self.api_options.token.get_token()},
            **self.api_options.embedding_api_key.get_headers(),
            **self.api_options.reranking_api_key.get_headers(),
            **self.api_options.database_additional_headers,
        }
        self._api_commander = self._get_api_commander()
        self._converter_agent: _TableConverterAgent[ROW] = _TableConverterAgent(
            options=self.api_options.serdes_options,
        )

    def __repr__(self) -> str:
        _db_desc = f'database.api_endpoint="{self.database.api_endpoint}"'
        return (
            f'{self.__class__.__name__}(name="{self.name}", '
            f'keyspace="{self.keyspace}", {_db_desc}, '
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
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncTable[ROW]:
        arg_api_options = APIOptions(
            embedding_api_key=embedding_api_key,
            reranking_api_key=reranking_api_key,
        )
        final_api_options = self.api_options.with_override(api_options).with_override(
            arg_api_options
        )
        return AsyncTable(
            database=self.database,
            name=self.name,
            keyspace=self.keyspace,
            api_options=final_api_options,
        )

    def with_options(
        self: AsyncTable[ROW],
        *,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncTable[ROW]:
        """
        Create a clone of this table with some changed attributes.

        Args:
            embedding_api_key: optional API key(s) for interacting with the table.
                If an embedding service is configured, and this parameter is not None,
                each Data API call will include the necessary embedding-related headers
                as specified by this parameter. If a string is passed, it translates
                into the one "embedding api key" header
                (i.e. `astrapy.authentication.EmbeddingAPIKeyHeaderProvider`).
                For some vectorize providers/models, if using header-based authentication,
                specialized subclasses of `astrapy.authentication.EmbeddingHeadersProvider`
                should be supplied.
            reranking_api_key: optional API key(s) for interacting with the table.
                If a reranker is configured for the table, and this parameter
                is not None, Data API calls will include the appropriate
                reranker-related headers according to this parameter. Reranker services
                may not necessarily require this setting (e.g. if the service needs no
                authentication, or one is configured as part of the table
                definition relying on a "shared secret").
                If a string is passed, it is translated into an instance of
                `astrapy.authentication.RerankingAPIKeyHeaderProvider`.
            api_options: any additional options to set for the clone, in the form of
                an APIOptions instance (where one can set just the needed attributes).
                In case the same setting is also provided as named parameter,
                the latter takes precedence.

        Returns:
            a new AsyncTable instance.

        Example:
            >>> table_with_api_key_configured = my_async_table.with_options(
            ...     embedding_api_key="secret-key-0123abcd...",
            ... )
        """

        return self._copy(
            embedding_api_key=embedding_api_key,
            reranking_api_key=reranking_api_key,
            api_options=api_options,
        )

    def to_sync(
        self: AsyncTable[ROW],
        *,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        reranking_api_key: str | RerankingHeadersProvider | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> Table[ROW]:
        """
        Create a Table from this one. Save for the arguments
        explicitly provided as overrides, everything else is kept identical
        to this table in the copy (the database is converted into
        an async object).

        Args:
            embedding_api_key: optional API key(s) for interacting with the table.
                If an embedding service is configured, and this parameter is not None,
                each Data API call will include the necessary embedding-related headers
                as specified by this parameter. If a string is passed, it translates
                into the one "embedding api key" header
                (i.e. `astrapy.authentication.EmbeddingAPIKeyHeaderProvider`).
                For some vectorize providers/models, if using header-based authentication,
                specialized subclasses of `astrapy.authentication.EmbeddingHeadersProvider`
                should be supplied.
            reranking_api_key: optional API key(s) for interacting with the table.
                If a reranker is configured for the table, and this parameter
                is not None, Data API calls will include the appropriate
                reranker-related headers according to this parameter. Reranker services
                may not necessarily require this setting (e.g. if the service needs no
                authentication, or one is configured as part of the table
                definition relying on a "shared secret").
                If a string is passed, it is translated into an instance of
                `astrapy.authentication.RerankingAPIKeyHeaderProvider`.
            api_options: any additional options to set for the result, in the form of
                an APIOptions instance (where one can set just the needed attributes).
                In case the same setting is also provided as named parameter,
                the latter takes precedence.

        Returns:
            the new copy, a Table instance.

        Example:
            >>> my_async_table.to_sync().find_one(
            ...     {"match_id": "fight4"},
            ...     projection={"winner": True},
            ... )
            {"pk": 1, "column": "value}
        """

        arg_api_options = APIOptions(
            embedding_api_key=embedding_api_key,
            reranking_api_key=reranking_api_key,
        )
        final_api_options = self.api_options.with_override(api_options).with_override(
            arg_api_options
        )
        return Table(
            database=self.database.to_sync(),
            name=self.name,
            keyspace=self.keyspace,
            api_options=final_api_options,
        )

    async def definition(
        self,
        *,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> ListTableDefinition:
        """
        Query the Data API and return a structure defining the table schema.
        If there are no unsupported colums in the table, the return value has
        the same contents as could have been provided to a `create_table` method call.

        Args:
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Returns:
            A `ListTableDefinition` object, available for inspection.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> asyncio.run(my_table.definition())
            ListTableDefinition(columns=[match_id,round,fighters, ...  # shortened
        """

        _table_admin_timeout_ms, _ta_label = _select_singlereq_timeout_ta(
            timeout_options=self.api_options.timeout_options,
            table_admin_timeout_ms=table_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info(f"getting tables in search of '{self.name}'")
        self_descriptors = [
            table_desc
            for table_desc in await self.database._list_tables_ctx(
                keyspace=None,
                timeout_context=_TimeoutContext(
                    request_ms=_table_admin_timeout_ms,
                    label=_ta_label,
                ),
            )
            if table_desc.name == self.name
        ]
        logger.info(f"finished getting tables in search of '{self.name}'")
        if self_descriptors:
            return self_descriptors[0].definition
        else:
            raise RuntimeError(
                f"Table {self.keyspace}.{self.name} not found.",
            )

    async def info(
        self,
        *,
        database_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> TableInfo:
        """
        Return information on the table. This should not be confused with the table
        definition (i.e. the schema).

        Args:
            database_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying DevOps API request.
                If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `database_admin_timeout_ms`.
            timeout_ms: an alias for `database_admin_timeout_ms`.

        Returns:
            A TableInfo object for inspection.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> # Note: output reformatted for clarity.
            >>> asyncio.run(my_async_table.info())
            TableInfo(
                database_info=AstraDBDatabaseInfo(id=..., name=..., ...),
                keyspace='default_keyspace',
                name='games',
                full_name='default_keyspace.games'
            )
        """

        db_info = await self.database.info(
            database_admin_timeout_ms=database_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
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
            raise RuntimeError("The table's DB is set with keyspace=None")
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
        The fully-qualified table name within the database,
        in the form "keyspace.table_name".

        Example:
            >>> my_async_table.full_name
            'default_keyspace.my_table'
        """

        return f"{self.keyspace}.{self.name}"

    async def _create_generic_index(
        self,
        i_name: str,
        ci_definition: dict[str, Any],
        ci_command: str,
        if_not_exists: bool | None,
        table_admin_timeout_ms: int | None,
        request_timeout_ms: int | None,
        timeout_ms: int | None,
    ) -> None:
        ci_options: dict[str, bool]
        if if_not_exists is not None:
            ci_options = {"ifNotExists": if_not_exists}
        else:
            ci_options = {}
        _table_admin_timeout_ms, _ta_label = _select_singlereq_timeout_ta(
            timeout_options=self.api_options.timeout_options,
            table_admin_timeout_ms=table_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
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
            timeout_context=_TimeoutContext(
                request_ms=_table_admin_timeout_ms, label=_ta_label
            ),
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
        column: str | dict[str, str],
        *,
        options: TableIndexOptions | dict[str, Any] | None = None,
        if_not_exists: bool | None = None,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        """
        Create an index on a non-vector column of the table.

        This is a blocking operation: the method returns once the index
        is created and ready to use.

        For creation of a vector index, see method `create_vector_index` instead.

        Args:
            name: the name of the index. Index names must be unique across the keyspace.
            column: the table column on which the index is to be created.
                For a map column, besides a simple string, it can be an object
                in one of the two formats {"column": "$values"}, {"column": "$keys"},
            options: if passed, it must be an instance of `TableIndexOptions`,
                or an equivalent dictionary, which specifies index settings
                such as -- for a text column -- case-sensitivity and so on.
                See the `astrapy.info.TableIndexOptions` class for more details.
            if_not_exists: if set to True, the command will succeed even if an index
                with the specified name already exists (in which case no actual
                index creation takes place on the database). The API default of False
                means that an error is raised by the API in case of name collision.
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Examples:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> from astrapy.info import TableIndexOptions
            >>>
            >>> # create an index on a column
            >>> await my_async_table.create_index(
            ...     "score_index",
            ...     "score",
            ... )
            >>>
            >>> # create an index on a textual column, specifying indexing options
            >>> await my_async_table.create_index(
            ...     "winner_index",
            ...     "winner",
            ...     options=TableIndexOptions(
            ...         ascii=False,
            ...         normalize=True,
            ...         case_sensitive=False,
            ...     ),
            ... )
        """

        ci_definition: dict[str, Any] = TableIndexDefinition(
            column=column,
            options=TableIndexOptions.coerce(options or {}),
        ).as_dict()
        ci_command = "createIndex"
        return await self._create_generic_index(
            i_name=name,
            ci_definition=ci_definition,
            ci_command=ci_command,
            if_not_exists=if_not_exists,
            table_admin_timeout_ms=table_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )

    async def create_vector_index(
        self,
        name: str,
        column: str,
        *,
        options: TableVectorIndexOptions | dict[str, Any] | None = None,
        if_not_exists: bool | None = None,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        """
        Create a vector index on a vector column of the table, enabling vector
        similarity search operations on it.

        This is a blocking operation: the method returns once the index
        is created and ready to use.

        For creation of a non-vector index, see method `create_index` instead.

        Args:
            name: the name of the index. Index names must be unique across the keyspace.
            column: the table column, of type "vector" on which to create the index.
            options: an instance of `TableVectorIndexOptions`, or an equivalent
                dictionary, which specifies settings for the vector index,
                such as the metric to use or, if desired, a "source model" setting.
                If omitted, the Data API defaults will apply for the index.
                See the `astrapy.info.TableVectorIndexOptions` class for more details.
            if_not_exists: if set to True, the command will succeed even if an index
                with the specified name already exists (in which case no actual
                index creation takes place on the database). The API default of False
                means that an error is raised by the API in case of name collision.
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> from astrapy.constants import VectorMetric
            >>> from astrapy.info import TableVectorIndexOptions
            >>>
            >>> # create a vector index with dot-product similarity
            >>> await my_async_table.create_vector_index(
            ...     "m_vector_index",
            ...     "m_vector",
            ...     options=TableVectorIndexOptions(
            ...         metric=VectorMetric.DOT_PRODUCT,
            ...     ),
            ... )
            >>> # specify a source_model (since the previous statement
            >>> # succeeded, this will do nothing because of `if_not_exists`):
            >>> await my_async_table.create_vector_index(
            ...     "m_vector_index",
            ...     "m_vector",
            ...     options=TableVectorIndexOptions(
            ...         metric=VectorMetric.DOT_PRODUCT,
            ...         source_model="nv-qa-4",
            ...     ),
            ...     if_not_exists=True,
            ... )
            >>> # leave the settings to the Data API defaults of cosine
            >>> # similarity metric (since the previous statement
            >>> # succeeded, this will do nothing because of `if_not_exists`):
            >>> await my_async_table.create_vector_index(
            ...     "m_vector_index",
            ...     "m_vector",
            ...     if_not_exists=True,
            ... )
        """

        ci_definition: dict[str, Any] = TableVectorIndexDefinition(
            column=column,
            options=TableVectorIndexOptions.coerce(options),
        ).as_dict()
        ci_command = "createVectorIndex"
        return await self._create_generic_index(
            i_name=name,
            ci_definition=ci_definition,
            ci_command=ci_command,
            if_not_exists=if_not_exists,
            table_admin_timeout_ms=table_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )

    async def list_index_names(
        self,
        *,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[str]:
        """
        List the names of all indexes existing on this table.

        Args:
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Returns:
            a list of the index names as strings, in no particular order.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> asyncio.run(my_async_table.list_index_names())
            ['m_vector_index', 'winner_index', 'score_index']
        """

        _table_admin_timeout_ms, _ta_label = _select_singlereq_timeout_ta(
            timeout_options=self.api_options.timeout_options,
            table_admin_timeout_ms=table_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        li_payload: dict[str, Any] = {"listIndexes": {"options": {}}}
        logger.info("listIndexes")
        li_response = await self._api_commander.async_request(
            payload=li_payload,
            timeout_context=_TimeoutContext(
                request_ms=_table_admin_timeout_ms, label=_ta_label
            ),
        )
        if "indexes" not in li_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from listIndexes API command.",
                raw_response=li_response,
            )
        else:
            logger.info("finished listIndexes")
            return li_response["status"]["indexes"]  # type: ignore[no-any-return]

    async def _list_indexes(
        self,
        *,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[TableIndexDescriptor]:
        """
        List the full definitions of all indexes existing on this table.

        WARNING: method not public yet, pending completion of its API.

        Args:
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Returns:
            a list of `astrapy.info.TableIndexDescriptor` objects in no particular
            order, each providing the details of an index present on the table.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> indexes = asyncio.run(my_async_table.list_indexes())
            >>> indexes
            [TableIndexDescriptor(name='m_vector_index', definition=...)...]
            >>> # (Note: shortened output above)
            >>> indexes[1].definition.column
            'winner'
            >>> indexes[1].definition.options.case_sensitive
            False
        """

        _table_admin_timeout_ms, _ta_label = _select_singlereq_timeout_ta(
            timeout_options=self.api_options.timeout_options,
            table_admin_timeout_ms=table_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        li_payload: dict[str, Any] = {"listIndexes": {"options": {"explain": True}}}
        logger.info("listIndexes")
        li_response = await self._api_commander.async_request(
            payload=li_payload,
            timeout_context=_TimeoutContext(
                request_ms=_table_admin_timeout_ms, label=_ta_label
            ),
        )
        columns = (
            await self.definition(
                table_admin_timeout_ms=table_admin_timeout_ms,
                request_timeout_ms=request_timeout_ms,
                timeout_ms=timeout_ms,
            )
        ).columns

        if "indexes" not in li_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from listIndexes API command.",
                raw_response=li_response,
            )
        else:
            logger.info("finished listIndexes")
            return [
                TableIndexDescriptor.coerce(index_object, columns=columns)
                for index_object in li_response["status"]["indexes"]
            ]

    @overload
    async def alter(
        self,
        operation: AlterTableOperation | dict[str, Any],
        *,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> AsyncTable[DefaultRowType]: ...

    @overload
    async def alter(
        self,
        operation: AlterTableOperation | dict[str, Any],
        *,
        row_type: type[NEW_ROW],
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> AsyncTable[NEW_ROW]: ...

    async def alter(
        self,
        operation: AlterTableOperation | dict[str, Any],
        *,
        row_type: type[Any] = DefaultRowType,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> AsyncTable[NEW_ROW]:
        """
        Executes one of the available alter-table operations on this table,
        such as adding/dropping columns.

        This is a blocking operation: the method returns once the index
        is created and ready to use.

        Args:
            operation: an instance of one of the `astrapy.info.AlterTable*` classes,
                representing which alter operation to perform and the details thereof.
                A regular dictionary can also be provided, but then it must have the
                alter operation name at its top level: {"add": {"columns": ...}}.
            row_type: this parameter acts a formal specifier for the type checker.
                If omitted, the resulting AsyncTable is implicitly
                an `AsyncTable[dict[str, Any]]`. If provided, it must match
                the type hint specified in the assignment.
                See the examples below.
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Examples:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> from astrapy.info import (
            ...     AlterTableAddColumns,
            ...     AlterTableAddVectorize,
            ...     AlterTableDropColumns,
            ...     AlterTableDropVectorize,
            ...     ColumnType,
            ...     TableScalarColumnTypeDescriptor,
            ...     VectorServiceOptions,
            ... )
            >>>
            >>> # Add a column
            >>> new_table_1 = await my_table.alter(
            ...     AlterTableAddColumns(
            ...         columns={
            ...             "tie_break": TableScalarColumnTypeDescriptor(
            ...                 column_type=ColumnType.BOOLEAN,
            ...             ),
            ...         }
            ...     )
            ... )
            >>>
            >>> # Drop a column
            >>> new_table_2 = await new_table_1.alter(AlterTableDropColumns(
            ...     columns=["tie_break"]
            ... ))
            >>>
            >>> # Add vectorize to a (vector) column
            >>> new_table_3 = await new_table_2.alter(
            ...     AlterTableAddVectorize(
            ...         columns={
            ...             "m_vector": VectorServiceOptions(
            ...                 provider="openai",
            ...                 model_name="text-embedding-3-small",
            ...                 authentication={
            ...                     "providerKey": "ASTRA_KMS_API_KEY_NAME",
            ...                 },
            ...             ),
            ...         }
            ...     )
            ... )
            >>>
            >>> # Drop vectorize from a (vector) column
            >>> # (Also demonstrates type hint usage)
            >>> from typing import TypedDict
            >>> from astrapy import AsyncTable
            >>> from astrapy.data_types import (
            ...     DataAPISet,
            ...     DataAPITimestamp,
            ...     DataAPIVector,
            ... )
            >>> from astrapy.ids import UUID
            >>>
            >>> class MyMatch(TypedDict):
            ...     match_id: str
            ...     round: int
            ...     m_vector: DataAPIVector
            ...     score: int
            ...     when: DataAPITimestamp
            ...     winner: str
            ...     fighters: DataAPISet[UUID]
            ...
            >>> new_table_4: AsyncTable[MyMatch] = await new_table_3.alter(
            ...     AlterTableDropVectorize(columns=["m_vector"]),
            ...     row_type=MyMatch,
            ... )
        """

        n_operation: AlterTableOperation
        if isinstance(operation, AlterTableOperation):
            n_operation = operation
        else:
            n_operation = AlterTableOperation.from_full_dict(operation)
        _table_admin_timeout_ms, _ta_label = _select_singlereq_timeout_ta(
            timeout_options=self.api_options.timeout_options,
            table_admin_timeout_ms=table_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        at_operation_name = n_operation._name
        at_payload = {
            "alterTable": {
                "operation": {
                    at_operation_name: n_operation.as_dict(),
                },
            },
        }
        logger.info(f"alterTable({at_operation_name})")
        at_response = await self._api_commander.async_request(
            payload=at_payload,
            timeout_context=_TimeoutContext(
                request_ms=_table_admin_timeout_ms, label=_ta_label
            ),
        )
        if at_response.get("status") != {"ok": 1}:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from alterTable API command.",
                raw_response=at_response,
            )
        logger.info(f"finished alterTable({at_operation_name})")
        return AsyncTable(
            database=self.database,
            name=self.name,
            keyspace=self.keyspace,
            api_options=self.api_options,
        )

    async def insert_one(
        self,
        row: ROW,
        *,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> TableInsertOneResult:
        """
        Insert a single row in the table,
        with implied overwrite in case of primary key collision.

        Inserting a row whose primary key correspond to an entry alredy stored
        in the table has the effect of an in-place update: the row is overwritten.
        However, if the row being inserted is partially provided, i.e. some columns
        are not specified, these are left unchanged on the database. To explicitly
        reset them, specify their value as appropriate to their data type,
        i.e. `None`, `{}` or analogous.

        Args:
            row: a dictionary expressing the row to insert. The primary key
                must be specified in full, while any other column may be omitted
                if desired (in which case it is left as is on DB).
                The values for the various columns supplied in the row must
                be of the right data type for the insertion to succeed.
                Non-primary-key columns can also be explicitly set to null.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a TableInsertOneResult object, whose attributes are the primary key
            of the inserted row both in the form of a dictionary and of a tuple.

        Examples:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> # a full-row insert using astrapy's datatypes
            >>> from astrapy.data_types import (
            ...     DataAPISet,
            ...     DataAPITimestamp,
            ...     DataAPIVector,
            ... )
            >>> from astrapy.ids import UUID
            >>>
            >>> insert_result = asyncio.run(my_async_table.insert_one(
            ...     {
            ...         "match_id": "mtch_0",
            ...         "round": 1,
            ...         "m_vector": DataAPIVector([0.4, -0.6, 0.2]),
            ...         "score": 18,
            ...         "when": DataAPITimestamp.from_string("2024-11-28T11:30:00Z"),
            ...         "winner": "Victor",
            ...         "fighters": DataAPISet([
            ...             UUID("0193539a-2770-8c09-a32a-111111111111"),
            ...         ]),
            ...     },
            ... ))
            >>> insert_result.inserted_id
            {'match_id': 'mtch_0', 'round': 1}
            >>> insert_result.inserted_id_tuple
            ('mtch_0', 1)
            >>>
            >>> # a partial-row (which in this case overwrites some of the values)
            >>> asyncio.run(my_async_table.insert_one(
            ...     {
            ...         "match_id": "mtch_0",
            ...         "round": 1,
            ...         "winner": "Victor Vector",
            ...         "fighters": DataAPISet([
            ...             UUID("0193539a-2770-8c09-a32a-111111111111"),
            ...             UUID("0193539a-2880-8875-9f07-222222222222"),
            ...         ]),
            ...     },
            ... ))
            TableInsertOneResult(inserted_id={'match_id': 'mtch_0', 'round': 1} ...
            >>>
            >>> # another insertion demonstrating standard-library datatypes in values
            >>> import datetime
            >>>
            >>> asyncio.run(my_async_table.insert_one(
            ...     {
            ...         "match_id": "mtch_0",
            ...         "round": 2,
            ...         "winner": "Angela",
            ...         "score": 25,
            ...         "when": datetime.datetime(
            ...             2024, 7, 13, 12, 55, 30, 889,
            ...             tzinfo=datetime.timezone.utc,
            ...         ),
            ...         "fighters": {
            ...             UUID("019353cb-8e01-8276-a190-333333333333"),
            ...         },
            ...         "m_vector": [0.4, -0.6, 0.2],
            ...     },
            ... ))
            TableInsertOneResult(inserted_id={'match_id': 'mtch_0', 'round': 2}, ...
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        io_payload = self._converter_agent.preprocess_payload(
            {"insertOne": {"document": row}},
            map2tuple_checker=map2tuple_checker_insert_one,
        )
        logger.info(f"insertOne on '{self.name}'")
        io_response = await self._api_commander.async_request(
            payload=io_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
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
            inserted_id_tuple, inserted_id = self._converter_agent.postprocess_key(
                inserted_id_list,
                primary_key_schema_dict=io_response["status"]["primaryKeySchema"],
            )
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

    def _prepare_keys_from_status(
        self, status: dict[str, Any] | None, raise_on_missing: bool = False
    ) -> tuple[list[dict[str, Any]], list[tuple[Any, ...]]]:
        ids: list[dict[str, Any]]
        id_tuples: list[tuple[Any, ...]]
        if status is None:
            if raise_on_missing:
                raise UnexpectedDataAPIResponseException(
                    text="'status' not found in API response",
                    raw_response=None,
                )
            else:
                ids = []
                id_tuples = []
        else:
            if "documentResponses" not in status:
                raise UnexpectedDataAPIResponseException(
                    text=(
                        "received a 'status' without 'documentResponses' "
                        f"in API response (received: {status})"
                    ),
                    raw_response=None,
                )
            raw_inserted_ids = [
                row_resp["_id"]
                for row_resp in status["documentResponses"]
                if row_resp["status"] == "OK"
            ]
            if raw_inserted_ids:
                if "primaryKeySchema" not in status:
                    raise UnexpectedDataAPIResponseException(
                        text=(
                            "received a 'status' without 'primaryKeySchema' "
                            f"in API response (received: {status})"
                        ),
                        raw_response=None,
                    )
                id_tuples_and_ids = self._converter_agent.postprocess_keys(
                    raw_inserted_ids,
                    primary_key_schema_dict=status["primaryKeySchema"],
                )
                id_tuples = [tpl for tpl, _ in id_tuples_and_ids]
                ids = [id for _, id in id_tuples_and_ids]
            else:
                ids = []
                id_tuples = []
        return ids, id_tuples

    async def insert_many(
        self,
        rows: Iterable[ROW],
        *,
        ordered: bool = False,
        chunk_size: int | None = None,
        concurrency: int | None = None,
        request_timeout_ms: int | None = None,
        general_method_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> TableInsertManyResult:
        """
        Insert a number of rows into the table,
        with implied overwrite in case of primary key collision.

        Inserting rows whose primary key correspond to entries alredy stored
        in the table has the effect of an in-place update: the rows are overwritten.
        However, if the rows being inserted are partially provided, i.e. some columns
        are not specified, these are left unchanged on the database. To explicitly
        reset them, specify their value as appropriate to their data type,
        i.e. `None`, `{}` or analogous.

        Args:
            rows: an iterable of dictionaries, each expressing a row to insert.
                Each row must at least fully specify the primary key column values,
                while any other column may be omitted if desired (in which case
                it is left as is on DB).
                The values for the various columns supplied in each row must
                be of the right data type for the insertion to succeed.
                Non-primary-key columns can also be explicitly set to null.
            ordered: if False (default), the insertions can occur in arbitrary order
                and possibly concurrently. If True, they are processed sequentially.
                If there are no specific reasons against it, unordered insertions
                re to be preferred as they complete much faster.
            chunk_size: how many rows to include in each single API request.
                Exceeding the server maximum allowed value results in an error.
                Leave it unspecified (recommended) to use the system default.
            concurrency: maximum number of concurrent requests to the API at
                a given time. It cannot be more than one for ordered insertions.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                whole operation, which may consist of several API requests.
                If not provided, this object's defaults apply.
            request_timeout_ms: a timeout, in milliseconds, to impose on each
                individual HTTP request to the Data API to accomplish the operation.
                If not provided, this object's defaults apply.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a TableInsertManyResult object, whose attributes are the primary key
            of the inserted rows both in the form of dictionaries and of tuples.

        Examples:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> # Insert complete and partial rows at once (concurrently)
            >>> from astrapy.data_types import (
            ...     DataAPISet,
            ...     DataAPITimestamp,
            ...     DataAPIVector,
            ... )
            >>> from astrapy.ids import UUID
            >>>
            >>> insert_result = asyncio.run(my_async_table.insert_many(
            ...     [
            ...         {
            ...             "match_id": "fight4",
            ...             "round": 1,
            ...             "winner": "Victor",
            ...             "score": 18,
            ...             "when": DataAPITimestamp.from_string(
            ...                 "2024-11-28T11:30:00Z",
            ...             ),
            ...             "fighters": DataAPISet([
            ...                 UUID("0193539a-2770-8c09-a32a-111111111111"),
            ...                 UUID('019353e3-00b4-83f9-a127-222222222222'),
            ...             ]),
            ...             "m_vector": DataAPIVector([0.4, -0.6, 0.2]),
            ...         },
            ...         {"match_id": "fight5", "round": 1, "winner": "Adam"},
            ...         {"match_id": "fight5", "round": 2, "winner": "Betta"},
            ...         {"match_id": "fight5", "round": 3, "winner": "Caio"},
            ...         {
            ...             "match_id": "challenge6",
            ...             "round": 1,
            ...             "winner": "Donna",
            ...             "m_vector": [0.9, -0.1, -0.3],
            ...         },
            ...         {"match_id": "challenge6", "round": 2, "winner": "Erick"},
            ...         {"match_id": "challenge6", "round": 3, "winner": "Fiona"},
            ...         {"match_id": "tournamentA", "round": 1, "winner": "Gael"},
            ...         {"match_id": "tournamentA", "round": 2, "winner": "Hanna"},
            ...         {
            ...             "match_id": "tournamentA",
            ...             "round": 3,
            ...             "winner": "Ian",
            ...             "fighters": DataAPISet([
            ...                 UUID("0193539a-2770-8c09-a32a-111111111111"),
            ...             ]),
            ...         },
            ...         {"match_id": "fight7", "round": 1, "winner": "Joy"},
            ...         {"match_id": "fight7", "round": 2, "winner": "Kevin"},
            ...         {"match_id": "fight7", "round": 3, "winner": "Lauretta"},
            ...     ],
            ...     concurrency=10,
            ...     chunk_size=3,
            ... ))
            >>> insert_result.inserted_ids
            [{'match_id': 'fight4', 'round': 1}, {'match_id': 'fight5', ...
            >>> insert_result.inserted_id_tuples
            [('fight4', 1), ('fight5', 1), ('fight5', 2), ('fight5', 3), ...
            >>>
            >>> # Ordered insertion
            >>> # (would stop on first failure; predictable end result on DB)
            >>> asyncio.run(my_async_table.insert_many(
            ...     [
            ...         {"match_id": "fight5", "round": 1, "winner": "Adam0"},
            ...         {"match_id": "fight5", "round": 2, "winner": "Betta0"},
            ...         {"match_id": "fight5", "round": 3, "winner": "Caio0"},
            ...         {"match_id": "fight5", "round": 1, "winner": "Adam Zuul"},
            ...         {"match_id": "fight5", "round": 2, "winner": "Betta Vigo"},
            ...         {"match_id": "fight5", "round": 3, "winner": "Caio Gozer"},
            ...     ],
            ...     ordered=True,
            ... ))
            TableInsertManyResult(inserted_ids=[{'match_id': 'fight5', 'round': 1}, ...

        Note:
            Unordered insertions are executed with some degree of concurrency,
            so it is usually better to prefer this mode unless the order in the
            row sequence is important.

        Note:
            A failure mode for this command is related to certain faulty rows
            found among those to insert: validation may fail, for example, if the
            vector length does not match the table schema.

            For an ordered insertion, the method will raise an exception at
            the first such faulty row -- nevertheless, all rows processed
            until then will end up being written to the database.

            For unordered insertions, if the error stems from faulty rows
            the insertion proceeds until exhausting the input rows: then,
            an exception is raised -- and all insertable rows will have been
            written to the database, including those "after" the troublesome ones.

            Errors occurring during an insert_many operation, for that reason,
            may result in a `TableInsertManyException` being raised.
            This exception allows to inspect the list of row IDs that were
            successfully inserted, while accessing at the same time the underlying
            "root errors" that made the full method call to fail.
        """

        _general_method_timeout_ms, _gmt_label = _first_valid_timeout(
            (general_method_timeout_ms, "general_method_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (
                self.api_options.timeout_options.general_method_timeout_ms,
                "general_method_timeout_ms",
            ),
        )
        _request_timeout_ms, _rt_label = _first_valid_timeout(
            (request_timeout_ms, "request_timeout_ms"),
            (self.api_options.timeout_options.request_timeout_ms, "request_timeout_ms"),
        )
        if concurrency is None:
            if ordered:
                _concurrency = 1
            else:
                _concurrency = DEFAULT_INSERT_MANY_CONCURRENCY
        else:
            _concurrency = concurrency
        if _concurrency > 1 and ordered:
            raise ValueError("Cannot run ordered insert_many concurrently.")
        if chunk_size is None:
            _chunk_size = DEFAULT_INSERT_MANY_CHUNK_SIZE
        else:
            _chunk_size = chunk_size
        _rows = list(rows)
        logger.info(f"inserting {len(_rows)} rows in '{self.name}'")
        raw_results: list[dict[str, Any]] = []
        im_payloads: list[dict[str, Any] | None] = []
        timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=_general_method_timeout_ms,
            timeout_label=_gmt_label,
        )
        if ordered:
            options = {"ordered": True, "returnDocumentResponses": True}
            inserted_ids: list[Any] = []
            inserted_id_tuples: list[Any] = []
            for i in range(0, len(_rows), _chunk_size):
                im_payload = self._converter_agent.preprocess_payload(
                    {
                        "insertMany": {
                            "documents": _rows[i : i + _chunk_size],
                            "options": options,
                        },
                    },
                    map2tuple_checker=map2tuple_checker_insert_many,
                )
                logger.info(f"insertMany(chunk) on '{self.name}'")
                chunk_response = await self._api_commander.async_request(
                    payload=im_payload,
                    raise_api_errors=False,
                    timeout_context=timeout_manager.remaining_timeout(
                        cap_time_ms=_request_timeout_ms,
                        cap_timeout_label=_rt_label,
                    ),
                )
                logger.info(f"finished insertMany(chunk) on '{self.name}'")
                # accumulate the results in this call
                chunk_inserted_ids, chunk_inserted_ids_tuples = (
                    self._prepare_keys_from_status(chunk_response.get("status"))
                )
                inserted_ids += chunk_inserted_ids
                inserted_id_tuples += chunk_inserted_ids_tuples
                raw_results += [chunk_response]
                # if errors, quit early
                if chunk_response.get("errors", []):
                    response_exception = DataAPIResponseException.from_response(
                        command=im_payload,
                        raw_response=chunk_response,
                    )
                    raise TableInsertManyException(
                        inserted_ids=inserted_ids,
                        inserted_id_tuples=inserted_id_tuples,
                        exceptions=[response_exception],
                    )

            # return
            full_result = TableInsertManyResult(
                raw_results=raw_results,
                inserted_ids=inserted_ids,
                inserted_id_tuples=inserted_id_tuples,
            )
            logger.info(f"finished inserting {len(_rows)} rows in '{self.name}'")
            return full_result

        else:
            # unordered: concurrent or not, do all of them and parse the results
            options = {"ordered": False, "returnDocumentResponses": True}

            sem = asyncio.Semaphore(_concurrency)

            async def concurrent_insert_chunk(
                row_chunk: list[ROW],
            ) -> tuple[dict[str, Any] | None, dict[str, Any]]:
                async with sem:
                    im_payload = self._converter_agent.preprocess_payload(
                        {
                            "insertMany": {
                                "documents": row_chunk,
                                "options": options,
                            },
                        },
                        map2tuple_checker=map2tuple_checker_insert_many,
                    )
                    logger.info(f"insertMany(chunk) on '{self.name}'")
                    im_response = await self._api_commander.async_request(
                        payload=im_payload,
                        raise_api_errors=False,
                        timeout_context=timeout_manager.remaining_timeout(
                            cap_time_ms=_request_timeout_ms,
                            cap_timeout_label=_rt_label,
                        ),
                    )
                    logger.info(f"finished insertMany(chunk) on '{self.name}'")
                    return im_payload, im_response

            raw_pl_results_pairs: list[tuple[dict[str, Any] | None, dict[str, Any]]]
            if _concurrency > 1:
                tasks = [
                    asyncio.create_task(
                        concurrent_insert_chunk(_rows[i : i + _chunk_size])
                    )
                    for i in range(0, len(_rows), _chunk_size)
                ]
                raw_pl_results_pairs = await asyncio.gather(*tasks)
            else:
                raw_pl_results_pairs = [
                    await concurrent_insert_chunk(_rows[i : i + _chunk_size])
                    for i in range(0, len(_rows), _chunk_size)
                ]

            if raw_pl_results_pairs:
                im_payloads, raw_results = list(zip(*raw_pl_results_pairs))
            else:
                im_payloads, raw_results = [], []

            # recast raw_results. Each response has its schema: unfold appropriately
            ids_and_tuples_per_chunk = [
                self._prepare_keys_from_status(chunk_response.get("status"))
                for chunk_response in raw_results
            ]
            inserted_ids = [
                inserted_id
                for chunk_ids, _ in ids_and_tuples_per_chunk
                for inserted_id in chunk_ids
            ]
            inserted_id_tuples = [
                inserted_id_tuple
                for _, chunk_id_tuples in ids_and_tuples_per_chunk
                for inserted_id_tuple in chunk_id_tuples
            ]

            # check-raise
            response_exceptions = [
                DataAPIResponseException.from_response(
                    command=chunk_payload,
                    raw_response=chunk_response,
                )
                for chunk_payload, chunk_response in zip(im_payloads, raw_results)
                if chunk_response.get("errors", [])
            ]
            if response_exceptions:
                raise TableInsertManyException(
                    inserted_ids=inserted_ids,
                    inserted_id_tuples=inserted_id_tuples,
                    exceptions=response_exceptions,
                )

            # return
            full_result = TableInsertManyResult(
                raw_results=raw_results,
                inserted_ids=inserted_ids,
                inserted_id_tuples=inserted_id_tuples,
            )
            logger.info(f"finished inserting {len(_rows)} rows in '{self.name}'")
            return full_result

    @overload
    def find(
        self,
        filter: FilterType | None = None,
        *,
        projection: ProjectionType | None = None,
        row_type: None = None,
        skip: int | None = None,
        limit: int | None = None,
        include_similarity: bool | None = None,
        include_sort_vector: bool | None = None,
        sort: SortType | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> AsyncTableFindCursor[ROW, ROW]: ...

    @overload
    def find(
        self,
        filter: FilterType | None = None,
        *,
        projection: ProjectionType | None = None,
        row_type: type[ROW2],
        skip: int | None = None,
        limit: int | None = None,
        include_similarity: bool | None = None,
        include_sort_vector: bool | None = None,
        sort: SortType | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> AsyncTableFindCursor[ROW, ROW2]: ...

    def find(
        self,
        filter: FilterType | None = None,
        *,
        projection: ProjectionType | None = None,
        row_type: type[ROW2] | None = None,
        skip: int | None = None,
        limit: int | None = None,
        include_similarity: bool | None = None,
        include_sort_vector: bool | None = None,
        sort: SortType | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> AsyncTableFindCursor[ROW, ROW2]:
        """
        Find rows on the table matching the provided filters
        and according to sorting criteria including vector similarity.

        The returned AsyncTableFindCursor object, representing the stream of results,
        can be iterated over, or consumed and manipulated in several other ways
        (see the examples below and the `TableFindCursor` documentation for details).
        Since the amount of returned items can be large, TableFindCursor is a lazy
        object, that fetches new data while it is being read using the Data API
        pagination mechanism.

        Invoking `.to_list()` on a TableFindCursor will cause it to consume all
        rows and materialize the entire result set as a list. This is not recommended
        if the amount of results is very large.

        Args:
            filter: a dictionary expressing which condition the returned rows
                must satisfy. The filter can use operators, such as "$eq" for equality,
                and require columns to compare with literal values. Simple examples
                are `{}` (zero filter, not recommended for large tables),
                `{"match_no": 123}` (a shorthand for `{"match_no": {"$eq": 123}}`,
                or `{"match_no": 123, "round": "C"}` (multiple conditions are
                implicitly combined with "$and").
                Please consult the Data API documentation for a more detailed
                explanation of table search filters and tips on their usage.
            projection: a prescription on which columns to return for the matching rows.
                The projection can take the form `{"column1": True, "column2": True}`.
                `{"*": True}` (i.e. return the whole row), or the complementary
                form that excludes columns: `{"column1": False, "column2": False}`.
                To optimize bandwidth usage, it is recommended to use a projection,
                especially to avoid unnecessary columns of type vector with
                high-dimensional embeddings.
            row_type: this parameter acts a formal specifier for the type checker.
                If omitted, the resulting cursor is implicitly an
                `AsyncTableFindCursor[ROW, ROW]`, i.e. maintains the same type for
                the items it returns as that for the rows in the table. Strictly
                typed code may want to specify this parameter especially when a
                projection is given.
            skip: if provided, it is a number of rows that would be obtained first
                in the response and are instead skipped.
            limit: a maximum amount of rows to get from the table. The returned cursor
                will stop yielding rows when either this number is reached or there
                really are no more matches in the table.
            include_similarity: a boolean to request the numeric value of the
                similarity to be returned as an added "$similarity" key in each returned
                row. It can be used meaningfully only in a vector search (see `sort`).
            include_sort_vector: a boolean to request the search query vector.
                If set to True (and if the search is a vector search), calling
                the `get_sort_vector` method on the returned cursor will yield
                the vector used for the ANN search.
            sort: this dictionary parameter controls the order in which the rows
                are returned. The sort parameter can express either a vector search or
                a regular (ascending/descending, even hierarchical) sorting.
                * For a vector search the parameter takes the form
                `{"vector_column": qv}`, with the query vector `qv` of the appropriate
                type (list of floats or DataAPIVector). If the table has automatic
                embedding generation ("vectorize") enabled on that column, the form
                `{"vectorize_enabled_column": "query text"}` is also valid.
                * In the case of non-vector sorting, the parameter specifies the
                column(s) and the ascending/descending ordering required.
                If multiple columns are provided, the sorting applies them
                hierarchically to the rows. Examples are `{"score": SortMode.ASCENDING}`
                (equivalently `{"score": +1}`), `{"score": +1, "when": -1}`.
                Note that, depending on the column(s) chosen for sorting, the table
                partitioning structure, and the presence of indexes, the sorting
                may be done in-memory by the API. In that case, there may be performance
                implications and limitations on the amount of items returned.
                Consult the Data API documentation for more details on this topic.
            request_timeout_ms: a timeout, in milliseconds, to impose on each
                individual HTTP request to the Data API to accomplish the operation.
                If not provided, this object's defaults apply.
            timeout_ms: an alias for `request_timeout_ms`.

        Returns:
            a AsyncTableFindCursor object, that can be iterated over (and manipulated
            in several ways). The cursor, if needed, handles pagination under the hood
            as the rows are consumed.

        Note:
            As the rows are retrieved in chunks progressively, while the cursor
            is being iterated over, it is possible that the actual results
            obtained will reflect changes occurring to the table contents in
            real time.

        Examples:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> # Iterate over results:
            >>> async def loop1():
            ...     async for row in my_async_table.find({"match_id": "challenge6"}):
            ...         print(f"(R:{row['round']}): winner {row['winner']}")
            ...
            >>> asyncio.run(loop1())
            (R:1): winner Donna
            (R:2): winner Erick
            (R:3): winner Fiona
            >>>
            >>> # Optimize bandwidth using a projection:
            >>> proj = {"round": True, "winner": True}
            >>> async def loop2():
            ...     async for row in my_async_table.find(
            ...           {"match_id": "challenge6"},
            ...           projection=proj,
            ...     ):
            ...         print(f"(R:{row['round']}): winner {row['winner']}")
            ...
            >>> asyncio.run(loop2())
            (R:1): winner Donna
            (R:2): winner Erick
            (R:3): winner Fiona
            >>>
            >>> # Filter on the partitioning:
            >>> asyncio.run(
            ...     my_async_table.find({"match_id": "challenge6"}).to_list()
            ... )
            [{'match_id': 'challenge6', 'round': 1, 'fighters': DataAPISet([]), ...
            >>>
            >>> # Filter on primary key:
            >>> asyncio.run(
            ...     my_async_table.find(
            ...         {"match_id": "challenge6", "round": 1}
            ...     ).to_list()
            ... )
            [{'match_id': 'challenge6', 'round': 1, 'fighters': DataAPISet([]), ...
            >>>
            >>> # Filter on a regular indexed column:
            >>> asyncio.run(my_async_table.find({"winner": "Caio Gozer"}).to_list())
            [{'match_id': 'fight5', 'round': 3, 'fighters': DataAPISet([]), ...
            >>>
            >>> # Non-equality filter on a regular indexed column:
            >>> asyncio.run(my_async_table.find({"score": {"$gte": 15}}).to_list())
            [{'match_id': 'fight4', 'round': 1, 'fighters': DataAPISet([UUID('0193...
            >>>
            >>> # Filter on a regular non-indexed column:
            >>> # (not recommended performance-wise)
            >>> asyncio.run(my_async_table.find(
            ...     {"when": {
            ...         "$gte": DataAPITimestamp.from_string("1999-12-31T01:23:44Z")
            ...     }}
            ... ).to_list())
            The Data API returned a warning: {'errorCode': 'MISSING_INDEX', ...
            [{'match_id': 'fight4', 'round': 1, 'fighters': DataAPISet([UUID('0193...
            >>>
            >>> # Empty filter (not recommended performance-wise):
            >>> asyncio.run(my_async_table.find({}).to_list())
            The Data API returned a warning: {'errorCode': 'ZERO_FILTER_OPERATIONS', ...
            [{'match_id': 'fight4', 'round': 1, 'fighters': DataAPISet([UUID('0193...
            >>>
            >>> # Filter on the primary key and a regular non-indexed column:
            >>> # (not recommended performance-wise)
            >>> asyncio.run(my_async_table.find(
            ...     {"match_id": "fight5", "round": 3, "winner": "Caio Gozer"}
            ... ).to_list())
            The Data API returned a warning: {'errorCode': 'MISSING_INDEX', ...
            [{'match_id': 'fight5', 'round': 3, 'fighters': DataAPISet([]), ...
            >>>
            >>> # Filter on a regular non-indexed column (and incomplete primary key)
            >>> # (not recommended performance-wise)
            >>> asyncio.run(my_async_table.find(
            ...     {"round": 3, "winner": "Caio Gozer"}
            ... ).to_list())
            The Data API returned a warning: {'errorCode': 'MISSING_INDEX', ...
            [{'match_id': 'fight5', 'round': 3, 'fighters': DataAPISet([]), ...
            >>>
            >>> # Vector search with "sort" (on an appropriately-indexed vector column):
            >>> asyncio.run(my_async_table.find(
            ...     {},
            ...     sort={"m_vector": DataAPIVector([0.2, 0.3, 0.4])},
            ...     projection={"winner": True},
            ...     limit=3,
            ... ).to_list())
            [{'winner': 'Donna'}, {'winner': 'Victor'}]
            >>>
            >>> # Hybrid search with vector sort and non-vector filtering:
            >>> my_table.find(
            ...     {"match_id": "fight4"},
            ...     sort={"m_vector": DataAPIVector([0.2, 0.3, 0.4])},
            ...     projection={"winner": True},
            ... ).to_list()
            [{'winner': 'Victor'}]
            >>>
            >>> # Return the numeric value of the vector similarity
            >>> # (also demonstrating that one can pass a plain list for a vector):
            >>> asyncio.run(my_async_table.find(
            ...     {},
            ...     sort={"m_vector": [0.2, 0.3, 0.4]},
            ...     projection={"winner": True},
            ...     limit=3,
            ...     include_similarity=True,
            ... ).to_list())
            [{'winner': 'Donna', '$similarity': 0.515}, {'winner': 'Victor', ...
            >>>
            >>> # Non-vector sorting on a 'partitionSort' column:
            >>> asyncio.run(my_async_table.find(
            ...     {"match_id": "fight5"},
            ...     sort={"round": SortMode.DESCENDING},
            ...     projection={"winner": True},
            ... ).to_list())
            [{'winner': 'Caio Gozer'}, {'winner': 'Betta Vigo'}, ...
            >>>
            >>> # Using `skip` and `limit`:
            >>> asyncio.run(my_async_table.find(
            ...     {"match_id": "fight5"},
            ...     sort={"round": SortMode.DESCENDING},
            ...     projection={"winner": True},
            ...     skip=1,
            ...     limit=2,
            ... ).to_list())
            The Data API returned a warning: {'errorCode': 'IN_MEMORY_SORTING...
            [{'winner': 'Betta Vigo'}, {'winner': 'Adam Zuul'}]
            >>>
            >>> # Non-vector sorting on a regular column:
            >>> # (not recommended performance-wise)
            >>> asyncio.run(my_async_table.find(
            ...     {"match_id": "fight5"},
            ...     sort={"winner": SortMode.ASCENDING},
            ...     projection={"winner": True},
            ... ).to_list())
            The Data API returned a warning: {'errorCode': 'IN_MEMORY_SORTING...
            [{'winner': 'Adam Zuul'}, {'winner': 'Betta Vigo'}, ...
            >>>
            >>> # Using `.map()` on a cursor:
            >>> winner_cursor = my_async_table.find(
            ...     {"match_id": "fight5"},
            ...     sort={"round": SortMode.DESCENDING},
            ...     projection={"winner": True},
            ...     limit=5,
            ... )
            >>> print("/".join(asyncio.run(
            ...     winner_cursor.map(lambda row: row["winner"].upper()).to_list())
            ... ))
            CAIO GOZER/BETTA VIGO/ADAM ZUUL
            >>>
            >>> # Some other examples of cursor manipulation
            >>> matches_async_cursor = my_async_table.find(
            ...     sort={"m_vector": DataAPIVector([-0.1, 0.15, 0.3])}
            ... )
            >>> asyncio.run(matches_async_cursor.has_next())
            True
            >>> asyncio.run(matches_async_cursor.__anext__())
            {'match_id': 'fight4', 'round': 1, 'fighters': DataAPISet([UUID('0193...
            >>> matches_async_cursor.consumed
            1
            >>> matches_async_cursor.rewind()
            >>> matches_async_cursor.consumed
            0
            >>> asyncio.run(matches_async_cursor.has_next())
            True
            >>> matches_async_cursor.close()
            >>>
            >>> async def try_consume():
            ...     try:
            ...         await matches_async_cursor.__anext__()
            ...     except StopAsyncIteration:
            ...         print("StopAsyncIteration triggered.")
            ...
            >>> asyncio.run(try_consume())
            StopAsyncIteration triggered.
        """

        # lazy-import here to avoid circular import issues
        from astrapy.cursors import AsyncTableFindCursor

        _request_timeout_ms, _rt_label = _first_valid_timeout(
            (request_timeout_ms, "request_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (self.api_options.timeout_options.request_timeout_ms, "request_timeout_ms"),
        )
        return (
            AsyncTableFindCursor(
                table=self,
                request_timeout_ms=_request_timeout_ms,
                overall_timeout_ms=None,
                request_timeout_label=_rt_label,
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
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> ROW | None:
        """
        Run a search according to the given filtering and sorting criteria
        and return the top row matching it, or nothing if there are none.

        The parameters are analogous to some of the parameters to the `find` method
        (which has a few more that do not make sense in this case, such as `limit`).

        Args:
            filter: a dictionary expressing which condition the returned row
                must satisfy. The filter can use operators, such as "$eq" for equality,
                and require columns to compare with literal values. Simple examples
                are `{}` (zero filter), `{"match_no": 123}` (a shorthand for
                `{"match_no": {"$eq": 123}}`, or `{"match_no": 123, "round": "C"}`
                (multiple conditions are implicitly combined with "$and").
                Please consult the Data API documentation for a more detailed
                explanation of table search filters and tips on their usage.
            projection: a prescription on which columns to return for the matching row.
                The projection can take the form `{"column1": True, "column2": True}`.
                `{"*": True}` (i.e. return the whole row), or the complementary
                form that excludes columns: `{"column1": False, "column2": False}`.
                To optimize bandwidth usage, it is recommended to use a projection,
                especially to avoid unnecessary columns of type vector with
                high-dimensional embeddings.
            include_similarity: a boolean to request the numeric value of the
                similarity to be returned as an added "$similarity" key in the returned
                row. It can be used meaningfully only in a vector search (see `sort`).
            sort: this dictionary parameter controls the sorting order, hence determines
                which row is being returned.
                The sort parameter can express either a vector search or
                a regular (ascending/descending, even hierarchical) sorting.
                * For a vector search the parameter takes the form
                `{"vector_column": qv}`, with the query vector `qv` of the appropriate
                type (list of floats or DataAPIVector). If the table has automatic
                embedding generation ("vectorize") enabled on that column, the form
                `{"vectorize_enabled_column": "query text"}` is also valid.
                * In the case of non-vector sorting, the parameter specifies the
                column(s) and the ascending/descending ordering required.
                If multiple columns are provided, the sorting applies them
                hierarchically to the rows. Examples are `{"score": SortMode.ASCENDING}`
                (equivalently `{"score": +1}`), `{"score": +1, "when": -1}`.
                Note that, depending on the column(s) chosen for sorting, the table
                partitioning structure, and the presence of indexes, the sorting
                may be done in-memory by the API. In that case, there may be performance
                implications.
                Consult the Data API documentation for more details on this topic.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a dictionary expressing the result if a row is found, otherwise None.

        Examples:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> from astrapy.constants import SortMode
            >>> from astrapy.data_types import DataAPITimestamp, DataAPIVector
            >>>
            >>> # Filter on the partitioning:
            >>> asyncio.run(my_async_table.find_one({"match_id": "challenge6"}))
            {'match_id': 'challenge6', 'round': 1, 'fighters': DataAPISet([]), ...
            >>>
            >>> # A find with no matches:
            >>> str(asyncio.run(my_async_table.find_one({"match_id": "not_real"})))
            'None'
            >>>
            >>> # Optimize bandwidth using a projection:
            >>> asyncio.run(my_async_table.find_one(
            ...     {"match_id": "challenge6"},
            ...     projection={"round": True, "winner": True},
            ... ))
            {'round': 1, 'winner': 'Donna'}
            >>>
            >>> # Filter on primary key:
            >>> asyncio.run(
            ...     my_async_table.find_one({"match_id": "challenge6", "round": 1})
            ... )
            {'match_id': 'challenge6', 'round': 1, 'fighters': DataAPISet([]), ...
            >>>
            >>> # Filter on a regular indexed column:
            >>> asyncio.run(my_async_table.find_one({"winner": "Caio Gozer"}))
            {'match_id': 'fight5', 'round': 3, 'fighters': DataAPISet([]), ...
            >>>
            >>> # Non-equality filter on a regular indexed column:
            >>> asyncio.run(my_async_table.find_one({"score": {"$gte": 15}}))
            {'match_id': 'fight4', 'round': 1, 'fighters': DataAPISet([UUID('0193...
            >>>
            >>> # Filter on a regular non-indexed column:
            >>> # (not recommended performance-wise)
            >>> asyncio.run(my_async_table.find_one(
            ...     {"when": {
            ...         "$gte": DataAPITimestamp.from_string("1999-12-31T01:23:44Z")
            ...     }}
            ... ))
            The Data API returned a warning: {'errorCode': 'MISSING_INDEX', ...
            {'match_id': 'fight4', 'round': 1, 'fighters': DataAPISet([UUID('0193...
            >>>
            >>> # Empty filter:
            >>> asyncio.run(my_async_table.find_one({}))
            The Data API returned a warning: {'errorCode': 'ZERO_FILTER_OPERATIONS', ...
            {'match_id': 'fight4', 'round': 1, 'fighters': DataAPISet([UUID('0193...
            >>>
            >>> # Filter on the primary key and a regular non-indexed column:
            >>> # (not recommended performance-wise)
            >>> asyncio.run(my_async_table.find_one(
            ...     {"match_id": "fight5", "round": 3, "winner": "Caio Gozer"}
            ... ))
            The Data API returned a warning: {'errorCode': 'MISSING_INDEX', ...
            {'match_id': 'fight5', 'round': 3, 'fighters': DataAPISet([]), ...
            >>>
            >>> # Filter on a regular non-indexed column (and incomplete primary key)
            >>> # (not recommended performance-wise)
            >>> asyncio.run(
            ...     my_async_table.find_one({"round": 3, "winner": "Caio Gozer"})
            ... )
            The Data API returned a warning: {'errorCode': 'MISSING_INDEX', ...
            {'match_id': 'fight5', 'round': 3, 'fighters': DataAPISet([]), ...
            >>>
            >>> # Vector search with "sort" (on an appropriately-indexed vector column):
            >>> asyncio.run(my_async_table.find_one(
            ...     {},
            ...     sort={"m_vector": DataAPIVector([0.2, 0.3, 0.4])},
            ...     projection={"winner": True},
            ... ))
            {'winner': 'Donna'}
            >>>
            >>> # Hybrid search with vector sort and non-vector filtering:
            >>> asyncio.run(my_table.find_one(
            ...     {"match_id": "fight4"},
            ...     sort={"m_vector": DataAPIVector([0.2, 0.3, 0.4])},
            ...     projection={"winner": True},
            ... ))
            {'winner': 'Victor'}
            >>>
            >>> # Return the numeric value of the vector similarity
            >>> # (also demonstrating that one can pass a plain list for a vector):
            >>> asyncio.run(my_async_table.find_one(
            ...     {},
            ...     sort={"m_vector": [0.2, 0.3, 0.4]},
            ...     projection={"winner": True},
            ...     include_similarity=True,
            ... ))
            {'winner': 'Donna', '$similarity': 0.515}
            >>>
            >>> # Non-vector sorting on a 'partitionSort' column:
            >>> asyncio.run(my_async_table.find_one(
            ...     {"match_id": "fight5"},
            ...     sort={"round": SortMode.DESCENDING},
            ...     projection={"winner": True},
            ... ))
            {'winner': 'Caio Gozer'}
            >>>
            >>> # Non-vector sorting on a regular column:
            >>> # (not recommended performance-wise)
            >>> asyncio.run(my_async_table.find_one(
            ...     {"match_id": "fight5"},
            ...     sort={"winner": SortMode.ASCENDING},
            ...     projection={"winner": True},
            ... ))
            The Data API returned a warning: {'errorCode': 'IN_MEMORY_SORTING...
            {'winner': 'Adam Zuul'}
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
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
            },
            map2tuple_checker=None,
        )
        fo_response = await self._api_commander.async_request(
            payload=fo_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
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
            similarity_pseudocolumn="$similarity" if include_similarity else None,
        )

    async def distinct(
        self,
        key: str | Iterable[str | int],
        *,
        filter: FilterType | None = None,
        request_timeout_ms: int | None = None,
        general_method_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[Any]:
        """
        Return a list of the unique values of `key` across the rows
        in the table that match the provided filter.

        Args:
            key: the name of the field whose value is inspected across rows.
                Keys can be just column names (as is typically the case), but
                the dot-notation is also accepted to mean subkeys or indices
                within lists (for example, "map_column.subkey" or "list_column.2").
                If a column has literal dots or ampersands in its name, this
                parameter must be escaped to be treated properly.
                The key can also be a list of strings and numbers, in which case
                no escape is necessary: each item in the list is a field name/index,
                for example ["map_column", "subkey"] or ["list_column", 2].
                For set and list columns, individual entries are "unrolled"
                automatically.
            filter: a dictionary expressing which condition the inspected rows
                must satisfy. The filter can use operators, such as "$eq" for equality,
                and require columns to compare with literal values. Simple examples
                are `{}` (zero filter), `{"match_no": 123}` (a shorthand for
                `{"match_no": {"$eq": 123}}`, or `{"match_no": 123, "round": "C"}`
                (multiple conditions are implicitly combined with "$and").
                Please consult the Data API documentation for a more detailed
                explanation of table search filters and tips on their usage.
            general_method_timeout_ms: a timeout, in milliseconds, for the whole
                requested operation (which may involve multiple API requests).
                This method, being based on `find` (see) may entail successive HTTP API
                requests, depending on the amount of involved rows.
                If not provided, this object's defaults apply.
            request_timeout_ms: a timeout, in milliseconds, for each API request.
                If not provided, this object's defaults apply.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a list of all different values for `key` found across the rows
            that match the filter. The result list has no repeated items.

        Examples:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> asyncio.run(my_async_table.distinct(
            ...     "winner",
            ...     filter={"match_id": "challenge6"},
            ... ))
            ['Donna', 'Erick', 'Fiona']
            >>>
            >>> # distinct values across the whole table:
            >>> # (not recommended performance-wise)
            >>> asyncio.run(my_async_table.distinct("winner"))
            The Data API returned a warning: {'errorCode': 'ZERO_FILTER_OPERATIONS', ...
            ['Victor', 'Adam Zuul', 'Betta Vigo', 'Caio Gozer', 'Donna', 'Erick', ...
            >>>
            >>> # Over a column containing null values
            >>> # (also with composite filter):
            >>> asyncio.run(my_async_table.distinct(
            ...     "score",
            ...     filter={"match_id": {"$in": ["fight4", "tournamentA"]}},
            ... ))
            [18, None]
            >>>
            >>> # distinct over a set column (automatically "unrolled"):
            >>> asyncio.run(my_async_table.distinct(
            ...     "fighters",
            ...     filter={"match_id": {"$in": ["fight4", "tournamentA"]}},
            ... ))
            [UUID('0193539a-2770-8c09-a32a-111111111111'), UUID('019353e3-00b4-...

        Note:
            It must be kept in mind that `distinct` is a client-side operation,
            which effectively browses all required rows using the logic
            of the `find` method and collects the unique values found for `key`.
            As such, there may be performance, latency and ultimately
            billing implications if the amount of matching rows is large.

        Note:
            For details on the behaviour of "distinct" in conjunction with
            real-time changes in the table contents, see the
            Note of the `find` command.
        """

        # lazy-import here to avoid circular import issues
        from astrapy.cursors import AsyncTableFindCursor

        _general_method_timeout_ms, _gmt_label = _first_valid_timeout(
            (general_method_timeout_ms, "general_method_timeout_ms"),
            (timeout_ms, "timeout_ms"),
            (
                self.api_options.timeout_options.general_method_timeout_ms,
                "general_method_timeout_ms",
            ),
        )
        _request_timeout_ms, _rt_label = _first_valid_timeout(
            (request_timeout_ms, "request_timeout_ms"),
            (self.api_options.timeout_options.request_timeout_ms, "request_timeout_ms"),
        )
        # preparing cursor:
        _extractor = _create_document_key_extractor(key)
        _key = _reduce_distinct_key_to_shallow_safe(key)
        # relaxing the type hint (limited to within this method body)
        f_cursor: AsyncTableFindCursor[dict[str, Any], dict[str, Any]] = (
            AsyncTableFindCursor(
                table=self,
                request_timeout_ms=_request_timeout_ms,
                overall_timeout_ms=_general_method_timeout_ms,
                request_timeout_label=_rt_label,
                overall_timeout_label=_gmt_label,
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
                _item_hash = _hash_table_document(
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
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
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
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            the exact count of matching rows.

        Examples:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> asyncio.run(my_async_table.insert_many([{"seq": i} for i in range(20)]))
            TableInsertManyResult(...)
            >>> asyncio.run(my_async_table.count_documents({}, upper_bound=100))
            20
            >>> asyncio.run(my_async_table.count_documents({"seq":{"$gt": 15}}, upper_bound=100))
            4
            >>> asyncio.run(my_async_table.count_documents({}, upper_bound=10))
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

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        cd_payload = {"countDocuments": {"filter": filter}}
        logger.info(f"countDocuments on '{self.name}'")
        cd_response = await self._api_commander.async_request(
            payload=cd_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
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

    async def estimated_document_count(
        self,
        *,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> int:
        """
        Query the API server for an estimate of the document count in the table.

        Contrary to `count_documents`, this method has no filtering parameters.

        Args:
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a server-provided estimate count of the documents in the table.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> asyncio.run(my_async_table.estimated_document_count())
            5820
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        ed_payload: dict[str, Any] = {"estimatedDocumentCount": {}}
        logger.info(f"estimatedDocumentCount on '{self.name}'")
        ed_response = await self._api_commander.async_request(
            payload=ed_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
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

    async def update_one(
        self,
        filter: FilterType,
        update: dict[str, Any],
        *,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        """
        Update a single document on the table, changing some or all of the columns,
        with the implicit behaviour of inserting a new row if no match is found.

        Args:
            filter: a predicate expressing the table primary key in full,
                i.e. a dictionary defining values for all columns that form the
                primary key. An example may be `{"match_id": "fight4", "round": 1}`.
            update: the update prescription to apply to the row, expressed
                as a dictionary conforming to the Data API syntax. The update
                operators for tables are `$set` and `$unset` (in particular,
                setting a column to None has the same effect as the $unset operator).
                Examples are `{"$set": {"round": 12}}` and
                `{"$unset": {"winner": "", "score": ""}}`.
                Note that the update operation cannot alter the primary key columns.
                See the Data API documentation for more details.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Examples:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> from astrapy.data_types import DataAPISet
            >>>
            >>> # Set a new value for a column
            >>> await my_async_table.update_one(
            ...     {"match_id": "fight4", "round": 1},
            ...     update={"$set": {"winner": "Winona"}},
            ... )
            >>>
            >>> # Set a new value for a column while unsetting another colum
            >>> await my_async_table.update_one(
            ...     {"match_id": "fight4", "round": 1},
            ...     update={"$set": {"winner": None, "score": 24}},
            ... )
            >>>
            >>> # Set a 'set' column to empty
            >>> await my_async_table.update_one(
            ...     {"match_id": "fight4", "round": 1},
            ...     update={"$set": {"fighters": DataAPISet()}},
            ... )
            >>>
            >>> # Set a 'set' column to empty using None
            >>> await my_async_table.update_one(
            ...     {"match_id": "fight4", "round": 1},
            ...     update={"$set": {"fighters": None}},
            ... )
            >>>
            >>> # Set a 'set' column to empty using a regular (empty) set
            >>> await my_async_table.update_one(
            ...     {"match_id": "fight4", "round": 1},
            ...     update={"$set": {"fighters": set()}},
            ... )
            >>>
            >>> # Set a 'set' column to empty using $unset
            >>> await my_async_table.update_one(
            ...     {"match_id": "fight4", "round": 1},
            ...     update={"$unset": {"fighters": None}},
            ... )
            >>>
            >>> # A non-existing primary key creates a new row
            >>> await my_async_table.update_one(
            ...     {"match_id": "bar_fight", "round": 4},
            ...     update={"$set": {"score": 8, "winner": "Jack"}},
            ... )
            >>>
            >>> # Delete column values for a row (they'll read as None now)
            >>> await my_async_table.update_one(
            ...     {"match_id": "challenge6", "round": 2},
            ...     update={"$unset": {"winner": None, "score": None}},
            ... )

        Note:
            a row created entirely with update operations (as opposed to insertions)
            may, correspondingly, be deleted by means of an $unset update on all columns.
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        uo_payload = self._converter_agent.preprocess_payload(
            {
                "updateOne": {
                    k: v
                    for k, v in {
                        "filter": filter,
                        "update": update,
                    }.items()
                    if v is not None
                }
            },
            map2tuple_checker=map2tuple_checker_update_one,
        )
        logger.info(f"updateOne on '{self.name}'")
        uo_response = await self._api_commander.async_request(
            payload=uo_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        logger.info(f"finished updateOne on '{self.name}'")
        if "status" in uo_response:
            # the contents are disregarded and the method just returns:
            return
        else:
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from updateOne API command.",
                raw_response=uo_response,
            )

    async def delete_one(
        self,
        filter: FilterType,
        *,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        """
        Delete a row, matching the provided value of the primary key.
        If no row is found with that primary key, the method does nothing.

        Args:
            filter: a predicate expressing the table primary key in full,
                i.e. a dictionary defining values for all columns that form the
                primary key. A row (at most one) is deleted if it matches that primary
                key. An example filter may be `{"match_id": "fight4", "round": 1}`.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Examples:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> # Count the rows matching a certain filter
            >>> len(asyncio.run(my_async_table.find({"match_id": "fight7"}).to_list()))
            3
            >>>
            >>> # Delete a row belonging to the group
            >>> asyncio.run(
            ...     my_async_table.delete_one({"match_id": "fight7", "round": 2})
            ... )
            >>>
            >>> # Count again
            >>> len(asyncio.run(my_async_table.find({"match_id": "fight7"}).to_list()))
            2
            >>>
            >>> # Attempt the delete again (nothing to delete)
            >>> asyncio.run(
            ...     my_async_table.delete_one({"match_id": "fight7", "round": 2})
            ... )
            >>>
            >>> # The count is unchanged
            >>> len(asyncio.run(my_async_table.find({"match_id": "fight7"}).to_list()))
            2
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
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
            },
            map2tuple_checker=None,
        )
        logger.info(f"deleteOne on '{self.name}'")
        do_response = await self._api_commander.async_request(
            payload=do_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
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
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        """
        Delete all rows matching a provided filter condition.
        This operation can target from a single row to the entirety of the table.

        Args:
            filter: a filter dictionary to specify which row(s) must be deleted.
                1. If the filter is in the form `{"pk1": val1, "pk2": val2 ...}`
                and specified the primary key in full, at most one row is deleted,
                the one with that primary key.
                2. If the table has "partitionSort" columns, some or all of them
                may be left out (the least significant of them can also employ
                an inequality, or range, predicate): a range of rows, but always
                within a single partition, will be deleted.
                3. If an empty filter, `{}`, is passed, this operation empties
                the table completely. *USE WITH CARE*.
                4. Other kinds of filtering clauses are forbidden.
                In the following examples, the table is partitioned
                by columns ["pa1", "pa2"] and has partitionSort "ps1" and "ps2" in that
                order.
                Valid filter examples:
                - `{"pa1": x, "pa2": y, "ps1": z, "ps2": t}`: deletes one row
                - `{"pa1": x, "pa2": y, "ps1": z}`: deletes multiple rows
                - `{"pa1": x, "pa2": y, "ps1": z, "ps2": {"$lt": q}}`: del. multiple rows
                - `{"pa1": x, "pa2": y}`: deletes all rows in the partition
                - `{}`: empties the table (*CAUTION*)
                Invalid filter examples:
                - `{"pa1": x}`: incomplete partition key
                - `{"pa1": x, "ps1" z}`: incomplete partition key (whatever is added)
                - `{"pa1": x, "pa2": y, "ps1": {"$lt": r}, "ps2": t}`: inequality on
                  a non-least-significant partitionSort column provided.
                - `{"pa1": x, "pa2": y, "ps2": t}`: cannot skip "ps1"
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Examples:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> # Delete a single row (full primary key specified):
            >>> await my_async_table.delete_many({"match_id": "fight4", "round": 1})
            >>>
            >>> # Delete part of a partition (inequality on the
            >>> # last-mentioned 'partitionSort' column):
            >>> await my_async_table.delete_many({"match_id": "fight5", "round": {"$gte": 5}})
            >>>
            >>> # Delete a whole partition (leave 'partitionSort' unspecified):
            >>> await my_async_table.delete_many({"match_id": "fight7"})
            >>>
            >>> # empty the table entirely with empty filter (*CAUTION*):
            >>> await my_async_table.delete_many({})
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
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
            },
            map2tuple_checker=None,
        )
        logger.info(f"deleteMany on '{self.name}'")
        dm_response = await self._api_commander.async_request(
            payload=dm_payload,
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
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
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        Drop the table, i.e. delete it from the database along with
        all the rows stored therein.

        Args:
            if_exists: if passed as True, trying to drop a non-existing table
                will not error, just silently do nothing instead. If not provided,
                the API default behaviour will hold.
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> # List tables:
            >>> asyncio.run(my_async_table.database.list_table_names())
            ['games']
            >>>
            >>> # Drop this table:
            >>> asyncio.run(my_table.drop())
            >>>
            >>> # List tables again:
            >>> asyncio.run(my_table.database.list_table_names())
            []
            >>>
            >>> # Try working on the table now:
            >>> from astrapy.exceptions import DataAPIResponseException
            >>>
            >>> async def try_use_table():
            ...     try:
            ...         my_table.find_one({})
            ...     except DataAPIResponseException as err:
            ...         print(str(err))
            ...
            >>> asyncio.run(try_use_table())
            Collection does not exist [...] (COLLECTION_NOT_EXIST)

        Note:
            Use with caution.

        Note:
            Once the method succeeds, methods on this object can still be invoked:
            however, this hardly makes sense as the underlying actual table
            is no more.
            It is responsibility of the developer to design a correct flow
            which avoids using a deceased collection any further.
        """

        logger.info(f"dropping table '{self.name}' (self)")
        drop_result = await self.database.drop_table(
            self.name,
            if_exists=if_exists,
            table_admin_timeout_ms=table_admin_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        logger.info(f"finished dropping table '{self.name}' (self)")
        return drop_result

    async def command(
        self,
        body: dict[str, Any] | None,
        *,
        raise_api_errors: bool = True,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> dict[str, Any]:
        """
        Send a POST request to the Data API for this table with
        an arbitrary, caller-provided payload.
        No transformations or type conversions are made on the provided payload.

        Args:
            body: a JSON-serializable dictionary, the payload of the request.
            raise_api_errors: if True, responses with a nonempty 'errors' field
                result in an astrapy exception being raised.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, this object's defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a dictionary with the response of the HTTP request.

        Example:
            >>> # NOTE: may require slight adaptation to an async context.
            >>>
            >>> asyncio.run(my_async_table.command({
            ...     "findOne": {
            ...         "filter": {"match_id": "fight4"},
            ...         "projection": {"winner": True},
            ...     }
            ... }))
            {'data': {'document': {'winner': 'Victor'}}, 'status': ...  # shortened
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
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
            timeout_context=_TimeoutContext(
                request_ms=_request_timeout_ms, label=_rt_label
            ),
        )
        logger.info(f"finished command={_cmd_desc} on '{self.name}'")
        return command_result
