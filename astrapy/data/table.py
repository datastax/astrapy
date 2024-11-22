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
from typing import TYPE_CHECKING, Any, Generic, Iterable, Sequence, TypeVar, overload

from astrapy.constants import (
    ROW,
    CallerType,
    DefaultRowType,
    FilterType,
    ProjectionType,
    SortType,
    normalize_optional_projection,
)
from astrapy.data.info.table_descriptor import AlterTableOperation
from astrapy.data.utils.distinct_extractors import (
    _create_document_key_extractor,
    _hash_document,
    _reduce_distinct_key_to_shallow_safe,
)
from astrapy.data.utils.table_converters import _TableConverterAgent
from astrapy.database import AsyncDatabase, Database
from astrapy.exceptions import (
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
from astrapy.utils.api_options import APIOptions, FullAPIOptions, TimeoutOptions
from astrapy.utils.unset import _UNSET, UnsetType

if TYPE_CHECKING:
    from astrapy.authentication import EmbeddingHeadersProvider
    from astrapy.cursors import AsyncTableFindCursor, TableFindCursor
    from astrapy.data.info.table_descriptor import AlterTableOperation
    from astrapy.info import ListTableDefinition


logger = logging.getLogger(__name__)

NEW_ROW = TypeVar("NEW_ROW")


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
        >>> from astrapy import DataAPIClient, Table
        >>> my_client = astrapy.DataAPIClient()
        >>> my_db = my_client.get_database(
        ...     "https://01234567-....apps.astra.datastax.com",
        ...     token="AstraCS:..."
        ... )

        >>> from astrapy.constants import SortMode
        >>> from astrapy.info import (
        ...     CreateTableDefinition,
        ...     TablePrimaryKeyDescriptor,
        ...     TableScalarColumnTypeDescriptor,
        ...     TableVectorColumnTypeDescriptor,
        ... )
        >>> table_definition_1 = CreateTableDefinition(
        ...     columns={
        ...         "p_text": TableScalarColumnTypeDescriptor(column_type="text"),
        ...         "p_int": TableScalarColumnTypeDescriptor(column_type="int"),
        ...         "p_vector": TableVectorColumnTypeDescriptor(
        ...             column_type="vector", dimension=3, service=None
        ...         ),
        ...     },
        ...     primary_key=TablePrimaryKeyDescriptor(
        ...         partition_by=["p_text"],
        ...         partition_sort={"p_int": SortMode.ASCENDING},
        ...     ),
        ... )
        >>> my_table_1 = my_db.create_table(
        ...     "my_v_table_1",
        ...     definition=table_definition_1,
        ... )

        >>> table_definition_2 = {
        ...     'columns': {
        ...         'p_text': {'type': 'text'},
        ...         'p_int': {'type': 'int'},
        ...         'p_vector': {'type': 'vector', 'dimension': 3}
        ...     },
        ...     'primaryKey': {
        ...         'partitionBy': ['p_text'],
        ...         'partitionSort': {'p_int': SortMode.ASCENDING}
        ...     }
        ... }
        >>> my_table_2 = my_db.create_table(
        ...     "my_v_table_2",
        ...     definition=table_definition_2,
        ... )

        >>> table_definition_3 = (
        ...     CreateTableDefinition.zero()
        ...     .add_column("p_text", "text")
        ...     .add_column("p_int", "int")
        ...     .add_vector_column("p_vector", dimension=3)
        ...     .add_partition_by(["p_text"])
        ...     .add_partition_sort({"p_int": SortMode.ASCENDING})
        ... )
        >>> my_table_3 = my_db.create_table(
        ...     "my_v_table_3",
        ...     definition=table_definition_3,
        ... )

        >>> my_table_4 = my_db.get_table("my_already_existing_table")

    Note:
        creating an instance of Table does not trigger actual creation
        of the table on the database. The latter should have been created
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
        database: Database | None = None,
        name: str | None = None,
        keyspace: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        callers: Sequence[CallerType] | UnsetType = _UNSET,
        request_timeout_ms: int | UnsetType = _UNSET,
        table_timeout_ms: int | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> Table[ROW]:
        arg_api_options = APIOptions(
            callers=callers,
            embedding_api_key=embedding_api_key,
            timeout_options=TimeoutOptions(
                request_timeout_ms=table_timeout_ms,
            ),
        )
        # a double override for the timeout aliasing
        arg_api_options_2 = APIOptions(
            timeout_options=TimeoutOptions(
                request_timeout_ms=request_timeout_ms,
            ),
        )
        final_api_options = (
            self.api_options.with_override(api_options)
            .with_override(arg_api_options)
            .with_override(arg_api_options_2)
        )
        return Table(
            database=database or self.database,
            name=name or self.name,
            keyspace=keyspace or self.keyspace,
            api_options=final_api_options,
        )

    def with_options(
        self: Table[ROW],
        *,
        name: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        callers: Sequence[CallerType] | UnsetType = _UNSET,
        request_timeout_ms: int | UnsetType = _UNSET,
        table_timeout_ms: int | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> Table[ROW]:
        """
        Create a clone of this table with some changed attributes.

        Args:
            name: the name of the table. This parameter is useful to
                quickly spawn Table instances each pointing to a different
                table existing in the same keyspace.
            embedding_api_key: optional API key(s) for interacting with the table.
                If an embedding service is configured, and this parameter is not None,
                each Data API call will include the necessary embedding-related headers
                as specified by this parameter. If a string is passed, it translates
                into the one "embedding api key" header
                (i.e. `astrapy.authentication.EmbeddingAPIKeyHeaderProvider`).
                For some vectorize providers/models, if using header-based authentication,
                specialized subclasses of `astrapy.authentication.EmbeddingHeadersProvider`
                should be supplied.
            callers: a list of caller identities, i.e. applications, or frameworks,
                on behalf of which the Data API calls are performed. These end up
                in the request user-agent.`
                Each caller identity is a ("caller_name", "caller_version") pair.
            request_timeout_ms: a default timeout, in millisecond, for the duration of
                each API request on the table.  For a more fine-grained
                control of table timeouts (suggested e.g. with regard to
                methods involving multiple requests, such as `find`), use of the
                `api_options` parameter is suggested; alternatively,
                bear in mind that individual table methods also accept timeout
                parameters.
            table_timeout_ms: an alias for `request_timeout_ms`.
            api_options: any additional options to set for the clone, in the form of
                an APIOptions instance (where one can set just the needed attributes).
                In case the same setting is also provided as named parameter,
                the latter takes precedence.

        Returns:
            a new Table instance.

        Example:
            >>> my_other_table = my_table.with_options(
            ...     name="the_other_table",
            ...     callers=[("caller_identity", "0.1.2")],
            ... )
        """

        return self._copy(
            name=name,
            embedding_api_key=embedding_api_key,
            callers=callers,
            request_timeout_ms=request_timeout_ms,
            table_timeout_ms=table_timeout_ms,
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
        table_timeout_ms: int | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncTable[ROW]:
        """
        TODO
        """

        arg_api_options = APIOptions(
            callers=callers,
            embedding_api_key=embedding_api_key,
            timeout_options=TimeoutOptions(
                request_timeout_ms=table_timeout_ms,
            ),
        )
        # a double override for the timeout aliasing
        arg_api_options_2 = APIOptions(
            timeout_options=TimeoutOptions(
                request_timeout_ms=request_timeout_ms,
            ),
        )
        final_api_options = (
            self.api_options.with_override(api_options)
            .with_override(arg_api_options)
            .with_override(arg_api_options_2)
        )
        return AsyncTable(
            database=database or self.database.to_async(),
            name=name or self.name,
            keyspace=keyspace or self.keyspace,
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
        TODO
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, the Table defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.
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
            raise ValueError(
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
        TODO
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
        *,
        column: str,
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
            name: the name of the index.
            column: the table column on which the index is to be created.
            options: if passed, it must be an instance of `TableIndexOptions`,
                or an equivalent dictionary, which specifies index settings
                such as -- for a text column -- case-sensitivity and so on.
                See the `astrapy.info.TableIndexOptions` class for more details.
            if_not_exists: if set to True, the command will succeed even if an index
                with the specified name already exists (in which case no actual
                index creation takes place on the database). The API default of False
                means that an error is raised by the API in case of name collision.
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, the Table defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Example:
            >>> from astrapy.info import TableIndexOptions
            >>> my_table.create_index(
            ...     "score_index",
            ...     column="score",
            ... )
            >>>
            >>> my_table.create_index(
            ...     "winner_index",
            ...     column="winner",
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
        *,
        column: str,
        options: TableVectorIndexOptions | dict[str, Any],
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
            name: the name of the index.
            column: the table column, of type "vector" on which to create the index.
            options: an instance of `TableVectorIndexOptions`, or an equivalent
                dictionary, which specifies settings for the vector index,
                such as the metric to use or, if desired, a "source model" setting.
                See the `astrapy.info.TableVectorIndexOptions` class for more details.
            if_not_exists: if set to True, the command will succeed even if an index
                with the specified name already exists (in which case no actual
                index creation takes place on the database). The API default of False
                means that an error is raised by the API in case of name collision.
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, the Table defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Example:
            >>> from astrapy.constants import VectorMetric
            >>> from astrapy.info import TableVectorIndexOptions
            >>>
            >>> my_table.create_vector_index(
            ...     "m_vector_index",
            ...     column="m_vector",
            ...     options=TableVectorIndexOptions(metric=VectorMetric.DOT_PRODUCT),
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
                underlying API request. If not provided, the Table defaults apply.
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

    def list_indexes(
        self,
        *,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[TableIndexDescriptor]:
        """
        List the full definitions of all indexes existing on this table.

        Args:
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, the Table defaults apply.
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
        if "indexes" not in li_response.get("status", {}):
            raise UnexpectedDataAPIResponseException(
                text="Faulty response from listIndexes API command.",
                raw_response=li_response,
            )
        else:
            logger.info("finished listIndexes")
            return [
                TableIndexDescriptor.coerce(index_object)
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
                underlying API request. If not provided, the Table defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Example:
            >>> from astrapy.info import (
            ...     AlterTableAddColumns,
            ...     AlterTableAddVectorize,
            ...     AlterTableDropColumns,
            ...     AlterTableDropVectorize,
            ...     TableScalarColumnTypeDescriptor,
            ...     VectorServiceOptions,
            ... )
            >>>
            >>> new_table_1 = my_table.alter(
            ...     AlterTableAddColumns(
            ...         columns={
            ...             "tie_break": TableScalarColumnTypeDescriptor(
            ...                 column_type="boolean",
            ...             ),
            ...         }
            ...     )
            ... )
            >>>
            >>> new_table_2 = new_table_1.alter(
            ...     AlterTableDropColumns(columns=["tie_break"]),
            ... )
            >>>
            >>> new_table_3 = new_table_2.alter(
            ...     AlterTableAddVectorize(
            ...         columns={
            ...             "m_vector": VectorServiceOptions(
            ...                 provider="openai",
            ...                 model_name="text-embedding-3-small",
            ...                 authentication={
            ...                     "providerKey": "MY_API_KEY_STORED_SECRET_NAME",
            ...                 },
            ...             ),
            ...         }
            ...     )
            ... )
            >>>
            >>> from typing import TypedDict
            >>> from astrapy import Table
            >>> from astrapy.data_types import DataAPITimestamp
            >>>
            >>> class MyCustomDictClass(TypedDict):
            ...     match_no: int
            ...     round: str
            ...     winner: str
            ...     score: int
            ...     when: DataAPITimestamp
            ...
            >>> new_table_4: Table[MyCustomDictClass] = new_table_3.alter(
            ...     AlterTableDropVectorize(columns=["m_vector"]),
            ...     row_type=MyCustomDictClass,
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
                if desired.
                The values for the various columns supplied in the row must
                be of the right data type for the insertion to succeed.
                Non-primary-key columns can also be explicitly set to null.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, the Table defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a TableInsertOneResult object, whose attributes are the primary key
            of the inserted row both in the form of a dictionary and of a tuple.

        Examples:
            >>> # a full-row insert using astrapy's datatypes when available
            >>> from astrapy.data_types import (
            ...     DataAPISet,
            ...     DataAPITimestamp,
            ...     DataAPIVector,
            ... )
            >>> insert_result = my_table.insert_one(
            ...     {
            ...         "match_no": 1012,
            ...         "round": "A",
            ...         "winner": "Victor",
            ...         "score": 18,
            ...         "when": DataAPITimestamp.from_string("2024-11-28T11:30:00Z"),
            ...         "tags": DataAPISet(["worldcup", "placeholder_tag"]),
            ...         "m_vector": DataAPIVector([0.4, -0.6, 0.2]),
            ...     },
            ... )
            >>> insert_result.inserted_id
            {'match_no': 1012, 'round': 'A'}
            >>> insert_result.inserted_id_tuple
            (1012, 'A')

            >>> # a partial-row overwrite
            >>> my_table.insert_one(
            ...     {
            ...         "match_no": 1012,
            ...         "round": "A",
            ...         "winner": "Victor Vector",
            ...         "tags": DataAPISet(["worldcup", "championship"]),
            ...     },
            ... )
            TableInsertOneResult(inserted_id={'match_no': 1012, ...)  # Note: shortened

            >>> # an insert using only standard library data types
            >>> from datetime import datetime, timezone
            >>> my_table.insert_one(
            ...     {
            ...         "match_no": 975,
            ...         "round": "B",
            ...         "winner": "Angela",
            ...         "score": 25,
            ...         "when": datetime(
            ...             2024, 7, 13, 12, 55, 30, 889, tzinfo=timezone.utc
            ...         ),
            ...         "tags": {"tiebreak", "epic"},
            ...         "m_vector": [0.4, -0.6, 0.2],
            ...     },
            ... )
            TableInsertOneResult(inserted_id={'match_no': 975, ...)  # Note: shortened
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        io_payload = self._converter_agent.preprocess_payload(
            {"insertOne": {"document": row}}
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
            if "primaryKeySchema" not in status:
                raise UnexpectedDataAPIResponseException(
                    text=(
                        "received a 'status' without 'primaryKeySchema' "
                        f"in API response (received: {status})"
                    ),
                    raw_response=None,
                )
            if "insertedIds" not in status:
                raise UnexpectedDataAPIResponseException(
                    text=(
                        "received a 'status' without 'insertedIds' "
                        f"in API response (received: {status})"
                    ),
                    raw_response=None,
                )
            primary_key_schema = status["primaryKeySchema"]
            id_tuples_and_ids = self._converter_agent.postprocess_keys(
                status["insertedIds"],
                primary_key_schema_dict=primary_key_schema,
            )
            id_tuples = [tpl for tpl, _ in id_tuples_and_ids]
            ids = [id for _, id in id_tuples_and_ids]
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
                while any other column may be omitted if desired.
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
                If not provided, the corresponding Table defaults apply.
            request_timeout_ms: a timeout, in milliseconds, to impose on each
                individual HTTP request to the Data API to accomplish the operation.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a TableInsertManyResult object, whose attributes are the primary key
            of the inserted rows both in the form of dictionaries and of tuples.

        Examples:
            >>> from datetime import datetime, timezone
            >>> from astrapy.data_types import DataAPISet, DataAPITimestamp, DataAPIVector
            >>>
            >>> # Unordered insertion (with concurrency for performance)
            >>> insert_result = my_table.insert_many(
            ...     [
            ...         {
            ...             "match_no": 1012,
            ...             "round": "A",
            ...             "winner": "Victor",
            ...             "score": 18,
            ...             "when": DataAPITimestamp.from_string(
            ...                 "2024-11-28T11:30:00Z",
            ...             ),
            ...             "tags": DataAPISet(["worldcup", "placeholder_tag"]),
            ...             "m_vector": DataAPIVector([0.4, -0.6, 0.2]),
            ...         },
            ...         {"match_no": 991, "round": "A", "winner": "Adam"},
            ...         {"match_no": 991, "round": "B", "winner": "Betta"},
            ...         {"match_no": 991, "round": "C", "winner": "Caio"},
            ...         {
            ...             "match_no": 995,
            ...             "round": "A",
            ...             "winner": "Donna",
            ...             "m_vector": [0.9, -0.1, -0.3],
            ...         },
            ...         {"match_no": 995, "round": "B", "winner": "Erick"},
            ...         {"match_no": 995, "round": "C", "winner": "Fiona"},
            ...         {"match_no": 997, "round": "A", "winner": "Gael"},
            ...         {"match_no": 997, "round": "B", "winner": "Hanna"},
            ...         {
            ...             "match_no": 997,
            ...             "round": "C",
            ...             "winner": "Ian",
            ...             "when": datetime(
            ...                 2023, 9, 28, 18, 12, 45, tzinfo=timezone.utc
            ...             ),
            ...             "tags": {"dull"},
            ...         },
            ...         {"match_no": 443, "round": "A", "winner": "Joy"},
            ...         {"match_no": 443, "round": "B", "winner": "Kevin"},
            ...         {"match_no": 443, "round": "C", "winner": "Lauretta"},
            ...     ],
            ...     concurrency=10,
            ... )
            >>> insert_result.inserted_ids
            [{'match_no': 1012, 'round': 'A'}, {'match_no': 991, ...}, ...]  # Note: shortened
            >>> insert_result.inserted_id_tuples
            [(1012, 'A'), (991, 'A'), (991, 'B'), (991, 'C'), (995, 'A'), ...]  # Note: shortened

            >>> # Ordered insertion (stop on first failure and predictable end result on DB)
            >>> my_table.insert_many(
            ...     [
            ...         {"match_no": 991, "round": "A", "winner": "Adam0"},
            ...         {"match_no": 991, "round": "B", "winner": "Bett0a"},
            ...         {"match_no": 991, "round": "C", "winner": "Caio0"},
            ...         {"match_no": 991, "round": "A", "winner": "Adam1"},
            ...         {"match_no": 991, "round": "B", "winner": "Betta1"},
            ...         {"match_no": 991, "round": "C", "winner": "Caio1"},
            ...         {"match_no": 991, "round": "A", "winner": "Adam2"},
            ...         {"match_no": 991, "round": "B", "winner": "Betta2"},
            ...         {"match_no": 991, "round": "C", "winner": "Caio2"},
            ...         {"match_no": 991, "round": "A", "winner": "Adam3"},
            ...         {"match_no": 991, "round": "B", "winner": "Betta3"},
            ...         {"match_no": 991, "round": "C", "winner": "Caio3"},
            ...     ],
            ...     ordered=True,
            ... )
            TableInsertManyResult(inserted_ids=[{'match_no': 991, ...}, ...]  # Note: shortened

        Note:
            Unordered insertions are executed with some degree of concurrency,
            so it is usually better to prefer this mode unless the order in the
            row sequence is important.

        Note:
            If some of the rows are unsuitable for insertion, for instance
            have the wrong data type for a column or lack the primary key,
            the Data API validation check will fail for those specific requests
            that contain the faulty rows. Depending on concurrency and the value
            of the `ordered` parameter, a number of rows in general could have
            been successfully inserted.
            It is possible to capture such a scenario, and inspect which rows
            actually got inserted, by catching an error of type
            `astrapy.exceptions.TableInsertManyException`: its `partial_result`
            attribute is precisely a `TableInsertManyResult`, encoding details
            on the successful writes.
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
        timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=_general_method_timeout_ms,
            timeout_label=_gmt_label,
        )
        if ordered:
            options = {"ordered": True}
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
                # if errors, quit early
                if chunk_response.get("errors", []):
                    partial_result = TableInsertManyResult(
                        raw_results=raw_results,
                        inserted_ids=inserted_ids,
                        inserted_id_tuples=inserted_id_tuples,
                    )
                    raise TableInsertManyException.from_response(
                        command=None,
                        raw_response=chunk_response,
                        partial_result=partial_result,
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
            options = {"ordered": False}
            if _concurrency > 1:
                with ThreadPoolExecutor(max_workers=_concurrency) as executor:

                    def _chunk_insertor(
                        row_chunk: list[dict[str, Any]],
                    ) -> dict[str, Any]:
                        im_payload = self._converter_agent.preprocess_payload(
                            {
                                "insertMany": {
                                    "documents": row_chunk,
                                    "options": options,
                                },
                            },
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
                        return im_response

                    raw_results = list(
                        executor.map(
                            _chunk_insertor,
                            (
                                _rows[i : i + _chunk_size]
                                for i in range(0, len(_rows), _chunk_size)
                            ),
                        )
                    )
            else:
                for i in range(0, len(_rows), _chunk_size):
                    im_payload = self._converter_agent.preprocess_payload(
                        {
                            "insertMany": {
                                "documents": _rows[i : i + _chunk_size],
                                "options": options,
                            },
                        },
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
            if any(
                [chunk_response.get("errors", []) for chunk_response in raw_results]
            ):
                partial_result = TableInsertManyResult(
                    raw_results=raw_results,
                    inserted_ids=inserted_ids,
                    inserted_id_tuples=inserted_id_tuples,
                )
                raise TableInsertManyException.from_responses(
                    commands=[None for _ in raw_results],
                    raw_responses=raw_results,
                    partial_result=partial_result,
                )

            # return
            full_result = TableInsertManyResult(
                raw_results=raw_results,
                inserted_ids=inserted_ids,
                inserted_id_tuples=inserted_id_tuples,
            )
            logger.info(f"finished inserting {len(_rows)} rows in '{self.name}'")
            return full_result

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
        timeout_ms: int | None = None,
    ) -> TableFindCursor[ROW, ROW]:
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
            filter: a dictionary expressing which condition must the returned rows
                satisfy. The filter can use operators, such as "$eq" for equality,
                and require columns to compare with literal values. Simple examples
                are `{}` (zero filter), `{"match_no": 123}` (a shorthand for
                `{"match_no": {"$eq": 123}}`, or `{"match_no": 123, "round": "C"}`
                (multiple conditions are implicitly combined with "$and").
                Please consult the Data API documentation for a more detailed
                explanation of table search filters and tips on their usage.
            projection: a prescription on which columns to return for the matching rows.
                The projection can take the form `{"column1": True, "column2": True}`.
                `{"*": True}` (i.e. return the whole row), or the complementary
                form that excludes columns: `{"column1": False, "column2": False}`.
                To optimize bandwidth usage, it is recommended to use a projection,
                especially for columns of type vector with high-dimensional embeddings.
            skip: X,
            limit: X,
            include_similarity: X,
            include_sort_vector: X,
            sort: X,
            request_timeout_ms: X,
            timeout_ms: X,

        Note:
            As the rows are retrieved in chunks progressively, while the cursor
            is being iterated over, it is possible that the actual results
            obtained will reflect changes occurring to the table contents in
            real time.

        Examples:
            >>> # Iterate over results:
            >>> for row in my_table.find({"match_no": 995}):
            ...     print(f"({row['match_no']}/{row['round']}): winner {row['winner']}")
            ...
            (995/A): winner Donna
            (995/B): winner Erick
            (995/C): winner Fiona

            >>> # Optimize bandwidth using a projection:
            >>> projection = {"round": True, "winner": True}
            >>> for row in my_table.find({"match_no": 995}, projection=projection):
            ...     print(f"(995/{row['round']}): winner {row['winner']}")
            ...
            (995/A): winner Donna
            (995/B): winner Erick
            (995/C): winner Fiona

            >>> # Filter on the partition key:
            >>> my_table.find({"match_no": 991}).to_list()
            [{'match_no': 991, 'round': 'A', ...}, ...]  # Note: shortened

            >>> # Filter on primary key:
            >>> my_table.find({"match_no": 991, "round": "C"}).to_list()
            [{'match_no': 991, 'round': 'C', ...}, ...]  # Note: shortened

            >>> # Filter on a regular indexed column:
            >>> my_table.find({"winner": "Caio3"}).to_list()
            [{'match_no': 991, 'round': 'C', ...}, ...]  # Note: shortened

            >>> # Non-equality filter on a regular indexed column:
            >>> my_table.find({"score": {"$gte": 15}}).to_list()
            [{'match_no': 1012, 'round': 'A', ...}, ...]  # Note: shortened

            >>> # Filter on a regular non-indexed column:
            >>> my_table.find(
            ...     {"when": DataAPITimestamp.from_string("1999-12-31T01:23:44Z")}
            ... ).to_list()
            The Data API returned a warning: {'errorCode': 'MISSING_INDEX', ...
            []

            >>> # Empty filter (not recommended performance-wise):
            >>> my_table.find({}).to_list()
            The Data API returned a warning: {'errorCode': 'ZERO_FILTER_OPERATIONS', ...
            [{'match_no': 123, 'round': 'A', ...}, ...]  # Note: shortened

            >>> # Filter on the primary key and a regular non-indexed column:
            >>> # (not recommended performance-wise)
            >>> my_table.find(
            ...     {"match_no": 991, "round": "C", "winner": "Caio3"}
            ... ).to_list()
            The Data API returned a warning: {'errorCode': 'MISSING_INDEX',...
            [{'match_no': 991, 'round': 'C', ...}, ...]  # Note: shortened

            >>> # Filter on a regular non-indexed column, omitting part of the pr. key:
            >>> # (not recommended performance-wise)
            >>> my_table.find({"round": "C", "winner": "Caio3"}).to_list()
            The Data API returned a warning: {'errorCode': 'MISSING_INDEX', ...
            [{'match_no': 991, 'round': 'C', ...}, ...]  # Note: shortened

            >>> # Vector search with "sort" (on a vector column with an index on it):
            >>> my_table.find(
            ...     {},
            ...     sort={'m_vector': DataAPIVector([0.2, 0.3, 0.4])},
            ...     projection={"winner": True},
            ...     limit=3,
            ... ).to_list()
            [{'winner': 'Donna'}, {'winner': 'Victor'}, {'winner': 'Angela'}]

            >>> # Return the numeric value of the vector similarity
            >>> # (also demonstrating that one can pass a plain list for a vector):
            >>> my_table.find(
            ...     {},
            ...     sort={'m_vector': [0.2, 0.3, 0.4]},
            ...     projection={"winner": True},
            ...     limit=3,
            ...     include_similarity=True,
            ... ).to_list()
            [{'winner': 'Donna', '$similarity': 0.515}, ...]  # Note: shortened

            >>> # Regular sorting on a column:
            >>> my_table.find(
            ...     {"match_no": 991},
            ...     sort={'round': SortMode.DESCENDING},
            ...     projection={"winner": True},
            ... ).to_list()
            [{'winner': 'Caio3'}, {'winner': 'Betta3'}, {'winner': 'Adam3'}]

            >>> # Using `skip` and `limit`:
            >>> my_table.find(
            ...     {"match_no": 991},
            ...     sort={'round': SortMode.DESCENDING},
            ...     projection={"winner": True},
            ...     skip=1,
            ...     limit=2,
            ... ).to_list()
            The Data API returned a warning: {'errorCode': 'IN_MEMORY_SORTING...
            [{'winner': 'Betta3'}, {'winner': 'Adam3'}]

            >>> # Using `.map()` on a cursor:
            >>> winner_cursor = my_table.find(
            ...     {"match_no": 991},
            ...     sort={'round': SortMode.DESCENDING},
            ...     projection={"winner": True},
            ...     limit=5,
            ... )
            >>> print("/".join(winner_cursor.map(lambda row: row["winner"].upper())))
            CAIO3/BETTA3/ADAM3

            >>> # Some other examples of cursor manipulation
            >>> matches_cursor = my_table.find(
            ...     sort={"m_vector": DataAPIVector([-0.1, 0.15, 0.3])}
            ... )
            >>> matches_cursor.has_next()
            True
            >>> next(matches_cursor)
            {'match_no': 1012, 'round': 'A', 'm_vector':...}  # Note: shortened
            >>> matches_cursor.consumed
            1
            >>> matches_cursor.rewind()
            >>> matches_cursor.consumed
            0
            >>> matches_cursor.has_next()
            True
            >>> matches_cursor.close()
            >>> next(matches_cursor)
            StopIteration  # Exception raised
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
        TODO
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
            }
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
        key: str,
        *,
        filter: FilterType | None = None,
        general_method_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[Any]:
        """
        TODO
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
        if _key == "":
            raise ValueError(
                "The 'key' parameter for distinct cannot be empty "
                "or start with a list index."
            )
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
            general_method_timeout_ms: TODO
            request_timeout_ms: a timeout, in milliseconds, for the API HTTP request.
                If not passed, the table-level setting is used instead.
            timeout_ms: an alias for `request_timeout_ms`.

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
            general_method_timeout_ms: TODO
            request_timeout_ms: a timeout, in milliseconds, for the API HTTP request.
                If not passed, the table-level setting is used instead.
            timeout_ms: an alias for `request_timeout_ms`.

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
        Update a single document on the table as requested,
        with the implicit behaviour of inserting a new one if no match is found.

        Args:
            filter: a predicate expressing in full a primary key, i.e. a dictionary
                defining values for all columns that form the table's primary key.
                Examples:
                    {"code": 123}
                    {"country": "UK", "year": 2024}
            update: the update prescription to apply to the row, expressed
                as a dictionary as per Data API syntax. Examples are:
                    {"$set": {"field": "value}}
                    {"$unset": {"field": ""}}
                Primary key fields cannot be provided for a "$set" operation.
                For Tables, a limited set of update operators apply.
                See the Data API documentation for more details.
            general_method_timeout_ms: TODO
            request_timeout_ms: a timeout, in milliseconds, for the API HTTP request.
                If not passed, the table-level setting is used instead.
            timeout_ms: an alias for `request_timeout_ms`.

        Example:
            >>> # Assume "country" and "year" make the primary key and the table starts empty.
            >>> my_table.insert_one({"country": "UK", "year": 2024, "colours": ["yellow", "blue"]})
            TableInsertOneResult(...)
            >>> my_table.update_one({"country": "UK", "year": 2024}, update={"$set": {"colours": []}})
            >>> # the following will create a new row:
            >>> my_table.update_one({"country": "ES", "year": 2020}, update={"$set": {"colours": ["amarillo"]}})
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        uo_payload = {
            "updateOne": {
                k: v
                for k, v in {
                    "filter": filter,
                    "update": self._converter_agent.preprocess_payload(update),
                }.items()
                if v is not None
            }
        }
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
        TODO
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
            }
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
        TODO
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
            }
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
        TODO

            if_exists: if passed as True, trying to drop a non-existing table
                will not error, just silently do nothing instead. If not provided,
                the API default behaviour will hold.
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, the Table defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.
        """

        logger.info(f"dropping table '{self.name}' (self)")
        self.database.drop_table(
            self,
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
        TODO
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
    wherefrom the Table inherits its API options such as authentication
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
        >>> from astrapy import DataAPIClient, Table
        >>> my_client = astrapy.DataAPIClient()
        >>> my_async_db = my_client.get_async_database(
        ...     "https://01234567-....apps.astra.datastax.com",
        ...     token="AstraCS:..."
        ... )

        >>> from astrapy.constants import SortMode
        >>> from astrapy.info import (
        ...     CreateTableDefinition,
        ...     TablePrimaryKeyDescriptor,
        ...     TableScalarColumnTypeDescriptor,
        ...     TableVectorColumnTypeDescriptor,
        ... )
        >>> table_definition_1 = CreateTableDefinition(
        ...     columns={
        ...         "p_text": TableScalarColumnTypeDescriptor(column_type="text"),
        ...         "p_int": TableScalarColumnTypeDescriptor(column_type="int"),
        ...         "p_vector": TableVectorColumnTypeDescriptor(
        ...             column_type="vector", dimension=3, service=None
        ...         ),
        ...     },
        ...     primary_key=TablePrimaryKeyDescriptor(
        ...         partition_by=["p_text"],
        ...         partition_sort={"p_int": SortMode.ASCENDING},
        ...     ),
        ... )
        >>> my_async_table_1 = await my_async_db.create_table(
        ...     "my_v_table_1",
        ...     definition=table_definition_1,
        ... )

        >>> table_definition_2 = {
        ...     'columns': {
        ...         'p_text': {'type': 'text'},
        ...         'p_int': {'type': 'int'},
        ...         'p_vector': {'type': 'vector', 'dimension': 3}
        ...     },
        ...     'primaryKey': {
        ...         'partitionBy': ['p_text'],
        ...         'partitionSort': {'p_int': SortMode.ASCENDING}
        ...     }
        ... }
        >>> my_async_table_2 = await my_async_db.create_table(
        ...     "my_v_table_2",
        ...     definition=table_definition_2,
        ... )

        >>> table_definition_3 = (
        ...     CreateTableDefinition.zero()
        ...     .add_column("p_text", "text")
        ...     .add_column("p_int", "int")
        ...     .add_vector_column("p_vector", dimension=3)
        ...     .add_partition_by(["p_text"])
        ...     .add_partition_sort({"p_int": SortMode.ASCENDING})
        ... )
        >>> my_async_table_3 = await my_async_db.create_table(
        ...     "my_v_table_3",
        ...     definition=table_definition_3,
        ... )

        >>> my_table_4 = await my_db.get_table("my_already_existing_table")

    Note:
        creating an instance of Table does not trigger actual creation
        of the table on the database. The latter should have been created
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
            raise ValueError("Attempted to create Table with 'keyspace' unset.")

        self._database = database._copy(
            keyspace=_keyspace, api_options=self.api_options
        )
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
        database: AsyncDatabase | None = None,
        name: str | None = None,
        keyspace: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        callers: Sequence[CallerType] | UnsetType = _UNSET,
        request_timeout_ms: int | UnsetType = _UNSET,
        table_timeout_ms: int | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncTable[ROW]:
        arg_api_options = APIOptions(
            callers=callers,
            embedding_api_key=embedding_api_key,
            timeout_options=TimeoutOptions(
                request_timeout_ms=table_timeout_ms,
            ),
        )
        # a double override for the timeout aliasing
        arg_api_options_2 = APIOptions(
            timeout_options=TimeoutOptions(
                request_timeout_ms=request_timeout_ms,
            ),
        )
        final_api_options = (
            self.api_options.with_override(api_options)
            .with_override(arg_api_options)
            .with_override(arg_api_options_2)
        )
        return AsyncTable(
            database=database or self.database,
            name=name or self.name,
            keyspace=keyspace or self.keyspace,
            api_options=final_api_options,
        )

    def with_options(
        self: AsyncTable[ROW],
        *,
        name: str | None = None,
        embedding_api_key: str | EmbeddingHeadersProvider | UnsetType = _UNSET,
        callers: Sequence[CallerType] | UnsetType = _UNSET,
        request_timeout_ms: int | UnsetType = _UNSET,
        table_timeout_ms: int | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> AsyncTable[ROW]:
        """
        Create a clone of this table with some changed attributes.

        Args:
            name: the name of the table. This parameter is useful to
                quickly spawn AsyncTable instances each pointing to a different
                table existing in the same keyspace.
            embedding_api_key: optional API key(s) for interacting with the table.
                If an embedding service is configured, and this parameter is not None,
                each Data API call will include the necessary embedding-related headers
                as specified by this parameter. If a string is passed, it translates
                into the one "embedding api key" header
                (i.e. `astrapy.authentication.EmbeddingAPIKeyHeaderProvider`).
                For some vectorize providers/models, if using header-based authentication,
                specialized subclasses of `astrapy.authentication.EmbeddingHeadersProvider`
                should be supplied.
            callers: a list of caller identities, i.e. applications, or frameworks,
                on behalf of which the Data API calls are performed. These end up
                in the request user-agent.`
                Each caller identity is a ("caller_name", "caller_version") pair.
            request_timeout_ms: a default timeout, in millisecond, for the duration of
                each API request on the table.  For a more fine-grained
                control of table timeouts (suggested e.g. with regard to
                methods involving multiple requests, such as `find`), use of the
                `api_options` parameter is suggested; alternatively,
                bear in mind that individual table methods also accept timeout
                parameters.
            table_timeout_ms: an alias for `request_timeout_ms`.
            api_options: any additional options to set for the clone, in the form of
                an APIOptions instance (where one can set just the needed attributes).
                In case the same setting is also provided as named parameter,
                the latter takes precedence.

        Returns:
            a new AsyncTable instance.

        Example:
            >>> my_other_async_table = my_async_table.with_options(
            ...     name="the_other_table",
            ...     callers=[("caller_identity", "0.1.2")],
            ... )
        """

        return self._copy(
            name=name,
            embedding_api_key=embedding_api_key,
            callers=callers,
            request_timeout_ms=request_timeout_ms,
            table_timeout_ms=table_timeout_ms,
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
        table_timeout_ms: int | UnsetType = _UNSET,
        api_options: APIOptions | UnsetType = _UNSET,
    ) -> Table[ROW]:
        """
        TODO
        """

        arg_api_options = APIOptions(
            callers=callers,
            embedding_api_key=embedding_api_key,
            timeout_options=TimeoutOptions(
                request_timeout_ms=table_timeout_ms,
            ),
        )
        # a double override for the timeout aliasing
        arg_api_options_2 = APIOptions(
            timeout_options=TimeoutOptions(
                request_timeout_ms=request_timeout_ms,
            ),
        )
        final_api_options = (
            self.api_options.with_override(api_options)
            .with_override(arg_api_options)
            .with_override(arg_api_options_2)
        )
        return Table(
            database=database or self.database.to_sync(),
            name=name or self.name,
            keyspace=keyspace or self.keyspace,
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
        TODO
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, the Table defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.
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
            raise ValueError(
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
        TODO
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
        *,
        column: str,
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
            name: the name of the index.
            column: the table column on which the index is to be created.
            options: if passed, it must be an instance of `TableIndexOptions`,
                or an equivalent dictionary, which specifies index settings
                such as -- for a text column -- case-sensitivity and so on.
                See the `astrapy.info.TableIndexOptions` class for more details.
            if_not_exists: if set to True, the command will succeed even if an index
                with the specified name already exists (in which case no actual
                index creation takes place on the database). The API default of False
                means that an error is raised by the API in case of name collision.
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, the Table defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Example:
            >>> from astrapy.info import TableIndexOptions
            >>> await my_table.create_index(
            ...     "score_index",
            ...     column="score",
            ... )
            >>>
            >>> await my_table.create_index(
            ...     "winner_index",
            ...     column="winner",
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
        *,
        column: str,
        options: TableVectorIndexOptions | dict[str, Any],
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
            name: the name of the index.
            column: the table column, of type "vector" on which to create the index.
            options: an instance of `TableVectorIndexOptions`, or an equivalent
                dictionary, which specifies settings for the vector index,
                such as the metric to use or, if desired, a "source model" setting.
                See the `astrapy.info.TableVectorIndexOptions` class for more details.
            if_not_exists: if set to True, the command will succeed even if an index
                with the specified name already exists (in which case no actual
                index creation takes place on the database). The API default of False
                means that an error is raised by the API in case of name collision.
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, the Table defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Example:
            >>> from astrapy.constants import VectorMetric
            >>> from astrapy.info import TableVectorIndexOptions
            >>>
            >>> await my_table.create_vector_index(
            ...     "m_vector_index",
            ...     column="m_vector",
            ...     options=TableVectorIndexOptions(metric=VectorMetric.DOT_PRODUCT),
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
                underlying API request. If not provided, the Table defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Returns:
            a list of the index names as strings, in no particular order.

        Example:
            >>> await my_table.list_index_names()
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

    async def list_indexes(
        self,
        *,
        table_admin_timeout_ms: int | None = None,
        request_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[TableIndexDescriptor]:
        """
        List the full definitions of all indexes existing on this table.

        Args:
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, the Table defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Returns:
            a list of `astrapy.info.TableIndexDescriptor` objects in no particular
            order, each providing the details of an index present on the table.

        Example:
            >>> indexes = await my_table.list_indexes()
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
            return [
                TableIndexDescriptor.coerce(index_object)
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
                If omitted, the resulting Table is implicitly a `Table[dict[str, Any]]`.
                If provided, it must match the type hint specified in the assignment.
                See the examples below.
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, the Table defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.

        Example:
            >>> from astrapy.info import (
            ...     AlterTableAddColumns,
            ...     AlterTableAddVectorize,
            ...     AlterTableDropColumns,
            ...     AlterTableDropVectorize,
            ...     TableScalarColumnTypeDescriptor,
            ...     VectorServiceOptions,
            ... )
            >>>
            >>> new_table_1 = await my_table.alter(
            ...     AlterTableAddColumns(
            ...         columns={
            ...             "tie_break": TableScalarColumnTypeDescriptor(
            ...                 column_type="boolean",
            ...             ),
            ...         }
            ...     )
            ... )
            >>>
            >>> new_table_2 = await new_table_1.alter(
            ...     AlterTableDropColumns(columns=["tie_break"]),
            ... )
            >>>
            >>> new_table_3 = await new_table_2.alter(
            ...     AlterTableAddVectorize(
            ...         columns={
            ...             "m_vector": VectorServiceOptions(
            ...                 provider="openai",
            ...                 model_name="text-embedding-3-small",
            ...                 authentication={
            ...                     "providerKey": "MY_API_KEY_STORED_SECRET_NAME",
            ...                 },
            ...             ),
            ...         }
            ...     )
            ... )
            >>>
            >>> from typing import TypedDict
            >>> from astrapy import Table
            >>> from astrapy.data_types import DataAPITimestamp
            >>>
            >>> class MyCustomDictClass(TypedDict):
            ...     match_no: int
            ...     round: str
            ...     winner: str
            ...     score: int
            ...     when: DataAPITimestamp
            ...
            >>> new_table_4: Table[MyCustomDictClass] = await new_table_3.alter(
            ...     AlterTableDropVectorize(columns=["m_vector"]),
            ...     row_type=MyCustomDictClass,
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
                if desired.
                The values for the various columns supplied in the row must
                be of the right data type for the insertion to succeed.
                Non-primary-key columns can also be explicitly set to null.
            general_method_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, the AsyncTable defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `general_method_timeout_ms`.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a TableInsertOneResult object, whose attributes are the primary key
            of the inserted row both in the form of a dictionary and of a tuple.

        Examples:
            >>> # a full-row insert using astrapy's datatypes when available
            >>> from astrapy.data_types import (
            ...     DataAPISet,
            ...     DataAPITimestamp,
            ...     DataAPIVector,
            ... )
            >>> insert_result = await my_table.insert_one(
            ...     {
            ...         "match_no": 1012,
            ...         "round": "A",
            ...         "winner": "Victor",
            ...         "score": 18,
            ...         "when": DataAPITimestamp.from_string("2024-11-28T11:30:00Z"),
            ...         "tags": DataAPISet(["worldcup", "placeholder_tag"]),
            ...         "m_vector": DataAPIVector([0.4, -0.6, 0.2]),
            ...     },
            ... )
            >>> insert_result.inserted_id
            {'match_no': 1012, 'round': 'A'}
            >>> insert_result.inserted_id_tuple
            (1012, 'A')

            >>> # a partial-row overwrite
            >>> await my_table.insert_one(
            ...     {
            ...         "match_no": 1012,
            ...         "round": "A",
            ...         "winner": "Victor Vector",
            ...         "tags": DataAPISet(["worldcup", "championship"]),
            ...     },
            ... )
            TableInsertOneResult(inserted_id={'match_no': 1012, ...)  # Note: shortened

            >>> # an insert using only standard library data types
            >>> from datetime import datetime, timezone
            >>> await my_table.insert_one(
            ...     {
            ...         "match_no": 975,
            ...         "round": "B",
            ...         "winner": "Angela",
            ...         "score": 25,
            ...         "when": datetime(
            ...             2024, 7, 13, 12, 55, 30, 889, tzinfo=timezone.utc
            ...         ),
            ...         "tags": {"tiebreak", "epic"},
            ...         "m_vector": [0.4, -0.6, 0.2],
            ...     },
            ... )
            TableInsertOneResult(inserted_id={'match_no': 975, ...)  # Note: shortened
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        io_payload = self._converter_agent.preprocess_payload(
            {"insertOne": {"document": row}}
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
            if "primaryKeySchema" not in status:
                raise UnexpectedDataAPIResponseException(
                    text=(
                        "received a 'status' without 'primaryKeySchema' "
                        f"in API response (received: {status})"
                    ),
                    raw_response=None,
                )
            if "insertedIds" not in status:
                raise UnexpectedDataAPIResponseException(
                    text=(
                        "received a 'status' without 'insertedIds' "
                        f"in API response (received: {status})"
                    ),
                    raw_response=None,
                )
            primary_key_schema = status["primaryKeySchema"]
            id_tuples_and_ids = self._converter_agent.postprocess_keys(
                status["insertedIds"],
                primary_key_schema_dict=primary_key_schema,
            )
            id_tuples = [tpl for tpl, _ in id_tuples_and_ids]
            ids = [id for _, id in id_tuples_and_ids]
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
                while any other column may be omitted if desired.
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
                If not provided, the corresponding AsyncTable defaults apply.
            request_timeout_ms: a timeout, in milliseconds, to impose on each
                individual HTTP request to the Data API to accomplish the operation.
            timeout_ms: an alias for `general_method_timeout_ms`.

        Returns:
            a TableInsertManyResult object, whose attributes are the primary key
            of the inserted rows both in the form of dictionaries and of tuples.

        Examples:
            >>> from datetime import datetime, timezone
            >>> from astrapy.data_types import DataAPISet, DataAPITimestamp, DataAPIVector
            >>>
            >>> # Unordered insertion (with concurrency for performance)
            >>> insert_result = await my_table.insert_many(
            ...     [
            ...         {
            ...             "match_no": 1012,
            ...             "round": "A",
            ...             "winner": "Victor",
            ...             "score": 18,
            ...             "when": DataAPITimestamp.from_string(
            ...                 "2024-11-28T11:30:00Z",
            ...             ),
            ...             "tags": DataAPISet(["worldcup", "placeholder_tag"]),
            ...             "m_vector": DataAPIVector([0.4, -0.6, 0.2]),
            ...         },
            ...         {"match_no": 991, "round": "A", "winner": "Adam"},
            ...         {"match_no": 991, "round": "B", "winner": "Betta"},
            ...         {"match_no": 991, "round": "C", "winner": "Caio"},
            ...         {
            ...             "match_no": 995,
            ...             "round": "A",
            ...             "winner": "Donna",
            ...             "m_vector": [0.9, -0.1, -0.3],
            ...         },
            ...         {"match_no": 995, "round": "B", "winner": "Erick"},
            ...         {"match_no": 995, "round": "C", "winner": "Fiona"},
            ...         {"match_no": 997, "round": "A", "winner": "Gael"},
            ...         {"match_no": 997, "round": "B", "winner": "Hanna"},
            ...         {
            ...             "match_no": 997,
            ...             "round": "C",
            ...             "winner": "Ian",
            ...             "when": datetime(
            ...                 2023, 9, 28, 18, 12, 45, tzinfo=timezone.utc
            ...             ),
            ...             "tags": {"dull"},
            ...         },
            ...         {"match_no": 443, "round": "A", "winner": "Joy"},
            ...         {"match_no": 443, "round": "B", "winner": "Kevin"},
            ...         {"match_no": 443, "round": "C", "winner": "Lauretta"},
            ...     ],
            ...     concurrency=10,
            ... )
            >>> insert_result.inserted_ids
            [{'match_no': 1012, 'round': 'A'}, {'match_no': 991, ...}, ...]  # Note: shortened
            >>> insert_result.inserted_id_tuples
            [(1012, 'A'), (991, 'A'), (991, 'B'), (991, 'C'), (995, 'A'), ...]  # Note: shortened

            >>> # Ordered insertion (stop on first failure and predictable end result on DB)
            >>> await my_table.insert_many(
            ...     [
            ...         {"match_no": 991, "round": "A", "winner": "Adam0"},
            ...         {"match_no": 991, "round": "B", "winner": "Bett0a"},
            ...         {"match_no": 991, "round": "C", "winner": "Caio0"},
            ...         {"match_no": 991, "round": "A", "winner": "Adam1"},
            ...         {"match_no": 991, "round": "B", "winner": "Betta1"},
            ...         {"match_no": 991, "round": "C", "winner": "Caio1"},
            ...         {"match_no": 991, "round": "A", "winner": "Adam2"},
            ...         {"match_no": 991, "round": "B", "winner": "Betta2"},
            ...         {"match_no": 991, "round": "C", "winner": "Caio2"},
            ...         {"match_no": 991, "round": "A", "winner": "Adam3"},
            ...         {"match_no": 991, "round": "B", "winner": "Betta3"},
            ...         {"match_no": 991, "round": "C", "winner": "Caio3"},
            ...     ],
            ...     ordered=True,
            ... )
            TableInsertManyResult(inserted_ids=[{'match_no': 991, ...}, ...]  # Note: shortened

        Note:
            Unordered insertions are executed with some degree of concurrency,
            so it is usually better to prefer this mode unless the order in the
            row sequence is important.

        Note:
            If some of the rows are unsuitable for insertion, for instance
            have the wrong data type for a column or lack the primary key,
            the Data API validation check will fail for those specific requests
            that contain the faulty rows. Depending on concurrency and the value
            of the `ordered` parameter, a number of rows in general could have
            been successfully inserted.
            It is possible to capture such a scenario, and inspect which rows
            actually got inserted, by catching an error of type
            `astrapy.exceptions.TableInsertManyException`: its `partial_result`
            attribute is precisely a `TableInsertManyResult`, encoding details
            on the successful writes.
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
        timeout_manager = MultiCallTimeoutManager(
            overall_timeout_ms=_general_method_timeout_ms,
            timeout_label=_gmt_label,
        )
        if ordered:
            options = {"ordered": True}
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
                    partial_result = TableInsertManyResult(
                        raw_results=raw_results,
                        inserted_ids=inserted_ids,
                        inserted_id_tuples=inserted_id_tuples,
                    )
                    raise TableInsertManyException.from_response(
                        command=None,
                        raw_response=chunk_response,
                        partial_result=partial_result,
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
            options = {"ordered": False}

            sem = asyncio.Semaphore(_concurrency)

            async def concurrent_insert_chunk(
                row_chunk: list[ROW],
            ) -> dict[str, Any]:
                async with sem:
                    im_payload = self._converter_agent.preprocess_payload(
                        {
                            "insertMany": {
                                "documents": row_chunk,
                                "options": options,
                            },
                        },
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
                    return im_response

            if _concurrency > 1:
                tasks = [
                    asyncio.create_task(
                        concurrent_insert_chunk(_rows[i : i + _chunk_size])
                    )
                    for i in range(0, len(_rows), _chunk_size)
                ]
                raw_results = await asyncio.gather(*tasks)
            else:
                raw_results = [
                    await concurrent_insert_chunk(_rows[i : i + _chunk_size])
                    for i in range(0, len(_rows), _chunk_size)
                ]

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
            if any(
                [chunk_response.get("errors", []) for chunk_response in raw_results]
            ):
                partial_result = TableInsertManyResult(
                    raw_results=raw_results,
                    inserted_ids=inserted_ids,
                    inserted_id_tuples=inserted_id_tuples,
                )
                raise TableInsertManyException.from_responses(
                    commands=[None for _ in raw_results],
                    raw_responses=raw_results,
                    partial_result=partial_result,
                )

            # return
            full_result = TableInsertManyResult(
                raw_results=raw_results,
                inserted_ids=inserted_ids,
                inserted_id_tuples=inserted_id_tuples,
            )
            logger.info(f"finished inserting {len(_rows)} rows in '{self.name}'")
            return full_result

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
        timeout_ms: int | None = None,
    ) -> AsyncTableFindCursor[ROW, ROW]:
        """
        TODO
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
        TODO
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
            }
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
        key: str,
        *,
        filter: FilterType | None = None,
        request_timeout_ms: int | None = None,
        general_method_timeout_ms: int | None = None,
        timeout_ms: int | None = None,
    ) -> list[Any]:
        """
        TODO
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
        if _key == "":
            raise ValueError(
                "The 'key' parameter for distinct cannot be empty "
                "or start with a list index."
            )
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
            general_method_timeout_ms: TODO
            request_timeout_ms: a timeout, in milliseconds, for the API HTTP request.
                If not passed, the table-level setting is used instead.
            timeout_ms: an alias for `request_timeout_ms`.

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
            general_method_timeout_ms: TODO
            request_timeout_ms: a timeout, in milliseconds, for the API HTTP request.
                If not passed, the table-level setting is used instead.
            timeout_ms: an alias for `request_timeout_ms`.

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
        Update a single document on the table as requested,
        with the implicit behaviour of inserting a new one if no match is found.

        Args:
            filter: a predicate expressing in full a primary key, i.e. a dictionary
                defining values for all columns that form the table's primary key.
                Examples:
                    {"code": 123}
                    {"country": "UK", "year": 2024}
            update: the update prescription to apply to the row, expressed
                as a dictionary as per Data API syntax. Examples are:
                    {"$set": {"field": "value}}
                    {"$unset": {"field": ""}}
                Primary key fields cannot be provided for a "$set" operation.
                For Tables, a limited set of update operators apply.
                See the Data API documentation for more details.
            general_method_timeout_ms: TODO
            request_timeout_ms: a timeout, in milliseconds, for the API HTTP request.
                If not passed, the table-level setting is used instead.
            timeout_ms: an alias for `request_timeout_ms`.

        Example:
            TODO async
        """

        _request_timeout_ms, _rt_label = _select_singlereq_timeout_gm(
            timeout_options=self.api_options.timeout_options,
            general_method_timeout_ms=general_method_timeout_ms,
            request_timeout_ms=request_timeout_ms,
            timeout_ms=timeout_ms,
        )
        uo_payload = {
            "updateOne": {
                k: v
                for k, v in {
                    "filter": filter,
                    "update": self._converter_agent.preprocess_payload(update),
                }.items()
                if v is not None
            }
        }
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
        TODO
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
            }
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
        TODO
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
            }
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
        TODO

            if_exists: if passed as True, trying to drop a non-existing table
                will not error, just silently do nothing instead. If not provided,
                the API default behaviour will hold.
            table_admin_timeout_ms: a timeout, in milliseconds, to impose on the
                underlying API request. If not provided, the Table defaults apply.
                (This method issues a single API request, hence all timeout parameters
                are treated the same.)
            request_timeout_ms: an alias for `table_admin_timeout_ms`.
            timeout_ms: an alias for `table_admin_timeout_ms`.
        """

        logger.info(f"dropping table '{self.name}' (self)")
        drop_result = await self.database.drop_table(
            self,
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
        TODO
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
