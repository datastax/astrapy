import argparse
import logging
import os
import sys
from code import interact
from typing import Any

from astrapy import (
    AstraDBAdmin,
    AstraDBDatabaseAdmin,
    AsyncCollection,
    AsyncDatabase,
    AsyncTable,
    Collection,
    DataAPIClient,
    DataAPIDatabaseAdmin,
    Database,
    Table,
)
from astrapy.admin import (
    ParsedAPIEndpoint,
    parse_api_endpoint,
)
from astrapy.api_options import (
    APIOptions,
    DataAPIURLOptions,
    DevOpsAPIURLOptions,
    SerdesOptions,
    TimeoutOptions,
)
from astrapy.authentication import (
    AWSEmbeddingHeadersProvider,
    EmbeddingAPIKeyHeaderProvider,
    StaticTokenProvider,
    TokenProvider,
    UsernamePasswordTokenProvider,
)
from astrapy.constants import (
    DefaultIdType,
    Environment,
    MapEncodingMode,
    ReturnDocument,
    SortMode,
    VectorMetric,
)
from astrapy.cursors import (
    AbstractCursor,
    AsyncCollectionFindAndRerankCursor,
    AsyncCollectionFindCursor,
    AsyncTableFindCursor,
    CollectionFindAndRerankCursor,
    CollectionFindCursor,
    CursorState,
    RerankedResult,
    TableFindCursor,
)
from astrapy.data_types import (
    DataAPIDate,
    DataAPIDictUDT,
    DataAPIDuration,
    DataAPIMap,
    DataAPISet,
    DataAPITime,
    DataAPITimestamp,
    DataAPIVector,
)
from astrapy.event_observers import (
    ObservableError,
    ObservableEvent,
    ObservableEventType,
    ObservableRequest,
    ObservableResponse,
    ObservableWarning,
    Observer,
    event_collector,
)
from astrapy.ids import (
    UUID,
    ObjectId,
    uuid1,
    uuid3,
    uuid4,
    uuid5,
    uuid6,
    uuid7,
    uuid8,
)
from astrapy.info import (
    AlterTableAddColumns,
    AlterTableAddVectorize,
    AlterTableDropColumns,
    AlterTableDropVectorize,
    AlterTypeAddFields,
    AlterTypeOperation,
    AlterTypeRenameFields,
    AstraDBAdminDatabaseInfo,
    AstraDBDatabaseInfo,
    CollectionDefaultIDOptions,
    CollectionDefinition,
    CollectionDescriptor,
    CollectionInfo,
    CollectionLexicalOptions,
    CollectionRerankOptions,
    CollectionVectorOptions,
    ColumnType,
    CreateTableDefinition,
    CreateTypeDefinition,
    EmbeddingProvider,
    EmbeddingProviderAuthentication,
    EmbeddingProviderModel,
    EmbeddingProviderParameter,
    EmbeddingProviderToken,
    FindEmbeddingProvidersResult,
    FindRerankingProvidersResult,
    ListTableDefinition,
    ListTableDescriptor,
    ListTypeDescriptor,
    RerankingProvider,
    RerankingProviderAuthentication,
    RerankingProviderModel,
    RerankingProviderParameter,
    RerankingProviderToken,
    RerankServiceOptions,
    TableAPIIndexSupportDescriptor,
    TableAPISupportDescriptor,
    TableBaseIndexDefinition,
    TableIndexDefinition,
    TableIndexDescriptor,
    TableIndexOptions,
    TableInfo,
    TableKeyValuedColumnType,
    TableKeyValuedColumnTypeDescriptor,
    TablePrimaryKeyDescriptor,
    TableScalarColumnTypeDescriptor,
    TableTextIndexDefinition,
    TableTextIndexOptions,
    TableUDTColumnDescriptor,
    TableUnsupportedColumnTypeDescriptor,
    TableUnsupportedIndexDefinition,
    TableValuedColumnType,
    TableValuedColumnTypeDescriptor,
    TableVectorColumnTypeDescriptor,
    TableVectorIndexDefinition,
    TableVectorIndexOptions,
    VectorServiceOptions,
)
from astrapy.utils.api_options import defaultAPIOptions
from astrapy.utils.document_paths import (
    escape_field_names,
    unescape_field_path,
)

ipython_available: bool
try:
    from IPython import embed

    ipython_available = True
except ImportError:
    ipython_available = False

BANNER_TEMPLATE = """*******************************************************************
*
*    █████╗ ███████╗████████╗██████╗  █████╗ ██████╗ ██╗   ██╗
*   ██╔══██╗██╔════╝╚══██╔══╝██╔══██╗██╔══██╗██╔══██╗╚██╗ ██╔╝
*   ███████║███████╗   ██║   ██████╔╝███████║██████╔╝ ╚████╔╝
*   ██╔══██║╚════██║   ██║   ██╔══██╗██╔══██║██╔═══╝   ╚██╔╝
*   ██║  ██║███████║   ██║   ██║  ██║██║  ██║██║        ██║
*   ╚═╝  ╚═╝╚══════╝   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝        ╚═╝
*
*   ██████╗ ███████╗██████╗ ██╗
*   ██╔══██╗██╔════╝██╔══██╗██║
*   ██████╔╝█████╗  ██████╔╝██║
*   ██╔══██╗██╔══╝  ██╔═══╝ ██║
*   ██║  ██║███████╗██║     ███████╗
*   ╚═╝  ╚═╝╚══════╝╚═╝     ╚══════╝
*
* astrapy-repl - an interactive astrapy Python shell.
*
*     Variables 'client' and 'database' are ready to use.
*     All astrapy useful symbols are imported already.
*
* Targeting:
*  {endpoint:59.59}
*  keyspace={keyspace}, environment={environment}
*******************************************************************"""

PRIVATE_GLOBALS = {
    "argparse",
    "BANNER_TEMPLATE",
    "embed",
    "interact",
    "ipython_available",
    "main",
    "os",
    "parser",
    "sys",
}

LOGGING_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

parser = argparse.ArgumentParser(
    description=(
        "Interactive REPL for astrapy. Supplied parameters take precedence "
        "over environment variables."
    ),
)
# DB connection
parser.add_argument(
    "--endpoint",
    "-e",
    type=str,
    help=(
        "Data API endpoint (Astra or other deployment). "
        "Env vars: ASTRA_DB_API_ENDPOINT, LOCAL_DATA_API_ENDPOINT"
    ),
)
parser.add_argument(
    "--token",
    "-t",
    type=str,
    help="Database token (alternative to user/password). Env var: ASTRA_DB_APPLICATION_TOKEN",
)
parser.add_argument(
    "--username",
    "-u",
    type=str,
    help="Username (alternative to token; requires password). Env var: LOCAL_DATA_API_USERNAME",
)
parser.add_argument(
    "--password",
    "-p",
    type=str,
    help="Password (alternative to token; requires username). Env var: LOCAL_DATA_API_PASSWORD",
)
parser.add_argument(
    "--keyspace",
    "-k",
    type=str,
    help="Targeted keyspace. Env vars: ASTRA_DB_KEYSPACE, LOCAL_DATA_API_KEYSPACE",
)
parser.add_argument(
    "--environment",
    choices=Environment.values,
    help="Target environment. Usually auto-detected from endpoint. ",
)
# Logger error level
parser.add_argument(
    "--log-level",
    "-l",
    type=str.upper,
    dest="loglevel",
    choices=[
        "DEBUG",
        "INFO",
        "WARNING",
        "ERROR",
        "CRITICAL",
    ],
    help="Logging level. Use 'DEBUG' for comprehensive inspection of requests and responses.",
)
# REPL type
parser.add_argument(
    "--repl",
    choices=["stdlib", "ipython"],
    help="REPL mode (interactive Python shell) to use",
)


def main() -> None:
    args = parser.parse_args()

    # Manual validation and integration with env. variables.
    # We uniformly apply (1) parameters over env vars, (2) astra+local,
    # and then check for conflicts and re-parse endpoint to find out the target env.

    _endpoint = (
        args.endpoint
        or os.getenv("ASTRA_DB_API_ENDPOINT")
        or os.getenv("LOCAL_DATA_API_ENDPOINT")
    )
    _keyspace = (
        args.keyspace
        or os.getenv("ASTRA_DB_KEYSPACE")
        or os.getenv("LOCAL_DATA_API_KEYSPACE")
        or None
    )
    _environment = args.environment or None
    #
    _token_provider: TokenProvider
    _token = args.token or os.getenv("ASTRA_DB_APPLICATION_TOKEN")
    _username = args.username or os.getenv("LOCAL_DATA_API_USERNAME")
    _password = args.password or os.getenv("LOCAL_DATA_API_PASSWORD")
    if _token and (_username or _password):
        parser.error("Specify either a token or username/password pair, not both.")
    if (_username and not _password) or (_password and not _username):
        parser.error("Username and password must must be provided together.")
    if _username and _password:
        _token_provider = UsernamePasswordTokenProvider(_username, _password)
    else:
        _token_provider = StaticTokenProvider(_token)

    if not _endpoint:
        raise SystemExit(
            "The endpoint is required, either via env. var or command-line argument. "
            "Hint: '-h' for help."
        )

    # instantiate client and database
    if not _environment:
        parsed_endpoint = parse_api_endpoint(_endpoint)
        if parsed_endpoint:
            _environment = parsed_endpoint.environment
        else:
            _environment = Environment.HCD
    client = DataAPIClient(environment=_environment)
    database = client.get_database(  # noqa: F841
        _endpoint,
        token=_token_provider,
        keyspace=_keyspace,
    )

    # pick the target REPL mode
    ipython_repl: bool
    if args.repl == "ipython":
        if ipython_available:
            ipython_repl = True
        else:
            raise SystemExit(
                "'IPython' package not found: cannot start the required REPL. "
                "Hint: '-h' for help."
            )
    elif args.repl == "stdlib":
        ipython_repl = False
    else:
        # unspecified repl choice, pick best default
        ipython_repl = ipython_available

    banner = BANNER_TEMPLATE.format(
        endpoint=_endpoint,
        keyspace=_keyspace or "(default)",
        environment=_environment,
    )

    # spawn the repl
    namespace: dict[str, Any] = {
        **{k: v for k, v in globals().items() if k not in PRIVATE_GLOBALS},
        **{
            "client": client,
            "database": database,
        },
    }
    if args.loglevel:
        logging.basicConfig(level=LOGGING_LEVELS[args.loglevel])
    if ipython_repl:
        embed(
            banner1=banner,
            header=" ",
            user_ns=namespace,
            colors="Neutral",
        )  # type: ignore[no-untyped-call]
    else:
        stdbanner = banner + ("\n" * 3)
        if not sys.flags.interactive:
            interact(
                banner=stdbanner,
                local=namespace,
                exitmsg="Closing astrapy-repl.",
            )
