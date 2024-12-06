# AstraPy

A pythonic client for [DataStax Astra DB](https://astra.datastax.com).

_This README targets **AstraPy version 2.0+**. Click [here](https://github.com/datastax/astrapy/blob/4601c5fa749925d961de1f114ca27690d1a71b13/README.md) for v1 and [here](https://github.com/datastax/astrapy/blob/cd3f5ce8146093e10a095709c0f5c3f8e3f2c7da/README.md) for the v0 API (which you should not really be using by now)._


## Quickstart

Install with `pip install astrapy`.

Get the *API Endpoint* and the *Token* to your Astra DB instance at [astra.datastax.com](https://astra.datastax.com).

Try the following code after replacing the connection parameters:

```python
from astrapy import DataAPIClient
from astrapy.constants import VectorMetric
from astrapy.ids import UUID
from astrapy.info import CollectionDefinition


ASTRA_DB_APPLICATION_TOKEN = "AstraCS:..."
ASTRA_DB_API_ENDPOINT = "https://01234567-....apps.astra.datastax.com"

# Connect and create the Database object
my_client = DataAPIClient()
my_database = my_client.get_database(
    ASTRA_DB_API_ENDPOINT,
    token=ASTRA_DB_APPLICATION_TOKEN,
)

# Create a vector collection
my_collection = my_database.create_collection(
    "dreams_collection",
    definition=(
        CollectionDefinition.builder()
        .set_vector_dimension(3)
        .set_vector_metric(VectorMetric.COSINE)
        .build()
    )
)

# Populate the collection with some documents
my_collection.insert_many(
    [
        {
            "_id": UUID("018e65c9-e33d-749b-9386-e848739582f0"),
            "summary": "Riding the waves",
            "tags": ["sport"],
            "$vector": [0, 0.2, 1],
        },
        {
            "summary": "Friendly aliens in town",
            "tags": ["scifi"],
            "$vector": [-0.3, 0, 0.8],
        },
        {
            "summary": "Meeting Beethoven at the dentist",
            "$vector": [0.2, 0.6, 0],
        },
    ],
)

my_collection.update_one(
    {"tags": "sport"},
    {"$set": {"summary": "Surfers' paradise"}},
)

# Run a vector search
cursor = my_collection.find(
    {},
    sort={"$vector": [0, 0.2, 0.4]},
    limit=2,
    include_similarity=True,
)

for result in cursor:
    print(f"{result['summary']}: {result['$similarity']}")

# This would print:
#   Surfers' paradise: 0.98238194
#   Friendly aliens in town: 0.91873914

# Resource cleanup
my_collection.drop()
```

Next steps:

- More info and usage patterns are given in the docstrings of classes and methods
- [Data API reference](https://docs.datastax.com/en/astra-db-serverless/api-reference/overview.html)
- [AstraPy reference](https://docs.datastax.com/en/astra-api-docs/_attachments/python-client/astrapy/index.html)
- Package on [PyPI](https://pypi.org/project/astrapy/)

### Using Tables

The example above uses a _collection_, where schemaless "documents" can be stored and retrieved.
Here is an equivalent code that uses Tables, i.e. uniform, structured data where each _row_ has the
same _columns_, which are of a specific type:

```python
from astrapy import DataAPIClient
from astrapy.constants import VectorMetric
from astrapy.data_types import DataAPIVector
from astrapy.info import (
    CreateTableDefinition,
    ColumnType,
    TableVectorIndexDefinition,
    TableVectorIndexOptions,
)


ASTRA_DB_APPLICATION_TOKEN = "AstraCS:..."
ASTRA_DB_API_ENDPOINT = "https://01234567-....apps.astra.datastax.com"

# Connect and create the Database object
my_client = DataAPIClient()
my_database = my_client.get_database(
    ASTRA_DB_API_ENDPOINT,
    token=ASTRA_DB_APPLICATION_TOKEN,
)

# Create a table and a vector index on it
table_definition = (
    CreateTableDefinition.builder()
    .add_column("dream_id", ColumnType.INT)
    .add_column("summary", ColumnType.TEXT)
    .add_set_column("tags", ColumnType.TEXT)
    .add_vector_column("dream_vector", dimension=3)
    .add_partition_by(["dream_id"])
    .build()
)
index_options=TableVectorIndexOptions(
    metric=VectorMetric.COSINE,
)
my_table = my_database.create_table("dreams_table", definition=table_definition, if_not_exists=True)
my_table.create_vector_index("dreams_table_vec_idx", column="dream_vector", options=index_options, if_not_exists=True)

# Populate the table with some rows
my_table.insert_many(
    [
        {
            "dream_id": 103,
            "summary": "Riding the waves",
            "tags": ["sport"],
            "dream_vector": DataAPIVector([0, 0.2, 1]),
        },
        {
            "dream_id": 119,
            "summary": "Friendly aliens in town",
            "tags": ["scifi"],
            "dream_vector": DataAPIVector([-0.3, 0, 0.8]),
        },
        {
            "dream_id": 37,
            "summary": "Meeting Beethoven at the dentist",
            "dream_vector": DataAPIVector([0.2, 0.6, 0]),
        },
    ],
)

my_table.update_one(
    {"dream_id": 103},
    {"$set": {"summary": "Surfers' paradise"}},
)

# Run a vector search
cursor = my_table.find(
    {},
    sort={"dream_vector": DataAPIVector([0, 0.2, 0.4])},
    limit=2,
    include_similarity=True,
)

for result in cursor:
    print(f"{result['summary']}: {result['$similarity']}")

# This would print:
#   Surfers' paradise: 0.98238194
#   Friendly aliens in town: 0.91873914

# Resource cleanup
my_table.drop()
```

For more on Tables, consult the [Data API documentation about Tables](https://docs.datastax.com/en/astra-db-serverless/api-reference/tables.html).

### Usage with HCD and other non-Astra installations

The main difference when targeting e.g. a Hyper-Converged Database (HCD)
installation is how the client is
initialized. Here is a short example showing just how to get to a `Database`
(what comes next is unchaged compared to using Astra DB).

```python
from astrapy import DataAPIClient
from astrapy.constants import Environment
from astrapy.authentication import UsernamePasswordTokenProvider


# Build a token
tp = UsernamePasswordTokenProvider("username", "password")

# Initialize the client and get a "Database" object
client = DataAPIClient(environment=Environment.HCD)
database = client.get_database("http://localhost:8181", token=tp)
```

For more on this case, please consult the [dedicated reference](https://docs.datastax.com/en/hyper-converged-database/1.0/connect/python-client.html).

## AstraPy's API

### Abstraction diagram

AstraPy's abstractions for working at the data and admin layers are structured
as depicted by this diagram:

![AstraPy, abstractions chart](https://raw.githubusercontent.com/datastax/astrapy/main/pictures/astrapy_abstractions.png)

Here's a small admin-oriented example:

```python
from astrapy import DataAPIClient


# this must have "Database Administrator" permissions:
ASTRA_DB_APPLICATION_TOKEN = "AstraCS:..."

my_client = DataAPIClient(ASTRA_DB_APPLICATION_TOKEN)

my_astra_admin = my_client.get_admin()

database_list = list(my_astra_admin.list_databases())

db_info = database_list[0].info
print(db_info.name, db_info.id, db_info.region)

my_database_admin = my_astra_admin.get_database_admin(db_info.id)

my_database_admin.list_keyspaces()
my_database_admin.create_keyspace("my_dreamspace")
```

### Exceptions

The package comes with its own set of exceptions, arranged in this hierarchy:

![AstraPy, exception hierarchy](https://raw.githubusercontent.com/datastax/astrapy/main/pictures/astrapy_exceptions.png)

For more information, and code examples, check out the docstrings and consult
the API reference linked above.

### API Options

You can configure many aspects of the interaction with the API by providing
customized "API Options" objects when either spawning a client, copying objects,
or spawning "children classes" (such as a Table from a Database).

For the details, please check the docstring for `astrapy.api_options.APIOptions`
and the other classes in that module. Here is a small example script to show a
practical starting point:

```python
from astrapy import DataAPIClient
from astrapy.api_options import (
    APIOptions,
    SerdesOptions,
)

# Disable custom datatypes in all reads:
no_cdt_options = APIOptions(
    serdes_options=SerdesOptions(
        custom_datatypes_in_reading=False,
    )
)
my_client = DataAPIClient(api_options=no_cdt_options)

# These spawned objects inherit that setting:
my_database = my_client.get_database(
    "https://...",
    token="my-token-1",
)
my_table = my_database.get_table("my_table")
```

### Working with dates in Collections and Tables

Date and datetime objects, i.e. instances of the standard library
`datetime.datetime` and `datetime.date` classes, can be used
anywhere when sending documents and queries to the API.

By default, what you get back is an instance of `astrapy.data_types.DataAPITimestamp`
(which has a much wider range of expressable timestamps than Python's stdlib).
If you want to revert to using the standard library `datetime.datetime`, you can do so
by turn on the `APIOptions.SerdesOptions.custom_datatypes_in_reading` API Options setting for the
collection/table object (note that this setting affects the returned format for several other table data types).

If you choose to have timestamps returned as standard-library `datetime.datetime` objects,
both for collections and tables, you may supply a specific timezone for these
(the default is UTC). You do so by providing an appropriate `datetime.timezone` value
to the `APIOptions.SerdesOptions.datetime_tz` API Options setting for the
collection/table object. You can also specify `None` for a timezone, in which case
the resulting values will be timezone-unaware (or "naive") datetimes.

_Naive_ datetimes (i.e. those without a timezone information attached)
are inherently ambiguous when it comes to translating them into a unambiguous timestamp.
For this reason, if you want to work with naive datetimes, and in particular you want
AstraPy to accept them for writes, you need to explicitly
turn on the `APIOptions.SerdesOptions.accept_naive_datetimes` API Options setting for the
collection/table object, otherwise AstraPy will raise an error.

_Remember that what effectively gets_
_written to DB is always a (numeric) **timestamp**: for naive quantities, this timestamp value depends_
_on the implied timezone used in the conversion, potentially leading to unexpected results_
_e.g. if multiple applications are running with different locale settings._

The following diagram summarizes the behaviour of the write and read paths for datetime objects,
depending on the `SerdesOptions` settings:

![AstraPy, abstractions chart](https://raw.githubusercontent.com/datastax/astrapy/main/pictures/astrapy_datetime_serdes_options.png)

Here an example code snippet showing how to switch to having reads return regular `datetime` objects
and have them set to one's desired timezone offset:

```python
from datetime import timezone,timedelta

from astrapy import DataAPIClient
from astrapy.api_options import APIOptions, SerdesOptions

my_timezone = timezone(timedelta(hours=4, minutes=30))

my_client = DataAPIClient()
my_database = my_client.get_database(
    ASTRA_DB_API_ENDPOINT,
    token=ASTRA_DB_APPLICATION_TOKEN,
    spawn_api_options=APIOptions(
        serdes_options=SerdesOptions(
            custom_datatypes_in_reading=False,
            datetime_tzinfo=my_timezone,
        ),
    ),
)

my_collection = my_database.get_collection("my_collection")
# This document will have datetimes set to the desired timezone
document = my_collection.find_one({"code": 123})
```

### Working with ObjectIds and UUIDs in Collections

Astrapy repackages the ObjectId from `bson` and the UUID class and utilities
from the `uuid` package and its `uuidv6` extension. You can also use them directly.

Even when setting a default ID type for a collection, you still retain the freedom
to use any ID type for any document:

```python
from astrapy import DataAPIClient
from astrapy.constants import DefaultIdType
from astrapy.ids import ObjectId, uuid8, UUID

import bson

ASTRA_DB_APPLICATION_TOKEN = "AstraCS:..."
ASTRA_DB_API_ENDPOINT = "https://01234567-....apps.astra.datastax.com"

my_client = DataAPIClient()
my_database = my_client.get_database(
    ASTRA_DB_API_ENDPOINT,
    token=ASTRA_DB_APPLICATION_TOKEN,
)

my_collection = my_database.create_collection(
    "ecommerce",
    definition=CollectionDefinition.builder().set_default_id(
        DefaultIdType.UUIDV6
    ).build(),
)

my_collection.insert_one({"_id": ObjectId("65fd9b52d7fabba03349d013")})
my_collection.find({
    "_id": UUID("018e65c9-e33d-749b-9386-e848739582f0"),
})

my_collection.update_one(
    {"tag": "in_stock"},
    {"$set": {"inventory_id": bson.objectid.ObjectId()}},
    upsert=True,
)

my_collection.insert_one({"_id": uuid8()})
```

## For contributors

First install poetry with `pip install poetry` and then the project dependencies with `poetry install --with dev`.

Linter, style and typecheck should all pass for a PR:

```bash
make format
```

With `make format-fix` the style and imports are autofixed (by `ruff`)

Features must be thoroughly covered in tests (see `tests/idiomatic/*` for
naming convention and module structure).

### Running tests

Tests are grouped in three _blocks_ (in as many subdirs of `tests/`):

- **idiomatic**: all 2.0+ classes and APIs, except...
- **tables**: the "tables" part only; and,
- **vectorize**: ... everything making use of `$vectorize` (within the idiomatic classes)

Actually, for convenience, _sub-blocks_ of tests are considered:

- **idiomatic regular**: everything except the admin parts
- **idiomatic admin Astra**: the Astra-specific admin operations
- **idiomatic admin nonAstra**: the nonAstra-specific admin operations
- **tables**: everything around just tables
- **vectorize in-depth**: many Data API interactions for a single choice of provider/model. This is mostly test the client
- **vectorize all-providers**: a slightly more shallow test repeated for all providers, models, auth methods etc. This is mostly testing the API

Tests can be run on three types of Data API _targets_ (with slight differences in what is applicable):

- **DockerCompose**: HCD started by the test initialization with `docker-compose`. _Note that in this case you will have to manually destroy the created containers._
- **nonAstra**: a ready-to-use (user-supplied) local Data API
- **Astra**: an Astra DB target account (or two, as some tests are specific to dev environment)

Depending on the (sub-block, target) combination, some environment variables may be needed.
Templates for the environment variables are to be found in `tests/env_templates`.

The general expectation is that idiomatic non-Admin tests, and vectorize in-depth tests, are
part of the main CI flow; conversely, admin and vectorize all-providers are kept as a
manual task to run (locally in most cases) when circumstances require it (use your judgement).

#### Required environment variables

Below is a detail of the reference template files needed for the various types
of testing:

- **DockerCompose**: generally no variables needed, except:
  - **vectorize in-depth**: provide as in `env.vectorize-minimal.template`
  - **vectorize all-providers**: provide as in `env.vectorize.template`
  - (also note that _idiomatic admin Astra_ amounts to nothing in this case)
- **nonAstra**: all tests require as in `env.local.template`, plus:
  - **vectorize in-depth**: also provide as in `env.vectorize-minimal.template`
  - **vectorize all-providers**: also provide as in `env.vectorize.template`
  - (also note that _idiomatic admin Astra_ amounts to nothing in this case)
- **Astra**: all tests require as in `env.astra.template`, plus:
  - **idiomatic admin Astra**: also provide as in `env.astra.admin.template`
  - **vectorize in-depth**: also provide as in `env.vectorize-minimal.template`
  - **vectorize all-providers**: also provide as in `env.vectorize.template`
  - (also note that _idiomatic admin nonAstra_ amounts to nothing in this case)

#### Sample testing commands

For the **DockerCompose** case, prepend all of the following with `DOCKER_COMPOSE_LOCAL_DATA_API="yes" `.

All the usual `pytest` ways of restricting the test selection hold in addition
(e.g. `poetry run pytest tests/idiomatic/unit` or `[...] -k <test_name_selector>`).

##### _idiomatic regular_:

Warning: this will also trigger the very long-running _idiomatic admin Astra_ if the vars as in `env.astra.admin.template` are also detected. Likewise, the _idiomatic admin nonAstra_ may start (if `DO_IDIOMATIC_ADMIN_TESTS` is set), which however takes few seconds.

```
poetry run pytest tests/idiomatic
```

##### _idiomatic admin Astra_:

```
poetry run pytest tests/idiomatic/integration/test_admin.py 
```

##### _idiomatic admin nonAstra_:

```
DO_IDIOMATIC_ADMIN_TESTS="1" poetry run pytest tests/idiomatic/integration/test_nonastra_admin.py
```

##### _vectorize in-depth_:

```
poetry run pytest tests/vectorize_idiomatic/integration/test_vectorize_methods*.py
```

or just:

```
poetry run pytest tests/vectorize_idiomatic/integration/test_vectorize_methods_sync.py
```

##### _vectorize all-providers_:

This generates all possible test cases and runs them:

```
poetry run pytest tests/vectorize_idiomatic
```

For a spot test, you may restrict to one case, e.g.

```
EMBEDDING_MODEL_TAGS="openai/text-embedding-3-large/HEADER/0" poetry run pytest tests/vectorize_idiomatic/integration/test_vectorize_providers.py -k test_vectorize_usage_auth_type_header_sync
```

#### Useful flags for testing

Remove logging noise with:

```
poetry run pytest [...] -o log_cli=0
```

Increase logging level to `DEBUG` (i.e. level `10`):

```
poetry run pytest [...] -o log_cli=1 --log-cli-level=10
```


## Appendices

### Appendix A: quick reference for imports

Client, data and admin abstractions (_Note: table-related imports to be added_):

```python
from astrapy import (
    AstraDBAdmin,
    AstraDBDatabaseAdmin,
    AsyncCollection,
    AsyncDatabase,
    Collection,
    DataAPIClient,
    DataAPIDatabaseAdmin,
    Database,
)
```

Constants for data-related use:

```python
from astrapy.constants import (
    DefaultIdType,
    Environment,
    ReturnDocument,
    SortMode,
    VectorMetric,
)
```

ObjectIds and UUIDs:

```python
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
```

API Options:

```python
from astrapy.api_options import (
    APIOptions,
    DataAPIURLOptions,
    DevOpsAPIURLOptions,
    SerdesOptions,
    TimeoutOptions,
)
```

Result classes:

```python
from astrapy.results import (
    CollectionDeleteResult,
    CollectionInsertManyResult,
    CollectionInsertOneResult,
    CollectionUpdateResult,
    OperationResult,
    TableDeleteResult,
    TableInsertManyResult,
    TableInsertOneResult,
    TableUpdateResult,
)
```

Exceptions:

```python
from astrapy.exceptions import (
    CollectionDeleteManyException,
    CollectionInsertManyException,
    CollectionUpdateManyException,
    CumulativeOperationException,
    CursorException,
    DataAPIDetailedErrorDescriptor,
    DataAPIErrorDescriptor,
    DataAPIException,
    DataAPIHttpException,
    DataAPIResponseException,
    DataAPITimeoutException,
    DevOpsAPIErrorDescriptor,
    DevOpsAPIException,
    DevOpsAPIHttpException,
    DevOpsAPIResponseException,
    DevOpsAPITimeoutException,
    MultiCallTimeoutManager,
    TableDeleteManyException,
    TableInsertManyException,
    TableUpdateManyException,
    TooManyDocumentsToCountException,
    TooManyRowsToCountException,
    UnexpectedDataAPIResponseException,
    UnexpectedDevOpsAPIResponseException,
)
```

Info/metadata classes:

```python
from astrapy.info import (
    AstraDBAdminDatabaseInfo,
    CollectionDefaultIDOptions,
    CollectionDefinition,
    CollectionDescriptor,
    CollectionInfo,
    CollectionVectorOptions,
    AstraDBDatabaseInfo,
    EmbeddingProvider,
    EmbeddingProviderAuthentication,
    EmbeddingProviderModel,
    EmbeddingProviderParameter,
    EmbeddingProviderToken,
    FindEmbeddingProvidersResult,
    VectorServiceOptions,
)
```

Admin-related classes, functions and constants:

```python
from astrapy.admin import (
    DatabaseAdmin,
    ParsedAPIEndpoint,
    fetch_database_info,
    parse_api_endpoint,
)
```

Cursors:

```python
from astrapy.cursors import (
    AsyncCursor,
    Cursor,
    FindCursorState,
)
```

### Appendix B: compatibility with pre-1.0.0 library

If your code still uses the pre-1.0.0 astrapy (i.e. `from astrapy.db import AstraDB, AstraDBCollection` and so on)
you are strongly advised to migrate to the current API. All of the astrapy pre-1.0 API (later dubbed "core")
works throughout *astrapy v1*, albeit with a deprecation warning on astrapy v. 1.5.

Version 2 drops "core" support entirely. In order to use astrapy version 2.0+, you need to migrate your application.
Check the links at the beginning of this README for the updated documentation and API reference.

Check out previous versions of this README for more on "core": [1.5.2](https://github.com/datastax/astrapy/blob/4601c5fa749925d961de1f114ca27690d1a71b13/README.md) and [pre-1.0](https://github.com/datastax/astrapy/blob/cd3f5ce8146093e10a095709c0f5c3f8e3f2c7da/README.md).
