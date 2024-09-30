# AstraPy

A pythonic client for [DataStax Astra DB](https://astra.datastax.com).

_This README targets AstraPy version **1.0.0+**, which introduces a whole new API.
Click [here](https://github.com/datastax/astrapy/blob/cd3f5ce8146093e10a095709c0f5c3f8e3f2c7da/README.md) for the pre-existing API (fully compatible with newer versions)._


## Quickstart

Install with `pip install astrapy`.

Get the *API Endpoint* and the *Token* to your Astra DB instance at [astra.datastax.com](https://astra.datastax.com).

Try the following code after replacing the connection parameters:

```python
import astrapy

ASTRA_DB_APPLICATION_TOKEN = "AstraCS:..."
ASTRA_DB_API_ENDPOINT = "https://01234567-....apps.astra.datastax.com"

my_client = astrapy.DataAPIClient()
my_database = my_client.get_database(
    ASTRA_DB_API_ENDPOINT,
    token=ASTRA_DB_APPLICATION_TOKEN,
)

my_collection = my_database.create_collection(
    "dreams",
    dimension=3,
    metric=astrapy.constants.VectorMetric.COSINE,
)

my_collection.insert_one({"summary": "I was flying", "$vector": [-0.4, 0.7, 0]})

my_collection.insert_many(
    [
        {
            "_id": astrapy.ids.UUID("018e65c9-e33d-749b-9386-e848739582f0"),
            "summary": "A dinner on the Moon",
            "$vector": [0.2, -0.3, -0.5],
        },
        {
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
```

Next steps:

- More info and usage patterns are given in the docstrings of classes and methods
- [Data API reference](https://docs.datastax.com/en/astra-db-serverless/api-reference/overview.html)
- [AstraPy reference](https://docs.datastax.com/en/astra-api-docs/_attachments/python-client/astrapy/index.html)
- Package on [PyPI](https://pypi.org/project/astrapy/)

### Usage with HCD and other non-Astra installations

The main difference to target e.g. a Hyper-Converged Database (HCD)
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
client = DataAPIClient(token=tp, environment=Environment.HCD)
database = client.get_database("http://localhost:8181", token=tp)
```

For more on this case, please consult the [dedicated reference](https://docs.datastax.com/en/hyper-converged-database/1.0/connect/python-client.html).

## AstraPy's API

### Abstraction diagram

AstraPy's abstractions for working at the data and admin layers are structured
as depicted by this diagram:

![AstraPy, abstractions chart](https://raw.githubusercontent.com/datastax/astrapy/master/pictures/astrapy_abstractions.png)

Here's a small admin-oriented example:

```python
import astrapy


# this must have "Database Administrator" permissions:
ASTRA_DB_APPLICATION_TOKEN = "AstraCS:..."

my_client = astrapy.DataAPIClient(ASTRA_DB_APPLICATION_TOKEN)

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

![AstraPy, exception hierarchy](https://raw.githubusercontent.com/datastax/astrapy/master/pictures/astrapy_exceptions.png)

For more information, and code examples, check out the docstrings and consult
the API reference linked above.

### Working with dates

Date and datetime objects, i.e. instances of the standard library
`datetime.datetime` and `datetime.date` classes, can be used anywhere in documents:

```python
import datetime
import astrapy


ASTRA_DB_APPLICATION_TOKEN = "AstraCS:..."
ASTRA_DB_API_ENDPOINT = "https://01234567-....apps.astra.datastax.com"

my_client = astrapy.DataAPIClient()
my_database = my_client.get_database(
    ASTRA_DB_API_ENDPOINT,
    token=ASTRA_DB_APPLICATION_TOKEN,
)
my_collection = my_database.dreams

my_collection.insert_one({"when": datetime.datetime.now()})
my_collection.insert_one({"date_of_birth": datetime.date(2000, 1, 1)})

my_collection.update_one(
    {"registered_at": datetime.date(1999, 11, 14)},
    {"$set": {"message": "happy Sunday!"}},
)

print(
    my_collection.find_one(
        {"date_of_birth": {"$lt": datetime.date(2001, 1, 1)}},
        projection={"_id": False},
    )
)
# This would print:
#    {'date_of_birth': datetime.datetime(2000, 1, 1, 0, 0)}
```

_**Note**: reads from a collection will always_
_return the `datetime` class regardless of wheter a `date` or a `datetime` was provided_
_in the insertion._

### Working with ObjectIds and UUIDs

Astrapy repackages the ObjectId from `bson` and the UUID class and utilities
from the `uuid` package and its `uuidv6` extension. You can also use them directly.

Even when setting a default ID type for a collection, you still retain the freedom
to use any ID type for any document:

```python
import astrapy
import bson

ASTRA_DB_APPLICATION_TOKEN = "AstraCS:..."
ASTRA_DB_API_ENDPOINT = "https://01234567-....apps.astra.datastax.com"

my_client = astrapy.DataAPIClient()
my_database = my_client.get_database(
    ASTRA_DB_API_ENDPOINT,
    token=ASTRA_DB_APPLICATION_TOKEN,
)

my_collection = my_database.create_collection(
    "ecommerce",
    default_id_type=astrapy.constants.DefaultIdType.UUIDV6,
)

my_collection.insert_one({"_id": astrapy.ids.ObjectId("65fd9b52d7fabba03349d013")})
my_collection.find({
    "_id": astrapy.ids.UUID("018e65c9-e33d-749b-9386-e848739582f0"),
})

my_collection.update_one(
    {"tag": "in_stock"},
    {"$set": {"inventory_id": bson.objectid.ObjectId()}},
    upsert=True,
)

my_collection.insert_one({"_id": astrapy.ids.uuid8()})
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

- **idiomatic**: all 1.0+ classes and APIs, except...
- **vectorize**: ... everything making use of `$vectorize` (within the idiomatic classes)
- _(core: pre-1.0 classes). Frozen as of v1.5, deprecated for removal in v2.0_

Actually, for convenience, _sub-blocks_ of tests are considered:

- **idiomatic regular**: everything except the admin parts
- **idiomatic admin Astra**: the Astra-specific admin operations
- **idiomatic admin nonAstra**: the nonAstra-specific admin operations
- **vectorize in-depth**: many Data API interactions for a single choice of provider/model. This is mostly test the client
- **vectorize all-providers**: a slightly more shallow test repeated for all providers, models, auth methods etc. This is mostly testing the API
- _(core regular: everything except DevOps interactions)_
- _(core ops: core DevOps operations)_

Tests can be run on three types of Data API _targets_ (with slight differences in what is applicable):

- **DockerCompose**: HCD started by the test initialization with `docker-compose`. _Note that in this case you will have to manually destroy the created containers._
- **nonAstra**: a ready-to-use (user-supplied) local Data API
- **Astra**: an Astra DB target account (or two, as some tests are specific to dev environment)

Depending on the (sub-block, target) combination, some environment variables may be needed.
Templates for the environment variables are to be found in `tests/env_templates`.

The general expectation is that idiomatic non-Admin tests, and vectorize in-depth tests, are
part of the main CI flow; conversely, core, admin and vectorize all-providers are kept as a
manual task to run (locally in most cases) when circumstances require it (use your judgement).

#### Required environment variables

Below is a detail of the reference template files needed for the various types
of testing:

- **DockerCompose**: generally no variables needed, except:
  - **vectorize in-depth**: provide as in `env.vectorize-minimal.template`
  - **vectorize all-providers**: provide as in `env.vectorize.template`
  - (also note that _core ops_ and _idiomatic admin Astra_ amount to nothing in this case)
- **nonAstra**: all tests require as in `env.local.template`, plus:
  - **vectorize in-depth**: also provide as in `env.vectorize-minimal.template`
  - **vectorize all-providers**: also provide as in `env.vectorize.template`
  - (also note that _core ops_ and _idiomatic admin Astra_ amount to nothing in this case)
- **Astra**: all tests require as in `env.astra.template`, plus:
  - **core ops**: the token must have at least "Database Administrator" role (possibly through definition of a separate `ASTRA_DB_OPS_APPLICATION_TOKEN`), and `ASTRA_DB_ID` must also be defined
  - **idiomatic admin Astra**: also provide as in `env.astra.admin.template`
  - **vectorize in-depth**: also provide as in `env.vectorize-minimal.template`
  - **vectorize all-providers**: also provide as in `env.vectorize.template`
  - (also note that _idiomatic admin nonAstra_ amounts to nothing in this case)

#### Sample testing commands

For the **DockerCompose** case, prepend all of the following with `DOCKER_COMPOSE_LOCAL_DATA_API="yes" `.

All the usual `pytest` ways of restricting the test selection hold in addition
(e.g. `poetry run pytest tests/idiomatic/unit` or `[...] -k <test_name_selector>`).

##### _core regular_:

```
poetry run pytest tests/core
```

##### _core ops_:

Note the special variable needed to actually run this. You will have to manually clean up afterwards.

```
TEST_ASTRADBOPS="1" poetry run pytest tests/core/test_ops.py
```

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

Do not drop collections (valid for core):

```
TEST_SKIP_COLLECTION_DELETE=1 poetry run pytest [...]
```


## Appendices

### Appendix A: quick reference for imports

Client, data and admin abstractions:

```python
from astrapy import (
    DataAPIClient,
    Database,
    AsyncDatabase,
    Collection,
    AsyncCollection,
    AstraDBAdmin,
    AstraDBDatabaseAdmin,
    DataAPIDatabaseAdmin,
)
```

Constants for data-related use:

```python
from astrapy.constants import (
    ReturnDocument,
    SortDocuments,
    VectorMetric,
    DefaultIdType,
    Environment,
)
```

ObjectIds and UUIDs:

```python
from astrapy.ids import (
    ObjectId,
    uuid1,
    uuid3,
    uuid4,
    uuid5,
    uuid6,
    uuid7,
    uuid8,
    UUID,
)
```

Operations (for `bulk_write` collection method):

```python
from astrapy.operations import (
    BaseOperation,
    InsertOne,
    InsertMany,
    UpdateOne,
    UpdateMany,
    ReplaceOne,
    DeleteOne,
    DeleteMany,
    AsyncBaseOperation,
    AsyncInsertOne,
    AsyncInsertMany,
    AsyncUpdateOne,
    AsyncUpdateMany,
    AsyncReplaceOne,
    AsyncDeleteOne,
    AsyncDeleteMany,
)
```

Result classes:

```python
from astrapy.results import (
    OperationResult,
    DeleteResult,
    InsertOneResult,
    InsertManyResult,
    UpdateResult,
    BulkWriteResult,
)
```

Exceptions:

```python
from astrapy.exceptions import (
    BulkWriteException,
    CollectionAlreadyExistsException,
    CollectionNotFoundException,
    CumulativeOperationException,
    CursorIsStartedException,
    DataAPIDetailedErrorDescriptor,
    DataAPIErrorDescriptor,
    DataAPIException,
    DataAPIFaultyResponseException,
    DataAPIHttpException,
    DataAPIResponseException,
    DataAPITimeoutException,
    DeleteManyException,
    DevOpsAPIErrorDescriptor,
    DevOpsAPIException,
    DevOpsAPIFaultyResponseException,
    DevOpsAPIHttpException,
    DevOpsAPIResponseException,
    DevOpsAPITimeoutException,
    InsertManyException,
    TooManyDocumentsToCountException,
    UpdateManyException,
)
```

Info/metadata classes:

```python
from astrapy.info import (
    AdminDatabaseInfo,
    DatabaseInfo,
    CollectionInfo,
    CollectionVectorServiceOptions,
    CollectionDefaultIDOptions,
    CollectionVectorOptions,
    CollectionOptions,
    CollectionDescriptor,
    EmbeddingProviderParameter,
    EmbeddingProviderModel,
    EmbeddingProviderToken,
    EmbeddingProviderAuthentication,
    EmbeddingProvider,
    FindEmbeddingProvidersResult,
)
```

Admin-related classes and constants:

```python
from astrapy.admin import (
    ParsedAPIEndpoint,
)
```

Cursors:

```python
from astrapy.cursors import (
    BaseCursor,
    Cursor,
    AsyncCursor,
    CommandCursor,
    AsyncCommandCursor,
)
```

### Appendix B: compatibility with pre-1.0.0 library

If your code still uses the pre-1.0.0 astrapy (i.e. `from astrapy.db import AstraDB, AstraDBCollection` and so on)
you are strongly advised to migrate to the current API, which has more capabilities and improved interfaces.

All of the astrapy pre-1.0 API (now dubbed "core") works throughout *astrapy v1*, albeit with a deprecation warning
on astrapy v. 1.5.

Version 1.5 (the first to not wrap internally "core" as the engine of its own "idiomatic" API) introduces
several deprecation notices (nothing is retired yet), including a submodule-wide deprecation of "core".

**Version 2 of astrapy will finally remove "core" entirely (along with a few other things).**

#### v1 is fully compatible with "core", i.e. with pre-1.0.0

That being said, there are no known breakings of backward compatibility:
**legacy code would run with astrapy v1 just as well**
Here is a recap of the minor changes that came _to the old API_ with 1.0.0 (and beyond, up to 1.5):

- added a submodule-wide deprecation warning of the whole "core" library (v 1.5)
- added 'options' parameter to [Async]AstraDBCollection.update_one (v. 1.4.2+)
- prefetched find iterators: fix second-thread hangups in some cases (v. 1.4.2+)
- Added support for null tokens (with the effect of no authentication/token header in requests)
- Added Content-Type header to all HTTP requests to the API
- Added methods to `[Async]AstraDBCollection`: `delete_one_filter`, 
- Paginated find methods (sync/async) type change from Iterable to Generator
- Bugfix: handling of the mutable caller identity in copy and convert (sync/async) methods
- Default value of `sort` is `None` and not `{}` for `find` (sync/async)
- Introduction of `[Async]AstraDBCollection.chunked_delete_many` method
- Added `projection` parameter to `find_one_and[replace/update]` (sync/async)
- Bugfix: projection was silently ignored in `vector_find_one_and_[replace/update]` (sync/async)
- Added `options` to `update_many` (sync/async)
- `[Async]AstraDBDatabase.chunked_insert_many` does not intercept generic exceptions anymore, only `APIRequestError`
- Bugfix: `AsyncAstraDBCollection.async chunked_insert_many` stops at the first error when `ordered=True`
- Added payload info to `DataAPIException`
- Added `find_one_and_delete` method (sync/async)
- Added `skip_error_check` parameter to `delete_many` (sync/async)
- Timeout support throughout the library
- Added `sort` to `update_one`, `delete_one` and `delete_one_by_predicate` methods (sync/async)
- Full support for UUID v1,3,4,5,6,7,8 and ObjectID at the collection data I/O level
- `AstraDBOps.create_database` raises errors in case of failures
- `AstraDBOps.create_database`, return type corrected
- Fixed behaviour and return type of `AstraDBOps.create_keyspace` and `AstraDBOps.terminate_db`
- Added `AstraDBOps.delete_keyspace` method
- Method `create_collection` of `AstraDB` relaxes checks on passing `dimensions` for vector collections
- AstraDBOps core class acquired async methods: `async_get_databases`, `async_get_database`, `async_create_database`, `async_terminate_database`, `async_create_keyspace`, `async_delete_keyspace`

