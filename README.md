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
- [Data API reference](https://docs.datastax.com/en/astra/astra-db-vector/api-reference/overview.html)
- [AstraPy reference](https://docs.datastax.com/en/astra/astra-db-vector/api-reference/dataapiclient.html)
- Package on [PyPI](https://pypi.org/project/astrapy/)

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

my_database_admin.list_namespaces()
my_database_admin.create_namespace("my_dreamspace")
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

With `make format-fix` the style and imports are autofixed (by `black` and `isort` resp.)

Features must be thoroughly covered in tests (see `tests/idiomatic/*` for
naming convention and module structure).

### Running tests

"Full regular" testing requires environment variables:

```bash
export ASTRA_DB_APPLICATION_TOKEN="AstraCS:..."
export ASTRA_DB_API_ENDPOINT="https://.......apps.astra.datastax.com"

export ASTRA_DB_KEYSPACE="default_keyspace"
# Optional:
export ASTRA_DB_SECONDARY_KEYSPACE="..."
```

#### "Idiomatic" testing

Tests can be started in various ways: mostly `make tests-idiomatic`, but also:

```bash
# test the "idiomatic" layer
poetry run pytest tests/idiomatic
poetry run pytest tests/idiomatic/unit
poetry run pytest tests/idiomatic/integration

# remove logging noise:
poetry run pytest [...] -o log_cli=0
```

The above runs the regular testing (i.e. non-Admin, non-core).
The (idiomatic) Admin part is tested manually by you, on Astra accounts with room
for up to 3 new databases, possibly both on prod and dev, and uses specific env vars,
as can be seen on `tests/idiomatic/integration/test_admin.py`.

#### Other tests

Vectorize tests are confined in `tests/vectorize_idiomatic` and are run
separately. A separate set of credentials is required to do the full testing:
refer to `tests/.vectorize.env.template` for the complete listing, including
the secrets that should be added to the database beforehand, through the UI.

Should you be interested in testing the "core" modules, moreover,
this is also something for you to run manually (do that if you touch "core"):

```bash
# test the core modules
poetry run pytest tests/core

# do not drop collections:
TEST_SKIP_COLLECTION_DELETE=1 poetry run pytest [...]

# include astrapy.core.ops testing (tester must clean up after that):
TEST_ASTRADBOPS=1 poetry run pytest [...]
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
    DevOpsAPIException,
    DevOpsAPIResponseException,
    DevOpsAPIErrorDescriptor,
    DataAPIErrorDescriptor,
    DataAPIDetailedErrorDescriptor,
    DataAPIException,
    DataAPITimeoutException,
    CursorIsStartedException,
    CollectionNotFoundException,
    CollectionAlreadyExistsException,
    TooManyDocumentsToCountException,
    DataAPIFaultyResponseException,
    DataAPIResponseException,
    CumulativeOperationException,
    InsertManyException,
    DeleteManyException,
    UpdateManyException,
    BulkWriteException,
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

If your code uses the pre-1.0.0 astrapy (i.e. `from astrapy.db import Database, Collection` and so on) you are strongly advised to migrate to the current API.

That being said, there are no known breakings of backward compatibility:
**legacy code would run with a newest astrapy version just as well.**
Here is a recap of the minor changes that came _to the old API_ with 1.0.0:

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

Keep in mind that the pre-1.0.0 library, now dubbed "core", is what the current 1.0.0 API ("idiomatic") builds on.
