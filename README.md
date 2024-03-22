# AstraPy

AstraPy is a Pythonic SDK for [DataStax Astra](https://astra.datastax.com)

_This README targets AstraPy version **1.0.0+**, where a whole new API is introduced. Click [here](pre-overhaul readme
  https://github.com/datastax/astrapy/blob/cd3f5ce8146093e10a095709c0f5c3f8e3f2c7da/README.md) for the pre-existing API (fully compatible with newer versions)._


## Quickstart

Install with `pip install astrapy`.

Get the *API Endpoint* and the *Token* to your Astra DB instance.

Try the following code after replacing the connection parameters:

```python
import astrapy


my_client = astrapy.DataAPIClient("AstraCS:...")
my_database = my_client.get_database_by_api_endpoint(
   "https://01234567-....apps.astra.datastax.com"
)

my_collection = my_database.create_collection(
    "dreams",
    dimension=3,
    metric=astrapy.constants.VectorMetric.COSINE,
)

my_collection.insert_one({"summary": "I was flying"}, vector=[-0.4, 0.7, 0])

my_collection.insert_many(
    [
        {"summary": "A dinner on the Moon"},
        {"summary": "Riding the waves", "tags": ["sport"]},
        {"summary": "Friendly aliens in town", "tags": ["scifi"]},
        {"summary": "Meeting Beethoven at the dentist"},
    ],
    vectors=[
        [0.2, -0.3, -0.5],
        [0, 0.2, 1],
        [-0.3, 0, 0.8],
        [0.2, 0.6, 0],
    ],
)

my_collection.update_one(
    {"tags": "sport"},
    {"$set": {"summary": "Surfers' paradise"}},
)

cursor = my_collection.find(
    {},
    vector=[0, 0.2, 0.4],
    limit=2,
    include_similarity=True,
)

for result in cursor:
    print(f"{result['summary']}: {result['$similarity']}")
```

Next steps:

- More info and usage patterns are given in the docstrings of classes and methods
- [AstraPy reference]()
- [Data API reference](https://docs.datastax.com/en/astra/astra-db-vector/api-reference/data-api-commands.html)

## AstraPy's API

AstraPy's abstractions for working at the data and admin layers are structured
as depicted by this diagram:

![AstraPy, abstractions chart](pictures/astrapy_abstractions.png)

Here's a small admin-oriented example:

```python
import astrapy

my_client = astrapy.DataAPIClient("AstraCS:...")

my_astra_admin = my_client.get_admin()

database_list = list(my_astra_admin.list_databases())

db_info = database_list[0].info
print(db_info.name, db_info.id, db_info.region)

my_database_admin = my_astra_admin.get_database_admin(db_info.id)

my_database_admin.list_namespaces()
my_database_admin.create_namespace("my_dreamspace")
```

The package comes with its own set of exceptions, arranged in this hierarchy:

![AstraPy, exception hierarchy](pictures/astrapy_exceptions.png)

For more information, and code examples, check out the docstrings and consult
the API reference linked above.

## For contributors

First install poetry with `pip install poetry` and then the project dependencies with `poetry install`.

Linter, style and typecheck should all pass for a PR:

```
poetry run black --check astrapy && poetry run ruff astrapy && poetry run mypy astrapy

poetry run black --check tests && poetry run ruff tests && poetry run mypy tests
```

Features must be thoroughly covered in tests (see `tests/idiomatic/*` for
naming convention and module structure).

### Running tests

Full testing requires environment variables:

```
export ASTRA_DB_APPLICATION_TOKEN="AstraCS:..."
export ASTRA_DB_API_ENDPOINT="https://.......apps.astra.datastax.com"

export ASTRA_DB_KEYSPACE="default_keyspace"
# Optional:
export ASTRA_DB_SECONDARY_KEYSPACE="..."
```

Tests can be started in various ways:

```bash
# test the core modules
poetry run pytest tests/core
# test the "idiomatic" layer
poetry run pytest tests/idiomatic
poetry run pytest tests/idiomatic/unit
poetry run pytest tests/idiomatic/integration

# remove logging noise:
poetry run pytest [...] -o log_cli=0

# do not drop collections:
TEST_SKIP_COLLECTION_DELETE=1 poetry run pytest [...]

# include astrapy.core.ops testing (must cleanup after that):
TEST_ASTRADBOPS=1 poetry run pytest [...]
```
