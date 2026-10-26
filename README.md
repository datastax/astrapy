# astrapy

`astrapy` is the Pythonic client for interacting with [DataStax Astra DB](https://astra.datastax.com/) and Hyper-Converged Database.

## Setup

Install with

```
pip install astrapy
```

A database is needed for the client to connect to.
The database can be either an Astra DB instance or a Hyper-Converged Database (HCD) instance:
follow the appropriate documentation links to get the connection parameters for your database.

## Documentation

For Astra DB Serverless:

- [Quickstart for collections](https://docs.datastax.com/en/astra-db-serverless/get-started/quickstart-python.html)
- [Quickstart for tables](https://docs.datastax.com/en/astra-db-serverless/get-started/quickstart-tables-python.html)
- [Get started with the Data API](https://docs.datastax.com/en/astra-db-serverless/api-reference/dataapiclient-python.html)

For Hyper-Converged Database (HCD):

- [Quickstart for collections](https://docs.datastax.com/en/hyper-converged-database/2.0/api-reference/quickstart-python.html)
- [Quickstart for tables](https://docs.datastax.com/en/hyper-converged-database/2.0/api-reference/quickstart-tables-python.html)
- [Get started with the Data API](https://docs.datastax.com/en/hyper-converged-database/2.0/api-reference/dataapiclient-python.html)

### Additional info

For technical details and further information on some specific astrapy topics (exception hierarchy, date/datetime handling, and such), please refer to:

- the ["additional technical info"](https://github.com/datastax/astrapy/blob/main/additional_info.md) document in the astrapy repository
- the Astra DB ["Python client internals"](https://docs.datastax.com/en/astra-db-serverless/api-reference/python-client.html) docs page

## At a glance

```python
from astrapy import DataAPIClient
from astrapy.info import CollectionDefinition


# Connect to the DB:
client = DataAPIClient()
database = client.get_database(
    "<Astra DB API Endpoint>",
    token="<Astra DB Application Token>",
)

# Create the collection
# (with server-side embedding generation, 'vectorize')
collection_definition = (
    CollectionDefinition.builder()
    .set_vector_dimension(768)
    .set_vector_service(
        "<vectorize provider>",
        "<vectorize model name>",
        authentication={"providerKey": "API_KEY_NAME"}
    )
    .build()
)
collection = database.create_collection(
    "COLLECTION_NAME",
    definition=collection_definition,
)

collection.insert_many([
    {"name": "Apple", "score": 10, "$vectorize": "This fruit ..."},
    {"name": "Peach", "score": 5, "$vectorize": "A delicious ..."},
    {"name": "Walnut", "score": 8, "$vectorize": "Rich in ..."},
    # ...
])

# Use vectorize search and filters to find a document:
result = collection.find_one(
    {
        "$and": [
            {"score": {"$gte": 5}},
            {"name": {"$ne": "Apple"}},
        ]
    },
    sort={"$vectorize": "Thirst-quenching fruit with a somewhat sour taste"},
)

print(result)
```
