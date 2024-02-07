# AstraPy

AstraPy is a Pythonic SDK for [DataStax Astra](https://astra.datastax.com)

[**Part I - Getting Started**](#part-i---getting-started)

- [1.1 Install AstraPy](#11-install-astrapy)
- [1.2 Setup your Astra client](#12-setup-your-astra-client)

[**Part II - Collections**](#part-ii---collections)

- [2.1 Create and delete vector collections](#21-create-and-delete-vector-collections)
- [2.2 Connect to existing collection](#22-connect-to-existing-collection)

[**Part III - Inserting Documents**](#part-iii---inserting-documents)

- [3.1 - Inserting a document](#31-inserting-a-document)
- [3.2 - Inserting multiple documents](#32-inserting-multiple-documents)
- [3.3 - Inserting multiple documents](#33-inserting-multiple-vector-documents)
- [3.4 - Creating a subdocument](#34-creating-a-subdocument)
- [3.5 - Create a document without an ID](#35-create-a-document-without-an-id)

[**Part IV - Updating Documents**](#part-iv---updating-documents)

- [4.1 - Update a Document](#41-update-a-document)
- [4.2 - Replace a Non-vector-document](#42-replace-a-non-vector-document)

[**Part V - Finding Documents**](#part-v---finding-documents)

- [5.1 - Find documents using vector search](#51-find-documents-using-vector-search)
- [5.2 - Find documents using vector search and projection](#52-find-documents-using-vector-search-and-projection)
- [5.3 - Find one and update with vector search](#53-find-one-and-update-with-vector-search)
- [5.4 Find one and replace with vector search](#54-find-one-and-replace-with-vector-search)

[**Part VI - Deleting Documents**](#part-vi---deleting-documents)

- [6.1 Delete a Subdocument](#61-delete-a-subdocument)
- [6.2 Delete a Document](#62-delete-a-document)

## Part I - Getting Started

### 1.1 Install AstraPy

```bash
pip install astrapy
```

### 1.2 Setup your Astra client

Create a `.env` file with the appropriate values:

```bash
ASTRA_DB_APPLICATION_TOKEN="<AstraCS:...>"
ASTRA_DB_API_ENDPOINT="<https://...>"
```

> If you have [Astra CLI](https://docs.datastax.com/en/astra-cli/docs/0.2/installation.html) installed, you can create the `.env` file with
> `astra db create-dotenv DATABASE_NAME`.

Load the variables in and then create the client. This collections client can make non-vector and vector calls, depending on the call configuration.

```python
import os

from dotenv import load_dotenv

from astrapy.db import AstraDB, AstraDBCollection
from astrapy.ops import AstraDBOps

load_dotenv()

# Grab the Astra token and api endpoint from the environment
token = os.getenv("ASTRA_DB_APPLICATION_TOKEN")
api_endpoint = os.getenv("ASTRA_DB_API_ENDPOINT")

# Initialize our vector db
astra_db = AstraDB(token=token, api_endpoint=api_endpoint)
```

## Part II - Collections

### 2.1 Create and Delete Vector Collections

Create a vector collection with dimension of 5
If you were using OpenAI here you would use 1376 as the value

```python
# Create a collection and then delete it
astra_db.create_collection(collection_name="collection_test_delete", dimension=5)
astra_db.delete_collection(collection_name="collection_test_delete")

# Double check the collections in your vector store
astra_db.get_collections()
```

At this point you have a collection named "collection_test" to do the following operations

In the next section, you will be creating the object for your collection

### 2.2 Connect to existing collection

```python
# The return of create_collection() will return the collection
collection = astra_db.create_collection(
    collection_name="collection_test", dimension=5
)

# Or you can connect to an existing connection directly
collection = AstraDBCollection(
    collection_name="collection_test", astra_db=astra_db
)

# You don't even need the astra_db object
collection = AstraDBCollection(
    collection_name="collection_test", token=token, api_endpoint=api_endpoint
)
```

## Part III - Inserting Documents

### 3.1 Inserting a document

Here is an example of inserting a vector object into your vector store (collection), followed by running a find command to retrieve the document. The first find command fails because that object does not exist. The second find command should succeed.

```python
collection.insert_one(
    {
        "_id": "5",
        "name": "Coded Cleats Copy",
        "description": "ChatGPT integrated sneakers that talk to you",
        "$vector": [0.25, 0.25, 0.25, 0.25, 0.25],
    }
)

collection.find_one({"name": "potato"})  # Not found
collection.find_one({"name": "Coded Cleats Copy"})
```

### 3.2 Inserting multiple documents

Here is an example of inserting a number of documents into your collection. Note that the json object is 'documents' here, not 'document' as it is in insert_one.

In the first insert, the default behavior is in place. If you are inserting documents that already exist, you will get an error and the process will end.

These two examples are using non-vector objects.

```python
documents = [
    {
        "_id": "id_1",
        "first_name": "Dang",
        "last_name": "Son",
    },
    {
        "_id": "id_2",
        "first_name": "Yep",
        "last_name": "Boss",
    },
]
response = collection.insert_many(documents=documents)
```

In the following insert_many example, options are set so that it skips errors and only inserts successful entries.

```python
documents2 = [
    {
        "_id": "id_2",
        "first_name": "Yep",
        "last_name": "Boss",
    },
    {
        "_id": "id_3",
        "first_name": "Miv",
        "last_name": "Fuff",
    },
]
response = collection.insert_many(
    documents=documents2,
    partial_failures_allowed=True,
)
```

### 3.3 Inserting multiple vector documents

The following code inserts vector objects into the collection in your vector store.

```python
json_query = [
    {
        "_id": str(uuid.uuid4()),
        "name": "Coded Cleats",
        "description": "ChatGPT integrated sneakers that talk to you",
        "$vector": [0.1, 0.15, 0.3, 0.12, 0.05],
    },
    {
        "_id": str(uuid.uuid4()),
        "name": "Logic Layers",
        "description": "An AI quilt to help you sleep forever",
        "$vector": [0.45, 0.09, 0.01, 0.2, 0.11],
    },
    {
        "_id": vv_uuid,
        "name": "Vision Vector Frame",
        "description": "Vision Vector Frame - A deep learning display that controls your mood",
        "$vector": [0.1, 0.05, 0.08, 0.3, 0.6],
    },
]

res = collection.insert_many(documents=json_query)
```

### 3.4 Creating a subdocument

The following code uses update to create or update a sub-document under one of your existing documents.

```python
document = collection.update_one(
    filter={"_id": "id_1"},
    update={"$set": {"name": "Eric"}},
)

document = collection.find_one(filter={"_id": "id_1"})
```

### 3.5 Create a document without an ID

```python
response = collection.insert_one(
        document={
            "first_name": "New",
            "last_name": "Guy",
        }
    )

document = collection.find_one(filter={"first_name": "New"})
```

## Part IV - Updating Documents

### 4.1 Update a document

```python
collection.update_one(
    filter={"_id": cliff_uuid},
    update={"$set": {"name": "Bob"}},
)

document = collection.find_one(filter={"_id": "id_1"})
```

### 4.2 Replace a non-vector document

```python
collection.find_one_and_replace(
        filter={"_id": "id_1"},
        replacement={
            "_id": "id_1",
            "addresses": {
                "work": {
                    "city": "New York",
                    "state": "NY",
                }
            },
        },
    )
document = collection.find_one(filter={"_id": "id_1"})
document_2 = collection.find_one(
        filter={"_id": cliff_uuid}, projection={"addresses.work.city": 1}
    )
```

## Part V - Finding Documents

The below examples show our high-level interfaces for finding documents. Note that corresponding low-level functionality exists, i.e., the high-level interface `vector_find` is a wrapper around the `find` function, as available in the JSON API.

### 5.1 Find documents using vector search

```python
documents = collection.vector_find(
    [0.15, 0.1, 0.1, 0.35, 0.55],
    limit=100,
)
```

### 5.2 Find documents using vector search and projection

```python
documents = collection.vector_find(
    [0.15, 0.1, 0.1, 0.35, 0.55],
    limit=100,
    fields=["$vector"],
)
```

### 5.3 Find one and update with vector search

```python
update = {"$set": {"status": "active"}}

document = collection.find_one(filter={"status": "active"})

collection.vector_find_one_and_update(
    [0.15, 0.1, 0.1, 0.35, 0.55],
    update=update,
)

document = collection.find_one(filter={"status": "active"})
```

### 5.4 Find one and replace with vector search

```python
replacement = {
    "_id": "1",
    "name": "Vision Vector Frame",
    "description": "Vision Vector Frame - A deep learning display that controls your mood",
    "$vector": [0.1, 0.05, 0.08, 0.3, 0.6],
    "status": "inactive",
}

collection.vector_find_one_and_replace(
    [0.15, 0.1, 0.1, 0.35, 0.55],
    replacement=replacement,
)

document = collection.find_one(filter={"name": "Vision Vector Frame"})
```

## Part VI - Deleting Documents

### 6.1 Delete a subdocument

```python
response = collection.delete_subdocument(id="id_1", subdoc="addresses")
document = collection.find(filter={"_id": "id_1"})
```

## 6.2 Delete a document

```python
response = collection.delete(id="id_1")
```

### More Information

Check out the [notebook](https://colab.research.google.com/github/synedra/astra_vector_examples/blob/main/notebook/vector.ipynb#scrollTo=f04a1806) which has examples for finding and inserting information into the database, including vector commands.

Take a look at the [Astra DB DML tests](https://github.com/datastax/astrapy/blob/master/tests/astrapy/test_db_dml.py) for specific endpoint examples. Further examples are available in the other corresponding [test files](https://github.com/datastax/astrapy/tree/master/tests/astrapy)

### Using the Ops Client

You can use the Ops client to work with the Astra DevOps API. Check the [devops tests](https://github.com/datastax/astrapy/blob/master/tests/astrapy/test_ops.py)

## For Developers

Install poetry
```bash
pip install poetry
```

Install the project dependencies
```bash
poetry install
```

### Style, linter, typing

AstraPy tries to be consistent in code style and linting.
Moreover, **type annotations** are used everywhere.

To ensure the code complies, you should get no errors
(_Success: no issues found..._) when running the following in the root dir:

```bash
poetry run black --check astrapy && poetry run ruff astrapy && poetry run mypy astrapy
```

Likewise, for the tests:

```bash
poetry run black --check tests && poetry run ruff tests && poetry run mypy tests
```

### Testing

Ensure you provide all required environment variables (you can do so by editing `tests/.env` after `tests/.env.template`):

```bash
export ASTRA_DB_APPLICATION_TOKEN="..."
export ASTRA_DB_API_ENDPOINT="..."
export ASTRA_DB_KEYSPACE="..."              # Optional

export ASTRA_DB_ID="..."                    # For the Ops testing only
export ASTRA_DB_OPS_APPLICATION_TOKEN="..." # Ops-only, falls back to the other token
```

then you can run:

```bash
poetry run pytest
```

To remove the noise from the logs (on by default), run `pytest -o log_cli=0`.

To skip all collection deletions (done by default):

```bash
TEST_SKIP_COLLECTION_DELETE=1 poetry run pytest [...]
```

To enable the `AstraDBOps` testing (off by default):

```bash
TEST_ASTRADBOPS=1 poetry run pytest [...]
```
