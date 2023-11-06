# AstraPy

AstraPy is a Pythonic SDK for [DataStax Astra](https://astra.datastax.com)

## Resources

- [DataStax Astra](https://astra.datastax.com)

## Getting Started

### Install AstraPy

```bash
pip install astrapy
```

### Setup your Astra client

Create a .env file with the appropriate values, or use the 'astra' cli to do the same.

```bash
ASTRA_DB_APPLICATION_TOKEN="<AstraCS:...>"
ASTRA_DB_API_ENDPOINT="<https://...>"
```

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

## Create a collection

Create a vector collection with dimension of 5
If you were using OpenAI here you would use 1376 as the value

```
astra_db.create_collection(collection_name="collection_test", dimension=5)

# Create a collection and then delete it
astra_db.create_collection(collection_name="collection_test_delete", dimension=5)
astra_db.delete_collection(collection_name="collection_test_delete")

# Double check the collections in your vector store
astra_db.get_collections()
```

At this point you have a collection named "collection_test" to do the following operations

In the next section, you will be creating the object for your collection

## Create collection object

```
# Collections
collection = AstraDBCollection(
    collection_name="collection_test", astra_db=astra_db
)
# Or...
collection = AstraDBCollection(
    collection_name="collection_test", token=token, api_endpoint=api_endpoint
)
```

## Inserting a document into your collection (vector)

Here is an example of inserting a vector object into your vector store (collection), followed by running a find command to retrieve the document. The first find command fails because that object does not exist. The second find command should succeed.

```
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

## Inserting multiple documents into your collection (non-vector)

Here is an example of inserting a number of documents into your collection. Note that the json object is 'documents' here, not 'document' as it is in insert_one.

In the first insert, the default behavior is in place. If you are inserting documents that already exist, you will get an error and the process will end.

These two examples are using non-vector objects.

```
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

```
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

## Insert many (vector)

The following code inserts vector objects into the collection in your vector store.

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

## Create a subdocument

The following code uses update to create or update a sub-document under one of your existing documents.

```
    document = collection.update_one(
        filter={"_id": "id_1"},
        update={"$set": {"name": "Eric"}},
    )

    document = collection.find_one(filter={"_id": "id_1"})
```

## Create a document without an ID

```
response = collection.insert_one(
        document={
            "first_name": "New",
            "last_name": "Guy",
        }
    )

document = collection.find_one(filter={"first_name": "New"})
```

## Update a document

```
collection.update_one(
    filter={"_id": cliff_uuid},
    update={"$set": {"name": "Bob"}},
)

document = collection.find_one(filter={"_id": "id_1"})
```

## Replace a non-vector document

```
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

## Delete a subdocument

```
@pytest.mark.describe("should delete a subdocument")
def test_delete_subdocument(collection, "id_1"):
    response = collection.delete_subdocument(id="id_1", subdoc="addresses")
    document = collection.find(filter={"_id": "id_1"})
```

## Delete a document

```
response = collection.delete(id="id_1")
```

## Find documents using vector search

```
sort = {"$vector": [0.15, 0.1, 0.1, 0.35, 0.55]}
options = {"limit": 100}

document = collection.find(sort=sort, options=options)
```

## Find documents using vector search and projection"

```
sort = {"$vector": [0.15, 0.1, 0.1, 0.35, 0.55]}
options = {"limit": 100}
projection = {"$vector": 1, "$similarity": 1}

document = collection.find(sort=sort, options=options, projection=projection)
```

## Find one and update with vector search

@pytest.mark.describe("Find one and update with vector search")

```
sort = {"$vector": [0.15, 0.1, 0.1, 0.35, 0.55]}
update = {"$set": {"status": "active"}}
options = {"returnDocument": "after"}

result = collection.find_one_and_update(sort=sort, update=update, options=options)

document = collection.find_one(filter={"status": "active"})
```

## Find one and replace with vector search

```
sort = {"$vector": [0.15, 0.1, 0.1, 0.35, 0.55]}
replacement = {
    "_id": vv_uuid,
    "name": "Vision Vector Frame",
    "description": "Vision Vector Frame - A deep learning display that controls your mood",
    "$vector": [0.1, 0.05, 0.08, 0.3, 0.6],
    "status": "inactive",
}
options = {"returnDocument": "after"}

collection.find_one_and_replace(sort=sort, replacement=replacement, options=options)
document = collection.find_one(filter={"name": "Vision Vector Frame"})
```

### More Information

Check out the [notebook](https://colab.research.google.com/github/synedra/astra_vector_examples/blob/main/notebook/vector.ipynb#scrollTo=f04a1806) which has examples for finding and inserting information into the database, including vector commands.

Take a look at the [astra db tests](https://github.com/datastax/astrapy/blob/master/tests/astrapy/test_db.py) for specific endpoint examples.

### Using the Ops Client

You can use the Ops client to work with the Astra DevOps API. Check the [devops tests](https://github.com/datastax/astrapy/blob/master/tests/astrapy/test_ops.py)

## For Developers

### Testing

Ensure you provide all required environment variables:

```bash
export ASTRA_DB_ID="..."
export ASTRA_DB_APPLICATION_TOKEN="..."
export ASTRA_DB_API_ENDPOINT="..."
```

then you can run:

```bash
PYTHONPATH=. pytest
```
