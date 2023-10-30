## AstraPy
token
[![Actions Status](https://github.com/datastax/astrapy/workflows/Tests/badge.svg)](https://github.com/datastax/astrapy/actions)

AstraPy is a Pythonic SDK for [DataStax Astra](https://astra.datastax.com) and [Stargate](https://stargate.io/)

### Resources

- [DataStax Astra](https://astra.datastax.com)
- [Stargate](https://stargate.io/)

### Getting Started

Install AstraPy

```shell
pip install astrapy
```

Setup your Astra client

Create a .env file with the appropriate values, or use the 'astra' cli to do the same.

```bash
ASTRA_DB_KEYSPACE="<keyspace>"
ASTRA_DB_APPLICATION_TOKEN="<AstraCS:...>"
ASTRA_DB_REGION="<region>"
ASTRA_DB_ID=<db_id>
```

Load the variables in and then create the client. This collections client can make non-vector and vector calls, depending on the call configuration.

```python
import os
import sys

from astrapy.db import AstraDB, AstraDBCollection
from astrapy.ops import AstraDBOps

# First, we work with devops
api_key = os.getenv("ASTRA_DB_APPLICATION_TOKEN")
astra_ops = AstraDBOps(api_key)

# Define a database to create
database_definition = {
    "name": "vector_test",
    "tier": "serverless",
    "cloudProvider": "GCP",
    "keyspace": os.getenv("ASTRA_DB_KEYSPACE", "default_keyspace"),
    "region": os.getenv("ASTRA_DB_REGION", None),
    "capacityUnits": 1,
    "user": "example",
    "password": api_key,
    "dbType": "vector",
}

# Create the database
create_result = astra_ops.create_database(database_definition=database_definition)

# Grab the new information from the database
database_id = create_result["id"]
database_region = astra_ops.get_database()[0]["info"]["region"]
database_base_url = "apps.astra.datastax.com"

# Build the endpoint URL:
api_endpoint = f"https://{database_id}-{database_region}.{database_base_url}"

# Initialize our vector db
astra_db = AstraDB(api_key=api_key, api_endpoint=api_endpoint)

# Possible Operations
astra_db.create_collection(collection_name="collection_test_delete", size=5)
astra_db.delete_collection(collection_name="collection_test_delete")
astra_db.create_collection(collection_name="collection_test", size=5)

# Collections
astra_db_collection = AstraDBCollection(
    collection_name="collection_test", astra_db=astra_db
)
# Or...
astra_db_collection = AstraDBCollection(
    collection_name="collection_test", api_key=api_key, api_endpoint=api_endpoint
)

astra_db_collection.insert_one(
    {
        "_id": "5",
        "name": "Coded Cleats Copy",
        "description": "ChatGPT integrated sneakers that talk to you",
        "$vector": [0.25, 0.25, 0.25, 0.25, 0.25],
    }
)

astra_db_collection.find_one({"name": "potato"})
astra_db_collection.find_one({"name": "Coded Cleats Copy"})
```

#### More Information

Check out the [notebook](https://colab.research.google.com/github/synedra/astra_vector_examples/blob/main/notebook/vector.ipynb#scrollTo=f04a1806) which has examples for finding and inserting information into the database, including vector commands.

Take a look at the [vector tests](https://github.com/datastax/astrapy/blob/master/tests/astrapy/test_collections.py) and the [collection tests](https://github.com/datastax/astrapy/blob/master/tests/astrapy/test_collections.py) for specific endpoint examples.

#### Using the Ops Client

You can use the Ops client to work with the Astra DevOps API. Check the [devops tests](https://github.com/datastax/astrapy/blob/master/tests/astrapy/test_devops.py)

### For Developers

#### Testing

Ensure you provide all required environment variables:

```
export ASTRA_DB_ID="..."
export ASTRA_DB_REGION="..."
export ASTRA_DB_APPLICATION_TOKEN="..."
export ASTRA_DB_KEYSPACE="..."
export ASTRA_CLIENT_ID="..."
export ASTRA_CLIENT_SECRET="..."
```

then you can run:

```
PYTHONPATH=. pytest
```
