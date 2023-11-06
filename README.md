# AstraPy

AstraPy is a Pythonic SDK for [DataStax Astra](https://astra.datastax.com) and [Stargate](https://stargate.io/)

## Resources

- [DataStax Astra](https://astra.datastax.com)
- [Stargate](https://stargate.io/)

## Getting Started

Install AstraPy

```bash
pip install astrapy
```

Setup your Astra client

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

# Possible Operations
astra_db.create_collection(collection_name="collection_test_delete", dimension=5)
astra_db.delete_collection(collection_name="collection_test_delete")
astra_db.create_collection(collection_name="collection_test", dimension=5)

# Collections
astra_db_collection = AstraDBCollection(
    collection_name="collection_test", astra_db=astra_db
)
# Or...
astra_db_collection = AstraDBCollection(
    collection_name="collection_test", token=token, api_endpoint=api_endpoint
)

astra_db_collection.insert_one(
    {
        "_id": "5",
        "name": "Coded Cleats Copy",
        "description": "ChatGPT integrated sneakers that talk to you",
        "$vector": [0.25, 0.25, 0.25, 0.25, 0.25],
    }
)

astra_db_collection.find_one({"name": "potato"})  # Not found
astra_db_collection.find_one({"name": "Coded Cleats Copy"})
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
