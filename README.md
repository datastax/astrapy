## AstraPy

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

from dotenv import load_dotenv
from astrapy.db import AstraDB, AstraDBCollection

load_dotenv()

# Grab the new information from the database
db_id = os.getenv("ASTRA_DB_ID")
token = os.getenv("ASTRA_DB_APPLICATION_TOKEN")

# Initialize our vector db
astra_db = AstraDB(db_id=db_id, token=token)

# Create a new collection
astra_db.create_collection(name="collection_test", size=5)

# Collections
astra_db_collection = AstraDBCollection(
    collection="collection_test",
    astra_db=astra_db
)

# Insert some data into the collection
astra_db_collection.insert_one(
    {
        "_id": "1",
        "name": "Coded Cleats Copy",
        "description": "ChatGPT integrated sneakers that talk to you",
        "$vector": [0.25, 0.25, 0.25, 0.25, 0.25],
    }
)

# Find the inserted data!
astra_db_collection.find_one({"name" : "Coded Cleats Copy"})
```

#### More Information

Check out the [notebook](https://colab.research.google.com/github/synedra/astra_vector_examples/blob/main/notebook/vector.ipynb#scrollTo=f04a1806) which has examples for finding and inserting information into the database, including vector commands.

Take a look at the [vector tests](https://github.com/datastax/astrapy/blob/master/tests/astrapy/test_collections.py) and the [collection tests](https://github.com/datastax/astrapy/blob/master/tests/astrapy/test_collections.py) for specific endpoint examples.

#### Using the Ops Client

You can use the Ops client to work the with Astra DevOps API. Check the [devops tests](https://github.com/datastax/astrapy/blob/master/tests/astrapy/test_devops.py)
