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
from astrapy.collections import AstraCollectionClient
from astrapy.base import AstraClient

from dotenv import load_dotenv

load_dotenv()

ASTRA_DB_ID = os.environ.get("ASTRA_DB_ID")
ASTRA_DB_REGION = os.environ.get("ASTRA_DB_REGION")
ASTRA_DB_APPLICATION_TOKEN = os.environ.get("ASTRA_DB_APPLICATION_TOKEN")
ASTRA_DB_KEYSPACE = os.environ.get("ASTRA_DB_KEYSPACE")
TEST_COLLECTION_NAME = "test_collection"

astra_client = AstraClient(
        astra_database_id=ASTRA_DB_ID,
        astra_database_region=ASTRA_DB_REGION,
        astra_application_token=ASTRA_DB_APPLICATION_TOKEN,
    )

vector_client = AstraVectorClient(astra_client=astra_client)
test_namespace = vector_client.namespace(ASTRA_DB_KEYSPACE)
test_collection = vector_client.namespace(ASTRA_DB_KEYSPACE).collection(TEST_COLLECTION_NAME)
```

####Getting started
Check out the [notebook](https://colab.research.google.com/github/synedra/astra_vector_examples/blob/main/notebook/vector.ipynb#scrollTo=f04a1806) which has examples for finding and inserting information into the database, including vector commands.

Take a look at the [vector tests](https://github.com/datastax/astrapy/blob/master/tests/astrapy/test_collections.py) and the [collection tests](https://github.com/datastax/astrapy/blob/master/tests/astrapy/test_collections.py) for specific endpoint examples.

#### Using the Ops Client

You can use the Ops client to work the with Astra DevOps API. Check the [devops tests](https://github.com/datastax/astrapy/blob/master/tests/astrapy/test_devops.py)
