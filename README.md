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

#### Using the HTTP Client
You can use the HTTP client to work with any Astra/Stargate endpoint. [API Reference](https://docs.datastax.com/en/astra/docs/getting-started-with-datastax-astra.html)

```python
from astrapy.rest import create_client, http_methods
import uuid

# get Astra connection information from environment variables
ASTRA_DB_ID = os.environ.get('ASTRA_DB_ID')
ASTRA_DB_REGION = os.environ.get('ASTRA_DB_REGION')
ASTRA_DB_APPLICATION_TOKEN = os.environ.get('ASTRA_DB_APPLICATION_TOKEN')
ASTRA_DB_KEYSPACE = os.environ.get('ASTRA_DB_KEYSPACE')
ASTRA_DB_COLLECTION = "test"

# setup an Astra Client
astra_http_client = create_client(astra_database_id=ASTRA_DB_ID,
                         astra_database_region=ASTRA_DB_REGION,
                         astra_application_token=ASTRA_DB_APPLICATION_TOKEN)

# create a document on Astra using the Document API
doc_uuid = uuid.uuid4()
astra_http_client.request(
    method=http_methods.PUT,
    path=f"/api/rest/v2/namespaces/{ASTRA_DB_KEYSPACE}/collections/{ASTRA_DB_COLLECTION}/{doc_uuid}",
    json_data={
        "first_name": "Cliff",
        "last_name": "Wicklow",
        "emails": ["cliff.wicklow@example.com"],
    })
```

#### Using the Collections module
You can use the collections module to work with Documents in a simple way. [API Reference](https://docs.datastax.com/en/astra/docs/document-api.html)

Refer to [this file](https://github.com/datastax/astrapy/blob/master/tests/astrapy/test_collections.py) for comprehensive examples.

```python
from astrapy.collections import create_client, AstraCollection
import uuid

# get Astra connection information from environment variables
ASTRA_DB_ID = os.environ.get('ASTRA_DB_ID')
ASTRA_DB_REGION = os.environ.get('ASTRA_DB_REGION')
ASTRA_DB_APPLICATION_TOKEN = os.environ.get('ASTRA_DB_APPLICATION_TOKEN')
ASTRA_DB_KEYSPACE = os.environ.get('ASTRA_DB_KEYSPACE')
TEST_COLLECTION_NAME = "test"

# setup an Astra Client and create a shortcut to our test colllection
astra_client = create_client(astra_database_id=ASTRA_DB_ID,
                                astra_database_region=ASTRA_DB_REGION,
                                astra_application_token=ASTRA_DB_APPLICATION_TOKEN)
test_collection = astra_client.namespace(ASTRA_DB_KEYSPACE).collection(TEST_COLLECTION_NAME)

# create a new document
cliff_uuid = str(uuid.uuid4())
test_collection.create(path=cliff_uuid, document={
    "first_name": "Cliff",
    "last_name": "Wicklow",
})
```