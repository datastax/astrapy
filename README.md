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
```python
from astrapy.client import create_astra_client

astra_client = create_astra_client(astra_database_id=ASTRA_DB_ID,
                                   astra_database_region=ASTRA_DB_REGION,
                                   astra_application_token=ASTRA_DB_APPLICATION_TOKEN)
```

Take a look at the [client tests](https://github.com/datastax/astrapy/blob/master/tests/astrapy/test_client.py) and the [collection tests](https://github.com/datastax/astrapy/blob/master/tests/astrapy/test_collections.py) for specific endpoint examples.

#### Using the Ops Client
You can use the Ops client to work the with Astra DevOps API. [API Reference](https://docs.datastax.com/en/astra/docs/_attachments/devopsv2.html)
```python
# astra_client created above
# create a keyspace using the Ops API
astra_client.ops.create_keyspace(database=ASTRA_DB_ID, keyspace=KEYSPACE_NAME)
```

#### Using the REST Client
You can use the REST client to work with the Astra REST API. [API Reference](https://docs.datastax.com/en/astra/docs/_attachments/restv2.html#tag/Data)
```python
# astra_client created above
# search a table
res = astra_client.rest.search_table(keyspace=ASTRA_DB_KEYSPACE,
                                     table=TABLE_NAME,
                                     query={"firstname": {"$eq": "Cliff"}})
print(res["count"]) # number of results
print(res["data"]) # list of rows
```

#### Using the Schemas Client
You can use the Schemas client to work with the Astra Schemas API. [API Reference](https://docs.datastax.com/en/astra/docs/_attachments/restv2.html#tag/Schemas)
```python
# astra_client created above
# create a table
astra_client.schemas.create_table(keyspace=ASTRA_DB_KEYSPACE, table_definition={
    "name": "my_table",
    "columnDefinitions": [
        {
            "name": "firstname",
            "typeDefinition": "text"
        },
        {
            "name": "lastname",
            "typeDefinition": "text"
        },
        {
            "name": "favorite_color",
            "typeDefinition": "text",
        }
    ],
    "primaryKey": {
        "partitionKey": [
            "firstname"
        ],
        "clusteringKey": [
            "lastname"
        ]
    }
})
```


#### Using the Collections Client
You can use the Collections client to work with the Astra Document API. [API Reference](https://docs.datastax.com/en/astra/docs/_attachments/docv2.html)
```python
# astra_client created above
# create multiple documents using the collections API
my_collection = astra_client.namespace(ASTRA_DB_KEYSPACE).collection(COLLECTION_NAME)
my_collection.batch(documents=[
    {
        "first_name": "Dang",
        "last_name": "Son",
    }, {
        "first_name": "Yep",
        "last_name": "Boss",
    }])
```

#### Using the GraphQL Client
You can use the GraphQL client to work with the Astra GraphQL API. [API Reference](https://docs.datastax.com/en/astra/docs/using-the-astra-graphql-api.html)
```python
# astra_client created above
# create multiple documents using the GraphQL API
astra_client.gql.execute(keyspace=ASTRA_DB_KEYSPACE, query="""
        mutation insert2Books {
            moby: insertbook(value: {title:"Moby Dick", author:"Herman Melville"}) {
                value {
                    title
                }
            }
            catch22: insertbook(value: {title:"Catch-22", author:"Joseph Heller"}) {
                value {
                    title
                }
            }
        }
    """)
```

#### Using the HTTP Client
You can use the HTTP client to work with any Astra/Stargate endpoint directly. [API Reference](https://docs.datastax.com/en/astra/docs/api.html)
```python
# astra_client created above
# create a document on Astra using the Document API
astra_client._rest_client.request(
    method="PUT",
    path=f"/api/rest/v2/namespaces/my_namespace/collections/my_collection/user_1",
    json_data={
        "first_name": "Cliff",
        "last_name": "Wicklow",
        "emails": ["cliff.wicklow@example.com"],
    })
```
