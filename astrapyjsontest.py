from astrapy.serverless import AstraCollection, AstraJsonClient
from astrapy.vector import AstraVectorClient
from astrapy.base import AstraClient
import uuid

# import astrapyjson

# from astrapyjson import astra_vector_client, astra_serverless_client
from dotenv import load_dotenv
import os, json

import http

http.client.HTTPConnection.debuglevel = 1
cliffu = str(uuid.uuid4())
load_dotenv()

astra_client = AstraClient(
    astra_database_id=os.environ["ASTRA_DB_ID"],
    astra_database_region=os.environ["ASTRA_DB_REGION"],
    astra_application_token=os.environ["ASTRA_DB_APPLICATION_TOKEN"],
)
json_client = AstraJsonClient(astra_client=astra_client)
test_collection = json_client.namespace("vector").collection("test")

# Create a document
json_query = {
    "_id": cliffu,
    "first_name": "Cliff",
    "last_name": "Wicklow",
}
test_collection.create(document=json_query)

# Check the document
document = test_collection.find_one(filter={"_id": cliffu})
print(document)
# Update a document with a subdocument
document = test_collection.update_one(
    filter={"_id": cliffu},
    update={"$set": {"addresses.city": "New York", "addresses.state": "NY"}},
)
print(document)

# Check the document
document = test_collection.find_one(filter={"_id": cliffu})
print(document)

# Replace the document
test_collection.find_one_and_replace(
    filter={"_id": cliffu},
    replacement={
        "_id": cliffu,
        "addresses": {
            "work": {
                "city": "New York",
                "state": "NY",
            }
        },
    },
)

# Check the document
document = test_collection.find_one(
    filter={"_id": cliffu}, projection={"addresses.work.city": 1}
)
print(document)
