import os
import sys

from dotenv import load_dotenv

from astrapy.db import AstraDB


sys.path.append("../")

load_dotenv()


# Grab the Astra token and api endpoint from the environment
token = os.environ["ASTRA_DB_APPLICATION_TOKEN"]
api_endpoint = os.environ["ASTRA_DB_API_ENDPOINT"]

# Initialize our vector db
astra_db = AstraDB(token=token, api_endpoint=api_endpoint)

# In case we already have the collection, let's clear it out
astra_db.delete_collection("collection_test")

# Create a new test collection for example
astra_db_collection = astra_db.create_collection("collection_test", dimension=5)

# Insert a document into the test collection
astra_db_collection.insert_one(
    {
        "_id": "1",
        "name": "Coded Cleats Copy",
        "description": "ChatGPT integrated sneakers that talk to you",
        "$vector": [0.25, 0.25, 0.25, 0.25, 0.25],
    }
)

# Perform a few vector find operations
astra_db_collection.vector_find([0.1, 0.1, 0.2, 0.5, 1], limit=3)

astra_db_collection.vector_find(
    [0.1, 0.1, 0.2, 0.5, 1], limit=3, filter={"name": "Coded Cleats Copy"}
)

astra_db_collection.vector_find(
    [0.1, 0.1, 0.2, 0.5, 1],
    limit=3,
    fields=["_id", "name"],
    include_similarity=False,
)
