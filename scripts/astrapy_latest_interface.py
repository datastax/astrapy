import os
import sys

import astrapy

from dotenv import load_dotenv


sys.path.append("../")

load_dotenv()


# Grab the Astra token and api endpoint from the environment
token = os.environ["ASTRA_DB_APPLICATION_TOKEN"]
api_endpoint = os.environ["ASTRA_DB_API_ENDPOINT"]

# Initialize our vector db
my_client = astrapy.DataAPIClient(token)
my_database = my_client.get_database_by_api_endpoint(api_endpoint)

# In case we already have the collection, let's clear it out
my_database.drop_collection("collection_test")

# Create a new test collection for example
my_collection = my_database.create_collection("collection_test", dimension=5)

# Insert a document into the test collection
my_collection.insert_one(
    {
        "_id": "1",
        "name": "Coded Cleats Copy",
        "description": "ChatGPT integrated sneakers that talk to you",
    },
    vector=[0.25, 0.25, 0.25, 0.25, 0.25],
)

cursor = my_collection.find(
    {},
    vector=[0, 0.2, 0.4, 0.6, 0.8],
    limit=2,
    include_similarity=True,
)

for result in cursor:
    print(f"{result['name']}: {result['$similarity']}")
