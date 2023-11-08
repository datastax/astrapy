import os
import sys

from dotenv import load_dotenv

from astrapy.db import AstraDB


sys.path.append("../")

load_dotenv()


# Grab the Astra token and api endpoint from the environment
token = os.getenv("ASTRA_DB_APPLICATION_TOKEN")
api_endpoint = os.getenv("ASTRA_DB_API_ENDPOINT")

# Initialize our vector db
astra_db = AstraDB(token=token, api_endpoint=api_endpoint)

# Create a new test collection for example
astra_db_collection = astra_db.create_collection(
    collection_name="collection_test", dimension=5
)

# Insert a document into the test collection
astra_db_collection.insert_one(
    {
        "_id": "1",
        "name": "Coded Cleats Copy",
        "description": "ChatGPT integrated sneakers that talk to you",
        "$vector": [0.25, 0.25, 0.25, 0.25, 0.25],
    }
)

# Perform a couple find operations
astra_db_collection.find_one({"name": "potato"})  # Not found
astra_db_collection.find_one({"name": "Coded Cleats Copy"})
