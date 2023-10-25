import os
import sys

from dotenv import load_dotenv

from astrapy.db import AstraDB, AstraDBCollection
from astrapy.ops import AstraDBOps

sys.path.append("../")

load_dotenv()

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
