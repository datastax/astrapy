import os
import sys

from dotenv import load_dotenv

from astrapy.base import AstraClient
from astrapy.vector import AstraVectorClient
from astrapy.ops import AstraOps

sys.path.append("../")

load_dotenv()

# First, we work with devops
token = os.getenv("ASTRA_DB_APPLICATION_TOKEN", None)
astra_ops = AstraOps(token)

# Define a database to create
database_definition = {
    "name": "vector_test",
    "tier": "serverless",
    "cloudProvider": "GCP",
    "keyspace": os.getenv("ASTRA_DB_KEYSPACE", "default_namespace"),
    "region": os.getenv("ASTRA_DB_REGION", None),
    "capacityUnits": 1,
    "user": "token",
    "password": token,
    "dbType": "vector",
}

# Create the database
create_result = astra_ops.create_database(database_definition=database_definition)

# Grab the new information from the database
db_id = create_result["id"]

# Initialize our AstraClient
astra_client = AstraClient(db_id=db_id, token=token)

## Possible Initializations
astra_client_vectordb1 = astra_client.vector_database()
astra_client_vectordb2 = AstraVectorClient(db_id=db_id, token=token)

## Possible Operations
astra_client_vectordb1.create_vector_collection(name="collection_test", size=5)
astra_client_vectordb1.delete_collection(name="collection_test")

astra_client_vectordb2.create_vector_collection(name="collection_test", size=5)
astra_client_vectordb2.delete_collection(name="collection_test")
