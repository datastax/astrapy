
import warnings
import os

from astrapy.operations import (
    AsyncInsertOne,
    AsyncInsertMany,
    AsyncUpdateOne,
    AsyncUpdateMany,
    AsyncReplaceOne,
    AsyncDeleteOne,
    AsyncDeleteMany,
)

warnings.simplefilter('default', DeprecationWarning)

from astrapy import DataAPIClient
db=DataAPIClient(os.environ['ASTRA_DB_APPLICATION_TOKEN']).get_database(os.environ['ASTRA_DB_API_ENDPOINT'])

# col0 = db.create_collection('t', dimension=2, check_exists=False)
col = db.t

op1 = AsyncInsertOne({"a":1}, vector=[10, 11])
op2 = AsyncInsertMany([{"a":1}], vectors=[[10, 11]])
op3 = AsyncUpdateOne({"a":1}, {"$set": {"a": 2}}, vector=[10, 11])
op4 = AsyncReplaceOne({"a":1}, {"a": 2}, vector=[10, 11])
op5 = AsyncDeleteOne({}, vector=[10, 11])

print("\n".join(["*" * 20] * 5))

import asyncio
res = asyncio.run(col.to_async().bulk_write([op1, op2, op3, op4, op5]))

print("\n\n", res)