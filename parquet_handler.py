import pymongo
import pyarrow.parquet as pq
import os
#import pymongoarrow as pma
import pyarrow.csv as csv

from pymongoarrow.monkey import patch_all
patch_all()

destination_db_string = os.getenv('ATLAS_DB_CONN')


# Connect to MongoDB server and select the database and collection
client = pymongo.MongoClient(destination_db_string)
db = client["fleetReport"]
collection = db["Coinbase_-_Atlas"]

arrow_table = collection.find_arrow_all({})

for each in arrow_table:
    print(each)

#pq.write_table(arrow_table, "example.parquet", compression=None)
#csv.write_csv(arrow_table, "coinbase.csv")

