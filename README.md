**Function to load MangoDB**
# I have loaded the data into delta table using databricks, i dont have access to mangoDB but fallowing syntax is used to load into MangoDB
from pymongo import MongoClient
def load_data_to_mongodb(data, db_name='mydb', collection_name='people'):
    client = MongoClient('mongodb://10.148.190.206:27017/')
    db = dev_greater_sydney_division_bronze
    collection = db[midt]
    collection.insert_many(data)

**Saving to JSON**
import json
def save_to_json(data, file_name='People.json'):
    with open(file_name, 'w') as f:
        json.dump(data, f, indent=4)
