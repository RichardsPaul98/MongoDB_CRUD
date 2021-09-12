import json
from pymongo import MongoClient


# Making Connection
myclient = MongoClient("mongodb://localhost:27017/")

# database
db = myclient["mongoDBCRUD"]

# Created or Switched to collection
# names: assignment
Collection = db["dataforcrud"]

with open('Data_1.json') as file:
	file_data = json.load(file)
	
# Inserting the loaded data in the Collection
# if JSON contains data more than one entry
# insert_many is used else inser_one is used
if isinstance(file_data, list):
	Collection.insert_many(file_data)
else:
	Collection.insert_one(file_data)
