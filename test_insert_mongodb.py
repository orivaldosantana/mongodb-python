from dotenv import dotenv_values
import pymongo
from pymongo.server_api import ServerApi

config = dotenv_values(".env")

print(config['ATLAS_URI']) 

client = pymongo.MongoClient(config['ATLAS_URI'], server_api=ServerApi('1'))
db = client['test'] 

userCollections = db['users']

user1 = { "name": "John", "address": "Highway 38" } 

userCollections.insert_one(user1) 
