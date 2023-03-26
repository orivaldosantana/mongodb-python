from dotenv import dotenv_values
import pymongo
from pymongo.server_api import ServerApi

config = dotenv_values(".env")

print(config['ATLAS_URI']) 

client = pymongo.MongoClient(config['ATLAS_URI'], server_api=ServerApi('1'))
db = client['dataviewert1'] 
classesCollections = db['tclasses']



import json 

with open('./dados/json_turmas.json', mode='r',  encoding=" UTF-8") as classes_file:
  classesData = json.load(classes_file) 
  classesCollections.insert_many(classesData)  
  for c in classesData:
    print('\n - - - ') 
    print(c) 
