from dotenv import dotenv_values
import pymongo
from pymongo.server_api import ServerApi
import json 

config = dotenv_values(".env")

client = pymongo.MongoClient(config['ATLAS_URI'], server_api=ServerApi('1'))
db = client['dataviewert1'] 

xCollections =  db['students']


with open('./dados/desempenhoPercentAlunoXTurma.json', mode='r',  encoding=" UTF-8") as classes_file:
  classesData = json.load(classes_file) 

  cont = 1 
  for classes in classesData:    
    print('\n - - - ', cont)
    cont = cont + 1  
    #print(classes['students'])  
    for s in classes['students']:
      print("Updating: ",s['student_id'], " with precent:", s['percent']) 
      xCollections.update_one({"id":s['student_id']}, {"$set": {"percent":s['percent']}}) 
      