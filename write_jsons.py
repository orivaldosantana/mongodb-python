from dotenv import dotenv_values
import pymongo
from pymongo.server_api import ServerApi
import json 

config = dotenv_values(".env")

client = pymongo.MongoClient(config['ATLAS_URI'], server_api=ServerApi('1'))
db = client['dataviewert1'] 


def insertManyJsons(db, nameJsonFile, nameCollection):
  collections = db[nameCollection]
  with open(nameJsonFile, mode='r',  encoding=" UTF-8") as json_file:
    collectionsData = json.load(json_file) 
    try: 
      collections.insert_many(collectionsData)  
      cont = 1
      for c in collectionsData:
        print('\n - - - ',cont) 
        print(c) 
        cont = cont + 1 
    except:
      print("Erro ao inserir no banco de dados!")  


#insertManyJsons(db,'./dados/prof_turmas.json','teacherclasses') 
#insertManyJsons(db,'./dados/desempenhoTurmaXassuntoXlista.json','listsubjectclasses') 
#insertManyJsons(db,'./dados/desempenhoTurmaXlista.json','classlists') 
#insertManyJsons(db,'./dados/desempenhoTurmaXdificuldadeXlista.json','classdifficulties') 
insertManyJsons(db,'./dados/users_students_2.json','students') 