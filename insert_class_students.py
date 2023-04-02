from dotenv import dotenv_values
import pymongo
from pymongo.server_api import ServerApi
import json 

config = dotenv_values(".env")

client = pymongo.MongoClient(config['ATLAS_URI'], server_api=ServerApi('1'))
db = client['dataviewert1'] 

xCollections =  db['classstudents']



def onlyStudentId(s):
  return {'student_id': s['student_id']}

with open('./dados/alunos_pTurma_att.json', mode='r',  encoding=" UTF-8") as classes_file:
  classesData = json.load(classes_file) 

  cont = 1 
  for c in classesData:    
    print('\n - - - ', cont)
    print('Tamanho da turma: ',len(c['students']) ) 
    xClass = {"class_id":"xxx","students":[1,2,3]} 
    cont = cont + 1  
    xClass['class_id'] = c['class_id'] 
    xStudents = map( onlyStudentId, c['students'] ) 
    xClass['students'] = list(xStudents)
    #print(classes['students']) 
    print(xClass) 
    # Insere um 
    xCollections.insert_one(xClass) 
     