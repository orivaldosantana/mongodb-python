from dotenv import dotenv_values
import pymongo
from pymongo.server_api import ServerApi
import pandas as pd 
import numpy as np 

config = dotenv_values(".env")

client = pymongo.MongoClient(config['ATLAS_URI'], server_api=ServerApi('1'))
db = client['dataviewert1'] 

 
def replaceOneStudentClass(db, data, nameCollection,classCode):
  #linha = {"reg":0, "name":"xxxx", "sub_class":"ax"} 
  collections = db[nameCollection]

  l, c =  data.shape
  studentArray = np.array([]) 
  regArray = np.array([])
  tempLinha = {}
  for i in range(l): 
    tempStudent = {}  
    for campo in data.columns:
      if campo == 'Nome':
        tempStudent['name'] = str( data.iloc[i][campo] )  
      if campo == 'Matrícula':
        tempStudent['reg_num'] = str( int(data.iloc[i][campo]) ) 
        regArray = np.append(regArray, str( int(data.iloc[i][campo]) ) )
      if campo == 'sub_turma':
        tempStudent['sub_class'] = str( data.iloc[i][campo] ) 
    studentArray = np.append(studentArray, tempStudent)   
  #print(studentArray)  
  #print(regArray) 
  tempLinha['reg_students'] = list( regArray )
  tempLinha['students'] = list( studentArray )
  tempLinha['class_code'] = classCode
  print(tempLinha)
  try: 
    print('\nGravando os dados de ', classCode) 
    #result = collections.replace_one( {'class_code': classCode }, {'class_code':classCode, 'teste': '3', 'reg_students': list( regArray)}, True)   
    result = collections.replace_one( {'class_code': classCode }, tempLinha, True)   
    
    if result.modified_count == 0:
      print('Inserido!') 
    else: 
      print('Atualizado!') 
  except:
    print("Erro ao inserir no banco de dados!") 
   


classCode = "lop2023_2t01" 



dataStudents =  pd.read_csv("./dados/{}/alunos.csv".format(classCode)) 
print( dataStudents.head() )
# Esse código cadastra no banco de dados dataviewer (teste) os estudantes de uma turma 
# Caso já exista estudantes da turma cadastrados, estes serão atualizadas 

replaceOneStudentClass(db,dataStudents, 'classstudents', classCode) 



 