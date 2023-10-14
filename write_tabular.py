from dotenv import dotenv_values
import pymongo
from pymongo.server_api import ServerApi
import pandas as pd 


config = dotenv_values(".env")

client = pymongo.MongoClient(config['ATLAS_URI'], server_api=ServerApi('1'))
db = client['dataviewert1'] 

 

def insertData(db, data, nameCollection,classCode):
  #linha = {"nota1":0, "comentario1":"", "nota1":0, "comentario1":"", "nota1":0, "comentario1":"", "matricula": "X" } 
  collections = db[nameCollection]

  l, c =  data.shape
  for i in range(l): 
    tempLinha = {}
    for campo in data.columns:
      #print(campo, ": ",dados.iloc[i][campo])
      if  campo == 'matricula':
        tempLinha[campo] = str( int(data.iloc[i][campo]) ) 
      else: 
        tempLinha[campo] = str( data.iloc[i][campo] ) 
    tempLinha["codigoTurma"] =  classCode 
    print(tempLinha)
    try: 
      #collections.insert_one(tempLinha)  
      collections.replace_one( {'matricula': tempLinha['matricula'] }, tempLinha, True)  
    except:
      print("Erro ao inserir no banco de dados!") 

#classCode = "lop2023_1t02" 
classCode = "lop2023_2t01" 

dataExam =  pd.read_csv("./dados/{}/provas.csv".format(classCode)) 
print( dataExam.head() )
#insertData(db,dataExam,'examgrades', classCode) 

dataPresence =  pd.read_csv("./dados/{}/presenca.csv".format(classCode)) 
print( dataPresence.head() )
insertData(db,dataPresence, 'studentparticipations', classCode) 



 