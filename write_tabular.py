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
      collections.insert_one(tempLinha)  
    except:
      print("Erro ao inserir no banco de dados!") 

classCode = "lop2023_1t02" 

data =  pd.read_csv("./dados/lop2023_1t02/provas.csv",sep=";") 
print( data.head() )
insertData(db,data,'examgrades', classCode) 

#dataPresence =  pd.read_csv("./dados/lop2023_1t02/presenca.csv") 
#print( dataPresence.head() )
#insertData(db,dataPresence, 'presences', classCode) 



 