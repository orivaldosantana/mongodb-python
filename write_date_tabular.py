from dotenv import dotenv_values
import pymongo
from pymongo.server_api import ServerApi
import pandas as pd 


config = dotenv_values(".env")

client = pymongo.MongoClient(config['ATLAS_URI'], server_api=ServerApi('1'))
db = client['dataviewert1'] 


def convertToUTCDate(date):
  s = date.split('/')
  return '{}-{}-{}'.format(s[2],s[1],s[0]) 


def insertData(db, data, nameCollection,classCode):
  #linha = {"nota1":0, "comentario1":"", "nota1":0, "comentario1":"", "nota1":0, "comentario1":"", "matricula": "X" } 
  collections = db[nameCollection]

  tempLine = {}
  tempLine['classCode'] = classCode

  l, c =  data.shape
  classes = []
  print(classCode)
  for i in range(l):   
    tempClass = {}         
    #print(campo, ": ",dados.iloc[i][campo]) 
    tempClass['date'] = str( convertToUTCDate( data.iloc[i]['date'] ) )
    tempClass['classTitle'] = str( data.iloc[i]['classTitle'] )
    classes.append(tempClass)       
    print(tempClass)
  print(classCode)
  tempLine['classTitles'] = classes

  
  try: 
    #collections.insert_one(tempLinha)  
    collections.replace_one( {'classCode':  classCode }, tempLine, True)  
  except:
    print("Erro ao inserir no banco de dados!") 
  

classCode = "lop2023_2t01" 

dataClassDate =  pd.read_csv("./dados/{}/datas_aulas.csv".format(classCode)) 
print( dataClassDate.head() ) 
insertData(db,dataClassDate,'classclasses', classCode) 

