from dotenv import dotenv_values
import pymongo
from pymongo.server_api import ServerApi
import pandas as pd 
import numpy as np 


config = dotenv_values(".env")

client = pymongo.MongoClient(config['ATLAS_URI'], server_api=ServerApi('1'))
db = client['dataviewert1'] 

 



def replaceOneListGrades(db, data, nameCollection,classCode,lu1, lu2, lu3):
  #linha = {"nota1":0, "comentario1":"", "nota1":0, "comentario1":"", "nota1":0, "comentario1":"", "matricula": "X" } 
  collections = db[nameCollection]

  l, c =  data.shape
  for i in range(l): 
    tempLinha = {}
    listArray = np.array([]) 
    for campo in data.columns:
      if campo != 'Nome':
        #print(campo, ": ",dados.iloc[i][campo])
        if  campo == 'Matrícula':
          tempLinha['reg_num'] = str( int(data.iloc[i][campo]) ) 
        else: 
          listArray = np.append(listArray, {'description': campo, 'percent': str( data.iloc[i][campo] ) } )
          #tempLinha[campo] = str( data.iloc[i][campo] ) 
    tempLinha['classCode'] =  classCode 
    tempLinha['lists'] =  list( listArray )
    #print(tempLinha)
    tempLinha['meanU1'] =  "{:.2f}".format( (data.iloc[i,lu1].mean()) /10 )
    tempLinha['meanU2'] =  "{:.2f}".format( (data.iloc[i,lu2].mean()) /10 )
    tempLinha['meanU3'] =  "{:.2f}".format( (data.iloc[i,lu3].mean()) /10 ) 

 
    try: 
      print('\nGravando os dados de ',data.iloc[i,0] ) 
      result = collections.replace_one( {'reg_num': tempLinha['reg_num'] }, tempLinha, True)   
      if result.modified_count == 0:
        print('Inserido!') 
      else: 
        print('Atualizado!') 
      print(tempLinha) 
    except:
      print("Erro ao inserir no banco de dados!") 
      break 

def showColumns(nome_arq_notas_dados, lu1, lu2, lu3):  
  try:
    notas_dados = pd.read_csv("./dados/{}".format(nome_arq_notas_dados) )
    cont = 0
    for l in notas_dados.columns:
      print(cont,l)
      cont = cont + 1  
    print("\nListas da Unidade 1:")
    for i in lu1:
      print(" - ",notas_dados.columns[i])
    print("\nListas da Unidade 2:")
    for i in lu2:
      print(" - ",notas_dados.columns[i])      
    print("\nListas da Unidade 3:")
    for i in lu3:
      print(" - ",notas_dados.columns[i])
  except FileNotFoundError:
    msg = "O arquivo './dados/{}' não existe.".format(nome_arq_notas_dados)
    print(msg)


classCode = "lop2023_2t01" 

#data =  pd.read_csv("./dados/lop2023_1t02/provas.csv",sep=";") 
#print( data.head() )
#insertData(db,data,'examgrades', classCode) 

dataLists =  pd.read_csv("./dados/{}/listas.csv".format(classCode)) 
print( dataLists.head() )
listU1 = [2,3,4,5,6,7]
listU2 = [16,11,8,9,12,10]
listU3 = [13,14,15]
# Esse código cadastra no banco de dados dataviewer (teste) os resultados das listas de exercícios 
# Caso já exista notas cadastradas, estas notas serão atualizadas 
# É importante checar se as colunas do csv gerado no lop estão coerentes. 
# Cadas lista em sua respectiva unidade 
showColumns("{}/listas.csv".format(classCode),listU1,listU2,listU3)
replaceOneListGrades(db,dataLists, 'studentlistgrades', classCode,listU1,listU2,listU3) 



 