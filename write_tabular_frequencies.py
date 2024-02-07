import pandas as pd 
from dotenv import dotenv_values
import pymongo
from pymongo.server_api import ServerApi

config = dotenv_values(".env")

client = pymongo.MongoClient(config['ATLAS_URI'], server_api=ServerApi('1'))
dbDataviewer = client['dataviewert1'] 

 

# Formato de cada linha obtida do csv para inserir no banco de dados
# {"regNum": "234243234", classFreqs: [ "2021-10-10", "2021-10-11", "2021-10-12" ] }
def replaceOneFrecStudents(db, data, nameCollection):
    collections = db[nameCollection]
    l, c =  data.shape
    for s in range(l):
        tempLine = {} 
        #print(data.iloc[i])        
        #print(data.iloc[s][:].values)
        dataColumns = data.columns[1:]
        freqs = []
        #print("regNum"," ", data.iloc[s][0])
        tempLine['regNum'] = str( int(data.iloc[s][0]) )
        for i in range(len(dataColumns)):    
            if data.iloc[s][i+1] == 1:
                #print(dataColumns[i], ' ', data.iloc[s][i+1])
                freqs.append(dataColumns[i])
        #print(freqs)
        tempLine['classFreqs'] = freqs
        print(tempLine) 
        try: 
            print('\nGravando os dados do estudante ', tempLine['regNum']) 
            result = collections.replace_one( {'regNum': tempLine['regNum'] }, tempLine, True) 
            if result.modified_count == 0:
                print('Inserido!')
            else:
                print('Atualizado!')
        except:
            print("Erro ao inserir no banco de dados!")
    

classCode = "lop2023_2t01" 
dataFrec =  pd.read_csv("./dados/{}/presenca.csv".format(classCode)) 
replaceOneFrecStudents(dbDataviewer, dataFrec, 'studentsfrequencies') 
 
print(dataFrec.columns)


