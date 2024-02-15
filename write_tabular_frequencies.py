import pandas as pd 
from dotenv import dotenv_values
import pymongo
from pymongo.server_api import ServerApi
import utils

config = dotenv_values(".env")

client = pymongo.MongoClient(config['ATLAS_URI'], server_api=ServerApi('1'))
dbDataviewer = client['dataviewert1'] 


# Função para inserir as datas de frequência de um estudante no banco de dados 
# Formato de cada linha obtida do csv para inserir no banco de dados
# {"regNum": "234243234", "classFreqs": [ "2021-10-10", "2021-10-11", "2021-10-12" ] }
def replaceOneFreqStudents(db, data, nameCollection, classCode):
    collections = db[nameCollection]
    l, c =  data.shape
    dataColumns = data.columns[1:]
    for s in range(l):
        tempLine = {} 
        #print(data.iloc[i])        
        #print(data.iloc[s][:].values)
        
        freqs = []
        #print("regNum"," ", data.iloc[s][0])
        tempLine['regNum'] = str( int(data.iloc[s][0]) )
        for i in range(len(dataColumns)):    
            if data.iloc[s][i+1] == 1:
                #print(dataColumns[i], ' ', data.iloc[s][i+1])
                freqs.append(dataColumns[i])
        #print(freqs)
        tempLine['classFreqs'] = freqs
        tempLine['classCode'] = classCode 
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
#replaceOneFreqStudents(dbDataviewer, dataFrec, 'studentfrequencies', classCode) 
  
#print(dataFrec.columns)

# Função para inserir ou atualizar as frequências de uma turma no banco de dados 
# Cada dia contem o somatário das frequências dos estudantes 
# O formato inserido no banco de dados contém a frequência de cada dia, o codigo da turma e a quantidade de estudantes da turma 
# { "classCode": "lop2023_2t01", "studentNumber": 23, "classFreqs": [ {"date": "2021-10-10", "freqs": 3 }, {"date": "2021-10-11", "freqs": 7 } ] }
def replaceOneFrequencyTotal(db, data, nameCollection, classCode):
    collections = db[nameCollection]
    l, c =  data.shape
    dataColumns = data.columns[1:]
    #print("Student number: ", l)
    tempLine = {}
    tempLine['classCode'] = classCode
    tempLine['studentNumber'] = l
    freqs = []
    # Cria um dicionário no qual cada dia é uma chave e o valor é a frequencia inicialmente 0  
    tempFreq = {}
    for i in range(c-1):        
        dateI = str(  dataColumns[i]  )
        #print(dateI)
        tempFreq[dateI] = 0
 
    # Preenche o dicionário com a soma das frequências de cada dia
    for j in range(l):
        for i in range(c-1):        
            dateI = str(  dataColumns[i]  )
            if data.iloc[j][i+1] == 1:
                tempFreq[dateI] = tempFreq[dateI] + 1
    tempLine['classFreqs'] = tempFreq
    print(tempLine)
    try: 
        print('\nGravando os dados de ', classCode) 
        result = collections.replace_one( {'classCode': classCode }, tempLine, True) 
        if result.modified_count == 0:
            print('Inserido!')
        else:
            print('Atualizado!')
    except:
        print("Erro ao inserir no banco de dados!")


replaceOneFrequencyTotal(dbDataviewer, dataFrec, 'classfrequencies', classCode) 