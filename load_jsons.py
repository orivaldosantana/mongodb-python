import json 

with open('./dados/json_turmas.json', mode='r',  encoding=" UTF-8") as classes_file:
  classes_data = json.load(classes_file) 
  for c in classes_data:
    print(c['id_class']) 
    print(c) 