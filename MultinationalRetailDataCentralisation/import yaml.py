import yaml 

with open('../db_creds.yaml') as f:
    data = yaml.safe_lod(f)

print (data)