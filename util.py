# Assuming you're using a Python backend
import dotml
import dotml.cli
import sqlalchemy
from sqlalchemy import create_engine
from openai import OpenAI
import json
import uuid
import random
import yaml

def load_json(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data

def generateUUid():
    return uuid.uuid4()

def generate_example_data(column_name, column_type):
    if column_name == "id":
        return "0013e9b8-7247-4b2b-b241-97c44401c12d"
    elif column_type == "integer":
        return random.randint(1, 100)
    elif column_type == "float":
        return round(random.uniform(1.0, 100.0), 2)
    elif column_type in ["text", "varchar", "character varying", "char"]:
        return "example_text"
    elif column_type == "boolean":
        return random.choice([True, False])
    elif column_type == "date":
        return "2024-07-16"
    elif column_type == "timestamp" or column_type == "timestamp with time zone" or column_type=="timestamp without time zone":
        return "2024-07-16 12:34:56"
    elif column_type == "json":
        return {"key": "test", "column_name": "demo", "example": "example_data"}
    elif column_type=="uuid":
        return "0a20449d-1691-431f-b63e-c11091184747"

    else:
        return "example_data"
    
async def generate_dotml(data):
    print(data)
    schema_info = {"cubes": []}
    for table in data:
        table_info = {
            "name": table["table_name"].replace('public.', ''),
            "table": table["table_name"].replace('public.', ''),
            # "relationships":table['relationships'],
            "dimensions": []
        }
        # if table['relationships']:
        #     table_info["relationships"]=table['relationships']

        for column in table["columns"]:
            dimension_info = {
                "name": column["name"].replace('public.', ''),
                "sql": f"${{{{table}}}}.{column['name']}",
                "type":column['type'],
                
            }
            if column["name"] == "id":
                dimension_info["primary_key"] = True           

            
            table_info["dimensions"].append(dimension_info)

        schema_info["cubes"].append(table_info)

    with open('schema_info1.json', 'w') as file:
        yaml.dump(schema_info, file,default_flow_style=False, sort_keys=False)
    return True