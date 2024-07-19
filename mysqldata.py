from fastapi import FastAPI, HTTPException,Request,Response
import pymysql
import yaml
import dotml
import json
import random
# from prompt2query import promptToQuery,load_json_schema

from pydantic import BaseModel

app = FastAPI()

db_config = {
    'database': 'sleep',
    'user': 'root',
    'password': 'password',
    'host': '10.10.10.162',
    'port': 3306
}
def generate_example_data(column_name, column_type):
    if column_name == "id":
        return random.randint(1, 100)
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
    

async def get_mysqltable_data(conn_str):
    try:
        conn = pymysql.connect(conn_str)
        cursor = conn.cursor()

        table_query = f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '{db_config['database']}'
        """

        cursor.execute(table_query)
        tables = cursor.fetchall()

        table_info_list = []

        for table in tables:
            table_name = table[0]
            table_info = {"table_name": table_name, "columns": [],"relationships":[]}

            column_query = f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = '{db_config['database']}' AND table_name = '{table_name}'
            """

            cursor.execute(column_query)
            columns = cursor.fetchall()

            for column in columns:
                column_name, data_type = column
                table_info["columns"].append({"name": column_name, "type": data_type})
            relationship_query = f"""
            SELECT 
                kcu.COLUMN_NAME AS column_name, 
                kcu.REFERENCED_TABLE_NAME AS referenced_table, 
                kcu.REFERENCED_COLUMN_NAME AS referenced_column
            FROM 
                information_schema.KEY_COLUMN_USAGE kcu
            WHERE 
                kcu.TABLE_SCHEMA = '{db_config['database']}' 
                AND kcu.TABLE_NAME = '{table_name}'
                AND kcu.REFERENCED_TABLE_NAME IS NOT NULL;
            """         


            cursor.execute(relationship_query)
            relationships = cursor.fetchall()
            for relationship in relationships:
                column_name, referenced_table, referenced_column = relationship
                
                table_info["relationships"].append({
                    "column": column_name,
                    "referenced_table": referenced_table,
                    "referenced_column": referenced_column
                })


            table_info_list.append(table_info)
            

        result = await generate_json_data(table_info_list)
        return result

    except pymysql.MySQLError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()



    
async def generate_json_data(data):
    try:

        schema_info = []

        for table in data:
            print(table,'.,.,.')
            table_name = table["table_name"]

            
            schema = { "tables": []}
            schema_info.append(schema)

            table_info = {

                "table_name": table_name,
                "fields": [],
                "relationships": table.get("relationships", [])
            }

            for column in table["columns"]:
                field_info = {
                    "name": column["name"],
                    "type": column["type"],
                    "example": generate_example_data(column['name'],column["type"])
                }
                table_info["fields"].append(field_info)

            schema["tables"].append(table_info)

        with open('schema_info_mysql.json', 'w') as file:
            json.dump(schema_info, file, indent=4)


        return schema_info
    except Exception as e:
        return e


class Item(BaseModel):
    query: str
  

# async def handle_query(request:Request,response:Response,body:Item):
#     sql_query = promptToQuery("show me the list of collection data",schema)
#     return sql_query