from fastapi import FastAPI, HTTPException,Response,Request
import psycopg2
from psycopg2 import OperationalError
from mysqldata import get_mysqltable_data
import json
import yaml
from flask import Flask, request, jsonify
from util import load_json,generateUUid,generate_example_data
from typing import Dict
from pydantic import BaseModel
# from prompt2query import promptToQuery,load_json_schema
from fastapi.middleware.cors import CORSMiddleware

# type: ignore[func-returns-value]
App = Flask(__name__)
app = FastAPI()

origins = [
    "*",
]


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# db_config = {
#     'dbname': 'test',
#     'user': 'postgres',
#     'password': 'XRUWjLGKz972xbupQStM3c',
#     'host': '43.204.128.202',
#     'port': '5432'
# }
class prompt(BaseModel):
    sentence: str
# @app.post('/query')
# async def queryFromPrompt(request:Request,response:Response,body:prompt):
#     schema = load_json_schema('schema_info.json')
#     print(body.sentence)
#     result=promptToQuery(body.sentence,schema)
#     return result
    


class tableScan(BaseModel):
    conn_str: str


# @app.get('/mysqldata')
# async def get_mysql():
#    await get_mysqltable_data()

@app.get("/gettable")
async def get_table_data(conn_str):
    try:
        conn = psycopg2.connect(conn_str)

        cursor = conn.cursor()

        schema_query = """
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog')
        """

        cursor.execute(schema_query)

        schemas = cursor.fetchall()

        schema_info = []

        for schema in schemas:
            schema_name = schema[0]

            table_query = f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{schema_name}'
            """

            cursor.execute(table_query)

            tables = cursor.fetchall()
            if len(tables)==0:
                table_info = {"table_name": schema_name, "columns": []}
                schema_info.append(table_info)
            else:
                for table in tables:
                    table_name = table[0]
                    full_table_name = f"{schema_name}.{table_name}"
                    table_info = {"schema": schema_name, "table_name": full_table_name, "columns": [], "relationships": []}

                    column_query = f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
                    """

                    cursor.execute(column_query)

                    columns = cursor.fetchall()

                    for column in columns:
                        column_name, data_type = column

                        table_info["columns"].append({"name": column_name, "type": data_type})

                    fk_query = f"""
                    SELECT
                        kcu.column_name,
                        ccu.table_schema AS referenced_schema,
                        ccu.table_name AS referenced_table,
                        ccu.column_name AS referenced_column
                    FROM
                        information_schema.key_column_usage kcu
                    JOIN
                        information_schema.constraint_column_usage ccu
                        ON kcu.constraint_name = ccu.constraint_name
                    WHERE
                        kcu.table_schema = '{schema_name}'
                        AND kcu.table_name = '{table_name}'
                        AND ccu.table_name IS NOT NULL
                    """

                    cursor.execute(fk_query)

                    relationships = cursor.fetchall()
                    for relationship in relationships:
                        column_name, referenced_schema, referenced_table, referenced_column = relationship
                        table_info["relationships"].append({
                            "column": column_name,
                            "referenced_schema": referenced_schema,
                            "referenced_table": referenced_table,
                            "referenced_column": referenced_column
                        })

                    schema_info.append(table_info)
        result=await generate_json_data(schema_info)
    
        return result
   
    except OperationalError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()


async def generate_json_data(data):
    schema_info = []

    for table in data:
        schema_name = table.get('schema')
        table_name = table["table_name"].replace(f'{schema_name}.', '')

        schema = next((s for s in schema_info if s["schema_name"] == schema_name), None)

        if not schema:
            schema = {"schema_name": schema_name, "tables": []}
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

    with open('schema_info.json', 'w') as file:
        json.dump(schema_info, file, indent=4)

    return schema_info








class createData(BaseModel):
    appid: str
    conn_str:str
    data_source_type:str
    db_url:str

@app.post('/adddata')
async def addData(request:Request,response:Response,body:createData):
    try:
        data = body.__dict__
        print(data['data_source_type'])

        if data['data_source_type'] == 'postgresql':
            scanDb = await get_table_data(data['conn_str'])
        elif data['data_source_type'] == 'mysql':
            scanDb = await get_mysqltable_data(data['conn_str'])
        else:
            return {'statusCode':400,'message':"Unsupported data source type"}

        if scanDb:
            conn = psycopg2.connect(data['db_url'])
            cur = conn.cursor()
            sql = """
            INSERT INTO data_table (id, table_name, schema_name, application_id, meta_data)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
            """

            schema_data = load_json('schema_info.json')
            for schema in schema_data:
                schema_name = schema.get('schema_name', 'public')
                tables = schema.get('tables', [])
                print(tables,'tttttttttt')
                for table in tables:
                    table_name = table.get('table_name', None)

                    if table_name:
                        tableid = generateUUid()
                        data_table_values = (
                            str(tableid),
                            table_name,
                            schema_name,
                            data['appid'],
                            json.dumps(table)
                        )
                        try:
                            cur.execute("SELECT * FROM data_table WHERE table_name = %s and application_id=%s", (table_name,data['appid']))
                            exData = cur.fetchone()
                            if exData is not None:
                                data_table_id = exData[0]
                            else:
                                cur.execute(sql, data_table_values)
                                conn.commit()
                                data_table_id = cur.fetchone()[0]
                        except psycopg2.errors.UniqueViolation:
                            print(f"Duplicate UUID detected: {data_table_id}")
                            conn.rollback()
                            continue
                        except Exception as e:
                            print(f"Error inserting into data_table: {e}")
                            conn.rollback()
                            continue

                        table_schema_sql = """
                        INSERT INTO table_schema (id, field_name, type, sample_data, data_table_id)
                        VALUES (%s, %s, %s, %s, %s)
                        """
                        for field_data in table.get('fields', []):
                            columnid = generateUUid()
                            example_data = json.dumps(field_data['example']) if isinstance(field_data['example'], dict) else field_data['example']
                            table_schema_values = (
                                str(columnid),
                                field_data['name'],
                                field_data['type'],
                                example_data,
                                data_table_id,
                            )
                            try:
                                cur.execute("SELECT * FROM table_schema WHERE field_name = %s AND data_table_id = %s", (field_data['name'], data_table_id))
                                exfieldData = cur.fetchone()
                                if exfieldData is None:
                                    cur.execute(table_schema_sql, table_schema_values)
                            except psycopg2.errors.UniqueViolation:
                                print(f"Duplicate UUID detected: {str(columnid)}")
                                conn.rollback()
                                continue
                            except Exception as e:
                                print(f"Error inserting into table_schema: {e}")
                                conn.rollback()
                                continue
            conn.commit()
            cur.close()
            conn.close()
            return {'statusCode': 200, "message": "Db data successfully scanned and stored in new database"}

    except OperationalError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
