from typing import Dict
from fastapi import FastAPI, HTTPException, Response, Request
import psycopg2
from psycopg2 import OperationalError
from mysqldata import get_mysqltable_data
import json
from flask import Flask
from util import get_description,scanDbData,generate_json_data
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import os
from haystack import Pipeline, PredefinedPipeline
from querys import pgGetSchema,pgGetTable,pgGetColumn,pgGetRelationship,latestSampleData
from  mssql import fetch_mssql_schema_data

# type: ignore[func-returns-value]
App = Flask(__name__)
app = FastAPI()

origins = [
    "*",
]

@app.post('/query')
async def queryFetch(request:Request,response:Response,body:Dict):
    # os.environ["OPENAI_API_KEY"] = "Your OpenAI API Key"

    pipeline = Pipeline.from_template(PredefinedPipeline.CHAT_WITH_WEBSITE)
    result = pipeline.run({
        "fetcher": {"urls": ["postgresql://postgres:XRUWjLGKz972xbupQStM3c@43.204.128.202:5432/test"]},
        "prompt": {"query": "how many tables in this db"}}
    )
    print(result["llm"]["replies"][0],'...........')

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["*"],
    allow_headers=["*"],
)


class prompt(BaseModel):
    sentence: str


class tableScan(BaseModel):
    conn_str: str


@app.get("/gettable")
async def get_table_data(conn_str):
    try:
        conn = psycopg2.connect(conn_str)
        cursor = conn.cursor()
        schema_query = pgGetSchema
        cursor.execute(schema_query)
        schemas = cursor.fetchall()
        schema_info = []

        for schema in schemas:
            schema_name = schema[0]
            table_query = pgGetTable.format(schema_name=schema_name)
            cursor.execute(table_query)
            tables = cursor.fetchall()

            if len(tables) == 0:
                table_info = {"table_name": schema_name, "columns": []}
                schema_info.append(table_info)
            else:
                for table in tables:
                    table_name = table[0]
                    full_table_name = f"{schema_name}.{table_name}"
                    table_info = {
                        "schema": schema_name,
                        "table_name": full_table_name,
                        "columns": [],
                        "relationships": [],
                    }

                    column_query =pgGetColumn.format(schema_name=schema_name,table_name=table_name)

                    cursor.execute(column_query)
                    columns = cursor.fetchall()

                    for column in columns:
                        column_name, data_type= column
                        description = get_description(column_name, table_name)
                        table_info["columns"].append(
                            {
                                "name": column_name,
                                "type": data_type,
                                "description": description,
                            }
                        )

                    fk_query = pgGetRelationship.format(schema_name=schema_name,table_name=table_name)

                    cursor.execute(fk_query)
                    relationships = cursor.fetchall()
                    for relationship in relationships:
                        (
                            column_name,
                            referenced_schema,
                            referenced_table,
                            referenced_column,
                        ) = relationship
                        table_info["relationships"].append(
                            {
                                "column": column_name,
                                "referenced_schema": referenced_schema,
                                "referenced_table": referenced_table,
                                "referenced_column": referenced_column,
                            }
                        )

                    latest_data_query = latestSampleData.format(schema_name=schema_name,table_name=table_name)
                    cursor.execute(latest_data_query)
                    latest_data = cursor.fetchall()

                    column_examples = {col["name"]: [] for col in table_info["columns"]}
                    for entry in latest_data:
                        if not isinstance(entry, tuple):
                            continue

                        for idx, column in enumerate(table_info["columns"]):
                            column_name = column["name"]
                            try:
                                value = entry[idx]
                                if isinstance(value, (int, float, str, bool)):
                                    column_examples[column_name].append(value)
                                elif isinstance(value, datetime):
                                    column_examples[column_name].append(
                                        value.isoformat()
                                    )
                                else:
                                    column_examples[column_name].append(str(value))
                            except IndexError:
                                column_examples[column_name].append(None)

                    for column_name in column_examples:
                        column_examples[column_name] = ", ".join(
                            map(str, column_examples[column_name])
                        )
                    table_info["example_data"] = column_examples

                    schema_info.append(table_info)

        result = await generate_json_data(schema_info)
        return result

    except OperationalError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    finally:
        if "conn" in locals():
            cursor.close()
            conn.close()




class createData(BaseModel):
    appid: str
    conn_str: str
    data_source_type: str
    db_url: str


@app.post("/adddata")
async def addData(request: Request, response: Response, body: createData):
    try:
        data = body.__dict__

        if data["data_source_type"] == "postgresql":
            scanDb = await get_table_data(data["conn_str"])
        elif data["data_source_type"] == "mysql":
            scanDb = await get_mysqltable_data(data["conn_str"])
        elif data["data_source_type"] == "mssql":
            scanDb = await fetch_mssql_schema_data(data["conn_str"])
        else:
            return {"statusCode": 400, "message": "Unsupported data source type"}

        if scanDb:
            result=await scanDbData(data)
           
            if result:
                
                return {
                        "statusCode": 200,
                        "message": "Db data successfully scanned and stored in new database",
                    }
            else:
                return {
                    "statusCode": 400,
                    "message": "Db data failed to scan",
                }

    except OperationalError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


if __name__ == "__app__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
