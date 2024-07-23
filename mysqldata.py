from fastapi import FastAPI, HTTPException, Request, Response
import pymysql
import yaml
import dotml
import json
import random
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from util import get_description
from querys import mysqlGetTable, mysqlGetColumns, mysqlGetRelations

from pydantic import BaseModel

app = FastAPI()

db_config = {
    "database": "sleep",
    "user": "root",
    "password": "password",
    "host": "10.10.10.162",
    "port": 3306,
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
    elif (
        column_type == "timestamp"
        or column_type == "timestamp with time zone"
        or column_type == "timestamp without time zone"
    ):
        return "2024-07-16 12:34:56"
    elif column_type == "json":
        return {"key": "test", "column_name": "demo", "example": "example_data"}
    elif column_type == "uuid":
        return "0a20449d-1691-431f-b63e-c11091184747"

    else:
        return "example_data"


async def get_mysqltable_data(conn_str):
    try:
        engine = create_engine(conn_str)

        Session = sessionmaker(bind=engine)
        session = Session()

        table_query = text(mysqlGetTable)

        result = session.execute(table_query, {"database": db_config["database"]})
        tables = result.fetchall()

        table_info_list = []

        for table in tables:
            table_name = table[0]
            table_info = {
                "table_name": table_name,
                "columns": [],
                "relationships": [],
                "example_data": {},
            }

            column_query = text(mysqlGetColumns)

            result = session.execute(
                column_query,
                {"database": db_config["database"], "table_name": table_name},
            )
            columns = result.fetchall()

            for column in columns:
                column_name, data_type, column_type = column

                if data_type == "enum":
                    enum_values = (
                        column_type.replace("enum(", "")
                        .replace(")", "")
                        .replace("'", "")
                        .split(",")
                    )
                    enum_values = [val.strip() for val in enum_values]
                    table_info["example_data"][column_name] = enum_values

                description = get_description(column_name, table_name)

                table_info["columns"].append(
                    {"name": column_name, "type": data_type, "description": description}
                )

            relationship_query = text(mysqlGetRelations)

            result = session.execute(
                relationship_query,
                {"database": db_config["database"], "table_name": table_name},
            )
            relationships = result.fetchall()

            for relationship in relationships:
                column_name, referenced_table, referenced_column = relationship

                table_info["relationships"].append(
                    {
                        "column": column_name,
                        "referenced_table": referenced_table,
                        "referenced_column": referenced_column,
                    }
                )

            latest_data_query = text(
                f"""
                     
                     SELECT DISTINCT *
                FROM {db_config['database']}.{table_name}
                
                LIMIT 3
                                """
            )
            result = session.execute(latest_data_query)
            latest_data = result.fetchall()

            column_examples = {col["name"]: [] for col in table_info["columns"]}

            for entry in latest_data:

                for idx, column in enumerate(table_info["columns"]):
                    column_name = column["name"]

                    value = entry[idx]
                    column_examples[column_name].append(
                        str(value) if value is not None else None
                    )

            for column_name in column_examples:
                column_examples[column_name] = ", ".join(
                    map(str, column_examples[column_name][:3])
                )

            for column_name in table_info["example_data"]:
                column_examples[column_name] = ", ".join(
                    map(str, table_info["example_data"][column_name])
                )

            table_info["example_data"] = column_examples

            table_info_list.append(table_info)

        session.close()

        result = await generate_json_data(table_info_list)
        return table_info_list

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


async def generate_json_data(data):
    try:

        schema_info = []

        for table in data:
            table_name = table["table_name"]

            schema = {"tables": []}
            schema_info.append(schema)

            table_info = {
                "table_name": table_name,
                "fields": [],
                "description": "",
                "relationships": table.get("relationships", []),
            }
            example_data = table.get("example_data", {})
            for column in table["columns"]:
                field_info = {
                    "name": column["name"],
                    "type": column["type"],
                    "description": column.get(
                        "description", "No description available."
                    ),
                    "example": example_data.get(column["name"], ["none"]),
                }
                table_info["fields"].append(field_info)

            schema["tables"].append(table_info)

        with open("schema_info_mysql.json", "w") as file:
            json.dump(schema_info, file, indent=4)

        return schema_info
    except Exception as e:
        return e


class Item(BaseModel):
    query: str


# async def handle_query(request:Request,response:Response,body:Item):
#     sql_query = promptToQuery("show me the list of collection data",schema)
#     return sql_query
