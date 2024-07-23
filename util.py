# Assuming you're using a Python backend
import dotml
import dotml.cli
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
import openai
import json
import uuid
import random
import yaml
from querys import addDataTableQuery,getDataTable,addTableSchemaQuery,getTableSchema,updateTableSchema

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

def get_description(column_name, table_name):
    return f"This is the '{column_name}' column of the '{table_name}' table."

async def scanDbData(dbdata):
    try:
            print(dbdata['db_url'])
            data=dbdata
            conn = psycopg2.connect(dbdata["db_url"])
            cur = conn.cursor()
            sql =addDataTableQuery

            schema_data = load_json("schema_info_mssql.json")
            for schema in schema_data:
                schema_name = schema.get("schema_name", "public")
                tables = schema.get("tables", [])
                for table in tables:
                    table_name = table.get("table_name", None)

                    if table_name:
                        tableid = generateUUid()
                        data_table_values = (
                            str(tableid),
                            table_name,
                            schema_name,
                            data["appid"],
                            json.dumps(table),
                        )
                        try:
                            cur.execute(
                                getDataTable,
                                (table_name, data["appid"]),
                            )
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

                        table_schema_sql = addTableSchemaQuery
                        for field_data in table.get("fields", []):
                            columnid = generateUUid()
                            example_data = (
                                json.dumps(field_data["example"])
                                if isinstance(field_data["example"], dict)
                                else field_data["example"]
                            )
                            table_schema_values = (
                                str(columnid),
                                field_data["name"],
                                field_data["type"],
                                example_data,
                                field_data["description"],
                                data_table_id,
                            )
                            try:
                                cur.execute(
                                   getTableSchema,
                                    (field_data["name"], data_table_id),
                                )
                                exfieldData = cur.fetchone()
                                if exfieldData is None:
                                    cur.execute(table_schema_sql, table_schema_values)
                                else:
                                    update_query =updateTableSchema
                                    update_values = (
                                        field_data["type"],
                                        example_data,
                                        field_data["description"],
                                        field_data["name"],
                                        data_table_id,
                                    )
                                    cur.execute(update_query, update_values)

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
            return {'statusCode':200}
    except Exception as e:
        return e
    

async def generate_json_data(data):
    schema_info = []

    for table in data:
        schema_name = table.get("schema")
        table_name = table["table_name"].replace(f"{schema_name}.", "")

        schema = next((s for s in schema_info if s["schema_name"] == schema_name), None)

        if not schema:
            schema = {"schema_name": schema_name, "tables": []}
            schema_info.append(schema)

        table_info = {
            "table_name": table_name,
            "description": "",
            "fields": [],
            "relationships": table.get("relationships", []),
        }
        example_data = table.get("example_data", {})

        for column in table["columns"]:
            field_info = {
                "name": column["name"],
                "type": column["type"],
                "description": column.get("description", "No description available."),
                "example": example_data.get(column["name"], "none"),
            }
            table_info["fields"].append(field_info)

        schema["tables"].append(table_info)

    with open("schema_info_mssql.json", "w") as file:
        json.dump(schema_info, file, indent=4)

    return schema_info
