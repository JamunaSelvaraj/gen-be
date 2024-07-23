from datetime import datetime
import pyodbc
from util import get_description,generate_json_data

async def fetch_mssql_schema_data(conn_str):
    try:

            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            print(cursor)
            # Fetch table names directly
            table_query = """
        SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_type = 'BASE TABLE'
            """

            cursor.execute(table_query)
            tables = cursor.fetchall()
            print(tables,'cfghj')
            table_info_list = []

            for table in tables:
                schema_name, table_name = table
                print(schema_name,table_name,'table')
            #     full_table_name = f"{schema_name}.{table_name}"
                table_info = {
                    "schema": schema_name,
                    "table_name": table_name,
                    "columns": [],
                    "relationships": [],
                }

            #     # Fetch columns for the current table
                column_query = f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
                """

                cursor.execute(column_query)
                columns = cursor.fetchall()

                for column in columns:
                    column_name, data_type = column
                    description = get_description(column_name, table_name)
                    table_info["columns"].append(
                        {
                            "name": column_name,
                            "type": data_type,
                            "description": description,
                        }
                    )

            #     # Fetch foreign key relationships for the current table
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

            #     # Fetch example data for the current table
                latest_data_query = f"""
                SELECT *
                FROM {schema_name}.{table_name}
                """
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
                                column_examples[column_name].append(value.isoformat())
                            else:
                                column_examples[column_name].append(str(value))
                        except IndexError:
                            column_examples[column_name].append(None)

                for column_name in column_examples:
                    column_examples[column_name] = ", ".join(
                        map(str, column_examples[column_name])
                    )

                table_info["example_data"] = column_examples

                table_info_list.append(table_info)
            

            jsonresult=await generate_json_data(table_info_list)
            return jsonresult
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
