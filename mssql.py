from datetime import datetime
import pyodbc
from util import get_description


async def fetch_mssql_schema_data(conn_str):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    schema_query = """
    SELECT schema_name 
    FROM information_schema.schemata 
    WHERE schema_name NOT IN ('information_schema', 'sys', 'guest', 'dbo', 'INFORMATION_SCHEMA')
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

                latest_data_query = f"""
                SELECT TOP 3 *
                FROM {schema_name}.{table_name}
                ORDER BY id DESC
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

    return schema_info

conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=your_server_name;DATABASE=your_database_name;UID=your_username;PWD=your_password'
schema_data = fetch_mssql_schema_data(conn_str)
print(schema_data)
