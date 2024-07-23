pgGetSchema="""SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog')"""
pgGetTable="""SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{schema_name}'"""
pgGetColumn="""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
                    """
pgGetRelationship="""
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
latestSampleData="""
                    SELECT *
                    FROM {schema_name}.{table_name}
                    ORDER BY id DESC
                    LIMIT 3
                    """
addDataTableQuery="""
            INSERT INTO data_table (id, table_name, schema_name, application_id, meta_data)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
            """
getDataTable="SELECT * FROM data_table WHERE table_name = %s and application_id=%s"
addTableSchemaQuery="""
                        INSERT INTO table_schema (id, field_name, type, sample_data,description, data_table_id)
                        VALUES (%s, %s, %s, %s,%s, %s)
                        """
getTableSchema= "SELECT * FROM table_schema WHERE field_name = %s AND data_table_id = %s"
updateTableSchema= """UPDATE table_schema
                                    SET type = %s, sample_data = %s, description = %s
                                    WHERE field_name = %s AND data_table_id = %s
                                    """
mysqlGetTable=  """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = :database
            """
mysqlGetColumns=  """
                    SELECT column_name, data_type, column_type 
                    FROM information_schema.columns 
                    WHERE table_schema = :database AND table_name = :table_name
                """
mysqlGetRelations="""
                    SELECT 
                        kcu.COLUMN_NAME AS column_name, 
                        kcu.REFERENCED_TABLE_NAME AS referenced_table, 
                        kcu.REFERENCED_COLUMN_NAME AS referenced_column
                    FROM 
                        information_schema.KEY_COLUMN_USAGE kcu
                    WHERE 
                        kcu.TABLE_SCHEMA = :database 
                        AND kcu.TABLE_NAME = :table_name
                        AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
                """