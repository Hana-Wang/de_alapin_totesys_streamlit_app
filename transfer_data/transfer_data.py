import pandas as pd
from pg8000.native import Connection
import os
from dotenv import load_dotenv
import pyarrow as pa
import pyarrow.parquet as pq

load_dotenv()
# Establish connection to PostgreSQL
# conn = Connection(user="your_user", password="your_password", database="your_database", host="your_host", port=your_port)

def get_connection():
    return Connection(
            user=os.getenv("POSTGRES_USERNAME"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=os.getenv("POSTGRES_DATABASE"),
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=int(os.getenv("POSTGRES_PORT")),
        )

def get_tables_in_database():
    # Connect to your PostgreSQL database
    conn = get_connection()

    # Query to get all table names across all schemas
    # query = """
    # SELECT table_schema, table_name
    # FROM information_schema.tables
    # WHERE table_type = 'BASE TABLE'
    #     AND table_schema NOT IN ('pg_catalog', 'information_schema');
    # """

    query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'project_team_7'
    """

    # Execute the query
    result = conn.run(query)
    
    # Close the connection
    conn.close()
    
    # Extract table names from the result
    tables = [row[0] for row in result]
    
    return tables

# Base directory for saving files
base_dir = 'db'

# Create directories for each format if they don't exist
formats_dirs = {
    'parquet': os.path.join(base_dir, 'parquet_files/tmp'),
    'csv': os.path.join(base_dir, 'csv_files/tmp'),
    'json': os.path.join(base_dir, 'json_files/tmp')
}


# Ensure that all format directories exist
for dir_path in formats_dirs.values():
    os.makedirs(dir_path, exist_ok=True)



tables = get_tables_in_database()
# print(tables)

conn = get_connection()
for table in tables:
    # Query to fetch the table data
    query = f'SELECT * FROM project_team_7.{table}'
    
    # Execute the query using connection.run and convert the result to a DataFrame
    result = conn.run(query)
    
    # Manually fetch the column names
    col_query = f"""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'project_team_7' AND table_name = '{table}'
    """
    col_result = conn.run(col_query)
    columns = [row[0] for row in col_result]
    
    # Convert the result to a pandas DataFrame
    df = pd.DataFrame(result, columns=columns)
    
    # Convert DataFrame to Arrow Table
    table_arrow = pa.Table.from_pandas(df)
    
   # Define the file paths for each format
    parquet_file = os.path.join(formats_dirs['parquet'], f'{table}.parquet')
    csv_file = os.path.join(formats_dirs['csv'], f'{table}.csv')
    json_file = os.path.join(formats_dirs['json'], f'{table}.json')


    # Write the table to a Parquet file
    pq.write_table(table_arrow, parquet_file)

    # Write the table to a CSV file
    df.to_csv(csv_file, index=False)
    
    # Write the table to an Excel file
    # df.to_excel(f'{table}.xlsx', index=False)

    # Write the table to a JSON file
    df.to_json(json_file, orient='records', lines=True)
 
# Close the connection
conn.close()