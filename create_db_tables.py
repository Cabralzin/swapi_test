import os
import json
import psycopg2
import pandas as pd
import numpy as np

def get_column_types(df):
    # Define mapping of pandas data types to SQL column types
    type_mapping = {
        np.dtype('int64'): "INTEGER",
        np.dtype('float64'): "NUMERIC",
        np.dtype('datetime64[ns]'): "TIMESTAMP",
        np.dtype('object'): "TEXT"
    }

    # Identify data types of columns in the DataFrame
    column_types = {}
    for column in df.columns:
        column_dtype = df[column].dtype
        if column_dtype in type_mapping:
            column_types[column] = type_mapping[column_dtype]
        else:
            # Check if the column contains dates in the format 'YYYY-MM-DD'
            if all(pd.to_datetime(df[column], errors='coerce').notnull()):
                column_types[column] = "DATE"
            else:
                # Default to TEXT for unsupported data types
                column_types[column] = "TEXT"
    return column_types

def create_table(cursor, table_name, df):
    # Generate SQL command to create table with dynamic column types
    column_types = get_column_types(df)
    columns_str = ", ".join([f"{column} {column_types[column]}" for column in df.columns])
    create_table_query = f"CREATE TABLE IF NOT EXISTS raw.{table_name} (id SERIAL PRIMARY KEY, {columns_str})"
    cursor.execute(create_table_query)

def main():
    # Connect to PostgreSQL database
    connection = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="D@nKlarH@ands1995",
        host="localhost"
    )
    cursor = connection.cursor()

    # Read JSON files, create tables, and insert data
    folder_name = "swapi_data"
    for filename in os.listdir(folder_name):
        table_name = os.path.splitext(filename)[0]  # Use filename as table name
        with open(os.path.join(folder_name, filename), "r") as file:
            data = json.load(file)
            if isinstance(data, dict) and "results" in data:
                results = data["results"]
                if results and isinstance(results, list):
                    # Create DataFrame from JSON data
                    df = pd.DataFrame(results)
                    # Create table with dynamic column types
                    create_table(cursor, table_name, df)

    # Commit changes and close connection
    connection.commit()
    connection.close()

if __name__ == "__main__":
    main()
