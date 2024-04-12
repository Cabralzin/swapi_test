import os
import json
import psycopg2
from datetime import datetime

def get_column_type(value):
    if isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        return "FLOAT"
    elif isinstance(value, bool):
        return "BOOLEAN"
    elif isinstance(value, list):
        # If it's a list, we'll just store it as JSON for now
        return "JSON"
    elif isinstance(value, datetime):
        # If it's a timestamp, we'll store it as TIMESTAMP
        return "TIMESTAMP"
    else:
        return "TEXT"

def create_table(cursor, table_name, columns):
    # Generate SQL command to create table with appropriate column types
    columns_str = ", ".join([f"{column} {get_column_type(columns[column])}" for column in columns])
    create_table_query = f"CREATE TABLE IF NOT EXISTS trusted.{table_name} (id SERIAL PRIMARY KEY, {columns_str})"
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

    # Read JSON files and create tables
    folder_name = "swapi_data"

    for filename in os.listdir(folder_name):
        table_name = os.path.splitext(filename)[0]  # Use filename as table name

        print(table_name)

        with open(os.path.join(folder_name, filename), "r") as file:
            data = json.load(file)
            if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                keys = {key: type(value) for key, value in data[0].items()}
                print(keys)
                create_table(cursor, table_name, keys)

    # Commit changes and close connection
    connection.commit()
    connection.close()

if __name__ == "__main__":
    main()
