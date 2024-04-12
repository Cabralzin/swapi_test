import os
import json
import psycopg2

def get_column_names(cursor, table_name):
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' AND column_name != 'id'")
    columns = [row[0] for row in cursor.fetchall()]
    return columns

def insert_data(cursor, table_name, data):
    # Get column names dynamically from the database
    columns = get_column_names(cursor, table_name)

    # Insert data into the table
    if columns:
        if isinstance(data, list):
            for item in data:
                values = [item.get(column, "") for column in columns]
                placeholders = ", ".join(["%s" for _ in columns])
                insert_query = f"INSERT INTO raw.{table_name} ({', '.join(columns)}) VALUES ({placeholders})"
                cursor.execute(insert_query, values)

def main():
    # Connect to PostgreSQL database
    connection = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="D@nKlarH@ands1995",
        host="localhost",
        options="-c search_path=raw"

        )
    cursor = connection.cursor()

    # Read JSON files and insert data into tables
    folder_name = "swapi_data"
    for filename in os.listdir(folder_name):
        table_name = os.path.splitext(filename)[0]  # Use filename as table name
        with open(os.path.join(folder_name, filename), "r") as file:
            data = json.load(file)
            insert_data(cursor, table_name, data)

    # Commit changes and close connection
    connection.commit()
    connection.close()

if __name__ == "__main__":
    main()
